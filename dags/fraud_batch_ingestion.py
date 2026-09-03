"""
DAG: fraud_batch_ingestion 
- TaskFlow API (@task)
- Chunked parquet loading (memory-safe for 6M rows)
- Pre-load validation (file + columns + fraud rate)
- Post-load verification (row count + nulls + frauds)
- Audit logging: both success AND failure in PIPELINE_RUNS
"""

from __future__ import annotations

import os
import time
import uuid
from datetime import timedelta

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowFailException

CSV_PATH = "/opt/airflow/data/PS_20174392719_1491204439457_log.csv"
CHUNK_SIZE = 200_000
TMP_DIR = "/tmp"

SNOWFLAKE_CONN_ID = "snowflake_default"
DATABASE = "FRAUD_DETECTION"
RAW_SCHEMA = "RAW"
TABLE = "RAW_TRANSACTIONS"
STAGE = "TRANSACTIONS_STAGE"

DEFAULT_ARGS = {
    "owner": "hamza",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

COLUMN_MAPPING = {
    "step": "STEP",
    "type": "TYPE",
    "amount": "AMOUNT",
    "nameOrig": "NAME_ORIG",
    "oldbalanceOrg": "OLDBALANCE_ORG",
    "newbalanceOrig": "NEWBALANCE_ORIG",
    "nameDest": "NAME_DEST",
    "oldbalanceDest": "OLDBALANCE_DEST",
    "newbalanceDest": "NEWBALANCE_DEST",
    "isFraud": "IS_FRAUD",
    "isFlaggedFraud": "IS_FLAGGED_FRAUD",
}


@dag(
    dag_id="fraud_batch_ingestion",
    default_args=DEFAULT_ARGS,
    description="Ingestion batch PaySim vers Snowflake RAW (chunked parquet)",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    tags=["fraud-detection", "ingestion", "batch", "snowflake"],
)
def fraud_batch_ingestion():

    @task
    def create_objects_if_not_exists():
        """Idempotent: crée table, audit table, stage."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        hook.run(f"""
            CREATE TABLE IF NOT EXISTS {DATABASE}.{RAW_SCHEMA}.{TABLE} (
                STEP                INTEGER,
                TYPE                STRING,
                AMOUNT              FLOAT,
                NAME_ORIG           STRING,
                OLDBALANCE_ORG      FLOAT,
                NEWBALANCE_ORIG     FLOAT,
                NAME_DEST           STRING,
                OLDBALANCE_DEST     FLOAT,
                NEWBALANCE_DEST     FLOAT,
                IS_FRAUD            INTEGER,
                IS_FLAGGED_FRAUD    INTEGER,
                INGESTION_TIMESTAMP TIMESTAMP_NTZ,
                SOURCE_FILE         STRING,
                BATCH_ID            STRING
            )
        """)

        hook.run(f"""
            CREATE TABLE IF NOT EXISTS {DATABASE}.{RAW_SCHEMA}.PIPELINE_RUNS (
                RUN_ID           STRING,
                DAG_ID           STRING,
                RUN_DATE         DATE,
                STEP_NAME        STRING,
                STATUS           STRING,
                ROWS_PROCESSED   INTEGER,
                DURATION_SECONDS FLOAT,
                LOGGED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)

        hook.run(f"""
            CREATE STAGE IF NOT EXISTS {DATABASE}.{RAW_SCHEMA}.{STAGE}
            FILE_FORMAT = (TYPE = 'PARQUET')
        """)

    @task
    def check_file_exists():
        """Vérifie que le CSV est accessible dans le conteneur."""
        if not os.path.exists(CSV_PATH):
            raise AirflowFailException(f"Fichier non trouvé : {CSV_PATH}")
        size_mb = os.path.getsize(CSV_PATH) / (1024 * 1024)
        return {"file_size_mb": round(size_mb, 2), "csv_path": CSV_PATH}

    @task
    def validate_csv_structure():
        """Valide les colonnes et le taux de fraude sur un échantillon."""
        df_sample = pd.read_csv(CSV_PATH, nrows=5_000)

        missing = [c for c in COLUMN_MAPPING.keys() if c not in df_sample.columns]
        if missing:
            raise AirflowFailException(f"Colonnes manquantes : {missing}")

        fraud_rate = df_sample["isFraud"].mean() * 100
        return {"sample_rows": len(df_sample), "fraud_rate_pct": round(fraud_rate, 4)}

    @task
    def load_csv_to_snowflake(**context) -> dict:
        """Chunking parquet -> PUT -> COPY INTO avec lineage et audit complet."""
        batch_id = str(uuid.uuid4())
        run_date = context["ds"]
        start_time = time.time()

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        total_rows = 0
        status = "failed"

        try:
            for i, chunk in enumerate(pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE)):
                chunk = chunk.rename(columns=COLUMN_MAPPING)
                chunk["INGESTION_TIMESTAMP"] = pd.Timestamp.utcnow()
                chunk["SOURCE_FILE"] = os.path.basename(CSV_PATH)
                chunk["BATCH_ID"] = batch_id

                tmp_path = f"{TMP_DIR}/transactions_{batch_id}_{i}.parquet"
                chunk.to_parquet(tmp_path, index=False)

                cursor.execute(
                    f"PUT file://{tmp_path} @{DATABASE}.{RAW_SCHEMA}.{STAGE} OVERWRITE = TRUE"
                )
                cursor.execute(f"""
                    COPY INTO {DATABASE}.{RAW_SCHEMA}.{TABLE}
                    FROM @{DATABASE}.{RAW_SCHEMA}.{STAGE}/transactions_{batch_id}_{i}.parquet
                    FILE_FORMAT = (TYPE = 'PARQUET')
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                """)

                total_rows += len(chunk)
                os.remove(tmp_path)

            status = "success"

        except Exception as exc:
            raise AirflowFailException(f"Erreur pendant le chargement : {exc}")

        finally:
            duration = time.time() - start_time
            cursor.close()
            conn.close()

            # Log ALWAYS (success or failure)
            hook.run(
                f"""
                INSERT INTO {DATABASE}.{RAW_SCHEMA}.PIPELINE_RUNS
                (RUN_ID, DAG_ID, RUN_DATE, STEP_NAME, STATUS, ROWS_PROCESSED, DURATION_SECONDS)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                parameters=(batch_id, "fraud_batch_ingestion", run_date, "csv_load", status, total_rows, duration),
            )

        return {"batch_id": batch_id, "rows_loaded": total_rows, "status": status}

    @task
    def verify_load(load_result: dict):
        """Assertions post-chargement: lignes, nulls, fraudes."""
        if load_result.get("status") != "success":
            raise AirflowFailException("Le chargement a échoué — vérification annulée.")

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        batch_id = load_result["batch_id"]

        total_rows, fraud_count, null_steps, null_amounts = hook.get_first(f"""
            SELECT
                COUNT(*)                                    AS total_rows,
                SUM(IS_FRAUD)                               AS fraud_count,
                SUM(CASE WHEN STEP IS NULL THEN 1 ELSE 0 END)   AS null_steps,
                SUM(CASE WHEN AMOUNT IS NULL THEN 1 ELSE 0 END) AS null_amounts
            FROM {DATABASE}.{RAW_SCHEMA}.{TABLE}
            WHERE BATCH_ID = '{batch_id}'
        """)

        if total_rows == 0:
            raise AirflowFailException("Aucune ligne chargée pour ce batch.")
        if null_steps > 0:
            raise AirflowFailException(f"NULL détectés dans STEP : {null_steps}")
        if null_amounts > 0:
            raise AirflowFailException(f"NULL détectés dans AMOUNT : {null_amounts}")

        return {
            "batch_id": batch_id,
            "loaded_rows": total_rows,
            "fraud_count": int(fraud_count) if fraud_count else 0,
            "null_checks": "passed"
        }

    # ─── Dépendances ───
    setup = create_objects_if_not_exists()
    file_check = check_file_exists()
    validation = validate_csv_structure()
    load_result = load_csv_to_snowflake()
    verification = verify_load(load_result)

    setup >> file_check >> validation >> load_result >> verification


fraud_batch_ingestion()