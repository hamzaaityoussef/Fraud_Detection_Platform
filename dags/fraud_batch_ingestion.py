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
    "nameOrig": "NAMEORIG",
    "oldbalanceOrg": "OLDBALANCEORG",
    "newbalanceOrig": "NEWBALANCEORIG",
    "nameDest": "NAMEDEST",
    "oldbalanceDest": "OLDBALANCEDEST",
    "newbalanceDest": "NEWBALANCEDEST",
    "isFraud": "ISFRAUD",
    "isFlaggedFraud": "ISFLAGGEDFRAUD",
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
                NAMEORIG            STRING,
                OLDBALANCEORG       FLOAT,
                NEWBALANCEORIG      FLOAT,
                NAMEDEST            STRING,
                OLDBALANCEDEST      FLOAT,
                NEWBALANCEDEST      FLOAT,
                ISFRAUD             INTEGER,
                ISFLAGGEDFRAUD      INTEGER,
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
        """Chunking -> pandas -> write_pandas (INSERT batch optimisé, pas de PUT/S3)."""
        from snowflake.connector.pandas_tools import write_pandas
        
        batch_id = str(uuid.uuid4())
        run_date = context["ds"]
        start_time = time.time()

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        
        total_rows = 0
        status = "failed"

        try:
            for i, chunk in enumerate(pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE)):
                chunk = chunk.rename(columns=COLUMN_MAPPING)
                chunk["INGESTION_TIMESTAMP"] = pd.Timestamp.utcnow().tz_localize(None)
                chunk["SOURCE_FILE"] = os.path.basename(CSV_PATH)
                chunk["BATCH_ID"] = batch_id

                # write_pandas : crée table temp + COPY INTO interne, pas de PUT vers S3
                success, num_chunks, num_rows, output = write_pandas(
                    conn=conn,
                    df=chunk,
                    table_name=TABLE,
                    database=DATABASE,
                    schema=RAW_SCHEMA,
                    quote_identifiers=False,
                    auto_create_table=False,   # table existe déjà
                    overwrite=False,           # append
                    use_logical_type=True,
                )

                if not success:
                    raise AirflowFailException(
                        f"Echec du chargement du chunk {i} dans {DATABASE}.{RAW_SCHEMA}.{TABLE}: {output}"
                    )
                
                total_rows += num_rows
                print(f"[chunk {i}] {num_rows} lignes insérées (total: {total_rows})")

            status = "success"

        except Exception as exc:
            raise AirflowFailException(f"Erreur pendant le chargement : {exc}")

        finally:
            duration = time.time() - start_time
            conn.close()

            # Log audit (success ou failure)
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
                SUM(ISFRAUD)                                 AS fraud_count,
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