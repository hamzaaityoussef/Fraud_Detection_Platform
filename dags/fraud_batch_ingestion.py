"""
DAG: fraud_batch_ingestion (v2)
--------------------------------
Améliorations vs v1:
- Dynamic task mapping pour l'extraction CSV -> parquet (retry granulaire par chunk)
- PUT + COPY INTO groupés (1 seule commande Snowflake, parallélisme natif via PATTERN)
- dtype fixe pour éviter les incohérences de schéma entre chunks
- Retries différenciés: 0 pour erreurs déterministes, retry+backoff pour erreurs transitoires
- Audit universel: callback on_success/on_failure attaché à TOUTES les tâches
- Airflow Params pour rendre le DAG paramétrable depuis l'UI / API
- Assets (Airflow 3) / Datasets (Airflow 2.4+) en outlet pour chaîner les DAGs avals
- Injection directe des paramètres de contexte (ds, params) au lieu de **context
"""

from __future__ import annotations

import logging
import math
import os
import time
import uuid
from datetime import timedelta

import pandas as pd
import pendulum
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowException, AirflowFailException
from airflow.sdk import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Compat Airflow 3 (airflow.sdk.Asset) / Airflow 2.4+ (airflow.datasets.Dataset)
try:
    from airflow.sdk import Asset
except ImportError:  # Airflow < 3
    from airflow.datasets import Dataset as Asset

log = logging.getLogger("airflow.task")

# ─────────────────────────────  CONSTANTES  ──────────────────────────────

TMP_DIR = "/tmp"
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
DATABASE = "FRAUD_DETECTION"
RAW_SCHEMA = "RAW"
TABLE = "RAW_TRANSACTIONS"
STAGE = "TRANSACTIONS_STAGE"


RAW_TRANSACTIONS_ASSET = Asset(f"snowflake://TCB14021/{DATABASE}/{RAW_SCHEMA}/{TABLE}")

DEFAULT_ARGS = {
    "owner": "hamza",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    # Audit universel: chaque tâche du DAG loggue son statut, succès ou échec.
    "on_success_callback": None,  # défini plus bas (audit_callback), évite la
    "on_failure_callback": None,  # référence circulaire avant sa définition
}

# Colonnes dans l'ORDRE du CSV source (préservé car dict Python 3.7+ est ordonné)
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
RAW_COLUMNS = list(COLUMN_MAPPING.keys())

# dtype fixe -> même schéma parquet garanti sur tous les chunks (évite les
# incohérences int64/float64 silencieuses que pandas peut introduire quand
# l'inférence de type se fait chunk par chunk)
DTYPE_MAP = {
    "step": "int32",
    "type": "category",
    "amount": "float64",
    "nameOrig": "string",
    "oldbalanceOrg": "float64",
    "newbalanceOrig": "float64",
    "nameDest": "string",
    "oldbalanceDest": "float64",
    "newbalanceDest": "float64",
    "isFraud": "int8",
    "isFlaggedFraud": "int8",
}

# ─────────────────────────────  AUDIT CALLBACK  ──────────────────────────


def audit_callback(context: dict) -> None:
    """Callback générique attaché à TOUTES les tâches (succès et échec).

    Contrairement à la v1 où seule `load_csv_to_snowflake` écrivait dans
    PIPELINE_RUNS, cette fonction est branchée sur `on_success_callback` /
    `on_failure_callback` dans DEFAULT_ARGS -> chaque tâche du DAG produit
    une ligne d'audit, sans code répété dans chaque tâche.
    """
    ti = context["task_instance"]
    status = "failed" if context.get("exception") else "success"
    duration = None
    if ti.start_date and ti.end_date:
        duration = (ti.end_date - ti.start_date).total_seconds()

    rows_processed = None
    if status == "success":
        result = ti.xcom_pull(task_ids=ti.task_id)
        if isinstance(result, dict):
            rows_processed = result.get("rows_loaded") or result.get("loaded_rows")

    # L'audit logging ne doit JAMAIS masquer l'erreur originale de la tâche.
    try:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run(
            f"""
            INSERT INTO {DATABASE}.{RAW_SCHEMA}.PIPELINE_RUNS
            (RUN_ID, DAG_ID, RUN_DATE, STEP_NAME, STATUS, ROWS_PROCESSED, DURATION_SECONDS)
            VALUES (%(run_id)s, %(dag_id)s, %(run_date)s, %(step_name)s, %(status)s, %(rows)s, %(duration)s)
            """,
            parameters={
                "run_id": context["run_id"],
                "dag_id": context["dag"].dag_id,
                "run_date": context["ds"],
                "step_name": ti.task_id,
                "status": status,
                "rows": rows_processed,
                "duration": duration,
            },
        )
    except Exception as audit_exc:  # noqa: BLE001
        log.warning("Audit logging échoué pour %s: %s", ti.task_id, audit_exc)


DEFAULT_ARGS["on_success_callback"] = audit_callback
DEFAULT_ARGS["on_failure_callback"] = audit_callback

# ─────────────────────────────  PARAMS  ───────────────────────────────────

DAG_PARAMS = {
    "csv_path": Param(
        "/opt/airflow/data/PS_20174392719_1491204439457_log.csv",
        type="string",
        description="Chemin vers le fichier CSV source (PaySim)",
    ),
    "chunk_size": Param(
        200_000,
        type="integer",
        minimum=10_000,
        description="Nombre de lignes par chunk parquet",
    ),
    "max_rows": Param(
        None,
        type=["null", "integer"],
        description="Limite de lignes pour tests locaux (null = fichier entier)",
    ),
}


@dag(
    dag_id="fraud_batch_ingestion",
    default_args=DEFAULT_ARGS,
    description="Ingestion batch PaySim vers Snowflake RAW (dynamic mapping + COPY groupé)",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    params=DAG_PARAMS,
    tags=["fraud-detection", "ingestion", "batch", "snowflake"],
)
def fraud_batch_ingestion():

    # ── Setup idempotent ───────────────────────────────────────────────
    @task(retries=1)
    def create_objects_if_not_exists():
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

    # ── Validations pré-chargement (erreurs déterministes -> pas de retry) ─
    @task(retries=0)
    def check_file_exists(params: dict) -> dict:
        csv_path = params["csv_path"]
        if not os.path.exists(csv_path):
            raise AirflowFailException(f"Fichier non trouvé : {csv_path}")
        size_mb = os.path.getsize(csv_path) / (1024 * 1024)
        return {"file_size_mb": round(size_mb, 2), "csv_path": csv_path}

    @task(retries=0)
    def validate_csv_structure(params: dict) -> dict:
        df_sample = pd.read_csv(params["csv_path"], nrows=5_000, dtype=DTYPE_MAP)

        missing = [c for c in COLUMN_MAPPING if c not in df_sample.columns]
        if missing:
            raise AirflowFailException(f"Colonnes manquantes : {missing}")

        fraud_rate = df_sample["isFraud"].mean() * 100
        # PaySim: taux de fraude attendu très faible (~0.1-0.2%). Un écart
        # important signale un fichier corrompu / mal ordonné -> on bloque.
        if not (0 <= fraud_rate <= 5):
            raise AirflowFailException(
                f"Taux de fraude suspect sur l'échantillon : {fraud_rate:.4f}% "
                "(attendu entre 0 et 5%)"
            )
        return {"sample_rows": len(df_sample), "fraud_rate_pct": round(fraud_rate, 4)}



    @task(retries=0)
    def generate_batch_id() -> str:
        return str(uuid.uuid4())

    # ── Découpage en chunks (métadonnées seulement, pas de lecture lourde) ─
    @task(retries=0)
    def compute_chunks(params: dict) -> list[dict]:
        csv_path = params["csv_path"]
        chunk_size = params["chunk_size"]
        max_rows = params.get("max_rows")

        with open(csv_path) as f:
            total_lines = sum(1 for _ in f) - 1  # -1 pour le header

        if max_rows:
            total_lines = min(total_lines, max_rows)

        num_chunks = math.ceil(total_lines / chunk_size)
        log.info("Fichier: %s lignes -> %s chunks de %s lignes", total_lines, num_chunks, chunk_size)

        return [
            {
                "chunk_index": i,
                "skiprows": 1 + i * chunk_size,  # +1 pour sauter le header
                "nrows": min(chunk_size, total_lines - i * chunk_size),
            }
            for i in range(num_chunks)
        ]

    # ── Extraction CSV -> parquet, mappée dynamiquement (1 task par chunk) ─
    @task(retries=3, retry_delay=timedelta(minutes=2))
    def extract_chunk_to_parquet(chunk_meta: dict, params: dict, batch_id: str) -> str:
        csv_path = params["csv_path"]
        chunk = pd.read_csv(
            csv_path,
            skiprows=chunk_meta["skiprows"],
            nrows=chunk_meta["nrows"],
            names=RAW_COLUMNS,
            header=None,
            dtype=DTYPE_MAP,
        )
        chunk = chunk.rename(columns=COLUMN_MAPPING)
        chunk["SOURCE_FILE"] = os.path.basename(csv_path)
        chunk["BATCH_ID"] = batch_id

        tmp_path = f"{TMP_DIR}/{batch_id}_chunk_{chunk_meta['chunk_index']:04d}.parquet"
        chunk.to_parquet(tmp_path, index=False)
        log.info("Chunk %s écrit : %s (%s lignes)", chunk_meta["chunk_index"], tmp_path, len(chunk))
        return tmp_path

    # ── PUT + COPY INTO groupés : Snowflake parallélise nativement ────────
    @task(retries=3, retry_delay=timedelta(minutes=2), outlets=[RAW_TRANSACTIONS_ASSET])
    def load_parquet_to_snowflake(parquet_paths: list[str], batch_id: str) -> dict:
        start_time = time.time()
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        try:
            with hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    # 1 seul PUT avec wildcard -> tous les fichiers du batch
                    cursor.execute(
                        f"PUT file://{TMP_DIR}/{batch_id}_chunk_*.parquet "
                        f"@{DATABASE}.{RAW_SCHEMA}.{STAGE} "
                        "OVERWRITE = TRUE AUTO_COMPRESS = FALSE PARALLEL = 8"
                    )

                    # 1 seul COPY INTO avec PATTERN -> Snowflake charge tous
                    # les fichiers en parallèle en interne (pas besoin de
                    # boucler côté Airflow), puis purge le stage au passage.
                    cursor.execute(f"""
                        COPY INTO {DATABASE}.{RAW_SCHEMA}.{TABLE}
                        (
                            STEP, TYPE, AMOUNT, NAMEORIG, OLDBALANCEORG,
                            NEWBALANCEORIG, NAMEDEST, OLDBALANCEDEST,
                            NEWBALANCEDEST, ISFRAUD, ISFLAGGEDFRAUD,
                            INGESTION_TIMESTAMP, SOURCE_FILE, BATCH_ID
                        )
                        FROM (
                            SELECT
                                $1:STEP::INTEGER,
                                $1:TYPE::VARCHAR,
                                $1:AMOUNT::FLOAT,
                                $1:NAMEORIG::VARCHAR,
                                $1:OLDBALANCEORG::FLOAT,
                                $1:NEWBALANCEORIG::FLOAT,
                                $1:NAMEDEST::VARCHAR,
                                $1:OLDBALANCEDEST::FLOAT,
                                $1:NEWBALANCEDEST::FLOAT,
                                $1:ISFRAUD::INTEGER,
                                $1:ISFLAGGEDFRAUD::INTEGER,
                                CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                                $1:SOURCE_FILE::VARCHAR,
                                $1:BATCH_ID::VARCHAR
                            FROM @{DATABASE}.{RAW_SCHEMA}.{STAGE}
                        )
                        PATTERN = '.*{batch_id}_chunk_.*\\\\.parquet'
                        FILE_FORMAT = (TYPE = 'PARQUET')
                        PURGE = TRUE
                    """)

                    result = cursor.fetchall()
                    total_rows = sum(row[3] for row in result) if result else 0

        except Exception as exc:
            # PAS de AirflowFailException ici : c'est probablement transitoire
            # (réseau, throttling warehouse) -> on veut que le retry s'applique.
            raise AirflowException(f"Erreur pendant le chargement Snowflake : {exc}") from exc
        finally:
            for path in parquet_paths:
                if os.path.exists(path):
                    os.remove(path)

        duration = time.time() - start_time
        log.info("Batch %s chargé : %s lignes en %.1fs", batch_id, total_rows, duration)
        return {"batch_id": batch_id, "rows_loaded": total_rows, "status": "success"}

    # ── Vérification post-chargement ───────────────────────────────────
    @task(retries=1)
    def verify_load(load_result: dict) -> dict:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        batch_id = load_result["batch_id"]

        total_rows, fraud_count, null_steps, null_amounts = hook.get_first(
            f"""
            SELECT
                COUNT(*)                                        AS total_rows,
                SUM(ISFRAUD)                                     AS fraud_count,
                SUM(CASE WHEN STEP IS NULL THEN 1 ELSE 0 END)    AS null_steps,
                SUM(CASE WHEN AMOUNT IS NULL THEN 1 ELSE 0 END)  AS null_amounts
            FROM {DATABASE}.{RAW_SCHEMA}.{TABLE}
            WHERE BATCH_ID = %(batch_id)s
            """,
            parameters={"batch_id": batch_id},
        )

        if total_rows == 0:
            raise AirflowFailException("Aucune ligne chargée pour ce batch.")
        if null_steps:
            raise AirflowFailException(f"NULL détectés dans STEP : {null_steps}")
        if null_amounts:
            raise AirflowFailException(f"NULL détectés dans AMOUNT : {null_amounts}")

        return {
            "batch_id": batch_id,
            "loaded_rows": total_rows,
            "fraud_count": int(fraud_count) if fraud_count else 0,
            "null_checks": "passed",
        }

    # ─────────────────────────  Dépendances  ──────────────────────────────
    setup = create_objects_if_not_exists()
    file_check = check_file_exists()
    validation = validate_csv_structure()
    batch_id = generate_batch_id()
    chunks_meta = compute_chunks()

    parquet_files = extract_chunk_to_parquet.partial(batch_id=batch_id).expand(
        chunk_meta=chunks_meta
    )
    load_result = load_parquet_to_snowflake(parquet_files, batch_id)
    verification = verify_load(load_result)

    setup >> file_check >> validation >> batch_id >> chunks_meta >> parquet_files


fraud_batch_ingestion()