from __future__ import annotations

import json
import logging
import os
import time
from importlib import import_module

from kafka import KafkaConsumer

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("fraud.kafka.consumer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "fraud.transactions.v1")
GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "fraud-snowflake-raw-v1")
FLUSH_SIZE = int(os.getenv("KAFKA_FLUSH_SIZE", "100"))
POLL_TIMEOUT_SECONDS = float(os.getenv("KAFKA_POLL_TIMEOUT_SECONDS", "1"))
DATABASE = "FRAUD_DETECTION"
SCHEMA = "RAW"
TABLE = "RAW_TRANSACTIONS"
INPUT_COLUMNS = (
    "EVENT_ID",
    "INGESTION_SOURCE",
    "STEP",
    "TYPE",
    "AMOUNT",
    "NAMEORIG",
    "OLDBALANCEORG",
    "NEWBALANCEORIG",
    "NAMEDEST",
    "OLDBALANCEDEST",
    "NEWBALANCEDEST",
    "ISFRAUD",
    "ISFLAGGEDFRAUD",
    "SOURCE_FILE",
    "BATCH_ID",
)


def merge_messages(cursor, messages: list[dict]) -> int:
    if not messages:
        return 0

    values_sql = ", ".join(["(" + ", ".join(["%s"] * len(INPUT_COLUMNS)) + ")"] * len(messages))
    params = []
    for event in messages:
        params.extend(
            (
                event["event_id"],
                event["ingestion_source"],
                event["step"],
                event["type"],
                event["amount"],
                event["nameOrig"],
                event["oldbalanceOrg"],
                event["newbalanceOrig"],
                event["nameDest"],
                event["oldbalanceDest"],
                event["newbalanceDest"],
                event["isFraud"],
                event["isFlaggedFraud"],
                event["source_file"],
                event["batch_id"],
            )
        )

    source_columns = ", ".join(
        f"COLUMN{i + 1} AS {column}" for i, column in enumerate(INPUT_COLUMNS)
    )
    target_columns = ", ".join(
        (
            "EVENT_ID", "INGESTION_SOURCE", "STEP", "TYPE", "AMOUNT", "NAMEORIG",
            "OLDBALANCEORG", "NEWBALANCEORIG", "NAMEDEST", "OLDBALANCEDEST",
            "NEWBALANCEDEST", "ISFRAUD", "ISFLAGGEDFRAUD", "INGESTION_TIMESTAMP",
            "SOURCE_FILE", "BATCH_ID",
        )
    )
    source_values = ", ".join(
        (
            "SRC.EVENT_ID", "SRC.INGESTION_SOURCE", "SRC.STEP", "SRC.TYPE", "SRC.AMOUNT",
            "SRC.NAMEORIG", "SRC.OLDBALANCEORG", "SRC.NEWBALANCEORIG", "SRC.NAMEDEST",
            "SRC.OLDBALANCEDEST", "SRC.NEWBALANCEDEST", "SRC.ISFRAUD",
            "SRC.ISFLAGGEDFRAUD",
            "CONVERT_TIMEZONE('Africa/Casablanca', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ",
            "SRC.SOURCE_FILE", "SRC.BATCH_ID",
        )
    )

    sql = f"""
        MERGE INTO {DATABASE}.{SCHEMA}.{TABLE} AS T
        USING (
            SELECT {source_columns}
            FROM VALUES {values_sql}
        ) AS SRC
        ON T.EVENT_ID = SRC.EVENT_ID
        WHEN NOT MATCHED THEN INSERT ({target_columns})
        VALUES ({source_values})
    """
    cursor.execute(sql, tuple(params))
    return len(messages)


def connect_snowflake():
    required = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_WAREHOUSE")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError(
            "Kafka consumer requires these environment variables: " + ", ".join(missing)
        )

    connection_options = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database": DATABASE,
        "schema": SCHEMA,
        "login_timeout": 30,
        "network_timeout": 60,
    }
    role = os.getenv("SNOWFLAKE_ROLE")
    if role:
        connection_options["role"] = role
    authenticator = os.getenv("SNOWFLAKE_AUTHENTICATOR")
    if authenticator:
        connection_options["authenticator"] = authenticator

    snowflake_connector = import_module("snowflake.connector")
    return snowflake_connector.connect(**connection_options)


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=FLUSH_SIZE,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    log.info("Consuming topic %s into %s.%s.%s", TOPIC, DATABASE, SCHEMA, TABLE)
    batch = []
    last_flush = time.monotonic()

    try:
        while True:
            polled = consumer.poll(timeout_ms=int(POLL_TIMEOUT_SECONDS * 1000), max_records=FLUSH_SIZE)
            for records in polled.values():
                batch.extend(record.value for record in records)

            if not batch or (
                len(batch) < FLUSH_SIZE and time.monotonic() - last_flush < 1.0
            ):
                continue

            connection = connect_snowflake()
            try:
                with connection.cursor() as cursor:
                    cursor.execute("BEGIN")
                    merge_messages(cursor, batch)
                    cursor.execute("COMMIT")
                consumer.commit()
                log.info("Merged and committed %s Kafka events", len(batch))
                batch.clear()
                last_flush = time.monotonic()
            except Exception:
                try:
                    connection.rollback()
                except Exception:
                    log.exception("Snowflake rollback failed")
                log.exception("Snowflake merge failed; Kafka offsets were not committed")
                raise
            finally:
                connection.close()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()