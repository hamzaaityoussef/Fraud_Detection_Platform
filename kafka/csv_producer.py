from __future__ import annotations

import csv
import json
import logging
import os
import time
import uuid
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("fraud.kafka.producer")

CSV_PATH = os.getenv("KAFKA_CSV_PATH", "/opt/airflow/data/test_data.csv")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "fraud.transactions.v1")
MAX_ROWS = int(os.getenv("KAFKA_MAX_ROWS", "0")) or None
DELAY_SECONDS = float(os.getenv("KAFKA_DELAY_SECONDS", "0.05"))
FIELDS = (
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
)


def transaction_payload(row: dict[str, str], source_key: str, row_number: int, batch_id: str) -> dict:
    return {
        "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"kafka:{source_key}:{row_number}")),
        "ingestion_source": "kafka",
        "source_file": Path(CSV_PATH).name,
        "batch_id": batch_id,
        "step": int(row["step"]),
        "type": row["type"],
        "amount": float(row["amount"]),
        "nameOrig": row["nameOrig"],
        "oldbalanceOrg": float(row["oldbalanceOrg"]),
        "newbalanceOrig": float(row["newbalanceOrig"]),
        "nameDest": row["nameDest"],
        "oldbalanceDest": float(row["oldbalanceDest"]),
        "newbalanceDest": float(row["newbalanceDest"]),
        "isFraud": int(row["isFraud"]),
        "isFlaggedFraud": int(row["isFlaggedFraud"]),
    }


def main() -> None:
    if not os.path.isfile(CSV_PATH):
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
    )
    batch_id = str(uuid.uuid4())
    source_key = str(Path(CSV_PATH).resolve())
    sent = 0

    try:
        with open(CSV_PATH, newline="", encoding="utf-8-sig") as csv_file:
            reader = csv.DictReader(csv_file)
            missing = set(FIELDS).difference(reader.fieldnames or ())
            if missing:
                raise ValueError(f"CSV missing expected columns: {sorted(missing)}")

            for row_number, row in enumerate(reader):
                if MAX_ROWS is not None and sent >= MAX_ROWS:
                    break
                payload = transaction_payload(row, source_key, row_number, batch_id)
                producer.send(TOPIC, key=payload["event_id"].encode("ascii"), value=payload)
                sent += 1
                if DELAY_SECONDS > 0:
                    time.sleep(DELAY_SECONDS)

        producer.flush(timeout=120)
        log.info("Published %s events to topic %s (batch_id=%s)", sent, TOPIC, batch_id)
    except KafkaError:
        log.exception("Kafka publish failed after sending %s events", sent)
        raise
    finally:
        producer.close(timeout=30)


if __name__ == "__main__":
    main()