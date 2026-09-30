# Kafka real-time ingestion test

This local flow replays `data/test_data.csv` as Kafka events and runs a
continuous Snowflake consumer. It assumes `RAW_TRANSACTIONS` has the
`EVENT_ID` and `INGESTION_SOURCE` columns from
`snowflake/02_add_ingestion_metadata.sql`.

Apply that SQL in Snowflake first, then build and start the consumer:

```powershell
docker compose build kafka-snowflake-consumer kafka-csv-producer
docker compose up -d kafka-snowflake-consumer
```

The standalone consumer does not run as an Airflow task, so it cannot read the
Snowflake connection saved only in the Airflow UI. Add these local-only entries
to `.env` using the corresponding values from the `snowflake_default` connection
(never commit `.env`):

```env
SNOWFLAKE_USER=your_snowflake_user
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_ROLE=your_role
```

`SNOWFLAKE_ACCOUNT` is already read from `.env`. If the account uses a custom
authenticator, set `SNOWFLAKE_AUTHENTICATOR` as well. Recreate the consumer after
changing these variables.

Replay the small test file once:

```powershell
docker compose --profile tools run --rm kafka-csv-producer
```

The producer publishes at a small interval to `fraud.transactions.v1`. The
consumer batches events, MERGEs by stable `EVENT_ID`, and commits Kafka offsets
only after Snowflake succeeds. Replaying the same file does not insert the same
Kafka event twice.

Verify the result in Snowflake:

```sql
SELECT INGESTION_SOURCE, COUNT(*) AS ROW_COUNT,
       COUNT_IF(INGESTION_TIMESTAMP IS NULL) AS NULL_TIMESTAMPS
FROM FRAUD_DETECTION.RAW.RAW_TRANSACTIONS
GROUP BY INGESTION_SOURCE
ORDER BY INGESTION_SOURCE;
```

To publish only a limited number of rows, configure `KAFKA_MAX_ROWS`; zero means
all rows. `KAFKA_CSV_PATH` defaults to `/opt/airflow/data/test_data.csv`.