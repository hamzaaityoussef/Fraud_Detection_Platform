-- 02_raw_tables.sql
USE DATABASE FRAUD_DETECTION;
USE SCHEMA RAW;
USE WAREHOUSE FRAUD_WH;

CREATE OR REPLACE TABLE RAW_TRANSACTIONS (
    step INTEGER,
    type VARCHAR(50),
    amount FLOAT,
    nameOrig VARCHAR(50),
    oldbalanceOrg FLOAT,
    newbalanceOrig FLOAT,
    nameDest VARCHAR(50),
    oldbalanceDest FLOAT,
    newbalanceDest FLOAT,
    isFraud INTEGER,
    isFlaggedFraud INTEGER,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file VARCHAR(500),
    batch_id VARCHAR(100)
);



ALTER TABLE FRAUD_DETECTION.RAW.RAW_TRANSACTIONS
    ADD COLUMN IF NOT EXISTS EVENT_ID VARCHAR(36);

ALTER TABLE FRAUD_DETECTION.RAW.RAW_TRANSACTIONS
    ADD COLUMN IF NOT EXISTS INGESTION_SOURCE VARCHAR(20);


    

-- Table for pipeline audit/monitoring
CREATE OR REPLACE TABLE PIPELINE_RUNS (
    run_id VARCHAR(100),
    dag_id VARCHAR(100),
    run_date TIMESTAMP_NTZ,
    step_name VARCHAR(100),
    status VARCHAR(50),
    rows_processed INTEGER,
    error_message VARCHAR(5000),
    duration_seconds FLOAT
);

desc schema raw;