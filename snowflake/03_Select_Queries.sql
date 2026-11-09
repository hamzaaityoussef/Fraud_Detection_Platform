select * from fraud_detection.raw.pipeline_runs;


select distinct batch_id   from fraud_detection.raw.raw_transactions  limit 100;
-- select distinct batch_id , *  from fraud_detection.raw.raw_transactions  ;



-- SELECT ingestion_timestamp, COUNT(*) AS cnt
-- FROM fraud_detection.raw.raw_transactions
-- GROUP BY ingestion_timestamp
-- ORDER BY cnt DESC
-- LIMIT 20;
-- SELECT CURRENT_ACCOUNT();

select count(*) from  fraud_detection.raw.raw_transactions   ;
select * from  fraud_detection.raw.raw_transactions  order by ingestion_timestamp desc  limit 100;




-- SELECT
--     BATCH_ID,
--     COUNT(*) AS rows_loaded,
--     MIN(INGESTION_TIMESTAMP) AS first_inserted,
--     MAX(INGESTION_TIMESTAMP) AS last_inserted
-- FROM FRAUD_DETECTION.RAW.RAW_TRANSACTIONS
-- WHERE INGESTION_TIMESTAMP >= '2026-09-04'
-- GROUP BY BATCH_ID
-- ORDER BY first_inserted;





-- truncate table raw_transactions;




DESC TABLE FRAUD_DETECTION.RAW.RAW_TRANSACTIONS;



SELECT INGESTION_SOURCE,
       COUNT(*) AS ROW_COUNT,
       COUNT(DISTINCT EVENT_ID) AS UNIQUE_EVENTS,
       COUNT_IF(INGESTION_TIMESTAMP IS NULL) AS NULL_TIMESTAMPS
FROM FRAUD_DETECTION.RAW.RAW_TRANSACTIONS
GROUP BY INGESTION_SOURCE
ORDER BY INGESTION_SOURCE;





SHOW COMPUTE POOLS;

SHOW SERVICES IN ACCOUNT;

SHOW SERVICES;

SELECT
    SERVICE_TYPE,
    SUM(CREDITS_USED)
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
WHERE USAGE_DATE >= DATEADD(day,-7,CURRENT_DATE())
GROUP BY 1;




SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
WHERE START_TIME >= DATEADD(day,-7,CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC;



SHOW STREAMLITS;


drop streamlit ST7B2A403868E377CA9DB965B34CE8D854C05DAA2B ;





select current_user() , current_account() , current_role();


select * 
from SNOWFLAKE.ACCOUNT_USAGE.QUERY_INSIGHTS
where query_id = '01c7e080-040c-5e46-0030-974b014d00aa' ;