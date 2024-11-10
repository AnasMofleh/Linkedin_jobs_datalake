-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "4eeceddb-7785-4937-8ff7-3c8eb951a1c9",
-- META       "default_lakehouse_name": "bronze",
-- META       "default_lakehouse_workspace_id": "532e5dad-1105-4559-9a96-cb92da36c95f"
-- META     }
-- META   }
-- META }

-- CELL ********************

CREATE TABLE IF NOT EXISTS silver.company_followers (
    url STRING
    , Nbr_of_followers BIGINT NOT NULL
    , extraction_time TIMESTAMP            
)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC WITH updated_companies_followers AS (
-- MAGIC     SELECT url
-- MAGIC     , Nbr_of_followers
-- MAGIC     , extraction_time
-- MAGIC     
-- MAGIC     FROM bronze.company_followers
-- MAGIC )
-- MAGIC 
-- MAGIC MERGE INTO silver.company_followers a USING updated_companies_followers
-- MAGIC ON updated_companies_followers.url = a.url
-- MAGIC WHEN MATCHED THEN UPDATE SET *
-- MAGIC WHEN NOT MATCHED THEN INSERT *

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
