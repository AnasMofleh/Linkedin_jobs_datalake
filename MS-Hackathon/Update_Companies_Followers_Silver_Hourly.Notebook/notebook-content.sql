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

-- MARKDOWN ********************

-- ## Creating the Table for Follower Data
-- 
-- This code creates the `silver.company_followers` table if it does not already exist.

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

-- MARKDOWN ********************

-- ## Updating Follower Data in Silver Layer
-- 
-- This query checks if the follower data for each company URL from the bronze layer already exists in the silver layer. If a match is found (i.e., the company URL already exists in the silver layer), it updates the follower count with the new data. If no match is found, it inserts the new follower data as a new entry.

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC WITH updated_companies_followers AS (
-- MAGIC     SELECT distinct url
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
