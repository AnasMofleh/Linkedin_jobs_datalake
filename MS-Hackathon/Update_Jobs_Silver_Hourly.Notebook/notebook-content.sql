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
-- META       "default_lakehouse_workspace_id": "532e5dad-1105-4559-9a96-cb92da36c95f",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "4eeceddb-7785-4937-8ff7-3c8eb951a1c9"
-- META         },
-- META         {
-- META           "id": "233192ef-9c27-4674-9997-18aa8394f9c5"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

CREATE TABLE IF NOT EXISTS silver.linkedin_jobs (
    JobId BIGINT NOT NULL
    , JobTitle STRING
    , CompanyName STRING          
    , CompanyLinkedinUrl STRING            
    , City STRING      
    , TimePosting STRING             
    , NbrOfApplicants INT             
    , JobDescription STRING              
    , SeniorityLevel STRING              
    , EmploymentType STRING              
    , JobFunction STRING
    , Industries STRING
    , ExtractionDatetime TIMESTAMP            
)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

WITH linkedin_jobs_transformed_view AS (
    SELECT CAST(JobId AS BIGINT) AS JobId
    , JobTitle
    , NULLIF(CompanyName, 'N/A') AS CompanyName
    , NULLIF(CompanyLinkedinUrl, 'N/A') AS CompanyLinkedinUrl
    , SPLIT(Location, ',')[0] AS City
    , TimePosting
    , TRY_CAST(SPLIT(NbrOfApplicants, ' ')[0] AS INT) AS NbrOfApplicants
    , SeniorityLevel
    , EmploymentType
    , JobFunction
    , JobDescription
    , Industries
    , COALESCE(TRY_CAST(publishdatetime AS TIMESTAMP), ExtractionDatetime) AS ExtractionDatetime
    
    FROM bronze.linkedin_jobs
)

MERGE INTO silver.linkedin_jobs a USING linkedin_jobs_transformed_view
ON linkedin_jobs_transformed_view.JobId = a.JobId
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
