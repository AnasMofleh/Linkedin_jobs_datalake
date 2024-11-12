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
    SELECT distinct CAST(JobId AS BIGINT) AS JobId
    , ROW_NUMBER() OVER (PARTITION BY JobId ORDER BY ExtractionDatetime) rn
    , JobTitle
    , NULLIF(CompanyName, 'N/A') AS CompanyName
    , NULLIF(CompanyLinkedinUrl, 'N/A') AS CompanyLinkedinUrl
    , SPLIT(Location, ',')[0] AS City -- This is used to only get the city part of the location description
    , TRY_CAST(SPLIT(NbrOfApplicants, ' ')[0] AS INT) AS NbrOfApplicants
    , SeniorityLevel
    , EmploymentType
    , JobFunction
    , JobDescription
    , Industries
    , COALESCE(TRY_CAST(publishdatetime AS TIMESTAMP), ExtractionDatetime) AS ExtractionDatetime
    
    FROM bronze.linkedin_jobs
),

final as (
    SELECT JobId
        , JobTitle
        , CompanyName
        , CompanyLinkedinUrl
        , City
        , NbrOfApplicants
        , SeniorityLevel
        , EmploymentType
        , JobFunction
        , JobDescription
        , Industries
        , ExtractionDatetime
    from linkedin_jobs_transformed_view
    where rn = 1
)

MERGE INTO silver.linkedin_jobs a USING final
ON final.JobId = a.JobId
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

DELETE FROM silver.linkedin_jobs
WHERE DATEDIFF(day, ExtractionDatetime, CURRENT_TIMESTAMP) > 30;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
