-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "233192ef-9c27-4674-9997-18aa8394f9c5",
-- META       "default_lakehouse_name": "silver",
-- META       "default_lakehouse_workspace_id": "532e5dad-1105-4559-9a96-cb92da36c95f",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "233192ef-9c27-4674-9997-18aa8394f9c5"
-- META         },
-- META         {
-- META           "id": "4eeceddb-7785-4937-8ff7-3c8eb951a1c9"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

CREATE TABLE IF NOT EXISTS silver.sweden_population (
    City STRING NOT NULL,
    MenPopulation INT,
    WomenPopulation INT,
    TotalPopulation INT
)
USING DELTA

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************


INSERT OVERWRITE silver.sweden_population (
SELECT substring(city,6,len(city)) AS city
    , MenPopulation
    , WomenPopulation
    , MenPopulation + WomenPopulation AS TotalPopulation

    FROM bronze.sweden_population_stats_monthly
    PIVOT (
        SUM(monthlyPopulation) for gender in ('män' AS MenPopulation,'kvinnor' as WomenPopulation)
        )
)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
