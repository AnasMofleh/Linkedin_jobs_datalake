# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "233192ef-9c27-4674-9997-18aa8394f9c5",
# META       "default_lakehouse_name": "silver",
# META       "default_lakehouse_workspace_id": "532e5dad-1105-4559-9a96-cb92da36c95f"
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS silver.inriched_job 
# MAGIC (
# MAGIC     JobId BIGINT NOT NULL
# MAGIC     , Tools STRING 
# MAGIC     , Requirements STRING 
# MAGIC     , Offer STRING 
# MAGIC     , WorkType STRING 
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pip install openai

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from openai import AzureOpenAI
import json
import time
import re

ENDPOINT = "https://mango-bush-0a9e12903.5.azurestaticapps.net/api/v1"
API_KEY = "0f07fa20-4b6b-4029-a168-60027185eb46"

API_VERSION = "2024-02-01"
MODEL_NAME = "gpt-4-turbo-2024-04-09"

client = AzureOpenAI(
    azure_endpoint=ENDPOINT,
    api_key=API_KEY,
    api_version=API_VERSION,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def gpt_chat(persona, prompt):
  MESSAGES = [
      {"role": "system", "content": persona},
      {"role": "user", "content":prompt}]

  completion = client.chat.completions.create(
      model=MODEL_NAME,
      messages=MESSAGES,
  )
  return completion.choices[0].message.content

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_exists = spark.catalog.tableExists("silver.inriched_job")

if table_exists: # first time the pipeline will run the silver.inriched_job would not exist
    df = spark.sql("""
        SELECT * FROM silver.linkedin_jobs
        WHERE JobId NOT IN (SELECT JobId FROM silver.inriched_job)
    """)
else:
    df = spark.sql("SELECT * FROM silver.linkedin_jobs")

prompt = df.select("JobId", "JobDescription").collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

persona = """
You are a helpful assistant that summarizes job descriptions. Your response should always be in JSON format with the following keys:
Tools: A list of relevant tools mentioned in the description.
Requirements: A list of required skills, qualifications, or experience (excluding tools listed in Tools).
Offer: A list of benefits provided by the employer.
WorkType: This key can only be one of these four values: (Remote, Hybrid, In-Office, Null). Specify based on the description. If insufficient information is provided, set this to Null.

The summary should be written from the applicant's perspective, with each list containing only key items as keywords.
Your response should only contain JSON output, with no extra formatting, code block wrappers, or explanations.
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from tqdm import tqdm
result = []

for idx, job in tqdm(enumerate(prompt)):
    row = {"JobId": job.JobId}
    retry_count = 0
    max_retries = 20

    while retry_count < max_retries:
        try:
            res_summerize = gpt_chat(persona, job.JobDescription)
            row.update(json.loads(res_summerize))
            print(f"Success {idx+1}/{len(prompt)}. Status code:200! Fetching job: {job.JobId} information.")
            break
        except Exception as e:
            error_message = str(e)
            if "429" in error_message:
                wait_time_match = re.search(r"retry after (\d+) seconds", error_message)
                wait_time = int(wait_time_match.group(1)) if wait_time_match else 10
                retry_count += 1
                print(f"Retry: {retry_count}/{max_retries}. Status code: 429. Retrying after {wait_time} seconds")
                time.sleep(wait_time)
            else:
                print(f"An error occurred: {e}")
                break
    
    result.append(row)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = {
    'JobId':4062536805,
    'Tools':['Python'],
    'Requirements': ['Bachelor’s degree in Computer Science, Information Technology'],
    'Offer': ['Remote work'],
    'WorkType':'Remote'
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

df = spark.createDataFrame(
    pd.DataFrame(result)
    .explode('Tools', ignore_index=True)
    .explode('Requirements', ignore_index=True)
    .explode('Offer', ignore_index=True)
).createOrReplaceTempView("temp")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC MERGE INTO silver.inriched_job a
# MAGIC USING temp b
# MAGIC ON a.JobId = b.JobId
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }