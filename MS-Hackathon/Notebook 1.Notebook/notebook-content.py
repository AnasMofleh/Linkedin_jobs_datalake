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
from pyspark.sql.functions import collect_set


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

df = spark.sql("SELECT * FROM silver.inriched_job")
df = (
    df.groupBy("JobId")
    .agg(
        collect_set("Tools").alias("Tools"),
        collect_set("Requirements").alias("Requirements"),
        collect_set("Offer").alias("Offer")
    )
)
linkedin_jobs_df = spark.table("silver.linkedin_jobs").select("JobId","JobTitle", "CompanyName","TimePosting")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.join(
    linkedin_jobs_df,
    "JobId", 
    "inner" 
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def gpt_chat(applicant_skills, job_tools, job_requirements, TimePosting):
    prompt = f"""
The applicant has the following skills: {applicant_skills}.
Based on the job requirements: {job_requirements} and the job tools: {job_tools},
please estimate the probability (as a percentage) that the applicant’s skills match the requirements for this job.
Consider that newer job postings {TimePosting} are more likely to require up-to-date skills, 
so increase the probability accordingly.
"""

    MESSAGES = [
        {
            "role": "system", 
            "content": "You are a helpful assistant that analyzes the applicant's skills and the requirements of a given job. Respond with a percentage representing the likelihood that the applicant's skills match the job requirements, considering the recency of the job posting."
        },
        {"role": "user", "content": prompt}
    ]

    try:
        completion = client.chat.completions.create(
            model=MODEL_NAME,
            messages=MESSAGES,
        )
        
        response_content = completion.choices[0].message['content']
        
        probability = float(response_content.strip('%')) / 100 if '%' in response_content else float(response_content) / 100
        
        # Maybe add manually to the probability variable for newer
        
        return probability

    except Exception as e:
        # Return 0 if there's an error
        print(f"Error: {e}")
        return 0


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# We can also add columns for work type and offer and ask the applicants about their preferences.

# CELL ********************

df = spark.sql("SELECT * FROM silver.inriched_job")

prompt = df.select("JobId", "Tools","Requirements").collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

applicatns_skills = ["SQL","Python","Master degree in computer sience"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from tqdm import tqdm
result = []

for idx, job in tqdm(enumerate(prompt)):
    retry_count = 0
    max_retries = 20

    while retry_count < max_retries:
        try:
            job_match = gpt_chat(applicatns_skills, job.Requirements,job.Tools,job.TimePosting)
            if job_match:
                row = {"JobTitle": job.JobTitle}
                row["CompanyName"] = job.CompanyName
                row["JobLink"] = f'https://se.linkedin.com/jobs/view/{job.JobId}'
                row["Probability"] = job_match
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
