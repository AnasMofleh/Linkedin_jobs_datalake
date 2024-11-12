# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4eeceddb-7785-4937-8ff7-3c8eb951a1c9",
# META       "default_lakehouse_name": "bronze",
# META       "default_lakehouse_workspace_id": "532e5dad-1105-4559-9a96-cb92da36c95f",
# META       "known_lakehouses": [
# META         {
# META           "id": "4eeceddb-7785-4937-8ff7-3c8eb951a1c9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from bs4 import BeautifulSoup
from datetime import datetime
import requests
import time
import tqdm
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Extracting Job IDs

The first step in this process is to extract the job IDs from the LinkedIn job listings using the API. 

### API Details
The API used for this step is:
- `https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/`
  
The request is paginated, meaning we need to loop through the pages to get all the job IDs. Each page shows up to 10 jobs, and we increment the `start` parameter to move through pages. The `start` parameter is adjusted to fetch the next set of jobs, e.g., `start=10` for the second page, `start=20` for the third page, and so on.

### Error Handling
- **429 Status Code**: This indicates that we have hit a rate limit, so we increase the retry delay before making further requests.
- **Empty Page**: If a page returns no job listings (i.e., no `<li>` elements), the process stops.

### Retry Logic
To handle failures, we set a maximum retry limit (`max_retries`), and each retry will incrementally increase the delay.

### Output
The result of this process is a list of unique job IDs (`job_ids`), which are collected from each page.


# CELL ********************

time_windwo_in_seconds = 3600 # helps in getting jobs published one hour ago
retry = 0
max_retries = 10 # Maximum retries to handle failed requests
page_numb = 1 # Start from page 1
job_ids = [] # List to store job IDs

# Loop for extracting job IDs
while retry < max_retries:
    retry += 1
    delay = 0.3 # Initial delay between requests

    # Construct the URL for extracting job postings (pagination with 'start' parameter)
    list_url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?location=Sweden&start={page_numb}&f_TPR=r{time_windwo_in_seconds}"
    response = requests.get(list_url)
    
    # Check for a successful response and if there are no jobs on the page
    if response.status_code == 200 and response.text.count('<li>') == 0:
        # If no jobs are found, exit the loop (end of the job listings)
        #print("nothing to show, job is finished !") 
        break

    # If response is successful, parse the job listings page
    elif response.status_code == 200:
        #print(f"Page number {page_numb} returned {response.status_code} and Number of jobs on page: {response.text.count('<li>')}")
        
        # Use BeautifulSoup to parse the HTML and find job listings
        jobs_page = BeautifulSoup(response.text, 'html.parser').find_all("li", recursive=False)
            
        # Loop through each job entry and extract the job ID
        for job in jobs_page:
            base_card_div = job.find("div", {"class": "base-card"})
            if base_card_div is None: 
                base_card_div = job.find("a", {"class": "base-card"})
            
            if base_card_div is not None:
                # Extract the job ID from the data-entity-urn attribute
                job_id = base_card_div.get("data-entity-urn").split(':')[-1]
                job_ids.append(job_id)
                
            #else: 
            #    print("Could not extract job ID with a or div")
            #    print(job)
        
        # Move to the next page (pagination logic)
        page_numb += 10
        retry = 0

    # Handle rate-limiting (HTTP 429)
    elif response.status_code == 429:
        delay += 2 * retry   # Increase delay for each retry attempt
        #print(f"Retry: {retry}/{max_retries}. Page nr{page_numb} returned {response.status_code}. Retrying after {delay} seconds...")
        
    # If any other error occurs, break out of the loop
    else:
        #print(f"Error: {response.status_code} at page number {page_numb}")
        break


    # Sleep for the specified delay before making the next request
    time.sleep(delay)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Fetching Job Details

Once the job IDs are collected, the next step is to retrieve the detailed job postings for each job ID.

### API Details
The API used for fetching job details is:
- `https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}`

For each job ID collected in the previous step, a request is made to retrieve the job's information.

### Data Extraction
The data extracted for each job posting includes:
- **Job Title**
- **Company Name** and **Company LinkedIn URL** (Used in another Notebook that collect information about the company)
- **Location**
- **Time of Posting**
- **Number of Applicants**
- **Job Description**
- **Job Criteria** (e.g., seniority, job function, employment type, industries)

### Error Handling
If any of the fields are not found, the script will assign a `None` value to that field. The script also includes retry logic for failed requests with exponential backoff to handle errors like rate-limiting (HTTP 429).

### Output
The result is a list of dictionaries (`job_posts`), where each dictionary contains the details for a single job posting.

# CELL ********************

job_ids = list(set(job_ids)) # Remove duplicates
job_posts = [] # List to store job posts details

# Loop to fetch job details for each job ID
for idx, job_id in enumerate(job_ids):
    job_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
    
    for retry in range(max_retries):
        delay = 0.3 # Initial delay between requests
        
        # Send request to get detailed job posting information
        job_response = requests.get(job_url)
        if job_response.status_code == 200:
            print(f"Success {idx + 1}/{len(job_ids)}. Status code:{job_response.status_code}! Fetching job: {job_id} information. ")

            job_post = {} # Dictionary to store job details

            # Parse the job posting page using BeautifulSoup
            job_soup = BeautifulSoup(job_response.text, 'html.parser')
            job_post["JobId"] = job_id
            
            # The following is extracting different variables from the the job post
            try:
                job_post["JobTitle"] = job_soup.find("h2", {"class": "top-card-layout__title"}).text.strip()
            except:
                job_post["JobTitle"] = None
            
            try:
                company_tag = job_soup.find("a", {"class": "topcard__org-name-link"})
                job_post["CompanyName"] = company_tag.text.strip() if company_tag else None
                job_post["CompanyLinkedinUrl"] = company_tag['href'] if company_tag else None
            except:
                job_post["CompanyName"] = None
                job_post["CompanyLinkedinUrl"] = None
            
            try:
                location = job_soup.find("span", {"class": "topcard__flavor topcard__flavor--bullet"})
                job_post["Location"] = location.text.strip() if location else None
            except:
                job_post["Location"] = None
            
            try:
                job_post["TimePosting"] = job_soup.find("span", {"class": "posted-time-ago__text"}).text.strip()
            except:
                job_post["TimePosting"] = None
            
            try:
                num_applicants = job_soup.find("span", {"class": "num-applicants__caption"})
                job_post["NbrOfApplicants"] = num_applicants.text.strip() if num_applicants else None
            except:
                job_post["NbrOfApplicants"] = None
            
            try:
                html_job_soup = job_soup.find("div", {"class": "show-more-less-html__markup"})
                job_post["JobDescription"] = html_job_soup.get_text(separator=' ', strip=True) if html_job_soup else None
            except:
                job_post["JobDescription"] = None
            
            try:
                more_jobs_details = job_soup.find("ul", {"class": "description__job-criteria-list"}).find_all("li", recursive=False)
                job_post["SeniorityLevel"] = more_jobs_details[0].find("span", {"class": "description__job-criteria-text"}).text.strip()
            except:
                job_post["SeniorityLevel"] = None

            try:
                job_post["EmploymentType"] = more_jobs_details[1].find("span", {"class": "description__job-criteria-text"}).text.strip()
            except:
                job_post["EmploymentType"] = None

            try:
                job_post["JobFunction"] = more_jobs_details[2].find("span", {"class": "description__job-criteria-text"}).text.strip()

            except:
                job_post["JobFunction"] = None

            try:
                job_post["Industries"] = more_jobs_details[3].find("span", {"class": "description__job-criteria-text"}).text.strip()
            
            except:
                job_post["Industries"] = None
            
            # Add extraction timestamp
            job_post["ExtractionDatetime"] = datetime.utcnow()

            # Append the extracted job data to the list of job posts
            job_posts.append(job_post)
            break # Successfully fetched job details, break from the retry loop

        else:
            delay += 2 * retry
            #print(f"Retry: {retry}/{max_retries}. Status code: {job_response.status_code}. Retrying after {delay} seconds...")
    
        time.sleep(delay)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = StructType([
    StructField("JobId", StringType(), True),
    StructField("JobTitle", StringType(), True),
    StructField("CompanyName", StringType(), True),
    StructField("CompanyLinkedinUrl", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("TimePosting", StringType(), True),
    StructField("NbrOfApplicants", StringType(), True),
    StructField("JobDescription", StringType(), True),
    StructField("SeniorityLevel", StringType(), True),
    StructField("EmploymentType", StringType(), True),
    StructField("JobFunction", StringType(), True),
    StructField("Industries", StringType(), True),
    StructField("ExtractionDatetime", TimestampType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.createDataFrame(job_posts, schema=schema)
df = df.withColumn("ExtractionDatetime", col("ExtractionDatetime").cast(StringType()))
df.write.format("delta").mode("append").option("mergeSchema", "true").save("Files/linkedin_jobs") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS bronze.linkedin_jobs
# MAGIC USING DELTA
# MAGIC LOCATION 'Files/linkedin_jobs'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM bronze.linkedin_jobs
# MAGIC WHERE DATEDIFF(day, ExtractionDatetime, CURRENT_TIMESTAMP) > 30;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
