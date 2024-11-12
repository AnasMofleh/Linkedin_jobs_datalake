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
# META       "default_lakehouse_workspace_id": "532e5dad-1105-4559-9a96-cb92da36c95f"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Overview
# 
# This notebook is used to extract the number of followers for a company based on data in the `bronze.linkedin_jobs` database. 
# 
# The process involves:
# 
# 1. **Getting the companys URLs** - Accessing `bronze.linkedin_jobs` to fetch company url.
# 2. **Data extraction** - Extracting the number of followers for each company.
# 4. **Data Loading** - Saving the data in `bronze.company_followers` for further analysis and reporting.


# CELL ********************

from bs4 import BeautifulSoup
from datetime import datetime as dt
import requests
import re
import json
import time

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC create table if not exists bronze.company_followers (
# MAGIC     Nbr_of_followers Bigint,
# MAGIC     extraction_time Timestamp,
# MAGIC     url String Not Null
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 

# MARKDOWN ********************

# ## Restricting to Only New Data
# 
# We retrieve distinct `CompanyLinkedinUrl` values from the `bronze.linkedin_jobs` table where the `ExtractionDatetime` aligns with the current hour. This ensures we're only processing fresh data.
# 


# CELL ********************

df = spark.sql("""
    SELECT DISTINCT CompanyLinkedinUrl as url
    FROM bronze.linkedin_jobs 
    where date_format(current_timestamp(), 'yyyy-MM-dd HH') = date_format(ExtractionDatetime, 'yyyy-MM-dd HH') 
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = [x[0] for x in df.collect()]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Follower Count Extraction Methods
# 
# This code attempts to retrieve the follower count for each company using two methods:
# 
# 1. **Primary Method (Company Posts Section)**: 
#    - This method first searches for the follower count in the company’s posts section on LinkedIn, where follower information is usually more consistently structured. By parsing elements with specific tags and class names, this method captures the majority of follower counts.
#    
# 2. **Fallback Method (Profile Top Section)**: 
#    - If the company does not have any posts, it falls back to extracting the follower count from the top section of the company’s LinkedIn profile.
#    - This method is less reliable for certain pages, as LinkedIn profiles vary in structure. However, it helps minimize the number of companies that are missed by the first method.
#    
# By using these two methods, we increase the chances of retrieving follower counts while accounting for LinkedIn page variations.


# CELL ********************

company_followers = [] # Initialize an empty list to store follower data for each company URL
max_retries = 5 # Define the maximum number of retry attempts in case of request failures
retry = 0

# Loop through each URL in the DataFrame
for url in df:
    
    # Ensure the URL is not None before processing
    if url:
        retry = 0

        # Attempt to retrieve data, retrying up to the max_retries limit
        while retry < max_retries:
            retry += 1
            delay = 1

            response = requests.get(url, headers = headers)

            if response.status_code == 200:
                
                # Parse the HTML content
                list_soup = BeautifulSoup(response.text, 'html.parser')
                
                # Locate the followers count in the compnay posts section
                followers_element = list_soup.findAll('li', class_='mb-1')

                if followers_element:

                    # Collect each post made by the company on their linkedin profile
                    posts = [e for e in followers_element]
                    
                    # Attempt to extract follower count from parsed elements
                    if posts:
                        for post in posts:
                            try:
                                # Extract numbers from the text using regex and convert to integer
                                Nbr_of_followers = int(''.join(re.findall(r'\b\d+\b', post.p.get_text(strip=True))))
                                break
                            except:
                                print(f"Could find followers count trying with another element ..")
                                        
                        # Extraction successed and now the follower data will be appended to the list
                        company_followers.append({
                        "url" : url,
                        "Nbr_of_followers" : Nbr_of_followers,
                        "extraction_time" : dt.utcnow()
                        })
                        #print("success !")

                    else:
                        print(f"could not find followers count in the link{url}")

                # If the company does not have any posts on their LinkedIn profile, 
                # the follower count will be extracted from the company's profile top section.      
                else:
                    try:
                        # Locate the follower count in the meta description if no posts are available
                        followers_element = list_soup.find('meta', {'name': 'description'})

                        # Extract follower count using regex and convert to integer
                        Nbr_of_followers = re.findall(r'\| (.*?) f', followers_element.get("content"))
                        Nbr_of_followers = int(''.join(re.findall(r'\d+', Nbr_of_followers[0])))
                        company_followers.append({
                        "url" : url,
                        "Nbr_of_followers" : Nbr_of_followers,
                        "extraction_time" : dt.utcnow()
                        })
                        #print('success!')
                    except:
                        print(f"Follower count for {url}, could not be extracted")                    
                
                break

            else:
                delay += 2 * retry 
                #print(f"Retry: {retry}/{max_retries}. Returned {response.status_code}. Retrying after {delay} seconds...")
                
            time.sleep(delay)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC create or replace temp view new_batch as select * from bronze.company_followers limit 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    df = spark.createDataFrame(company_followers).createOrReplaceTempView('new_batch')
except:
    print('no data to add') 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC merge into bronze.company_followers a
# MAGIC using new_batch b
# MAGIC on a.url = b.url
# MAGIC when matched then update set * 
# MAGIC when not matched then insert * ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
