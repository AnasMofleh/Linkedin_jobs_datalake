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
# 2. **Data extraction** - Extracting the number of followers for.
# 3. **Data Transformation** - Structuring the data to capture hourly follower trends.
# 4. **Data Loading** - Saving or exporting the processed data for further analysis or reporting.


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

# CELL ********************

company_followers = []
max_retries = 5
retry = 0

for url in df:

    if url:
        retry = 0
        while retry < max_retries:
            retry += 1
            delay = 1

            response = requests.get(url, headers = headers)

            if response.status_code == 200:
                
                # Parse the HTML content
                list_soup = BeautifulSoup(response.text, 'html.parser')
                
                # Locate the followers count
                followers_element = list_soup.findAll('li', class_='mb-1')

                if followers_element:
                    posts = [e for e in followers_element]
                    
                    if posts:
                        for post in posts:
                            try:
                                Nbr_of_followers = int(''.join(re.findall(r'\b\d+\b', post.p.get_text(strip=True))))
                                break
                            except:
                                print(f"Could find followers count trying with another element ..")
                                        
                        company_followers.append({
                        "url" : url,
                        "Nbr_of_followers" : Nbr_of_followers,
                        "extraction_time" : dt.utcnow()
                        })
                        #print("success !")

                    else:
                        print(f"could not find followers count in the link{url}")
                                
                else:
                    try:
                        followers_element = list_soup.find('meta', {'name': 'description'})
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
