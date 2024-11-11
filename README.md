# Linkedin jobs Analysis for the Swedish job market 

## Overview

This project is an in-depth analysis designed to derive insights from LinkedIn job posts within Sweden. It utilizes a robust architecture integrating various technologies and services to capture, store, process, and analyze jobs posted on LinkedIn on an hourly basis. Additionally, it incorporates population data at the city level to provide a comprehensive view of the swedish job market dynamics.

## Architecture

The architecture consists of two data flows. The first flow involves LinkedIn job data, which is ingested on an hourly basis into the bronze lakehouse. The second flow involves data from the Swedish Central Statistics Office (SCB), which is ingested into the bronze lakehouse on a monthly basis.

Both datasets are curated and transformed, then loaded into new tables in the silver lakehouse. Here, we use Azure OpenAI to enrich the job data by extracting important features from the job descriptions.

Finally, the data is aggregated in the default semantic model of the SQL endpoint of the silver lakehouse, and a Power BI report is built on top of it.

An overall picture of the architecture looks as follows:

<img src="thumbnail.png">

## Fabric Workspace

The solution leverages a medallion architecture with two main lakehouses, `bronze` and `silver`, in the Fabric workspace. The workspace also consists of two main pipelines that orchestrate the data ingestion and transformations to and from the lakehouses:

1. `_jobs_data_hourly`:
   Fetches LinkedIn jobs data in Sweden on an hourly basis, with a sliding window of one month. This means that data older than one month is automatically deleted. It also fetches the company's follower count to show the popularity of the company posting the jobs. After that, it transforms and cleans the data, then upserts it to the silver lakehouse based on job IDs. It finishes by running the `AI_Inrich_Jobs_silver_Hourly` notebook, which feeds the job descriptions into Azure OpenAI to extract key information:

- **Tools**: A list of relevant tools mentioned in the job description.
- **Requirements**: A list of required skills, qualifications, or experience (excluding tools listed in Tools).
- **Offer**: A list of benefits provided by the employer.
- **WorkType**: This key can only be one of four values: Remote, Hybrid, In-Office, Null.

<img src="assets/_jobs_data_hourly.png">

2. `_population_data_monthly`:
   Fetches the population data using the Swedish Central Statistics Office SCB API using a copy activity that lands the data in the `bronze` lakehouse in a csv format, then transform and *overwrite* the data in the `silver` lakehouse, since it is monthly data and we are only intersted in current month population stats.

<img src="assets/_population_data_monthly_pipeline.png">

## Co-pilot


## Installation


## Usage

To use the PowerBI report:

## Authors

- [Mohammad Raja](https://www.linkedin.com/in/mohammad-raja-455a62229/) - Initial work and maintenance.
- [Anas Mofleh](https://www.linkedin.com/in/anas-mofleh/) - Initial work and maintenance.

---
