# DE_Homework_2
## Assignment: Hourly Support Call Enrichment Pipeline (Airflow/MySQL/JSON/DuckDB)
## Business reason
Support teams need near-real-time visibility into call quality and context.  
This pipeline enriches raw support call logs with telephony metadata and an LLM-style call summary, then loads a clean analytical table for reporting, QA, and monitoring.
## Preparing the data
Fistly, I generated the data to insert into SQL tables and the json-files using ChatGPT. Here's the link: https://chatgpt.com/share/69ae9b25-3d18-800a-9576-1f1f838ba9c6 
## Step 1: Detecting new calls
Identifying which calls should be processed. This part of the task checks the last processed timestamp and queries the MySQL calls table to find all records with a call_time greater than the watermark. The result is returned as a list.
## Step 2: Loading Telephony Details
In this part, we're creating a list called enriched_results and fill it with the data from json-files.
