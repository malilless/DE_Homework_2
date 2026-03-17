# DE_Homework_2
## Assignment: Hourly Support Call Enrichment Pipeline (Airflow/MySQL/JSON/DuckDB)
### Business reason
#### Support teams need near-real-time visibility into call quality and context.  
This pipeline enriches raw support call logs with telephony metadata and an LLM-style call summary, then loads a clean analytical table for reporting, QA, and monitoring.

### Preparing the data
#### Fistly, I generated the data to insert into SQL tables and the json-files using ChatGPT. Here's the link: https://chatgpt.com/share/69ae9b25-3d18-800a-9576-1f1f838ba9c6 

### Step 1: Detecting new calls
#### Identifying which calls should be processed. This part of the task checks the last processed timestamp and queries the MySQL calls table to find all records with a call_time greater than the watermark. The result is returned as a list.

### Step 2: Loading telephony details
#### In this part, we're creating a list of dictionaries called enriched_results and fill it with the data from json-files. For each call_id, the function looks for a corresponding file in the call_records/ directory. If a file is missing or does not contain required fields, it is skipped.

### Step 3: Creating the final dataframe and loading it into DuckDB
#### Firstly, we create a list of id's and transform them into a string, so SQL could work with them. Next move - taking all the necessary information from the SQL-tables, joining calls and employees on employee_id and merging the new table with json-data dataframe on call_id. Finally, the result is exported to DuckDB.

### Instruments used: 
#### 
- Python
- Pandas module
- MySQL
- Airflow
- DuckDB.

### Notes
#### I used my dad's help and a bit of Gemini to write some lines in my code. We did not cover a lot of things required for this task, therefore it was extremely hard not only to create a schema of the project, but to write it on Python (given that I've barely worked with airfow and duckdb modules). Hope that is okay.



