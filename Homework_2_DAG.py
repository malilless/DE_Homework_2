from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
import os
import json
import pandas as pd
import duckdb

DAG_FOLDER = os.path.dirname(__file__)
DATA_DIR = os.path.join(DAG_FOLDER, "call_records")
DUCKDB_PATH = os.path.join(DAG_FOLDER, "HW2_duckdb.db")

default_args = {
    "owner": "Me",
}
with DAG(
    dag_id="Homework2",
    start_date=datetime(2026, 3, 15),
    schedule="10 * * * *",
    catchup=False,
    default_args=default_args,
) as dag:
    
    @task
    def detect_new_calls():
        watermark = Variable.get(
        "last_call_time_watermark",
        default_var="2023-01-01 00:00:00"
    )
        mysql_hook = MySqlHook(mysql_conn_id='LOCAL_DB_HW2')
        
        query = f"SELECT call_id FROM calls WHERE call_time > '{watermark}' ORDER BY call_id"
        records = mysql_hook.get_records(sql=query)
        call_id_list = [row[0] for row in records]
        print(f"Pulled {len(call_id_list)} call ids from MySQL")
        return call_id_list

    @task
    def load_telephony_details(call_id_list: list[int]):
        if not call_id_list: return []
        enriched_results = []

        for call_id in call_id_list:
            json_path = f"{DATA_DIR}/{call_id}.json"
            if os.path.exists(json_path):
                with open(json_path, 'r') as f:
                    telephony_data = json.load(f)

                if 'duration_sec' not in telephony_data: continue
                if 'short_description' not in telephony_data: continue

                enriched_results.append({
                "call_id": call_id,
                "duration_sec": telephony_data.get('duration_sec'),
                "short_description": telephony_data.get('short_description')
            })
            else:
                continue

        return enriched_results

    @task
    def transform_and_load_into_duckdb(telephony_data: list):
        ids_list = [item['call_id'] for item in telephony_data] # [1, 5, 9]
        ids_as_strings = [str(id) for id in ids_list] # ["1", "5", "9"]
        target_ids = ", ".join(ids_as_strings) # "1, 5, 9"

        mysql_hook = MySqlHook(mysql_conn_id='LOCAL_DB_HW2')
        query = f"""
            SELECT 
                c.call_id, c.call_time, c.phone, c.direction, c.status, 
                e.full_name, e.team, e.hire_date 
            FROM 
                calls c
            JOIN employees e ON c.employee_id = e.employee_id
            WHERE c.call_id IN ({target_ids})
        """
        df_from_db = mysql_hook.get_pandas_df(sql=query)

        df_from_telephony = pd.DataFrame(telephony_data)
        final_df = df_from_db.merge(df_from_telephony, on='call_id', how='inner')

        conn = duckdb.connect(database=DUCKDB_PATH)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS support_call_enriched (
                call_id INTEGER PRIMARY KEY,
                call_time DATETIME,
                phone VARCHAR(20),
                direction VARCHAR(10),
                status VARCHAR(20),
                full_name VARCHAR(100),
                team VARCHAR(50),
                hire_date DATE,
                duration_sec INTEGER,
                short_description TEXT
            )
        """)
        conn.execute("INSERT OR IGNORE INTO support_call_enriched SELECT * FROM final_df")
        
        max_call_time = str(final_df['call_time'].max())
        Variable.set("last_call_time_watermark", max_call_time)
        
        conn.close()
        return

    call_ids = detect_new_calls()
    telephony_data = load_telephony_details(call_ids)
    transform_and_load_into_duckdb(telephony_data)