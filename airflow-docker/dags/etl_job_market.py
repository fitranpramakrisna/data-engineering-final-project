import sys
import pandas as pd
import pytz
from dotenv import load_dotenv
import os
from datetime import datetime
sys.path.append("/opt/airflow")

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from resources.scripts.extract_kalibrr import extract_web_kalibrr
from resources.scripts.extract_dealls import extract_web_dealls
from resources.scripts.transform_dealls import tranform_web_dealls
from resources.scripts.transform_kalibrr import tranform_web_kalibrr
from resources.scripts.mapping_industry import industry_mapping 
from resources.scripts.mapping_job_title import job_title_mapping
 
from google.oauth2 import service_account

load_dotenv()


# to map and normalize the industry name and job title
def normalize_industry(industry):
    return industry_mapping.get(industry, industry)

def normalize_job_title(title):
    for key, values in job_title_mapping.items():
        if title in values:
            return key
    return title

@dag(
    dag_id="etl_job_market",
    start_date=datetime(2025, 2, 15, tzinfo=pytz.timezone("Asia/Jakarta")),
    schedule_interval="0 10 * * *",
    tags=["final_project_dibimbing"],
    default_args={"owner": "Fitran"}
)

def etl_job_market():
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")

    sources = {
        "kalibrr": (extract_web_kalibrr, tranform_web_kalibrr),
        "dealls": (extract_web_dealls, tranform_web_dealls)
    }

    extract_tasks = {}
    transform_tasks = {}
    
    for source_name, (extract_func, transform_func) in sources.items():
        @task(task_id=f"extract_{source_name}")
        def extract(extract_f=extract_func):
            return extract_f()

        @task(task_id=f"transform_{source_name}")
        def transform(data, transform_f=transform_func):
            return transform_f(data)

        extract_task = extract()
        transform_task = transform(extract_task)

        extract_tasks[source_name] = extract_task
        transform_tasks[source_name] = transform_task

        start_task >> extract_task >> transform_task

    # Task to do merge tables
    @task(task_id="merge_transformed_data")
    def merge_table_task(kalibrr_data, dealls_data):

        kalibrr_df = pd.read_json(kalibrr_data)
        dealls_df = pd.read_json(dealls_data)

        merged_df = pd.concat([kalibrr_df, dealls_df], ignore_index=True)
        merged_df = merged_df.drop_duplicates()
        merged_df['job_type'] = merged_df['job_type'].str.lower()
        merged_df['job_type'] = merged_df['job_type'].replace({
        'contract': 'contractual',
        'fulltime': 'full time'
        })
        merged_df['job_title'] = merged_df['job_title'].apply(normalize_job_title)
        merged_df['industry'] = merged_df['industry'].apply(normalize_industry)
        
        merged_df.to_csv('./resources/csv/job_market.csv', index=False)

        return "Data saved"
        

    merged_table_task = merge_table_task(transform_tasks["kalibrr"], transform_tasks["dealls"])
    
    @task(task_id="load_to_bigquery")
    def load_to_bigquery():
        
        # project_id = os.getenv("BIGQUERY_PROJECT_ID")
        # dataset_id = os.getenv("BIGQUERY_DATASET_ID")
        # table_id = os.getenv("BIGQUERY_TABLE_ID")
        
        credentials = service_account.Credentials.from_service_account_file('/opt/airflow/resources/config/gcp/service_account.json')
        df = pd.read_csv('./resources/csv/job_market.csv')
        df.to_gbq(destination_table='job_market.jobs_detail', project_id='final-project-data-engineer', credentials=credentials, if_exists='replace')
        
        
    load_to_bigquery_task = load_to_bigquery()
    
    transform_tasks["kalibrr"] >> merged_table_task
    transform_tasks["dealls"] >> merged_table_task
    merged_table_task >> load_to_bigquery_task >> end_task

etl_job_market()
