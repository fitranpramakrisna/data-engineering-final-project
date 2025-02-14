import sys
import pandas as pd
sys.path.append("/opt/airflow")

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from resources.scripts.extract_kalibrr import extract_web_kalibrr
from resources.scripts.extract_dealls import extract_web_dealls
from resources.scripts.transform_dealls import tranform_web_dealls
from resources.scripts.transform_kalibrr import tranform_web_kalibrr
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from google.oauth2 import service_account



@dag(
    dag_id="etl_job_market",
    schedule_interval=None,
    tags=["final_project_dibimbing"],
    default_args={"owner": "Fitran"}
)
def etl_job_market():
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")

    sources = {
        "kalibrr": (extract_web_kalibrr, tranform_web_kalibrr),
        "dealls": (extract_web_dealls, tranform_web_dealls),
    }

    extract_tasks = {}
    transform_tasks = {}

    for source_name, (extract_func, transform_func) in sources.items():
        @task(task_id=f"extract_{source_name}")
        def extract():
            return extract_func()

        @task(task_id=f"transform_{source_name}")
        def transform(data):
            return transform_func(data)

        extract_task = extract()
        transform_task = transform(extract_task)

        extract_tasks[source_name] = extract_task
        transform_tasks[source_name] = transform_task

        start_task >> extract_task >> transform_task

    # Task to do merge tables
    @task(task_id="merge_transformed_data")
    def merge_table_task(kalibrr_data, dealls_data):
        import uuid
        
        kalibrr_df = pd.read_json(kalibrr_data)
        dealls_df = pd.read_json(dealls_data)
        print(f"total baris kalibr : {kalibrr_df.shape}")
        print(f"total baris dealls : {dealls_df.shape}")
        merged_df = pd.concat([kalibrr_df, dealls_df], ignore_index=True)
        print(merged_df.head())
        print(f"total baris : {merged_df.shape}")
        
        merged_df["id"] = [str(uuid.uuid4()) for _ in range(len(merged_df))]
        
        merged_df.to_csv('./resources/csv/job_market.csv', index=False)

        return f"Data save"
        

    merged_table_task = merge_table_task(transform_tasks["kalibrr"], transform_tasks["dealls"])
    
    @task(task_id="load_to_bigquery")
    def load_to_bigquery_task():
        credentials = service_account.Credentials.from_service_account_file('/opt/airflow/resources/config/gcp/service_account.json')
        df = pd.read_csv('./resources/csv/job_market.csv')
        df.to_gbq(destination_table='job_market.jobs_detail', project_id='final-project-data-engineer', credentials=credentials, if_exists='replace')
        
        
    load_to_bigquery_task = load_to_bigquery_task()
    
    # load_to_bigquery = BigQueryInsertJobOperator(
    # task_id="load_to_bigquery",
    # configuration={
    #     "query": {
    #         "query": f"""
    #             CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_id}` AS
    #             SELECT * FROM UNNEST({df.values.tolist()}) AS t({", ".join(df.columns)})
    #         """,
    #         "useLegacySql": False,
    #     }
    # },
    # gcp_conn_id="google_cloud_default",
    # project_id=project_id
# )
    # @task(task_id="load_to_bigquery")
    # def load_to_bigquery():
    #     import pandas as pd
    #     import os
    #     from dotenv import load_dotenv
    #     load_dotenv()

        # Konfigurasi BigQuery
        # project_id = os.getenv("BIGQUERY_PROJECT_ID") # Ganti dengan project ID Google Cloud
        # dataset_id = os.getenv("BIGQUERY_DATASET_ID")  # Ganti dengan nama dataset di BigQuery
        # table_id = os.getenv("BIGQUERY_TABLE_ID")  # Ganti dengan nama tabel di BigQuery
        # csv_path = "./resources/csv/job_market.csv"  # Lokasi file CSV

        # Load CSV ke DataFrame
        # df = pd.read_csv(csv_path)

        # # Upload langsung ke BigQuery
    #     bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
        
        
    #     bq_hook.insert_rows(table=f"{dataset_id}.{table_id}", rows=df.values.tolist(), target_fields=df.columns.tolist())

    # load_to_bigquery = load_to_bigquery()
    transform_tasks["kalibrr"] >> merged_table_task
    transform_tasks["dealls"] >> merged_table_task
    merged_table_task >> load_to_bigquery_task >> end_task

etl_job_market()
