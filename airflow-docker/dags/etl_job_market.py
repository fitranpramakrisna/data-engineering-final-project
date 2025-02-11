import sys
import pandas as pd
sys.path.append("/opt/airflow")

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from resources.scripts.extract_kalibrr import extract_web_kalibrr
from resources.scripts.extract_dealls import extract_web_dealls
from resources.scripts.transform_dealls import tranform_web_dealls
from resources.scripts.transform_kalibrr import tranform_web_kalibrr

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

    # Task untuk menggabungkan hasil transformasi
    @task(task_id="merge_transformed_data")
    def merge_table_task(kalibrr_data, dealls_data):
        kalibrr_df = pd.read_json(kalibrr_data)
        dealls_df = pd.read_json(dealls_data)
        print(f"total baris kalibr : {kalibrr_df.shape}")
        print(f"total baris dealls : {dealls_df.shape}")
        merged_df = pd.concat([kalibrr_df, dealls_df], ignore_index=True)
        print(merged_df.head())  # Debugging
        print(f"total baris : {merged_df.shape}")
        
        return merged_df.to_json()  # Bisa digunakan untuk penyimpanan lebih lanjut

    merged_table_task = merge_table_task(transform_tasks["kalibrr"], transform_tasks["dealls"])
    
    transform_tasks["kalibrr"] >> merged_table_task
    transform_tasks["dealls"] >> merged_table_task
    merged_table_task >> end_task

etl_job_market()
