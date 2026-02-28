from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Add project root to path
sys.path.append(r"C:\Users\dipud\OneDrive\Desktop\sales_etl_pipeline")

# Import ETL functions
from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load

default_args = {
    "owner": "deepak",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="sales_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load
    )

    extract_task >> transform_task >> load_task