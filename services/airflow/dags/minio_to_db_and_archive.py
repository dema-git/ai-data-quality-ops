############################################################
# minio_to_db_and_archive.py
#
# # DAG to fetch files from MinIO, save data to the database,
# and archive the files every 5 minutes
#############################################################
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from functools import partial
from airflow import DAG
import requests


default_args = {
    "owner": "Vlad",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

TIMEOUT = 30
INTERVAL_MINUTES = 5

# api_app - name of the service inside the container
endpoints = {
    "fetch_files": "http://api_app:8000/minino/getallfiles",
    "archive_files": "http://api_app:8000/minino/movetoarchive",
}


def _trigger_endpoint(url: str):
    """
    Trigger an endpoint and return JSON response.
    """
    response = requests.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


with DAG(
    dag_id="minio_fetch_save_to_db_and_archive",
    default_args=default_args,
    description="Fetch files from MinIO, save them to the database, and archive them",
    start_date=datetime(2025, 11, 10),
    schedule_interval=timedelta(minutes=INTERVAL_MINUTES),
    catchup=False,
    tags=["minio"],
) as dag:


    # Tasks
    fetch_files_task = PythonOperator(
        task_id="fetch_minio_files_and_save_them_to_db",
        python_callable=partial(_trigger_endpoint, endpoints["fetch_files"]),
    )

    archive_files_task = PythonOperator(
        task_id="move_saved_minio_files_to_archive",
        python_callable=partial(_trigger_endpoint, endpoints["archive_files"]),
    )


    fetch_files_task >> archive_files_task