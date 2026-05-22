##################################################
# archive_cleanup.py (archive Cleanup DAGs)
#
# These DAGs are responsible for controlled cleanup of
# archived Bronze and Silver buckets in MinIO.
#
# !!! WARNING !!!:
# These endpoints must never be triggered manually.
# They are strictly scheduled cleanup jobs.
##################################################

from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from _shared.http_client import call_api


def cleanup_bronze_archive(**_) -> dict:
    # Clear Bronze archive bucket
    return call_api("/bronze-archive/cleanup", timeout_s=60 * 10)


def cleanup_silver_archive(**_) -> dict:
    # Clear Silver archive bucket
    return call_api("/silver-archive/cleanup", timeout_s=60 * 10)


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

####################
# Bronze cleanup — every 30 min.
####################
with DAG(
    dag_id="bronze_archive_cleanup_every_30_min",
    start_date=datetime(2026, 2, 1),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["cleanup", "bronze"],
) as bronze_cleanup_dag:

    PythonOperator(
        task_id="cleanup_bronze_archive",
        python_callable=cleanup_bronze_archive,
    )


####################
# Silver cleanup — every 1 h.
####################
with DAG(
    dag_id="silver_archive_cleanup_every_hour",
    start_date=datetime(2026, 2, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["cleanup", "silver"],
) as silver_cleanup_dag:

    PythonOperator(
        task_id="cleanup_silver_archive",
        python_callable=cleanup_silver_archive,
    )
