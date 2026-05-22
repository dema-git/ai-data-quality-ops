###############################################
# outbox_archive.py (outbox Archive DAG)
#
# Processes outbox table entries and archives related objects.
# Runs independently from ETL to ensure decoupled processing.
###############################################

from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from _shared.http_client import call_api


def run_archive_worker(**_) -> dict:
    """
    Trigger archive worker:
    - Select eligible outbox records
    - Archive associated data
    - Atomically update status
    """
    return call_api("/outbox/archive-run", timeout_s=60 * 30)


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="outbox_archive_every_30_min",
    start_date=datetime(2026, 2, 1),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["outbox", "archive"],
) as dag:

    PythonOperator(
        task_id="run_archive_worker",
        python_callable=run_archive_worker,
    )
