"""
ETL DAG.

Triggers the full Medallion pipeline every 2 minutes.
Responsible for:
- Bronze -> Silver -> Gold processing
- Persisting Gold layer into the database
- Writing object state changes into the outbox table
"""

from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from _shared.http_client import call_api


def run_full_etl(**_) -> dict:
    # Trigger full ETL pipeline
    return call_api("/etl/run-full", timeout_s=60 * 30)


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="etl_full_every_2_min",
    start_date=datetime(2026, 2, 1),
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["etl", "medallion"],
) as dag:

    PythonOperator(
        task_id="run_full_etl",
        python_callable=run_full_etl,
    )
