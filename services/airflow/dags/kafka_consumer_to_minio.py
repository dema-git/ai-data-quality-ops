################################################################
# kafka_consumer_to_minio.py
#
# DAG to consume messages from Kafka
# and save them to MinIO every 2 minutes
#################################################################
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
import requests


default_args = {
    "owner": "Vlad",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

KAFKA_URL = "http://api_app:8000/kafka/consumer"
TIMEOUT = 30
INTERVAL_MINUTES = 2


def _consume_kafka_messages():
    """Consume all messages from Kafka and save to MinIO."""
    response = requests.get(KAFKA_URL, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


with DAG(
    dag_id="kafka_consumer_to_minio",
    default_args=default_args,
    description="Consume messages from Kafka and save to MinIO",
    start_date=datetime(2025, 11, 10),
    schedule_interval=timedelta(minutes=INTERVAL_MINUTES),
    catchup=False,
    tags=["kafka", "minio"],
) as dag:

    # task
    consume_task = PythonOperator(
        task_id="consume_kafka_messages",
        python_callable=_consume_kafka_messages,
    )