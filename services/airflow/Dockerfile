FROM apache/airflow:3.1.7-python3.11

USER root

RUN pip install --no-cache-dir apache-airflow-providers-fab

COPY scripts/ /opt/airflow/scripts/
RUN chmod +x /opt/airflow/scripts/*.sh
RUN chown -R airflow: /opt/airflow/scripts/

USER airflow
WORKDIR /opt/airflow