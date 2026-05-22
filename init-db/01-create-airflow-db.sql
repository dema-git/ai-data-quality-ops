-- ###########################################################
-- 01-create-airflow-db.sql
-- Initialization script for PostgreSQL database for Apache Airflow
-- ################################################################

CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow_db OWNER airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow;
