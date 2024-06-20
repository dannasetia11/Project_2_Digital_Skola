FROM apache/airflow:2.5.0

# Install dependencies
RUN pip install apache-airflow-providers-snowflake

RUN pip install apache-airflow-providers-postgres
