FROM apache/airflow:3.0.3
USER airflow
RUN pip install --no-cache-dir dbt-core dbt-snowflake
