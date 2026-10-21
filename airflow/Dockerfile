FROM apache/airflow:2.10.3-python3.11
# dbt in its own virtualenv so its pins never fight Airflow's.
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-core>=1.10,<2" "dbt-trino>=1.9" \
 && pip install --no-cache-dir "trino>=0.330"
ENV DBT_BIN=/home/airflow/dbt-venv/bin/dbt
