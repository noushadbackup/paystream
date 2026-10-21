FROM apache/airflow:2.10.3-python3.11
# dbt and the model tooling each in their own virtualenv, so their pins never fight Airflow's.
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-core>=1.10,<2" "dbt-trino>=1.9" \
 && pip install --no-cache-dir "trino>=0.330"
COPY --chown=airflow decision-service /opt/paystream/decision-service
COPY --chown=airflow ml /opt/paystream/ml
RUN python -m venv /home/airflow/ml-venv \
 && /home/airflow/ml-venv/bin/pip install --no-cache-dir /opt/paystream/decision-service "/opt/paystream/ml[trino]"
ENV DBT_BIN=/home/airflow/dbt-venv/bin/dbt ML_BIN=/home/airflow/ml-venv/bin/paystream-ml
