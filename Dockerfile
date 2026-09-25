FROM apache/airflow:3.3.2-python3.11

# dbt lives in its own virtualenv so its dependencies never conflict with Airflow's.
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/airflow/dbt-venv \
 && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt
