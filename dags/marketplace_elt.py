"""
Monthly ELT for the Olist marketplace warehouse.

Each run owns one month of orders (its data interval):

    extract_and_validate -> load_reference_tables -> load_order_batch
        -> dbt_build_staging -> dbt_build_marts -> report_data_quality

* Runs are ordered (max_active_runs=1, depends_on_past on the batch load) because
  the SCD Type 2 customer dimension and the incremental fact assume batches arrive
  in purchase-date order.
* Every load is idempotent: re-running a month deletes and reloads that batch.
* dbt build runs tests right after each model, so a failed test stops the models
  that depend on it.
"""
import logging
import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/opt/project")
DBT_DIR = f"{PROJECT_ROOT}/dbt"
DBT_BIN = os.environ.get("DBT_BIN", "dbt")
DBT_ENV = {"DBT_PROFILES_DIR": DBT_DIR, "PATH": os.environ.get("PATH", "")}
for key in ("WAREHOUSE_HOST", "WAREHOUSE_PORT", "WAREHOUSE_USER", "WAREHOUSE_PASSWORD", "WAREHOUSE_DB"):
    if key in os.environ:
        DBT_ENV[key] = os.environ[key]

log = logging.getLogger(__name__)


@dag(
    dag_id="marketplace_elt",
    schedule=CronDataIntervalTimetable("0 0 1 * *", timezone="UTC"),
    start_date=pendulum.datetime(2016, 9, 1, tz="UTC"),
    end_date=pendulum.datetime(2018, 11, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["elt", "dbt", "warehouse"],
)
def marketplace_elt():

    @task
    def extract_and_validate() -> dict:
        """Download sources to the landing zone and check each header against its contract."""
        from include.ingestion.extract import extract_all
        return extract_all()

    @task
    def load_reference_tables(_manifest: dict) -> dict:
        from include.ingestion.load import load_reference_tables as load_ref
        return load_ref()

    @task(depends_on_past=True)
    def load_order_batch(_ref: dict, data_interval_start=None, data_interval_end=None) -> dict:
        from include.ingestion.load import load_order_batch as load_batch
        log.info("loading orders purchased in [%s, %s)", data_interval_start, data_interval_end)
        return load_batch(data_interval_start, data_interval_end)

    dbt_build_staging = BashOperator(
        task_id="dbt_build_staging",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} build --select staging --indirect-selection=cautious",
        env=DBT_ENV,
    )

    dbt_build_marts = BashOperator(
        task_id="dbt_build_marts",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} build --select intermediate marts data_quality",
        env=DBT_ENV,
    )

    @task
    def report_data_quality() -> dict:
        from include.ingestion.load import connect
        with connect() as conn, conn.cursor() as cur:
            cur.execute("select rule, failing_rows from data_quality.dq_summary order by rule")
            summary = dict(cur.fetchall())
            cur.execute("select count(*) from marts.fct_order_items")
            fact_rows = cur.fetchone()[0]
        log.info("fct_order_items rows: %s", fact_rows)
        for rule, rows in summary.items():
            log.info("dq rule %-42s failing rows: %s", rule, rows)
        return {"fct_order_items_rows": fact_rows, "dq": summary}

    batch = load_order_batch(load_reference_tables(extract_and_validate()))
    batch >> dbt_build_staging >> dbt_build_marts >> report_data_quality()


marketplace_elt()
