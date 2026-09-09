"""
End-to-end ELT pipeline for the e-commerce data warehouse.

    extract_load (Python: raw CSVs -> raw schema, incremental + DQ checks)
        -> dbt_source_freshness (fail fast if raw data is stale)
        -> dbt_run (build staging -> intermediate -> marts)
        -> dbt_test (schema tests + custom singular tests)
        -> publish_summary (row counts / DQ summary, for visibility in logs)
"""
from __future__ import annotations

import datetime as dt

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowFailException

DBT_PROJECT_DIR = "/opt/airflow/dbt/ecommerce_dwh"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
SCRIPTS_DIR = "/opt/airflow/scripts"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": dt.timedelta(minutes=20),
    "email_on_failure": False,
}


def alert_on_failure(context):
    """Placeholder failure callback — swap in a Slack/PagerDuty call in prod."""
    ti = context["task_instance"]
    print(f"[ALERT] Task {ti.task_id} failed in DAG {ti.dag_id} (run {context['run_id']}).")


@dag(
    dag_id="ecommerce_elt_pipeline",
    description="Extract Olist CSVs -> raw Postgres -> dbt staging/marts -> tests",
    schedule="0 3 * * *",
    start_date=dt.datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    on_failure_callback=alert_on_failure,
    tags=["ecommerce", "dwh", "elt"],
)
def ecommerce_elt_pipeline():

    @task(task_id="extract_and_load_raw")
    def extract_and_load_raw():
        import subprocess

        result = subprocess.run(
            ["python", f"{SCRIPTS_DIR}/load_raw.py"],
            cwd=SCRIPTS_DIR,
            capture_output=True,
            text=True,
        )
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise AirflowFailException("load_raw.py failed — see logs / meta.load_audit_log for details")

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt source freshness --profiles-dir {DBT_PROFILES_DIR}"
        ),
        retries=0,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    @task(task_id="publish_run_summary")
    def publish_run_summary():
        import os
        from sqlalchemy import create_engine, text

        engine = create_engine(
            f"postgresql+psycopg2://{os.environ['DWH_USER']}:{os.environ['DWH_PASSWORD']}"
            f"@{os.environ['DWH_HOST']}:{os.environ['DWH_PORT']}/{os.environ['DWH_DB']}"
        )
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT source_table, rows_loaded, status, run_finished_at
                FROM meta.load_audit_log
                WHERE run_started_at > now() - interval '1 hour'
                ORDER BY run_finished_at DESC
            """)).fetchall()
        for r in rows:
            print(f"  {r.source_table:30s} loaded={r.rows_loaded!s:>8}  status={r.status}")

    extract = extract_and_load_raw()
    summary = publish_run_summary()

    extract >> dbt_source_freshness >> dbt_run >> dbt_test >> summary


ecommerce_elt_pipeline()