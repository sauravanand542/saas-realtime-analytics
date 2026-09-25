"""Observe the CDC connector. This DAG does not run the stream.

The Spark or Python consumer is a long-running process in the CDC Compose
profile. Airflow only reports whether Connect is up and prints lag.
When CDC_CONNECT_URL is unset, the check succeeds and explains that it skipped.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "${PROJECT_DIR:-/opt/airflow/project}"
PYTHON = "${PIPELINE_PYTHON:-/opt/pipeline-venv/bin/python}"

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="saas_cdc_health",
    default_args=default_args,
    description="Check Debezium connector status and print consumer lag.",
    schedule="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["phase2", "cdc"],
) as dag:
    connector = BashOperator(
        task_id="debezium_connector_status",
        bash_command=f"cd {PROJECT} && {PYTHON} -m streaming.health",
    )
    lag = BashOperator(
        task_id="cdc_lag",
        bash_command=f"cd {PROJECT} && {PYTHON} -m streaming.lag",
    )
    connector >> lag
