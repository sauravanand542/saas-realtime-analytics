"""Daily batch: generate a tick, load raw, test upstream, then publish marts.

The publish task uses the default trigger rule (all_success). If the upstream
dbt build fails a test, Airflow skips marts and snapshots.
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
    dag_id="saas_analytics_batch",
    default_args=default_args,
    description="Generate app activity, load raw, and publish marts if tests pass.",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["phase1", "batch"],
) as dag:
    generate = BashOperator(
        task_id="generate_tick",
        bash_command=(
            f"cd {PROJECT} && {PYTHON} -m generator --mode tick --seed {{{{ ds_nodash }}}}"
        ),
    )

    ingest = BashOperator(
        task_id="ingest_raw",
        bash_command=(
            f"cd {PROJECT} && {PYTHON} -m ingest --target "
            "${WAREHOUSE_TARGET:-duckdb}"
        ),
    )

    freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {PROJECT} && ./scripts/dbt_build.sh freshness",
        retries=1,
    )

    # Data-test failures are deterministic. Retrying them does not help.
    upstream = BashOperator(
        task_id="dbt_build_upstream",
        bash_command=f"cd {PROJECT} && ./scripts/dbt_build.sh upstream",
        retries=0,
    )

    publish = BashOperator(
        task_id="dbt_build_marts",
        bash_command=f"cd {PROJECT} && ./scripts/dbt_build.sh publish",
        retries=0,
    )

    generate >> ingest >> freshness >> upstream >> publish
