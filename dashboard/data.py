"""Read the published marts from Snowflake or the local DuckDB file.

WAREHOUSE_TARGET picks the source, the same variable ingest and dbt use.
Snowflake credentials come from the environment (see .env.example). Nothing is written.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

QUERIES = {
    "mrr": """
        select month_start, beginning_mrr_cents, new_mrr_cents, expansion_mrr_cents,
               contraction_mrr_cents, churn_mrr_cents, reactivation_mrr_cents,
               ending_mrr_cents, active_orgs_start, active_orgs_end, new_orgs,
               churned_orgs, logo_churn_rate, _dbt_built_at
        from marts.fct_mrr_monthly
        order by month_start
    """,
    "orgs": """
        select org_id, org_name, plan, subscription_status, mrr_cents, seats,
               user_count, active_user_count, is_active, is_paying
        from marts.dim_organizations
    """,
    "logins": """
        select activity_date,
               sum(login_count) as logins,
               sum(successful_login_count) as successful_logins,
               sum(distinct_users) as active_users
        from marts.fct_login_activity_daily
        group by activity_date
        order by activity_date
    """,
    "changes": """
        select c.occurred_at, c.occurred_month, c.movement_type, c.event_type,
               c.from_plan, c.to_plan, c.mrr_delta_cents, o.org_name
        from marts.fct_plan_changes c
        left join marts.dim_organizations o on o.org_id = c.org_id
        order by c.occurred_at
    """,
}


def _snowflake() -> dict[str, pd.DataFrame]:
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_REPORTER_ROLE", "REPORTER"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ANALYTICS_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "ANALYTICS"),
        session_parameters={"QUERY_TAG": "saas_analytics_dashboard"},
    )
    try:
        out = {}
        for name, sql in QUERIES.items():
            cur = conn.cursor()
            cur.execute(sql)
            cols = [d[0].lower() for d in cur.description]
            out[name] = pd.DataFrame(cur.fetchall(), columns=cols)
        return out
    finally:
        conn.close()


def _duckdb() -> dict[str, pd.DataFrame]:
    import duckdb

    path = os.environ.get("DUCKDB_PATH") or str(ROOT / "warehouse" / "analytics.duckdb")
    con = duckdb.connect(path, read_only=True)
    try:
        return {name: con.execute(sql).df() for name, sql in QUERIES.items()}
    finally:
        con.close()


def load_marts() -> tuple[dict[str, pd.DataFrame], str]:
    target = os.environ.get("WAREHOUSE_TARGET", "duckdb")
    frames = _snowflake() if target == "snowflake" else _duckdb()
    for df in frames.values():
        for col in df.columns:
            if col.endswith("_cents") or col in {"logo_churn_rate"}:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    return frames, target
