# Dashboard

A Streamlit and Plotly dashboard on the published dbt marts. It only reads `marts.*`, so it shows only data that passed the dbt tests.

Panels:

- KPI tiles: current MRR, MRR change, active customers, logo churn rate, new MRR, and expansion MRR for the latest month.
- MRR trend over every month in `fct_mrr_monthly`, with paying customers.
- MRR bridge as stacked bars (new, expansion, reactivation, contraction, churn), plus a waterfall for the latest month.
- MRR share and customers by plan, from `dim_organizations`.
- Daily logins, successful logins, and unique users, from `fct_login_activity_daily`.
- Plan changes per month, all-time MRR by movement type, and the latest changes, from `fct_plan_changes`.

## Run it

Build the marts first (`make seed`, `make ingest`, `make dbt-build`). Then, from the repo root with the venv active:

```bash
pip install -r dashboard/requirements.txt
make dashboard            # same as: streamlit run dashboard/app.py --server.address 127.0.0.1
```

Open http://127.0.0.1:8501.

`WAREHOUSE_TARGET` picks the source, the same way it does for ingest and dbt:

- `duckdb` (default) reads `warehouse/analytics.duckdb` read-only. Close any other writer first.
- `snowflake` reads `ANALYTICS.MARTS` with the `SNOWFLAKE_*` variables from `.env`. It uses the `REPORTER` role, or `SNOWFLAKE_REPORTER_ROLE` if set. Results are cached for an hour, so leaving the page open doesn't keep the warehouse running.

The server binds to 127.0.0.1. Keep it that way when Snowflake credentials are in the environment.
