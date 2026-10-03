# Snowflake run, 2026-10-02

This is the Snowflake run behind the demo video. The video covers the problem first, then a short look at the dashboard, then most of its time on an animated architecture diagram. The dashboard clips are a recording of `dashboard/app.py` reading the Snowflake marts, and every number on screen comes from those tables. The account is a new Snowflake trial (Standard edition, AWS us-east-1). Account identifiers and credentials are left out on purpose, and no frame of the video shows them.

## Setup

| Item | Value |
| --- | --- |
| Date | 2026-10-02 (US Eastern) |
| Warehouse target | `WAREHOUSE_TARGET=snowflake`, dbt target `snowflake` in `transform/profiles.yml` |
| Infrastructure | `infra/snowflake` applied with Terraform 1.16.5 and the `snowflakedb/snowflake` 2.x provider, using password auth from environment variables |
| Warehouse | `ANALYTICS_WH`, X-Small, auto-suspend 60 s, suspended at the end of the run |
| Database and schemas | `ANALYTICS`: `RAW`, `STAGING`, `INTERMEDIATE`, `MARTS`, `SNAPSHOTS` |
| Roles | `LOADER` for ingest, `TRANSFORMER` for dbt, `REPORTER` for the dashboard, granted to the trial user |
| App database | Local Postgres 17, `make seed` with `SEED=42` |
| Python and dbt | Python 3.12, dbt-core 1.11.15, dbt-snowflake 1.11.6 |
| Dashboard | Streamlit 1.65 and Plotly 7.1, `WAREHOUSE_TARGET=snowflake`, recorded in headless Chromium |

## What ran

- `make seed` wrote 40 organizations, 220 users, 2,409 logins, 40 subscriptions, 112 subscription events and 375 invoices.
- `make ingest` (batch) loaded six `RAW` tables, 3,196 rows. Organizations, users and subscriptions are full reloads. Logins, subscription events and invoices are watermarked upserts with a two-day lookback.
- `make dbt-build`:
  - Freshness: passed.
  - Staging and intermediate: `PASS=73 WARN=3 ERROR=0`. The three warnings are warn-severity source checks.
  - Marts and snapshot: `PASS=34 ERROR=0`.
- `make break-it`: `non_negative_stg_app__subscriptions_mrr_cents` failed, and the marts were not rebuilt. `_dbt_built_at` and the row counts of `fct_mrr_monthly` and `dim_organizations` stayed the same, and the negative MRR showed up in staging but not in `dim_organizations`. `make heal` then passed every test and republished the marts.
- Not run here: the CDC profile (Debezium, Kafka, Spark) and Airflow. Both are part of the repo's design. The gate ran through `scripts/dbt_build.sh`, the same script the Airflow DAG calls.

## Mart row counts

| Model | Rows |
| --- | ---: |
| `marts.fct_mrr_monthly` | 18 |
| `marts.dim_organizations` | 40 |
| `marts.fct_plan_changes` | 112 |
| `marts.fct_logins` | 2,397 |
| `marts.fct_login_activity_daily` | 1,030 |
| `marts.fct_invoices` | 375 |
| `snapshots.subscriptions_snapshot` | 47 |

## Numbers on the dashboard (the video reads only September MRR aloud)

- September 2026 MRR: $7,018, down $650 (-8.5%) from August's $7,668.
- 32 active customers. Logo churn 6.3% (2 customers), down from 11.8% in August.
- New MRR $98 from 2 new customers. Expansion MRR $1,400. Contraction -$150. Churn -$1,998.
- MRR by plan: enterprise $3,996 from 4 paying customers (57%), growth $2,189 from 11, starter $833 from 17.
- 2,397 logins from 2026-08-24 to 2026-10-02.
- All-time plan changes: 36 new (+$1,764), 24 expansions (+$8,150), 13 reactivations (+$1,837), 2 contractions (-$300), 17 churns (-$4,433).

## Credits

`INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY` showed about 0.34 credits for `ANALYTICS_WH` across the whole session, including setup, dry runs and both recordings. That view can lag. Use Snowsight for the final figure in `docs/cost.md`.

## Notes for the next run

- Leave `SNOWFLAKE_WAREHOUSE` unset for the first `terraform plan` and `apply`. The provider reads that variable and fails to connect while `ANALYTICS_WH` does not exist yet. Also unset `SNOWFLAKE_ACCOUNT` for Terraform, because the 2.x provider ignores it and prints a warning. The run used `env -u SNOWFLAKE_WAREHOUSE -u SNOWFLAKE_ACCOUNT terraform ...`.
- Set `TF_VAR_grant_to_user` in upper case. The provider quotes the name, so a lower-case value fails with `object does not exist or not authorized`. The first apply created 37 of 40 resources, and a second apply with the upper-case name added the last 3 grants.
- The Makefile includes `.env`, and values in that file override the shell environment. To switch targets for one command, pass the override on the make command line, for example `make ingest WAREHOUSE_TARGET=duckdb`.
- New trial users have secondary roles enabled by default. Queries under `LOADER` or `REPORTER` can still see objects granted to the user's other roles, so this run does not prove the three roles are isolated.
