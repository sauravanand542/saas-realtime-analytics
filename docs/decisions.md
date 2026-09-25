# Decision log

Short notes on choices in Phase 1. Prices in `generator/config.py` are inputs to the simulator. They are not measurements of a product.

## DuckDB is the default warehouse

Local development and CI need a warehouse that costs nothing and starts without an account. DuckDB is a file (`warehouse/analytics.duckdb`) that dbt-duckdb can build in one process. Snowflake stays a second profile target, configured only from environment variables, so the SQL is exercised against the engine used in the interviews when credentials exist.

dbt-core is pinned to 1.11 because that is the newest release line that publishes both `dbt-duckdb` and `dbt-snowflake`. A newer core would drop the local adapter.

## Batch first, CDC later

The teaching goal of this phase is the warehouse layout, tests, and orchestration. A batch extract is enough to produce a raw schema with a column contract, a load timestamp, a batch id, and `_source_system`. Phase 2 can write a CDC stream into the same tables with `_source_system = debezium`. Staging selects named columns, so new landing metadata does not have to flow into marts.

## Full extract for mutable entities, watermark for facts

Organizations, users, and subscriptions are current-state tables. Each batch replaces them. That keeps deletes and in-place edits simple at this volume.

Logins, subscription events, and invoices grow by append and by status updates. Ingest stores a watermark on `recorded_at` (invoices use the same column, which the generator bumps when status changes) and re-reads a two-day lookback (`LOOKBACK_DAYS`). The load deletes and reinserts by primary key, so replaying the window does not duplicate rows. If an invoice status changes and `recorded_at` does not move, the watermark misses the update. That is an application contract, called out so it can be discussed.

## Incremental logins filter on load time

`fct_logins` is incremental on `login_id`. The filter compares `_loaded_at` with the maximum already in the fact. A login whose `logged_in_at` is last week, but which arrived in today's batch, still has a new `_loaded_at` and is merged. Filtering on event time would drop that row forever after the first successful run.

The strategy is `merge` on Snowflake and `delete+insert` on DuckDB, which is what each adapter implements for this pattern. `on_schema_change` is `append_new_columns` so a new source column can land without a full refresh.

`fct_login_activity_daily` is a full table rebuild from that fact. A late login has to restate the old day. An incremental daily aggregate on activity date would keep the stale count.

## The MRR mart is a full rebuild

`fct_mrr_monthly` is a table, rebuilt every run. A late subscription event changes an old month, and the bridge identity has to hold for every month after that. The simulated history is small enough that an incremental month model would add restatement logic without changing the answer. Ending MRR is the last event per org at month end. The component columns are signed flows from events in the month (contraction and churn are negative). The singular test checks:

`ending = beginning + new + expansion + reactivation + contraction + churn`

Logo churn rate is churned orgs that were paying at the previous month end, divided by paying orgs at the start of the month. The rate is null when the start count is zero.

The warehouse trusts `mrr_cents` on the event. It does not recompute price from the catalog. Seats do not affect MRR in this simulator.

## Snapshot uses the check strategy

`subscriptions_snapshot` is SCD Type 2. The check columns are `plan`, `status`, `mrr_cents`, `seats`, and `canceled_at`. A timestamp strategy would open a new version whenever `updated_at` moves, including touches that do not change the commercial state.

The snapshot reads staging, where plan and status are already lowercased. A casing glitch in the app would otherwise look like a plan change. `hard_deletes` is `ignore`: a missing subscription does not close the row. The generator does not delete subscriptions.

## Tests do not block models inside one `dbt build`

dbt runs a model's tests after the model. A failing test does not stop a downstream model that was selected in the same invocation. `scripts/dbt_build.sh` therefore runs two builds: `staging` and `intermediate` first, then `marts` and `path:snapshots`. The selector name `snapshots` does not match snapshot nodes; the path selector does.

Airflow splits those into tasks. The publish task uses the default trigger rule `all_success`, and the test tasks use `retries=0` because a data test fails the same way on every retry. Generate and ingest retry with backoff. Freshness retries once.

Generic tests with arguments (`accepted_values`, `relationships`) nest those arguments under `arguments:` for dbt 1.11.

## Source freshness is on load time

Sources declare `loaded_at_field: _loaded_at`, warn after 36 hours, error after 72. That measures whether ingest ran, which is what a daily batch can promise. Freshness on `logged_in_at` would mix business time with pipeline health and would fail when the generator is quiet.

## Schema names are the custom schema only

`generate_schema_name` returns `staging`, `intermediate`, `marts`, or `snapshots` with no target-schema prefix. One environment is the whole project, and those names match the Terraform schemas. On a shared Snowflake account the usual pattern is a prefix per developer (`dbt_<user>_staging`) so people do not overwrite each other. That prefix is the change to make if this project ever has more than one writer.

## One warehouse, three roles

Terraform creates a single X-Small warehouse, `initially_suspended`, `auto_suspend` 60 seconds, `auto_resume` set. The provider expects `auto_resume` as the string `"true"`. One warehouse is a credit-saving choice for a trial. A separate warehouse for reporters would isolate their load. It is not created here.

Roles:

- `LOADER` can use the warehouse and database, create tables in `RAW`, and modify tables there. It cannot create schemas. Ingest fails if `RAW` is missing, which is what we want.
- `TRANSFORMER` can read `RAW` and create objects in the dbt schemas. It also has `CREATE SCHEMA` on the database because dbt runs `CREATE SCHEMA IF NOT EXISTS`.
- `REPORTER` can select current and future tables and views in `MARTS` only.

All three roles are granted to `SYSADMIN` so an admin can see objects they create. `grant_to_user` is optional and empty by default.

## Portable SQL, no dbt_utils

The project does not install a package. The generic tests it needs (`non_negative`, and two warn-level timestamp checks) are local macros. That keeps CI install small and avoids a package version to track.

`add_months` hides the one date function that differs. `QUALIFY` and `datediff` are used directly because both engines accept them.

## Airflow 2.10 in one container

The image is `apache/airflow:2.10.5` with `LocalExecutor`. The scheduler runs in the background and the webserver in the foreground. A second virtualenv (`/opt/pipeline-venv`) holds dbt and the pipeline libraries so they do not have to fit inside Airflow's dependency set. DAGs are paused at creation. Parallelism and webserver workers are kept low because this is a laptop.

The point of the DAG is the gate and the retry policy. A newer Airflow major version would change the container layout. It would not change that design.

## App database does not reject negative MRR

`app.subscriptions.mrr_cents` has no check constraint. The failure demo needs the application to accept a bad write so the warehouse test is the thing that stops publication. Staging does not clamp the value.

## Quoting and profiles

dbt quoting is left off. Ingest creates raw tables with unquoted identifiers so DuckDB and Snowflake fold them the same way dbt will. Relative `DUCKDB_PATH` values are resolved from the repo root by the Makefile and `scripts/dbt_build.sh`, because dbt resolves a relative profile path from `transform/`.
