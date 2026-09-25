# Snowflake cost log

This is a template. Fill it in after you run the project against your own Snowflake trial.
Do not put account identifiers, usernames, or passwords in this file.

Nothing below is a measurement. The `_fill in_` cells are for numbers you copy out of
Snowflake (Admin → Cost Management, or the warehouse's Query History) after a run.

The warehouse Terraform creates is X-Small, starts suspended, and auto-suspends after
60 seconds of idle time. That is a configuration choice, not a credit estimate.
A query run resumes the warehouse. Credits accrue only while it is running.

| Date (UTC) | What you ran | Warehouse | Auto-suspend (seconds) | Credits | Notes |
| --- | --- | --- | --- | --- | --- |
| YYYY-MM-DD | `terraform apply` | ANALYTICS_WH | 60 | _fill in_ | Expect no resume if you do not query yet. |
| YYYY-MM-DD | `make ingest` with `WAREHOUSE_TARGET=snowflake` | ANALYTICS_WH | 60 | _fill in_ | |
| YYYY-MM-DD | `make dbt-build` with `WAREHOUSE_TARGET=snowflake` | ANALYTICS_WH | 60 | _fill in_ | |
| YYYY-MM-DD | A second `make dbt-build` the same day | ANALYTICS_WH | 60 | _fill in_ | Useful to compare a warm run with a cold one. |
| YYYY-MM-DD | Warehouse left idle after the last query | ANALYTICS_WH | 60 | _fill in_ | Check that it actually suspended. |

## How to read a credit figure

1. In Snowsight, open the warehouse and note the resume and suspend timestamps around your command.
2. Copy the credit total for that window from Cost Management. Do not estimate it.
3. Write the figure in the table and a one-line note about what you ran.

## Local stack

Docker Compose caps Postgres at 512 MB and Airflow at 1536 MB. Those are limits, not
observed usage. After `make up` on your machine, record what you actually see:

| Date | Command you used to measure | Postgres | Airflow | Notes |
| --- | --- | --- | --- | --- |
| YYYY-MM-DD | _fill in, for example `docker stats`_ | _fill in_ | _fill in_ | |

## CDC profile

These caps are configuration from `docker-compose.cdc.yml`. They are not a measurement.
Phase 1's caps still apply, because Postgres and Airflow stay up.

| Service | mem_limit |
| --- | --- |
| Kafka (KRaft, no ZooKeeper) | 768 MB |
| Debezium Connect | 1024 MB |
| Spark consumer (`make cdc-up`) | 1536 MB |
| Python consumer (`make cdc-up-python`) | 384 MB |

After you start a profile, record what `docker stats` actually shows. Also paste the lines from `make cdc-lag` if you want a record of broker lag and the source-to-warehouse delay. Do not estimate either one.

| Date | Profile | Command | Kafka | Connect | Consumer | Lag you observed | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| YYYY-MM-DD | _fill in_ | _fill in_ | _fill in_ | _fill in_ | _fill in_ | _fill in_ | |
