# Walkthrough

Read this after the README. The goal is to be able to point at each directory and say what breaks if you change it.

## 1. Application database

`app_db/init/001_app_schema.sql` is the product database, schema `app`. Organizations, users, one subscription per org, subscription events, invoices, and logins.

Things that are deliberate:

- `mrr_cents` has no check constraint. The warehouse is responsible for rejecting a negative value.
- `logins.user_id` is not a foreign key. Some logins refer to a user who is not in `app.users`.
- `subscription_events.occurred_at` is when the commercial change happened. `recorded_at` is when the app stored the row. Ingest watermarks on `recorded_at`.

`000_airflow.sh` creates the Airflow metadata role and database on first Docker init. It is shell because `CREATE DATABASE` cannot run inside the entrypoint's SQL transaction.

## 2. Generator

`python -m generator --mode seed --seed 42` truncates `app` and writes a history. The same seed produces the same rows. `make tick` adds a small set of changes: new orgs, a member joining, a plan change or cancellation or reactivation, logins, a duplicate client event id, an orphan login, and one invoice moving to paid.

Mess the models are expected to handle, at warning severity rather than a failed build:

- null emails
- null identity-provider on some logins
- orphan logins
- duplicate `client_event_id` values, collapsed in staging
- plan text that arrives in upper case, lowercased in staging
- currency codes that arrive in lower case, uppercased in staging
- paid invoices with a null `paid_at`
- a canceled subscription with a null `canceled_at`
- logins whose `recorded_at` is later than `logged_in_at`

`generator/config.py` holds the plan catalog (trial, starter, growth, enterprise) and the cent prices the simulator writes. The marts do not import that file.

A tick rolls back the transaction opened by its earlier reads, then writes inside one `connection.transaction()` block. Without that rollback, psycopg treats the write block as a savepoint and closing the connection drops the new rows.

## 3. Ingest

`python -m ingest` reads `ingest/contract.py` and writes `raw`. Every table gets `_loaded_at`, `_batch_id`, and `_source_system = postgres_batch`.

Organizations, users, and subscriptions are deleted and reloaded. Logins, events, and invoices re-read the watermark minus two days and delete-plus-insert on the primary key. Run ingest twice. Fact row counts in `raw` should stay the same.

`--target snowflake` uses the loader role and refuses to create the `RAW` schema. Terraform is what creates it.

## 4. dbt

From `transform/`, or via `make dbt-build`, which calls `scripts/dbt_build.sh`.

Order inside one full run:

1. `dbt source freshness` on `_loaded_at`.
2. `dbt build --select staging intermediate`. Views, plus tests. Warning tests (null email, orphan login, paid without `paid_at`, canceled without `canceled_at`) are allowed. `non_negative` on MRR is not.
3. `dbt build --select marts path:snapshots`. Tables, the login incremental, the snapshot, and the singular tests.

Open the docs with `make dbt-docs` after a build. Model and column descriptions live next to the YAML.

Models worth reading in order:

1. `models/sources.yml` — the Phase 2 contract is described on the source.
2. `models/staging/stg_app__logins.sql` — `QUALIFY` dedupes a retried client event.
3. `models/staging/stg_app__subscriptions.sql` — plan is cleaned, MRR is not clamped.
4. `models/intermediate/int_subscription_movements.sql` — how an event becomes new, expansion, contraction, churn, reactivation, or other. A trial that stays at zero MRR is `other`.
5. `models/intermediate/int_org_month_mrr.sql` — month spine and the as-of MRR at each month end.
6. `models/marts/fct_mrr_monthly.sql` — the bridge. `tests/assert_mrr_bridge_balances.sql` fails the build if the identity does not hold.
7. `models/marts/fct_logins.sql` — incremental predicate.
8. `snapshots/subscriptions_snapshot.sql` — check strategy.

`dim_organizations.is_active` is an org that is still active and whose subscription is trialing, active, or past due. `is_paying` also requires MRR greater than zero and a status of active or past due. A trial is active and not paying.

## 5. Airflow

`airflow/dags/saas_analytics_batch.py` is a linear DAG:

`generate_tick` → `ingest_raw` → `dbt_source_freshness` → `dbt_build_upstream` → `dbt_build_marts`

The tick passes `ds_nodash` as the generator seed, so a given logical date is deterministic given the database it finds. Generate and ingest retry. The two dbt build tasks do not. If upstream fails, publish is skipped.

Trigger the DAG from the UI after `make up`, or keep using the Makefile on the host. Do not do both against the same DuckDB file at once.

## 6. Terraform

`infra/snowflake/` is safe to validate with no account:

```bash
make tf-validate
```

Read `grants.tf` before you apply. The loader cannot read marts. The reporter cannot read raw. The transformer can create schemas because dbt does. After apply, record credits in `docs/cost.md` from Snowsight.

## 7. Failure demo

`make break-it` updates one active subscription to `mrr_cents = -500` and does not write a subscription event, so the event ledger stays internally consistent while the current-state row is wrong. Ingest copies the bad row into `raw`. The staging test fails. The shell stops before the mart build. On DuckDB the script compares build timestamps and row counts on `fct_mrr_monthly` and `dim_organizations`.

`make heal` sets that row back to the catalog price for its plan, ingests, rebuilds, and checks that the MRR mart's `_dbt_built_at` moved.

The generator's tick skips subscriptions that already have negative MRR, so a later tick does not silently heal the demo.

## 8. Change data capture

Start Phase 1 first if you want the marts. The CDC profile is optional.

`docker-compose.cdc.yml` turns on logical replication, Kafka in KRaft mode (no ZooKeeper), and Debezium Connect. `make cdc-prepare` is idempotent: replication role `replicator`, `SELECT` on `app`, publication `app_publication` for the six tables. The connector config is `streaming/register_connector.py`: plugin `pgoutput`, slot `saas_app_slot`, JSON without schemas, tombstones left on.

The consumer reads topics `saas.app.<table>`. `streaming/messages.py` turns an envelope into a change or a dead letter. `streaming/store.py` inserts it. Spark (`streaming/spark_consumer.py`) is what `make cdc-up` runs. `make cdc-up-python` runs `streaming/python_consumer.py` with the same `apply_records` function. Offsets commit after the warehouse transaction. Spark's version of that commit is the checkpoint under `warehouse/cdc_checkpoints`.

`transform/macros/current_entity.sql` is the merge. `tests/assert_no_duplicate_current_keys.sql` fails if staging ever has two current rows for one key. `python -m streaming.overlap_check` deletes one real organization via a future CDC timestamp and adds a CDC-only organization with two versions, then checks the view. It removes the fixture afterward. Relationship tests are not part of that run: a deleted organization would fail them until the fixture is gone, which is the point of the delete.

`airflow/dags/saas_cdc_health.py` is the observer. Trigger it after the connector is up. With only `make up`, the task logs that `CDC_CONNECT_URL` is unset and succeeds.

`python -m streaming.lag` prints, per table, the row count, `max(_source_ts)`, and `max(_loaded_at)`, and the difference when both exist. If `KAFKA_BOOTSTRAP_SERVERS` is set and `kafka-python` is installed, it also prints committed offset, end offset, and lag. Copy those figures into `docs/cost.md` when you want a record. The repo does not ship a target lag.

## Failure demos for the stream

Each target uses a temporary DuckDB file. Exit 0 means the check behaved. `LIVE=1` also requires a reachable broker and does not replace the local proof.

1. `make cdc-replay` applies two organization changes, applies them again, and compares row count and `(org_id, dedup_token)`. They match. On a live stack, pause the consumer, note `make cdc-lag`, start a second pass from the earliest offset (or delete `warehouse/cdc_checkpoints` and let Spark restart), and compare the landing signature. The token is what makes the second pass a no-op.
2. `make cdc-schema-change` lands `billing_email` in `_extra` and shows the typed column is absent. It then writes a promoted-column file, alters the table, lands a second event, and backfills the first row from `_extra`. Staging still does not select `billing_email` until you add it to `stg_app__organizations`. On a live stack: `alter table app.organizations add column billing_email text`, update a row, and look at `_extra` in `raw.organizations_cdc`.
3. `make cdc-crash` writes the batch and does not commit offsets, then applies the same records again and commits. Inserted count on the second call is 0, skipped is 2, and the committed offset is the next offset to read. On a live stack: `docker compose -f docker-compose.yml -f docker-compose.cdc.yml kill spark-consumer` (or `python-consumer`), start it again, and compare `make cdc-lag` plus the landing signature with what you wrote down before the kill.
4. `make cdc-poison` puts a non-JSON payload between two valid events. The dead-letter table has the parse error. Both valid events are in `raw.organizations_cdc`. On a live stack, produce that payload to `saas.app.organizations` with `kafka-console-producer` and read `raw._cdc_dead_letter` and the `saas.dead_letter` topic.

`make consumer-test` runs the same four checks plus tombstone parsing under pytest.

## Things to try

1. Run `make break-it`, then query `app.subscriptions` and `raw.subscriptions` for the negative MRR. Confirm the mart build timestamp did not change. Run `make heal`.
2. Call `dbt build` once for the whole project, with the bad row loaded, and watch marts build anyway. That is why the shell splits the selection. Put the row back with `make heal` afterwards.
3. Change an invoice status in SQL without updating `recorded_at`, run ingest, and see that `raw.invoices` still has the old status. Then bump `recorded_at` and ingest again.
4. In `fct_logins.sql`, filter on `logged_in_at` instead of `_loaded_at`, full-refresh once, then insert a login with an old `logged_in_at` and a new `recorded_at`. The incremental run will skip it. Revert the model when you are done.
5. Switch the snapshot strategy to `timestamp` on `updated_at` and update a subscription's `updated_at` only. You get a new SCD2 version with the same plan and MRR. The check strategy does not.
6. Set the source freshness `error_after` to a few minutes, wait, and run `make dbt-build`. Freshness fails and the script never reaches the models. Put the threshold back.

## Interview questions

**Why is the app database not the warehouse?**
The app schema is shaped for one product transaction at a time. Analytics wants history, restated months, and a contract that still holds when the app adds a column. The raw layer is that contract. Marts are the only place a report should read.

**Why both a current subscription table and an event table?**
The current row answers "what is this org on today?" The events answer "how did MRR move?" The snapshot is a third view: the current row as it looked at each change of the commercial columns, including changes that never got an event. The failure demo is that case: MRR changes with no event, and the staging test is what stops it.

**Why is MRR ending balance a state, and the bridge components a flow?**
Summing event deltas tells you what happened in the month. Summing the last known MRR per org tells you what was still booked at month end. They should match. The singular test is the check that a classification bug (a reactivation counted as new, a sign error on churn) cannot ship quietly.

**Why can logo churn be null?**
Dividing by zero paying orgs at the start of the month is not a rate of zero. Zero would say "nobody churned" when there was nobody to churn. The bounds test allows null only in that case, and requires the rate to sit between 0 and 1 otherwise.

**Why incremental on `_loaded_at`?**
Event time and arrival time differ. Late data is normal (the generator writes it on purpose). An incremental model that predicates on event time will miss a row whose event time is already behind the high-water mark. Load time moves whenever the row arrives. The daily aggregate is rebuilt so the late row still changes the old day's count.

**Why is the MRR mart not incremental?**
A late event restates an old month and every identity check that depends on that month's beginning balance. Rebuilding a small monthly table is the straightforward way to get that restatement. Incremental would be worth it if the month grain were large and you had an explicit restatement window.

**Why a check snapshot instead of `updated_at`?**
`updated_at` is not a business change. Any write bumps it. Check columns list the attributes whose history you would show a finance reader. Sourcing the snapshot from staging means a lowercase fix is not a version.

**Why doesn't one `dbt build` protect the marts?**
dbt tests are nodes that run after their parent. Selecting the whole graph still builds children. The protection is an orchestration boundary: upstream build and tests, then a second task that only runs after success. Airflow's default trigger rule is that boundary. Retrying a failed data test hides nothing and spends a retry, so those tasks use zero retries.

**Why a two-day lookback if the load is idempotent?**
Clock skew and a row that is inserted with a `recorded_at` slightly behind the watermark you already stored. The lookback re-reads that window. Idempotency (delete plus insert on the primary key) is what makes the re-read safe. Lookback without idempotency duplicates. Idempotency without lookback misses a late commit that used an old timestamp.

**What does least privilege mean for these three roles?**
The loader can break raw and cannot publish a mart. The transformer can read raw and write the dbt schemas. The reporter can read marts and cannot see raw PII-shaped tables or half-built intermediates. `CREATE SCHEMA` on the transformer is the exception, and it exists because dbt issues that statement. The loader does not get it, so a bad ingest job cannot invent a new schema.

**Why auto-suspend, and why one warehouse?**
A suspended warehouse does not bill. The warehouse starts suspended and suspends after 60 seconds idle, which is a configuration in Terraform, not a credit forecast. One warehouse is enough for a single trial user running load and dbt. A second warehouse would matter if reporters ran heavy queries while transforms were running. Record what you actually spend in `docs/cost.md`.

**How do the batch snapshot and the CDC log meet?**
Batch ingest still writes `raw.<table>`. The consumer appends to `raw.<table>_cdc`. `current_entity` keeps the latest CDC row per key, then picks whichever of that row and the batch snapshot has the later timestamp. A delete wins only when it is the later one. Marts, the MRR bridge, and the publish gate still read staging.

## Interview questions, continued

**Why CDC instead of polling every minute?**
A poll sees the table as it is at one instant. A row that is inserted and deleted between polls never existed, as far as the warehouse is concerned. The log has both events, in order, and it has the LSN to deduplicate a replay. Polling is still the batch job: it is the backfill when the slot is dropped or the stream has been down.

**Why Debezium rather than a hand-written logical decoder?**
The hard part of CDC is the WAL protocol, slot management, snapshot coordination, and the envelope. Debezium already does that for Postgres and emits a stable JSON shape. The code in this repo is the part a warehouse team owns: landing, dedup, deletes, schema evolution, and the dbt merge. `pgoutput` avoids installing a decoding plugin into the image.

**Is this exactly-once?**
No. Kafka delivery here is at-least-once. The sink is idempotent on primary key plus LSN and offset, and the offset is committed only after the sink commit. Together that is effectively-once for this landing table: a replay does not create a second current row, and a crash does not drop a row that was not acknowledged. Exactly-once would need one transaction that commits the warehouse write and the Kafka offset together. DuckDB and Kafka do not offer that. Spark's checkpoint is the same pattern, not a distributed transaction.

**Why is the dedup token more than the primary key?**
The primary key is the entity. The LSN and offset identify one delivery of one change. Collapsing to the primary key inside the consumer would throw away history and make a replay indistinguishable from a new update. Staging does the collapse to one current row. The landing table keeps every change.

**How can a delete and a batch snapshot both be true?**
They are true at different times. The snapshot says what the batch load saw at `_loaded_at`. The delete says what committed at `_source_ts`. The later timestamp wins. If you re-seed the app, the new snapshot is only newer if its `_loaded_at` is after the CDC commit. Historical seed data often loses that comparison, so the operational step is to truncate the CDC tables when you throw the app database away.

**What does a tombstone do that `op=d` does not?**
`op=d` carries the before image and the LSN. The tombstone is a null value Kafka uses so log compaction can drop the key. The consumer records it as a delete keyed by topic, partition, and offset. If the delete event is also in the log, staging's offset order keeps the later of the two, which is still a delete. Ignoring tombstones would also be safe when `op=d` is present. Recording them means a compacted topic that only has the tombstone still deletes the key.

**What breaks when Postgres adds a column?**
The connector keeps producing. The consumer does not fail the batch. The new field sits in `_extra` until the contract and the staging select name it. Marts stay stable. The thing that breaks is the assumption that every source field is already a column. The fix is an `ALTER`, a backfill from `_extra` for rows already landed, and a one-line addition to the staging model.

**Why doesn't Airflow run the consumer?**
A task has a start and an end. The consumer is supposed to stay up. Putting it in a task either blocks the worker forever or stops the stream when the task times out. Airflow's job is the batch gate, plus a health check that fails when the connector is not `RUNNING`. Lag is printed so you can decide what "too far behind" means after you have measured it. There is no invented threshold in the DAG.

**A test warns on null emails. Why not fail the build?**
Some source dirt is real and should stay visible. Failing the build on it teaches people to delete the test. Warning severity surfaces the rows in the run results and still lets the marts publish. Negative revenue is different: publishing it would put a wrong total in the MRR mart, so that test is an error and sits in front of the publish task.
