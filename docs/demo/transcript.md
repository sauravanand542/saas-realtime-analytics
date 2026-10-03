# Demo video transcript

Narration for `saas-realtime-analytics-demo.mp4` (just under 2 minutes). It opens with the problem, takes a short look at the dashboard (a recording of `dashboard/app.py` on the Snowflake marts), and then walks through an animated architecture diagram. Each stage of the diagram has a caption naming the problem it solves.

## Intro

SaaS analytics dashboard, and the pipeline behind it.

## The problem

SaaS revenue and churn numbers have to be right. Querying the production database is the quick fix, but it slows the product down for customers. And ad-hoc queries aren't tested, so one bad row can reach leadership. You also want results you can reproduce as the data grows.

## What the business gets (dashboard)

Here's what the business gets. One trusted number: September MRR, at seven thousand and eighteen dollars. The bridge shows what moved it, new and expansion against downgrades and churn. This one splits revenue by plan. So how do these numbers get here?

## How it works (architecture)

**Postgres.** It starts with Postgres, the app's system of record. Analytics never touches it, so the product stays fast.

**Batch ingest and CDC.** A batch job copies the data into Snowflake incrementally. It re-reads a short lookback window and upserts by key, so late rows land without duplicates. The design also has a change data capture path, with Debezium, Kafka and Spark. This run used batch.

**Snowflake raw layer.** Everything lands in a raw layer, untransformed, so I can reprocess it any time.

**dbt layers.** Then dbt takes over. Staging cleans the data, intermediate models classify revenue movements, and the marts define MRR and churn once, with tests.

**Quality gate.** Those tests are the quality gate. If one fails, nothing publishes. I tried this on Snowflake. A negative MRR got caught, and the marts kept the last good numbers.

**Airflow.** In the design, Airflow runs it every day, and skips publishing when a test fails.

**Terraform.** Terraform sets up the Snowflake warehouse, schemas and roles as code.

**DuckDB.** And for development, the same pipeline runs locally on DuckDB, for free.

**Dashboard.** Finally, the dashboard reads only the published, tested marts. And Snowflake can scale compute as the volume grows.

## Outro

Thanks for watching. The code's on GitHub.
