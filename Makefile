ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))

-include .env
export

ifeq ($(DATABASE_URL),)
  DATABASE_URL := postgresql://app:app@localhost:5432/app
  export DATABASE_URL
endif

ifeq ($(WAREHOUSE_TARGET),)
  WAREHOUSE_TARGET := duckdb
  export WAREHOUSE_TARGET
endif

ifeq ($(DUCKDB_PATH),)
  DUCKDB_PATH := $(ROOT)/warehouse/analytics.duckdb
  export DUCKDB_PATH
endif

.DEFAULT_GOAL := help

COMPOSE_CDC := docker compose -f docker-compose.yml -f docker-compose.cdc.yml

.PHONY: help up down logs seed tick ingest dbt-build dbt-docs test break-it heal lint tf-fmt tf-validate \
	cdc-up cdc-up-python cdc-down cdc-prepare cdc-register cdc-lag \
	cdc-replay cdc-schema-change cdc-crash cdc-poison consumer-test

help:
	@echo "up          Start Postgres and Airflow"
	@echo "down        Stop the stack"
	@echo "logs        Follow compose logs"
	@echo "seed        Replace app data with a deterministic history (SEED, default 42)"
	@echo "tick        Apply one round of ongoing changes"
	@echo "ingest      Load Postgres into the warehouse raw layer"
	@echo "dbt-build   Freshness, upstream tests, then marts and snapshots"
	@echo "dbt-docs    Generate and serve dbt docs"
	@echo "test        Alias for dbt-build"
	@echo "break-it    Corrupt one subscription and show the publish gate"
	@echo "heal        Restore that subscription and rebuild"
	@echo "lint        ruff and sqlfluff"
	@echo "tf-fmt      terraform fmt"
	@echo "tf-validate terraform init and validate (no Snowflake credentials required)"
	@echo "cdc-up      Start Phase 1 plus Kafka, Debezium, and the Spark consumer"
	@echo "cdc-up-python  Same, with the Python consumer instead of Spark"
	@echo "cdc-down    Stop the CDC services and leave the Phase 1 containers up"
	@echo "cdc-prepare Create the replication role and publication"
	@echo "cdc-register  Register the Debezium connector"
	@echo "cdc-lag     Print broker lag and warehouse delay"
	@echo "cdc-replay  Show a second pass does not add landing rows"
	@echo "cdc-schema-change  Show an unknown column landing in _extra, then the fix"
	@echo "cdc-crash   Show a crash before offset commit does not duplicate"
	@echo "cdc-poison  Show a bad message on the dead-letter path"
	@echo "consumer-test  Unit tests for dedup, replay, crash, poison, schema"

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

seed:
	python -m generator --mode seed --seed $${SEED:-42}

tick:
	python -m generator --mode tick

ingest:
	python -m ingest --target $(WAREHOUSE_TARGET)

dbt-build:
	./scripts/dbt_build.sh

dbt-docs:
	cd transform && dbt docs generate --target $(WAREHOUSE_TARGET) && dbt docs serve --target $(WAREHOUSE_TARGET)

test: dbt-build

break-it:
	python -m scripts.break_it

heal:
	python -m scripts.heal_it

lint:
	ruff check generator ingest scripts airflow streaming
	sqlfluff lint transform/models transform/macros transform/snapshots transform/tests

cdc-up:
	$(COMPOSE_CDC) --profile cdc up -d --build

cdc-up-python:
	$(COMPOSE_CDC) --profile cdc-python up -d --build

cdc-down:
	$(COMPOSE_CDC) --profile cdc --profile cdc-python stop kafka connect spark-consumer python-consumer

cdc-prepare:
	psql "$(DATABASE_URL)" -v ON_ERROR_STOP=1 -f app_db/init/002_cdc.sql

cdc-register:
	CDC_CONNECT_URL=$${CDC_CONNECT_URL:-http://localhost:8083} python -m streaming.register_connector

cdc-lag:
	python -m streaming.lag

cdc-replay:
	python -m streaming.demos replay $(if $(LIVE),--live,)

cdc-schema-change:
	python -m streaming.demos schema $(if $(LIVE),--live,)

cdc-crash:
	python -m streaming.demos crash $(if $(LIVE),--live,)

cdc-poison:
	python -m streaming.demos poison $(if $(LIVE),--live,)

consumer-test:
	python -m pytest streaming/tests -q

tf-fmt:
	terraform -chdir=infra/snowflake fmt -recursive

tf-validate:
	terraform -chdir=infra/snowflake init -backend=false
	terraform -chdir=infra/snowflake validate
