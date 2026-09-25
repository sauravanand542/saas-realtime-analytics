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

.PHONY: help up down logs seed tick ingest dbt-build dbt-docs test break-it heal lint tf-fmt tf-validate

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
	ruff check generator ingest scripts airflow
	sqlfluff lint transform/models transform/macros transform/snapshots transform/tests

tf-fmt:
	terraform -chdir=infra/snowflake fmt -recursive

tf-validate:
	terraform -chdir=infra/snowflake init -backend=false
	terraform -chdir=infra/snowflake validate
