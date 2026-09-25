#!/bin/bash
# Creates the Airflow metadata database in the same Postgres instance as the app.
# A shell script is required because CREATE DATABASE cannot run inside the
# transaction the image wraps around *.sql files.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-'EOSQL'
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') THEN
            CREATE ROLE airflow LOGIN PASSWORD 'airflow';
        END IF;
    END
    $$;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-'EOSQL'
    SELECT 'CREATE DATABASE airflow OWNER airflow'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
EOSQL
