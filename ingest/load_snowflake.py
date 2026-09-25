"""Load a batch into Snowflake.

The RAW schema is created by Terraform. This loader does not create schemas,
so the loader role does not need CREATE SCHEMA on the database.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

from ingest.contract import TABLES, create_table_sql, warehouse_columns


def _connect():
    try:
        import snowflake.connector
    except ImportError as exc:
        raise SystemExit(
            "snowflake-connector-python is not installed. "
            "Install requirements.txt before using --target snowflake."
        ) from exc

    required = (
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
    )
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise SystemExit(
            "Missing Snowflake environment variables: " + ", ".join(missing) + ". See .env.example."
        )

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_LOADER_ROLE", "LOADER"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "ANALYTICS"),
        schema="RAW",
    )


def read_watermarks() -> dict[str, datetime | None]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select count(*)
            from information_schema.tables
            where table_schema = 'RAW' and table_name = '_INGEST_STATE'
            """
        )
        if cur.fetchone()[0] == 0:
            return {}
        cur.execute("select table_name, watermark from raw._ingest_state")
        rows = cur.fetchall()
    finally:
        conn.close()
    return {str(name).lower(): watermark for name, watermark in rows}


def load(batches: dict[str, list[tuple]], *, full_refresh: bool) -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        _require_raw_schema(cur)
        cur.execute(
            """
            create table if not exists raw._ingest_state (
                table_name varchar primary key,
                watermark timestamp,
                rows_loaded bigint,
                load_mode varchar,
                updated_at timestamp
            )
            """
        )
        for table, rows in batches.items():
            _load_one(cur, table, rows, full_refresh=full_refresh)
        conn.commit()
    finally:
        conn.close()
    print("loaded snowflake raw schema")


def _require_raw_schema(cur) -> None:
    cur.execute(
        """
        select count(*)
        from information_schema.schemata
        where schema_name = 'RAW'
        """
    )
    if cur.fetchone()[0] == 0:
        raise SystemExit(
            "Snowflake schema RAW does not exist. Apply infra/snowflake first. "
            "The loader role is not granted CREATE SCHEMA."
        )


def _placeholders(count: int) -> str:
    return ", ".join(["%s"] * count)


def _load_one(cur, table: str, rows: list[tuple], *, full_refresh: bool) -> None:
    spec = TABLES[table]
    cur.execute(create_table_sql(table))
    columns = warehouse_columns(table)
    column_list = ", ".join(name for name, _ in columns)
    placeholder_sql = _placeholders(len(columns))
    replace_all = full_refresh or spec["mode"] == "full"

    if replace_all:
        cur.execute(f"delete from raw.{table}")
        if rows:
            cur.executemany(
                f"insert into raw.{table} ({column_list}) values ({placeholder_sql})",
                rows,
            )
    elif rows:
        cur.execute(
            f"create or replace temporary table _incoming as select * from raw.{table} where 1 = 0"
        )
        cur.executemany(
            f"insert into _incoming ({column_list}) values ({placeholder_sql})",
            rows,
        )
        cur.execute(
            f"delete from raw.{table} where {spec['pk']} in (select {spec['pk']} from _incoming)"
        )
        cur.execute(
            f"insert into raw.{table} ({column_list}) select {column_list} from _incoming"
        )

    cur.execute(f"select max({spec['watermark_column']}) from raw.{table}")
    watermark = cur.fetchone()[0]
    cur.execute(f"select count(*) from raw.{table}")
    loaded = cur.fetchone()[0]
    updated_at = datetime.now(UTC).replace(tzinfo=None)
    mode = "full" if replace_all else "upsert"
    cur.execute("delete from raw._ingest_state where table_name = %s", (table,))
    cur.execute(
        """
        insert into raw._ingest_state (table_name, watermark, rows_loaded, load_mode, updated_at)
        values (%s, %s, %s, %s, %s)
        """,
        (table, watermark, loaded, mode, updated_at),
    )
    print(f"  raw.{table}: incoming={len(rows)} rows_now={loaded}")
