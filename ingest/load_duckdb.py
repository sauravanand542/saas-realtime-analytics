"""Load a batch into a local DuckDB file."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from ingest.contract import TABLES, create_table_sql, warehouse_columns


def duckdb_path() -> Path:
    raw = os.environ.get("DUCKDB_PATH", "warehouse/analytics.duckdb")
    path = Path(raw)
    if not path.is_absolute():
        path = Path.cwd() / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def read_watermarks() -> dict[str, datetime | None]:
    path = duckdb_path()
    if not path.exists():
        return {}
    con = duckdb.connect(str(path), read_only=True)
    try:
        found = con.execute(
            """
            select count(*)
            from information_schema.tables
            where table_schema = 'raw' and table_name = '_ingest_state'
            """
        ).fetchone()[0]
        if not found:
            return {}
        rows = con.execute("select table_name, watermark from raw._ingest_state").fetchall()
    finally:
        con.close()
    return {name: watermark for name, watermark in rows}


def load(batches: dict[str, list[tuple]], *, full_refresh: bool) -> None:
    path = duckdb_path()
    con = duckdb.connect(str(path))
    try:
        con.execute("create schema if not exists raw")
        con.execute(
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
        con.execute("begin transaction")
        try:
            for table, rows in batches.items():
                _load_one(con, table, rows, full_refresh=full_refresh)
            con.execute("commit")
        except Exception:
            con.execute("rollback")
            raise
    finally:
        con.close()
    print(f"loaded duckdb {path}")


def _placeholders(count: int) -> str:
    return ", ".join(["?"] * count)


def _load_one(
    con: duckdb.DuckDBPyConnection,
    table: str,
    rows: list[tuple],
    *,
    full_refresh: bool,
) -> None:
    spec = TABLES[table]
    con.execute(create_table_sql(table))
    columns = warehouse_columns(table)
    column_list = ", ".join(name for name, _ in columns)
    placeholder_sql = _placeholders(len(columns))
    replace_all = full_refresh or spec["mode"] == "full"

    if replace_all:
        con.execute(f"delete from raw.{table}")
        if rows:
            con.executemany(
                f"insert into raw.{table} ({column_list}) values ({placeholder_sql})",
                rows,
            )
    elif rows:
        con.execute("drop table if exists _incoming")
        con.execute(f"create temp table _incoming as select * from raw.{table} where false")
        con.executemany(
            f"insert into _incoming ({column_list}) values ({placeholder_sql})",
            rows,
        )
        con.execute(
            f"delete from raw.{table} where {spec['pk']} in (select {spec['pk']} from _incoming)"
        )
        con.execute(
            f"insert into raw.{table} ({column_list}) select {column_list} from _incoming"
        )
        con.execute("drop table _incoming")

    watermark = con.execute(
        f"select max({spec['watermark_column']}) from raw.{table}"
    ).fetchone()[0]
    loaded = con.execute(f"select count(*) from raw.{table}").fetchone()[0]
    updated_at = datetime.now(UTC).replace(tzinfo=None)
    con.execute("delete from raw._ingest_state where table_name = ?", [table])
    con.execute(
        """
        insert into raw._ingest_state (table_name, watermark, rows_loaded, load_mode, updated_at)
        values (?, ?, ?, ?, ?)
        """,
        [table, watermark, loaded, "full" if replace_all else "upsert", updated_at],
    )
    print(f"  raw.{table}: incoming={len(rows)} rows_now={loaded}")
