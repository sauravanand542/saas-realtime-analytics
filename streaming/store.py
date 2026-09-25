"""Idempotent landing of parsed changes into the raw CDC tables.

The write transaction commits before the caller commits Kafka offsets.
A crash in between redelivers the batch; the primary key plus dedup token
inserts nothing the second time.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any

from ingest.contract import TABLES, create_table_sql
from ingest.load_duckdb import duckdb_path

from streaming.messages import CONSUMER_GROUP, SOURCE_SYSTEM, Change, DeadLetter, RawMessage
from streaming.promoted import promoted_columns

CDC_TAIL = [
    ("_op", "varchar"),
    ("_source_lsn", "bigint"),
    ("_kafka_topic", "varchar"),
    ("_kafka_partition", "bigint"),
    ("_kafka_offset", "bigint"),
    ("_source_ts", "timestamp"),
    ("_deleted", "boolean"),
    ("_dedup_token", "varchar"),
    ("_extra", "varchar"),
    ("_loaded_at", "timestamp"),
    ("_batch_id", "varchar"),
    ("_source_system", "varchar"),
]


def cdc_columns(table: str) -> list[tuple[str, str]]:
    return list(TABLES[table]["columns"]) + promoted_columns(table) + list(CDC_TAIL)


def cdc_table_sql(table: str) -> str:
    spec = TABLES[table]
    columns = ",\n        ".join(f"{name} {typ}" for name, typ in cdc_columns(table))
    return (
        f"create table if not exists raw.{table}_cdc (\n"
        f"        {columns},\n"
        f"        primary key ({spec['pk']}, _dedup_token)\n"
        f")"
    )


def ensure_statements(*, create_schema: bool) -> list[str]:
    statements = []
    if create_schema:
        # DuckDB has no Terraform-managed schema. Snowflake RAW already exists,
        # and the loader role is not allowed to create schemas.
        statements.append("create schema if not exists raw")
    statements.extend(
        [
        """
        create table if not exists raw._cdc_dead_letter (
            dead_letter_id varchar primary key,
            topic varchar,
            partition_id bigint,
            offset_id bigint,
            error varchar,
            payload varchar,
            _loaded_at timestamp,
            _source_system varchar
        )
        """,
        """
        create table if not exists raw._cdc_offsets (
            consumer_group varchar,
            topic varchar,
            partition_id bigint,
            committed_offset bigint,
            updated_at timestamp,
            primary key (consumer_group, topic, partition_id)
        )
        """,
        ]
    )
    for table in TABLES:
        statements.append(create_table_sql(table))
        statements.append(cdc_table_sql(table))
    return statements


def _alter_promoted(con: Any) -> None:
    for table in TABLES:
        for name, typ in promoted_columns(table):
            con.execute(f"alter table raw.{table}_cdc add column if not exists {name} {typ}")


def write_batch(
    changes: list[Change],
    dead_letters: list[DeadLetter],
    records: list[RawMessage],
    *,
    batch_id: str,
    commit_offsets: bool,
    consumer_group: str = CONSUMER_GROUP,
) -> dict[str, int]:
    target = os.environ.get("WAREHOUSE_TARGET", "duckdb")
    if target == "snowflake":
        return _write_snowflake(
            changes,
            dead_letters,
            records,
            batch_id=batch_id,
            commit_offsets=commit_offsets,
            consumer_group=consumer_group,
        )
    return _write_duckdb(
        changes,
        dead_letters,
        records,
        batch_id=batch_id,
        commit_offsets=commit_offsets,
        consumer_group=consumer_group,
    )


def landing_signature(table: str) -> tuple[int, list[tuple[str, str]]]:
    """Row count and (primary key, dedup token) pairs for replay checks."""
    pk = TABLES[table]["pk"]
    path = duckdb_path()
    import duckdb

    con = duckdb.connect(str(path), read_only=True)
    try:
        count = con.execute(f"select count(*) from raw.{table}_cdc").fetchone()[0]
        rows = con.execute(
            f"select {pk}, _dedup_token from raw.{table}_cdc order by 1, 2"
        ).fetchall()
    finally:
        con.close()
    return int(count), [(str(pk_value), token) for pk_value, token in rows]


def _write_duckdb(
    changes: list[Change],
    dead_letters: list[DeadLetter],
    records: list[RawMessage],
    *,
    batch_id: str,
    commit_offsets: bool,
    consumer_group: str,
) -> dict[str, int]:
    import duckdb

    loaded_at = datetime.now(UTC).replace(tzinfo=None)
    con = duckdb.connect(str(duckdb_path()))
    try:
        for statement in ensure_statements(create_schema=True):
            con.execute(statement)
        _alter_promoted(con)
        con.execute("begin transaction")
        try:
            inserted, skipped = _insert_changes(con, changes, batch_id, loaded_at, "?")
            dead_inserted = _insert_dead(con, dead_letters, loaded_at, "?")
            if commit_offsets:
                _save_offsets(con, records, consumer_group, loaded_at, "?")
            con.execute("commit")
        except Exception:
            con.execute("rollback")
            raise
    finally:
        con.close()
    return {
        "inserted": inserted,
        "skipped": skipped,
        "dead_letters": dead_inserted,
    }


def _insert_changes(con: Any, changes: list[Change], batch_id: str, loaded_at: datetime, mark: str):
    inserted = 0
    skipped = 0
    for change in changes:
        if change.table not in TABLES:
            raise ValueError(f"refusing to write unknown table {change.table}")
        columns = [name for name, _typ in cdc_columns(change.table)]
        pk = change.pk_column
        exists = con.execute(
            f"select count(*) from raw.{change.table}_cdc "
            f"where {pk} = {mark} and _dedup_token = {mark}",
            [change.pk_value, change.dedup_token],
        ).fetchone()[0]
        if exists:
            skipped += 1
            continue
        payload = _change_values(change, columns, batch_id, loaded_at)
        placeholders = ", ".join([mark] * len(columns))
        column_list = ", ".join(columns)
        con.execute(
            f"insert into raw.{change.table}_cdc ({column_list}) values ({placeholders})",
            payload,
        )
        inserted += 1
    return inserted, skipped


def _change_values(change: Change, columns: list[str], batch_id: str, loaded_at: datetime) -> list:
    tail = {
        "_op": change.op,
        "_source_lsn": change.source_lsn,
        "_kafka_topic": change.kafka_topic,
        "_kafka_partition": change.kafka_partition,
        "_kafka_offset": change.kafka_offset,
        "_source_ts": change.source_ts,
        "_deleted": change.deleted,
        "_dedup_token": change.dedup_token,
        "_extra": json.dumps(change.extra, default=str) if change.extra else None,
        "_loaded_at": loaded_at,
        "_batch_id": batch_id,
        "_source_system": SOURCE_SYSTEM,
    }
    values: list[object] = []
    for column in columns:
        if column in tail:
            values.append(tail[column])
        else:
            values.append(change.values.get(column))
    return values


def _insert_dead(con: Any, dead_letters: list[DeadLetter], loaded_at: datetime, mark: str) -> int:
    inserted = 0
    for letter in dead_letters:
        exists = con.execute(
            f"select count(*) from raw._cdc_dead_letter where dead_letter_id = {mark}",
            [letter.dead_letter_id],
        ).fetchone()[0]
        if exists:
            continue
        con.execute(
            f"""
            insert into raw._cdc_dead_letter (
                dead_letter_id, topic, partition_id, offset_id, error, payload,
                _loaded_at, _source_system
            ) values ({mark}, {mark}, {mark}, {mark}, {mark}, {mark}, {mark}, {mark})
            """,
            [
                letter.dead_letter_id,
                letter.topic,
                letter.partition,
                letter.offset,
                letter.error,
                letter.payload,
                loaded_at,
                SOURCE_SYSTEM,
            ],
        )
        inserted += 1
    return inserted


def _save_offsets(
    con: Any,
    records: list[RawMessage],
    consumer_group: str,
    loaded_at: datetime,
    mark: str,
) -> None:
    highest: dict[tuple[str, int], int] = {}
    for record in records:
        key = (record.topic, record.partition)
        # Kafka commits the next offset to read.
        nxt = record.offset + 1
        highest[key] = max(highest.get(key, 0), nxt)
    for (topic, partition), committed in highest.items():
        con.execute(
            f"delete from raw._cdc_offsets where consumer_group = {mark} "
            f"and topic = {mark} and partition_id = {mark}",
            [consumer_group, topic, partition],
        )
        con.execute(
            f"""
            insert into raw._cdc_offsets (
                consumer_group, topic, partition_id, committed_offset, updated_at
            ) values ({mark}, {mark}, {mark}, {mark}, {mark})
            """,
            [consumer_group, topic, partition, committed, loaded_at],
        )


def _write_snowflake(
    changes: list[Change],
    dead_letters: list[DeadLetter],
    records: list[RawMessage],
    *,
    batch_id: str,
    commit_offsets: bool,
    consumer_group: str,
) -> dict[str, int]:
    from ingest.load_snowflake import _connect

    loaded_at = datetime.now(UTC).replace(tzinfo=None)
    conn = _connect()
    try:
        cur = conn.cursor()
        for statement in ensure_statements(create_schema=False):
            cur.execute(statement)
        _alter_promoted(cur)
        inserted, skipped = _insert_changes(cur, changes, batch_id, loaded_at, "%s")
        dead_inserted = _insert_dead(cur, dead_letters, loaded_at, "%s")
        if commit_offsets:
            _save_offsets(cur, records, consumer_group, loaded_at, "%s")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {
        "inserted": inserted,
        "skipped": skipped,
        "dead_letters": dead_inserted,
    }
