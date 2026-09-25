"""Read the app database for one ingest batch."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from generator.config import LOOKBACK_DAYS
from generator.db import connect, fetch_all

from ingest.contract import SOURCE_SYSTEM, TABLES


def normalize(value):
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(UTC).replace(tzinfo=None)
        return value
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return int(value)
    return value


def extract_table(
    table: str,
    *,
    batch_id: str,
    loaded_at: datetime,
    watermark: datetime | None,
    full_refresh: bool,
) -> list[tuple]:
    spec = TABLES[table]
    column_names = [name for name, _ in spec["columns"]]
    sql = f"select {', '.join(column_names)} from app.{table}"
    params: tuple = ()
    use_watermark = spec["mode"] == "upsert" and watermark is not None and not full_refresh
    if use_watermark:
        start = watermark - timedelta(days=LOOKBACK_DAYS)
        sql += f" where {spec['watermark_column']} >= %s"
        params = (start,)
    sql += f" order by {spec['pk']}"

    conn = connect()
    try:
        rows = fetch_all(conn, sql, params)
    finally:
        conn.close()

    extracted = []
    for row in rows:
        values = [normalize(row[name]) for name in column_names]
        values.extend([loaded_at, batch_id, SOURCE_SYSTEM])
        extracted.append(tuple(values))
    return extracted
