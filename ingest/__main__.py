"""Copy the app database into the warehouse raw layer.

Examples:
  python -m ingest
  python -m ingest --target duckdb
  python -m ingest --target snowflake
  python -m ingest --full-refresh
"""

from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime
from uuid import uuid4

from ingest.contract import TABLES
from ingest.extract import extract_table


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch-load the app database into raw")
    parser.add_argument(
        "--target",
        choices=["duckdb", "snowflake"],
        default=os.environ.get("WAREHOUSE_TARGET", "duckdb"),
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Ignore watermarks and reload every raw table.",
    )
    return parser.parse_args()


def _aware(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=UTC)


def main() -> None:
    args = _parse_args()
    if args.target == "snowflake":
        from ingest.load_snowflake import load, read_watermarks
    else:
        from ingest.load_duckdb import load, read_watermarks

    watermarks = {} if args.full_refresh else read_watermarks()
    batch_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    loaded_at = datetime.now(UTC).replace(tzinfo=None)
    batches = {}
    for table in TABLES:
        batches[table] = extract_table(
            table,
            batch_id=batch_id,
            loaded_at=loaded_at,
            watermark=_aware(watermarks.get(table)),
            full_refresh=args.full_refresh,
        )
        print(f"extracted app.{table}: {len(batches[table])} rows")
    load(batches, full_refresh=args.full_refresh)


if __name__ == "__main__":
    main()
