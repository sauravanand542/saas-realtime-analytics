"""Prove staging keeps one current row when CDC and batch disagree.

Runs against the DuckDB file from `make ingest`. Inserts a fixture, builds
stg_app__organizations, checks the result, then removes the fixture and rebuilds.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("WAREHOUSE_TARGET", "duckdb")
if not os.environ.get("DUCKDB_PATH"):
    os.environ["DUCKDB_PATH"] = str(ROOT / "warehouse" / "analytics.duckdb")

from streaming.apply import apply_records  # noqa: E402
from streaming.demos import _change  # noqa: E402


def main() -> None:
    import duckdb
    from ingest.load_duckdb import duckdb_path

    path = duckdb_path()
    if not path.exists():
        raise SystemExit("No DuckDB warehouse. Run `make seed` and `make ingest` first.")
    con = duckdb.connect(str(path), read_only=True)
    try:
        org_id, name = con.execute(
            "select org_id, name from raw.organizations order by org_id limit 1"
        ).fetchone()
    finally:
        con.close()

    future = 4_000_000_000_000
    delete_row = _change(
        "organizations",
        "d",
        {
            "org_id": org_id,
            "name": name,
            "updated_at": "2099-01-01T00:00:00",
        },
        lsn=9_000_000_000,
        offset=1,
        ts_ms=future,
    )
    create_old = _change(
        "organizations",
        "c",
        {"org_id": "org-cdc-only", "name": "Old Name", "status": "active"},
        lsn=9_000_000_001,
        offset=2,
        ts_ms=future + 1,
    )
    create_new = _change(
        "organizations",
        "u",
        {"org_id": "org-cdc-only", "name": "New Name", "status": "active"},
        lsn=9_000_000_002,
        offset=3,
        ts_ms=future + 2,
    )
    try:
        apply_records(
            [delete_row, create_old, create_new],
            commit_offsets=False,
            batch_id="overlap-check",
        )
        _dbt_run_organizations()
        con = duckdb.connect(str(path), read_only=True)
        try:
            gone = con.execute(
                "select count(*) from staging.stg_app__organizations where org_id = ?",
                [org_id],
            ).fetchone()[0]
            current = con.execute(
                """
                select org_name, count(*) over ()
                from staging.stg_app__organizations
                where org_id = 'org-cdc-only'
                """
            ).fetchall()
            dupes = con.execute(
                """
                select org_id
                from staging.stg_app__organizations
                group by org_id
                having count(*) > 1
                """
            ).fetchall()
        finally:
            con.close()
        if gone != 0:
            raise SystemExit(f"deleted org {org_id} is still in staging")
        if current != [("New Name", 1)]:
            raise SystemExit(f"expected one current CDC row named New Name, got {current}")
        if dupes:
            raise SystemExit(f"staging has duplicate org keys: {dupes}")
        print(f"overlap_ok deleted={org_id} cdc_only_rows=1 name=New Name")
    finally:
        con = duckdb.connect(str(path))
        try:
            con.execute("delete from raw.organizations_cdc where _batch_id = 'overlap-check'")
        finally:
            con.close()
        _dbt_run_organizations()


def _dbt_run_organizations() -> None:
    """Rebuild the view only. Relationship tests still expect the deleted org."""
    env = os.environ.copy()
    dbt_bin = env.get("PIPELINE_DBT", "dbt")
    result = subprocess.run(
        [
            dbt_bin,
            "run",
            "--select",
            "stg_app__organizations",
            "--target",
            env.get("WAREHOUSE_TARGET", "duckdb"),
        ],
        cwd=ROOT / "transform",
        env=env,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(f"dbt run for organizations failed with {result.returncode}")


if __name__ == "__main__":
    sys.exit(main())
