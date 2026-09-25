"""Introduce one bad subscription and prove marts are not republished.

The script leaves the bad row in Postgres. Run `make heal` afterwards.
Exit 0 means the demo behaved: dbt failed, and the mart tables did not change.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from generator.db import connect  # noqa: E402


def _duckdb_path() -> Path:
    raw = os.environ.get("DUCKDB_PATH", str(ROOT / "warehouse" / "analytics.duckdb"))
    path = Path(raw)
    if not path.is_absolute():
        path = ROOT / path
    return path


def _signature(path: Path) -> tuple:
    import duckdb

    if not path.exists():
        raise SystemExit(
            "No DuckDB warehouse yet. Run `make seed`, `make ingest`, and `make dbt-build` first."
        )
    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute(
            """
            select
                (select max(_dbt_built_at) from marts.fct_mrr_monthly),
                (select count(*) from marts.fct_mrr_monthly),
                (select max(_dbt_built_at) from marts.dim_organizations),
                (select count(*) from marts.dim_organizations)
            """
        ).fetchone()
    except duckdb.CatalogException as exc:
        raise SystemExit(
            "Marts are not built yet. Run `make dbt-build` before `make break-it`."
        ) from exc
    finally:
        con.close()


def _corrupt() -> str:
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select subscription_id
                from app.subscriptions
                where status = 'active' and mrr_cents > 0
                order by subscription_id
                limit 1
                """
            )
            row = cur.fetchone()
            if row is None:
                raise SystemExit("No active paid subscription to corrupt. Run `make seed` first.")
            subscription_id = row[0]
            cur.execute(
                """
                update app.subscriptions
                set mrr_cents = -500, updated_at = now()
                where subscription_id = %s
                """,
                (subscription_id,),
            )
        conn.commit()
    finally:
        conn.close()
    return str(subscription_id)


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def main() -> None:
    target = os.environ.get("WAREHOUSE_TARGET", "duckdb")
    before = _signature(_duckdb_path()) if target == "duckdb" else None
    subscription_id = _corrupt()
    print(f"Corrupted subscription {subscription_id}: set mrr_cents = -500 in Postgres.")

    ingest = _run([sys.executable, "-m", "ingest"])
    print(ingest.stdout)
    if ingest.stderr:
        print(ingest.stderr, file=sys.stderr)
    if ingest.returncode != 0:
        raise SystemExit(f"Ingest failed with exit code {ingest.returncode}.")

    build = _run(["bash", "scripts/dbt_build.sh"])
    combined = (build.stdout or "") + (build.stderr or "")
    print(combined)
    after = _signature(_duckdb_path()) if target == "duckdb" else None

    failed = build.returncode != 0
    mentioned = "non_negative" in combined
    unchanged = before == after if target == "duckdb" else True
    if failed and mentioned and unchanged:
        print(
            "Demo passed. The non_negative test failed, so marts and snapshots "
            "were not rebuilt. The bad MRR is still in Postgres. Run `make heal`."
        )
        return
    print("Demo did not behave as expected.", file=sys.stderr)
    print(f"dbt_exit={build.returncode} mentioned_test={mentioned} marts_unchanged={unchanged}")
    raise SystemExit(1)


if __name__ == "__main__":
    main()
