"""Restore subscriptions whose MRR was forced negative, then rebuild."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from generator.config import PLAN_MRR  # noqa: E402
from generator.db import connect  # noqa: E402


def _duckdb_path() -> Path:
    raw = os.environ.get("DUCKDB_PATH", str(ROOT / "warehouse" / "analytics.duckdb"))
    path = Path(raw)
    if not path.is_absolute():
        path = ROOT / path
    return path


def _built_at(path: Path):
    import duckdb

    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute("select max(_dbt_built_at) from marts.fct_mrr_monthly").fetchone()[0]
    finally:
        con.close()


def _restore() -> int:
    conn = connect()
    restored = 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select subscription_id, plan
                from app.subscriptions
                where mrr_cents < 0
                order by subscription_id
                """
            )
            rows = cur.fetchall()
            for subscription_id, plan in rows:
                canonical = str(plan).lower()
                if canonical not in PLAN_MRR:
                    raise SystemExit(f"Cannot heal plan {plan!r}; it is not in the catalog.")
                cur.execute(
                    """
                    update app.subscriptions
                    set mrr_cents = %s, updated_at = now()
                    where subscription_id = %s
                    """,
                    (PLAN_MRR[canonical], subscription_id),
                )
                restored += 1
        conn.commit()
    finally:
        conn.close()
    return restored


def main() -> None:
    target = os.environ.get("WAREHOUSE_TARGET", "duckdb")
    before = _built_at(_duckdb_path()) if target == "duckdb" else None
    restored = _restore()
    if restored == 0:
        print("No negative MRR rows found. Nothing to heal.")
    else:
        print(f"Restored {restored} subscription(s) to the catalog MRR for their plan.")

    ingest = subprocess.run(
        [sys.executable, "-m", "ingest"],
        cwd=ROOT,
        check=False,
    )
    if ingest.returncode != 0:
        raise SystemExit(ingest.returncode)

    build = subprocess.run(["bash", "scripts/dbt_build.sh"], cwd=ROOT, check=False)
    if build.returncode != 0:
        raise SystemExit(build.returncode)

    if target == "duckdb":
        after = _built_at(_duckdb_path())
        if after == before:
            raise SystemExit("dbt exited 0 but marts._dbt_built_at did not move.")
    print("Heal succeeded. Upstream tests passed and marts were rebuilt.")


if __name__ == "__main__":
    main()
