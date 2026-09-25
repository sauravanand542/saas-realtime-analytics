"""Failure demos for the CDC landing rules.

Each subcommand runs against a temporary DuckDB file, so it does not touch
warehouse/analytics.duckdb. Pass --live to also drive Kafka when the CDC
profile is up. Live mode is a separate check; it is not required for the
exit code of the default run.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path

from ingest.contract import TABLES

from streaming.apply import apply_records
from streaming.messages import RawMessage
from streaming.store import landing_signature


def main() -> None:
    parser = argparse.ArgumentParser(description="CDC failure demos")
    parser.add_argument("demo", choices=["replay", "schema", "crash", "poison"])
    parser.add_argument(
        "--live",
        action="store_true",
        help="After the local proof, run the same check against Kafka if it is reachable.",
    )
    args = parser.parse_args()
    os.environ["WAREHOUSE_TARGET"] = "duckdb"
    runners = {
        "replay": demo_replay,
        "schema": demo_schema,
        "crash": demo_crash,
        "poison": demo_poison,
    }
    runners[args.demo]()
    if args.live:
        from streaming.live import run_live

        run_live(args.demo)


def demo_replay() -> None:
    with _sandbox():
        records = [
            _org_change("c", "Northwind", lsn=10, offset=0, ts_ms=1_700_000_000_000),
            _org_change("u", "Northwind Inc", lsn=11, offset=1, ts_ms=1_700_000_100_000),
        ]
        apply_records(records, commit_offsets=True, batch_id="seed")
        before = landing_signature("organizations")
        apply_records(records, commit_offsets=True, batch_id="replay")
        after = landing_signature("organizations")
        if before != after:
            raise SystemExit(f"replay changed the landing table: {before} -> {after}")
        print(f"replay_ok rows={before[0]} distinct_keys_unchanged")


def demo_schema() -> None:
    with _sandbox() as root:
        first = _change(
            "organizations",
            "c",
            _org("org-1", "Northwind", billing_email="ap@northwind.example"),
            lsn=20,
            offset=0,
            ts_ms=1_700_000_200_000,
        )
        apply_records([first], commit_offsets=True, batch_id="extra")
        import duckdb
        from ingest.load_duckdb import duckdb_path

        con = duckdb.connect(str(duckdb_path()))
        try:
            extra = con.execute("select _extra from raw.organizations_cdc").fetchone()[0]
            columns = [row[0] for row in con.execute("describe raw.organizations_cdc").fetchall()]
        finally:
            con.close()
        if extra is None or "billing_email" not in extra:
            raise SystemExit(f"expected billing_email in _extra, got {extra}")
        if "billing_email" in columns:
            raise SystemExit("unexpected typed column before the contract change")
        print("schema_change_landed extra_holds_billing_email typed_column_absent")

        promoted = root / "promoted.json"
        promoted.write_text(json.dumps({"organizations": {"billing_email": "varchar"}}))
        os.environ["CDC_PROMOTED_COLUMNS"] = str(promoted)
        second = _change(
            "organizations",
            "u",
            _org("org-1", "Northwind", billing_email="finance@northwind.example"),
            lsn=21,
            offset=1,
            ts_ms=1_700_000_300_000,
        )
        apply_records([second], commit_offsets=True, batch_id="promoted")
        con = duckdb.connect(str(duckdb_path()))
        try:
            con.execute(
                """
                update raw.organizations_cdc
                set billing_email = json_extract_string(_extra, '$.billing_email')
                where billing_email is null
                    and _extra is not null
                """
            )
            rows = con.execute(
                """
                select _source_lsn, billing_email
                from raw.organizations_cdc
                order by _source_lsn
                """
            ).fetchall()
        finally:
            con.close()
        if rows != [(20, "ap@northwind.example"), (21, "finance@northwind.example")]:
            raise SystemExit(f"promotion did not project billing_email: {rows}")
        print("schema_change_fixed column_promoted_and_backfilled")
        print(
            "Staging still ignores billing_email until stg_app__organizations selects it. "
            "The stream did not stop."
        )


def demo_crash() -> None:
    with _sandbox():
        records = [
            _org_change("c", "Northwind", lsn=30, offset=0, ts_ms=1_700_000_400_000),
            _org_change("u", "Northwind Inc", lsn=31, offset=1, ts_ms=1_700_000_500_000),
        ]
        apply_records(records, commit_offsets=False, batch_id="crashed-after-write")
        after_crash = landing_signature("organizations")
        if _committed_offset("saas.app.organizations") is not None:
            raise SystemExit("offsets were committed before the consumer acknowledged the batch")
        restarted = apply_records(records, commit_offsets=True, batch_id="restart")
        after_restart = landing_signature("organizations")
        if after_crash != after_restart:
            raise SystemExit(f"restart duplicated rows: {after_crash} -> {after_restart}")
        if restarted["inserted"] != 0 or restarted["skipped"] != 2:
            raise SystemExit(f"restart should skip both events, got {restarted}")
        if _committed_offset("saas.app.organizations") != 2:
            raise SystemExit("restart did not commit the next offset")
        print("crash_ok no_loss_no_duplicates offsets_committed_after_write")


def demo_poison() -> None:
    with _sandbox():
        records = [
            _org_change("c", "Northwind", lsn=40, offset=0, ts_ms=1_700_000_600_000),
            RawMessage(
                topic="saas.app.organizations",
                partition=0,
                offset=1,
                key=None,
                value="not-json",
            ),
            _org_change("u", "Northwind Inc", lsn=41, offset=2, ts_ms=1_700_000_700_000),
        ]
        stats = apply_records(records, commit_offsets=True, batch_id="poison")
        if stats["parsed_dead_letters"] != 1 or stats["inserted"] != 2:
            raise SystemExit(f"poison batch was not isolated: {stats}")
        import duckdb
        from ingest.load_duckdb import duckdb_path

        con = duckdb.connect(str(duckdb_path()), read_only=True)
        try:
            error = con.execute("select error from raw._cdc_dead_letter").fetchone()[0]
            names = con.execute(
                "select count(*) from raw.organizations_cdc where name = 'Northwind Inc'"
            ).fetchone()[0]
        finally:
            con.close()
        if "json" not in error:
            raise SystemExit(f"dead letter error was {error}")
        if names != 1:
            raise SystemExit("valid event after the poison message did not land")
        print("poison_ok dead_lettered_and_stream_continued")


def _sandbox():
    tmp = tempfile.TemporaryDirectory()
    os.environ["DUCKDB_PATH"] = str(Path(tmp.name) / "analytics.duckdb")
    os.environ.pop("CDC_PROMOTED_COLUMNS", None)
    return _Sandbox(tmp)


class _Sandbox:
    def __init__(self, tmp: tempfile.TemporaryDirectory) -> None:
        self._tmp = tmp
        self.root = Path(tmp.name)

    def __enter__(self) -> Path:
        return self.root

    def __exit__(self, *exc: object) -> None:
        self._tmp.cleanup()


def _org_change(op: str, name: str, *, lsn: int, offset: int, ts_ms: int) -> RawMessage:
    return _change(
        "organizations",
        op,
        _org("org-1", name),
        lsn=lsn,
        offset=offset,
        ts_ms=ts_ms,
    )


def _org(org_id: str, name: str, billing_email: str | None = None) -> dict:
    row = {
        "org_id": org_id,
        "name": name,
        "domain": "example.com",
        "industry": "software",
        "employee_band": "11-50",
        "country_code": "US",
        "status": "active",
        "created_at": "2024-01-01T00:00:00",
        "updated_at": "2024-06-01T00:00:00",
    }
    if billing_email is not None:
        row["billing_email"] = billing_email
    return row


def _change(table: str, op: str, image: dict, *, lsn: int, offset: int, ts_ms: int) -> RawMessage:
    pk = TABLES[table]["pk"]
    body = {
        "op": op,
        "before": image if op == "d" else None,
        "after": None if op == "d" else image,
        "source": {"lsn": lsn, "ts_ms": ts_ms, "schema": "app", "table": table},
    }
    return RawMessage(
        topic=f"saas.app.{table}",
        partition=0,
        offset=offset,
        key=json.dumps({pk: image[pk]}),
        value=json.dumps(body),
    )


def _committed_offset(topic: str) -> int | None:
    import duckdb
    from ingest.load_duckdb import duckdb_path

    con = duckdb.connect(str(duckdb_path()), read_only=True)
    try:
        row = con.execute(
            "select committed_offset from raw._cdc_offsets where topic = ?",
            [topic],
        ).fetchone()
    finally:
        con.close()
    if row is None:
        return None
    return int(row[0])


if __name__ == "__main__":
    main()
