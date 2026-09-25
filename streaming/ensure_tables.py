"""Create empty raw landing tables so dbt can build before any CDC event arrives."""

from __future__ import annotations

import os

from streaming.store import _alter_promoted, ensure_statements


def main() -> None:
    target = os.environ.get("WAREHOUSE_TARGET", "duckdb")
    if target == "snowflake":
        from ingest.load_snowflake import _connect

        conn = _connect()
        try:
            cur = conn.cursor()
            for statement in ensure_statements(create_schema=False):
                cur.execute(statement)
            _alter_promoted(cur)
            conn.commit()
        finally:
            conn.close()
        print("ensured snowflake raw and cdc tables")
        return

    import duckdb
    from ingest.load_duckdb import duckdb_path

    path = duckdb_path()
    con = duckdb.connect(str(path))
    try:
        for statement in ensure_statements(create_schema=True):
            con.execute(statement)
        _alter_promoted(con)
    finally:
        con.close()
    print(f"ensured duckdb raw and cdc tables in {path}")


if __name__ == "__main__":
    main()
