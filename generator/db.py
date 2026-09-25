"""Postgres access for the simulated application."""

from __future__ import annotations

import os

import psycopg
from psycopg.rows import dict_row

DEFAULT_URL = "postgresql://app:app@localhost:5432/app"

TRUNCATE_SQL = """
truncate table
    app.logins,
    app.invoices,
    app.subscription_events,
    app.subscriptions,
    app.users,
    app.organizations
restart identity cascade
"""


def database_url() -> str:
    return os.environ.get("DATABASE_URL", DEFAULT_URL)


def connect() -> psycopg.Connection:
    try:
        return psycopg.connect(database_url())
    except psycopg.OperationalError as exc:
        message = (
            "Could not connect to Postgres. Start it with `make up` "
            f"and check DATABASE_URL. Underlying error: {exc}"
        )
        raise SystemExit(message) from exc


def truncate_app(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(TRUNCATE_SQL)


def executemany(conn: psycopg.Connection, sql: str, rows: list[dict]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(sql, rows)


def fetch_all(conn: psycopg.Connection, sql: str, params: tuple = ()) -> list[dict]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())
