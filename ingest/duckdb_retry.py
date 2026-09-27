"""Open a DuckDB file, retrying when another process holds the write lock.

DuckDB allows one writer on the warehouse file. The CDC consumer holds that
lock only while a micro-batch is landing. Callers wait briefly, then raise
DuckDBLockTimeout with a short message instead of the driver's traceback.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb


class DuckDBLockTimeout(RuntimeError):
    """The warehouse file stayed locked past the retry budget."""


def lock_settings() -> tuple[int, float]:
    attempts = int(os.environ.get("DUCKDB_LOCK_ATTEMPTS", "40"))
    delay = float(os.environ.get("DUCKDB_LOCK_DELAY_SECONDS", "0.25"))
    if attempts < 1:
        raise ValueError("DUCKDB_LOCK_ATTEMPTS must be at least 1")
    if delay < 0:
        raise ValueError("DUCKDB_LOCK_DELAY_SECONDS must be zero or more")
    return attempts, delay


def is_lock_conflict(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "conflicting lock" in text or "could not set lock" in text


def lock_timeout_message(path: Path, attempts: int) -> str:
    return (
        f"DuckDB file {path} stayed locked after {attempts} attempts. "
        "Another writer still has it open, often the CDC consumer during a batch. "
        "Wait for that write to finish and run this again. "
        "Snowflake does not take this file lock."
    )


def connect_duckdb(path: Path, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    attempts, delay = lock_settings()
    for attempt in range(1, attempts + 1):
        try:
            return duckdb.connect(str(path), read_only=read_only)
        except Exception as exc:
            if not is_lock_conflict(exc):
                raise
            if attempt == attempts:
                raise DuckDBLockTimeout(lock_timeout_message(path, attempts)) from None
            time.sleep(delay)
    raise DuckDBLockTimeout(lock_timeout_message(path, attempts))


@contextmanager
def duckdb_session(path: Path, *, read_only: bool = False) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open, yield, and close. The file lock does not outlive this block."""
    connection = connect_duckdb(path, read_only=read_only)
    try:
        yield connection
    finally:
        connection.close()
