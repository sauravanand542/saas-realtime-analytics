"""A second process must wait out a DuckDB writer, then proceed."""

from __future__ import annotations

import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path

import pytest
from ingest.load_duckdb import load
from streaming.lag import main as lag_main

HOLD = textwrap.dedent(
    """
    import os, sys, time
    import duckdb
    path, ready, release = sys.argv[1:]
    con = duckdb.connect(path)
    con.execute("select 1")
    open(ready, "w").close()
    while not os.path.exists(release):
        time.sleep(0.05)
        con.execute("select 1")
    con.close()
    """
)


def _hold_writer(path: Path, ready: Path, release: Path) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-c", HOLD, str(path), str(ready), str(release)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )


def _wait_ready(proc: subprocess.Popen[bytes], ready: Path) -> None:
    deadline = time.time() + 5
    while not ready.exists():
        if proc.poll() is not None:
            err = proc.stderr.read().decode() if proc.stderr else ""
            raise AssertionError(f"writer exited early: {err}")
        if time.time() > deadline:
            proc.kill()
            raise AssertionError("writer did not open the file")
        time.sleep(0.05)


def _release(proc: subprocess.Popen[bytes], release: Path) -> None:
    release.write_text("go")
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def test_lag_and_ingest_wait_until_the_writer_closes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "analytics.duckdb"
    ready = tmp_path / "ready"
    release = tmp_path / "release"
    monkeypatch.setenv("DUCKDB_PATH", str(path))
    monkeypatch.setenv("WAREHOUSE_TARGET", "duckdb")
    monkeypatch.setenv("DUCKDB_LOCK_ATTEMPTS", "40")
    monkeypatch.setenv("DUCKDB_LOCK_DELAY_SECONDS", "0.1")
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)

    errors: list[BaseException] = []

    def run_lag() -> None:
        try:
            lag_main()
        except SystemExit as exc:
            if exc.code not in (0, None):
                errors.append(exc)
        except Exception as exc:
            errors.append(exc)

    def run_ingest() -> None:
        try:
            load({}, full_refresh=True)
        except Exception as exc:
            errors.append(exc)

    for target in (run_lag, run_ingest):
        if release.exists():
            release.unlink()
        proc = _hold_writer(path, ready, release)
        try:
            _wait_ready(proc, ready)
            worker = threading.Thread(target=target)
            worker.start()
            time.sleep(0.35)
            assert worker.is_alive(), "gave up on the lock instead of waiting"
            _release(proc, release)
            worker.join(timeout=5)
            assert not worker.is_alive()
            assert errors == []
        finally:
            if proc.poll() is None:
                _release(proc, release)
        ready.unlink()


def test_lag_prints_broker_lag_when_the_warehouse_stays_locked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "analytics.duckdb"
    ready = tmp_path / "ready"
    release = tmp_path / "release"
    monkeypatch.setenv("DUCKDB_PATH", str(path))
    monkeypatch.setenv("WAREHOUSE_TARGET", "duckdb")
    monkeypatch.setenv("DUCKDB_LOCK_ATTEMPTS", "2")
    monkeypatch.setenv("DUCKDB_LOCK_DELAY_SECONDS", "0.05")
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)

    proc = _hold_writer(path, ready, release)
    try:
        _wait_ready(proc, ready)
        with pytest.raises(SystemExit) as raised:
            lag_main()
        assert raised.value.code == 1
        captured = capsys.readouterr().out
        assert "stayed locked" in captured
        assert "KAFKA_BOOTSTRAP_SERVERS is unset" in captured
    finally:
        _release(proc, release)
