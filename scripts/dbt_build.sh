#!/usr/bin/env bash
# Publish gate: staging and intermediate tests must pass before marts or snapshots build.
# dbt's own DAG does not treat a test as an upstream dependency of the next model,
# so the split is explicit. See docs/decisions.md.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STEP="${1:-all}"

if [[ -z "${DUCKDB_PATH:-}" ]]; then
  export DUCKDB_PATH="${ROOT}/warehouse/analytics.duckdb"
elif [[ "${DUCKDB_PATH}" != /* ]]; then
  export DUCKDB_PATH="${ROOT}/${DUCKDB_PATH}"
fi

export WAREHOUSE_TARGET="${WAREHOUSE_TARGET:-duckdb}"
DBT_BIN="${PIPELINE_DBT:-dbt}"
PY_BIN="${PIPELINE_PYTHON:-python3}"

# DuckDB allows one writer. Retry when the CDC consumer still has the file.
duckdb_lock_retry() {
  local attempts="${DUCKDB_LOCK_ATTEMPTS:-40}"
  local delay="${DUCKDB_LOCK_DELAY_SECONDS:-0.25}"
  local attempt=1
  local err
  err="$(mktemp)"
  while true; do
    if "$@" >"$err" 2>&1; then
      cat "$err"
      rm -f "$err"
      return 0
    fi
    if ! grep -Eqi "conflicting lock|could not set lock" "$err"; then
      cat "$err" >&2
      rm -f "$err"
      return 1
    fi
    if [ "$attempt" -ge "$attempts" ]; then
      echo "DuckDB file ${DUCKDB_PATH} stayed locked after ${attempts} attempts. Another writer still has it open, often the CDC consumer during a batch. Wait for that write to finish and run this again. Snowflake does not take this file lock." >&2
      rm -f "$err"
      return 1
    fi
    echo "DuckDB file is locked. Retrying in ${delay}s (${attempt}/${attempts})." >&2
    sleep "$delay"
    attempt=$((attempt + 1))
  done
}

# Empty CDC tables let staging compile before the stream has produced a row.
"${PY_BIN}" -m streaming.ensure_tables

cd "${ROOT}/transform"

run_freshness() {
  duckdb_lock_retry "${DBT_BIN}" source freshness --target "${WAREHOUSE_TARGET}"
}

run_upstream() {
  duckdb_lock_retry "${DBT_BIN}" build --target "${WAREHOUSE_TARGET}" --select staging intermediate
}

run_publish() {
  # `snapshots` is not a selector. path:snapshots is the snapshot directory.
  duckdb_lock_retry "${DBT_BIN}" build --target "${WAREHOUSE_TARGET}" --select marts path:snapshots
}

case "${STEP}" in
  freshness)
    run_freshness
    ;;
  upstream)
    run_upstream
    ;;
  publish)
    run_publish
    ;;
  all)
    run_freshness
    run_upstream
    run_publish
    ;;
  *)
    echo "Usage: scripts/dbt_build.sh [all|freshness|upstream|publish]" >&2
    exit 2
    ;;
esac
