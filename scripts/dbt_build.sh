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

# Empty CDC tables let staging compile before the stream has produced a row.
"${PY_BIN}" -m streaming.ensure_tables

cd "${ROOT}/transform"

run_freshness() {
  "${DBT_BIN}" source freshness --target "${WAREHOUSE_TARGET}"
}

run_upstream() {
  "${DBT_BIN}" build --target "${WAREHOUSE_TARGET}" --select staging intermediate
}

run_publish() {
  # `snapshots` is not a selector. path:snapshots is the snapshot directory.
  "${DBT_BIN}" build --target "${WAREHOUSE_TARGET}" --select marts path:snapshots
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
