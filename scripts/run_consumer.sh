#!/usr/bin/env bash
# Wait until the Debezium connector is RUNNING, then exec the consumer.
# Airflow does not run this. The CDC Compose services do.
set -euo pipefail

if [[ -n "${CDC_CONNECT_URL:-}" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    PY=python3
  else
    PY=python
  fi
  until "${PY}" -m streaming.health; do
    echo "waiting for the Debezium connector at ${CDC_CONNECT_URL}"
    sleep 5
  done
fi

exec "$@"
