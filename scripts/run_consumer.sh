#!/usr/bin/env bash
# Wait until the Debezium connector is RUNNING, then exec the consumer.
# Airflow does not run this. The CDC Compose services do.
set -euo pipefail

# Java and Python resolve the home directory from passwd. HOME alone is not
# enough: a missing entry makes Java's user.home "?".
if ! getent passwd "$(id -u)" >/dev/null 2>&1; then
  if [[ -w /etc/passwd ]]; then
    echo "$(id -u):x:$(id -u):$(id -g):consumer:/tmp:/bin/false" >> /etc/passwd
  fi
fi
export HOME="${HOME:-/tmp}"

if [[ -n "${CDC_CONNECT_URL:-}" ]]; then
  if [[ -n "${PYSPARK_PYTHON:-}" ]]; then
    PY="${PYSPARK_PYTHON}"
  elif command -v python3 >/dev/null 2>&1; then
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
