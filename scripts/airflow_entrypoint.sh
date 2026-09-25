#!/usr/bin/env bash
# One container, LocalExecutor: scheduler in the background, webserver in the foreground.
set -euo pipefail

airflow db migrate

if airflow users list 2>/dev/null | grep -q "${AIRFLOW_ADMIN_USER:-admin}"; then
  echo "Airflow admin user already exists."
else
  airflow users create \
    --username "${AIRFLOW_ADMIN_USER:-admin}" \
    --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
    --firstname Saurav \
    --lastname Anand \
    --role Admin \
    --email "${AIRFLOW_ADMIN_EMAIL:-saurav@example.com}"
fi

airflow scheduler &
scheduler_pid=$!
airflow webserver --port 8080 &
webserver_pid=$!

trap 'kill "$scheduler_pid" "$webserver_pid" 2>/dev/null || true' EXIT
wait -n "$scheduler_pid" "$webserver_pid"
exit $?
