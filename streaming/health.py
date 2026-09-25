"""Kafka Connect status check. This does not start or run the stream."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request


def main() -> None:
    base = os.environ.get("CDC_CONNECT_URL", "").rstrip("/")
    if not base:
        print("CDC_CONNECT_URL is unset. The CDC profile is not part of this process. Skipping.")
        return
    url = f"{base}/connectors/saas-app/status"
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise SystemExit(
                "Connector saas-app is not registered. Run: python -m streaming.register_connector"
            ) from exc
        raise SystemExit(f"Connect status request failed: HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise SystemExit(f"Connect is not reachable at {base}: {exc.reason}") from exc

    connector = body.get("connector", {})
    tasks = body.get("tasks", [])
    state = connector.get("state")
    task_states = [task.get("state") for task in tasks]
    print(f"connector={state} tasks={task_states}")
    if state != "RUNNING" or any(task_state != "RUNNING" for task_state in task_states):
        raise SystemExit("Debezium connector is not running.")
    if not tasks:
        raise SystemExit("Debezium connector has no tasks.")


if __name__ == "__main__":
    main()
