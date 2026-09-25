"""Register the Postgres connector with Kafka Connect. Safe to re-run."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request


def connector_config() -> dict[str, str]:
    tables = ",".join(
        [
            "app.organizations",
            "app.users",
            "app.logins",
            "app.subscriptions",
            "app.subscription_events",
            "app.invoices",
        ]
    )
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.environ.get("CDC_POSTGRES_HOST", "postgres"),
        "database.port": os.environ.get("CDC_POSTGRES_PORT", "5432"),
        "database.user": os.environ.get("CDC_REPLICATION_USER", "replicator"),
        "database.password": os.environ.get("CDC_REPLICATION_PASSWORD", "replicator"),
        "database.dbname": os.environ.get("CDC_POSTGRES_DB", "app"),
        "topic.prefix": "saas",
        "plugin.name": "pgoutput",
        "publication.name": "app_publication",
        "publication.autocreate.mode": "disabled",
        "slot.name": "saas_app_slot",
        "schema.include.list": "app",
        "table.include.list": tables,
        "tombstones.on.delete": "true",
        "decimal.handling.mode": "string",
        "time.precision.mode": "connect",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        "snapshot.mode": "initial",
        "heartbeat.interval.ms": "10000",
        "provide.transaction.metadata": "false",
    }


def main() -> None:
    base = os.environ.get("CDC_CONNECT_URL", "http://localhost:8083").rstrip("/")
    url = f"{base}/connectors/saas-app/config"
    payload = json.dumps(connector_config()).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=payload,
        method="PUT",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
    except urllib.error.URLError as exc:
        raise SystemExit(
            f"Kafka Connect is not reachable at {base}. Start the cdc profile first. {exc}"
        ) from exc
    print(body)
    print("Registered connector saas-app.")


if __name__ == "__main__":
    main()
