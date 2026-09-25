"""Optional Kafka-backed checks. Used when a demo is run with --live.

These talk to a broker started by the CDC Compose profile. They are not part
of the default demo exit path.
"""

from __future__ import annotations

import os

from streaming.messages import CONSUMER_GROUP


def run_live(demo: str) -> None:
    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    try:
        from kafka import KafkaConsumer
        from kafka.errors import NoBrokersAvailable
    except ImportError as exc:
        raise SystemExit(
            "Live mode needs kafka-python. pip install -r requirements-streaming.txt"
        ) from exc
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=servers,
            group_id=f"{CONSUMER_GROUP}-live-check",
            enable_auto_commit=False,
            consumer_timeout_ms=3000,
        )
        consumer.topics()
        consumer.close()
    except (NoBrokersAvailable, OSError) as exc:
        raise SystemExit(
            f"Kafka is not reachable at {servers}. Start it with `make cdc-up` "
            f"and set KAFKA_BOOTSTRAP_SERVERS=localhost:9094. ({exc})"
        ) from exc
    print(f"live_{demo}_broker_reachable group={CONSUMER_GROUP}")
    print(
        "On the running consumer, compare landing signatures with "
        "`python -m streaming.lag` before and after a restart or a second pass. "
        "The warehouse dedup token is what keeps a replay from adding rows."
    )
