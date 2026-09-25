"""Topic names shared by the connector config and both consumers."""

from __future__ import annotations

from ingest.contract import TABLES

from streaming.messages import DEAD_LETTER_TOPIC, TOPIC_PREFIX

APP_TOPICS = tuple(f"{TOPIC_PREFIX}{table}" for table in TABLES)


def dead_letter_producer(servers: str):
    try:
        from kafka import KafkaProducer
    except ImportError:
        print(
            f"kafka-python is not installed; dead letters stay in the warehouse "
            f"and were not copied to {DEAD_LETTER_TOPIC}."
        )
        return None
    return KafkaProducer(bootstrap_servers=servers)
