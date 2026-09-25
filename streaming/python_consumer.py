"""Lightweight Kafka consumer. Same landing rules as the Spark job.

Offsets are committed only after apply_records returns. Auto-commit stays off.
"""

from __future__ import annotations

import os

from streaming.apply import apply_records
from streaming.messages import CONSUMER_GROUP, DeadLetter, RawMessage, parse_message
from streaming.topics import APP_TOPICS, dead_letter_producer


def main() -> None:
    try:
        from kafka import KafkaConsumer
    except ImportError as exc:
        raise SystemExit(
            "kafka-python is not installed. pip install -r requirements-streaming.txt"
        ) from exc

    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    consumer = KafkaConsumer(
        *APP_TOPICS,
        bootstrap_servers=servers,
        group_id=CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=None,
        value_deserializer=_decode,
        key_deserializer=_decode,
    )
    print(f"python consumer reading {', '.join(APP_TOPICS)} from {servers}")
    while True:
        polled = consumer.poll(timeout_ms=1000, max_records=200)
        records: list[RawMessage] = []
        for _tp, messages in polled.items():
            for message in messages:
                records.append(
                    RawMessage(
                        topic=message.topic,
                        partition=message.partition,
                        offset=message.offset,
                        key=message.key,
                        value=message.value,
                    )
                )
        if not records:
            continue
        records.sort(key=lambda record: (record.topic, record.partition, record.offset))
        stats = apply_records(records, commit_offsets=True)
        _publish_dead_letters(records, servers)
        consumer.commit()
        print(f"landed {stats}")


def _publish_dead_letters(records: list[RawMessage], servers: str) -> None:
    letters = [
        parsed
        for record in records
        if isinstance(parsed := parse_message(record), DeadLetter)
    ]
    if not letters:
        return
    producer = dead_letter_producer(servers)
    if producer is None:
        return
    for letter in letters:
        producer.send(
            "saas.dead_letter",
            key=letter.dead_letter_id.encode(),
            value=letter.payload.encode(),
        )
    producer.flush()
    producer.close()


def _decode(raw: bytes | None) -> str | None:
    if raw is None:
        return None
    return raw.decode("utf-8")


if __name__ == "__main__":
    main()
