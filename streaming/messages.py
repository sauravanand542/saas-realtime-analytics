"""Debezium envelope parsing. No Kafka client and no warehouse."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from ingest.contract import TABLES

from streaming.promoted import promoted_columns

TOPIC_PREFIX = "saas.app."
DEAD_LETTER_TOPIC = "saas.dead_letter"
CONSUMER_GROUP = "saas-cdc-consumer"
SOURCE_SYSTEM = "debezium"

OPS = {"c", "u", "d", "r"}


@dataclass(frozen=True)
class RawMessage:
    topic: str
    partition: int
    offset: int
    key: str | None
    value: str | None


@dataclass(frozen=True)
class Change:
    table: str
    pk_column: str
    pk_value: str
    op: str
    values: dict[str, object]
    extra: dict[str, object]
    source_lsn: int | None
    source_ts: datetime | None
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    deleted: bool
    dedup_token: str


@dataclass(frozen=True)
class DeadLetter:
    dead_letter_id: str
    topic: str
    partition: int
    offset: int
    error: str
    payload: str


def table_from_topic(topic: str) -> str | None:
    if not topic.startswith(TOPIC_PREFIX):
        return None
    name = topic[len(TOPIC_PREFIX) :]
    if name in TABLES:
        return name
    return None


def parse_message(message: RawMessage) -> Change | DeadLetter:
    table = table_from_topic(message.topic)
    if table is None:
        return _dead(message, f"topic {message.topic} is not an app change feed")

    if message.value is None:
        return _tombstone(message, table)

    try:
        body = json.loads(message.value)
    except json.JSONDecodeError:
        return _dead(message, "payload is not json")
    if not isinstance(body, dict):
        return _dead(message, "payload is not a json object")

    op = body.get("op")
    if op not in OPS:
        return _dead(message, "missing or unknown op")

    source = body.get("source") if isinstance(body.get("source"), dict) else None
    if source is None:
        return _dead(message, "source metadata is missing")

    image = body.get("before") if op == "d" else body.get("after")
    if not isinstance(image, dict):
        return _dead(message, "change image is missing")

    try:
        source_lsn = _optional_int(source.get("lsn"))
        source_ts = _optional_timestamp(source.get("ts_ms"))
        values, extra = _project(table, image)
    except (TypeError, ValueError) as exc:
        return _dead(message, str(exc))

    pk_column = TABLES[table]["pk"]
    pk_value = values.get(pk_column)
    if pk_value is None:
        pk_value = _pk_from_key(message.key, pk_column)
        if pk_value is not None:
            values[pk_column] = pk_value
    if pk_value is None:
        return _dead(message, f"missing primary key {pk_column}")

    return Change(
        table=table,
        pk_column=pk_column,
        pk_value=str(pk_value),
        op=op,
        values=values,
        extra=extra,
        source_lsn=source_lsn,
        source_ts=source_ts,
        kafka_topic=message.topic,
        kafka_partition=message.partition,
        kafka_offset=message.offset,
        deleted=op == "d",
        dedup_token=_dedup_token(source_lsn, message),
    )


def _tombstone(message: RawMessage, table: str) -> Change | DeadLetter:
    pk_column = TABLES[table]["pk"]
    pk_value = _pk_from_key(message.key, pk_column)
    if pk_value is None:
        return _dead(message, f"tombstone key has no {pk_column}")
    values = {name: None for name, _typ in TABLES[table]["columns"]}
    values[pk_column] = pk_value
    return Change(
        table=table,
        pk_column=pk_column,
        pk_value=pk_value,
        op="d",
        values=values,
        extra={},
        source_lsn=None,
        source_ts=None,
        kafka_topic=message.topic,
        kafka_partition=message.partition,
        kafka_offset=message.offset,
        deleted=True,
        dedup_token=f"kafka:{message.topic}:{message.partition}:{message.offset}",
    )


def _dedup_token(source_lsn: int | None, message: RawMessage) -> str:
    # Same delivery replays at the same offset. LSN keeps two changes in one
    # transaction distinct when their offsets differ. Tombstones have no LSN.
    if source_lsn is not None:
        return f"lsn:{source_lsn}:offset:{message.offset}"
    return f"kafka:{message.topic}:{message.partition}:{message.offset}"


def _project(table: str, image: dict) -> tuple[dict[str, object], dict[str, object]]:
    known = dict(TABLES[table]["columns"])
    known.update(promoted_columns(table))
    values: dict[str, object] = {name: None for name in known}
    extra: dict[str, object] = {}
    for key, raw in image.items():
        if key in known:
            values[key] = _coerce(raw, known[key], key)
        else:
            extra[key] = raw
    return values, extra


def _coerce(value: object, typ: str, column: str) -> object:
    if value is None:
        return None
    try:
        if typ == "varchar":
            return str(value)
        if typ == "bigint":
            return int(value)
        if typ == "boolean":
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in {"true", "t", "1"}
            return bool(value)
        if typ == "timestamp":
            return _as_timestamp(value)
        if typ == "date":
            return _as_date(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"column {column} is not a {typ}") from exc
    raise ValueError(f"column {column} has an unknown type {typ}")


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


def _optional_timestamp(value: object) -> datetime | None:
    if value is None:
        return None
    return _as_timestamp(value)


def _as_timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(UTC).replace(tzinfo=None)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        seconds = value / 1000 if abs(value) > 10_000_000_000 else value
        return datetime.fromtimestamp(seconds, UTC).replace(tzinfo=None)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed
        return parsed.astimezone(UTC).replace(tzinfo=None)
    raise ValueError(f"cannot parse timestamp {value!r}")


def _as_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return date(1970, 1, 1) + timedelta(days=value)
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    raise ValueError(f"cannot parse date {value!r}")


def _pk_from_key(key: str | None, pk_column: str) -> str | None:
    if key is None or key == "":
        return None
    try:
        parsed = json.loads(key)
    except json.JSONDecodeError:
        return key
    if isinstance(parsed, dict):
        value = parsed.get(pk_column)
        if value is None:
            return None
        return str(value)
    if isinstance(parsed, str):
        return parsed
    return None


def _dead(message: RawMessage, error: str) -> DeadLetter:
    payload = message.value if message.value is not None else (message.key or "")
    return DeadLetter(
        dead_letter_id=f"{message.topic}:{message.partition}:{message.offset}",
        topic=message.topic,
        partition=message.partition,
        offset=message.offset,
        error=error,
        payload=payload[:8000],
    )
