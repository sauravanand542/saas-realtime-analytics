"""Parse a polled batch and land it. Offset commit is the caller's next step."""

from __future__ import annotations

from uuid import uuid4

from streaming.messages import CONSUMER_GROUP, Change, DeadLetter, RawMessage, parse_message
from streaming.store import write_batch


def apply_records(
    records: list[RawMessage],
    *,
    commit_offsets: bool,
    consumer_group: str = CONSUMER_GROUP,
    batch_id: str | None = None,
) -> dict[str, int]:
    changes: list[Change] = []
    dead_letters: list[DeadLetter] = []
    for record in records:
        parsed = parse_message(record)
        if isinstance(parsed, DeadLetter):
            dead_letters.append(parsed)
        else:
            changes.append(parsed)
    if not changes and not dead_letters:
        return {"inserted": 0, "skipped": 0, "dead_letters": 0}
    stats = write_batch(
        changes,
        dead_letters,
        records,
        batch_id=batch_id or uuid4().hex,
        commit_offsets=commit_offsets,
        consumer_group=consumer_group,
    )
    stats["parsed_changes"] = len(changes)
    stats["parsed_dead_letters"] = len(dead_letters)
    return stats
