import json

from streaming.messages import RawMessage, parse_message


def test_tombstone_is_a_delete_with_kafka_token() -> None:
    parsed = parse_message(
        RawMessage(
            topic="saas.app.organizations",
            partition=0,
            offset=7,
            key=json.dumps({"org_id": "org-9"}),
            value=None,
        )
    )
    assert parsed.deleted is True
    assert parsed.pk_value == "org-9"
    assert parsed.dedup_token == "kafka:saas.app.organizations:0:7"
    assert parsed.source_lsn is None


def test_unknown_topic_is_a_dead_letter() -> None:
    parsed = parse_message(
        RawMessage(topic="saas.app.not_a_table", partition=0, offset=1, key=None, value="{}")
    )
    assert parsed.error.startswith("topic")


def test_same_lsn_at_a_new_offset_is_a_different_delivery() -> None:
    def message(offset: int) -> RawMessage:
        body = {
            "op": "u",
            "after": {"org_id": "org-1", "name": "A"},
            "source": {"lsn": 50, "ts_ms": 1_700_000_000_000},
        }
        return RawMessage(
            topic="saas.app.organizations",
            partition=0,
            offset=offset,
            key='{"org_id":"org-1"}',
            value=json.dumps(body),
        )

    first = parse_message(message(1))
    second = parse_message(message(2))
    assert first.dedup_token != second.dedup_token
    assert first.dedup_token == "lsn:50:offset:1"
