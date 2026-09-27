"""Host lag uses the Compose listener on localhost:9094."""

from __future__ import annotations

import pytest
from kafka import TopicPartition
from streaming.lag import main as lag_main


class _Offset:
    def __init__(self, offset: int) -> None:
        self.offset = offset


class _Admin:
    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers

    def list_consumer_group_offsets(self, group_id: str) -> dict:
        assert group_id == "saas-cdc-consumer"
        return {TopicPartition("saas.app.organizations", 0): _Offset(4)}

    def close(self) -> None:
        return None


class _Consumer:
    def __init__(self, bootstrap_servers: str, enable_auto_commit: bool = False) -> None:
        self.bootstrap_servers = bootstrap_servers
        assert enable_auto_commit is False

    def end_offsets(self, partitions: list) -> dict:
        return {partitions[0]: 9}

    def close(self) -> None:
        return None


def test_broker_lag_uses_localhost_9094(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    monkeypatch.setenv("WAREHOUSE_TARGET", "snowflake")
    created: list[tuple[str, str]] = []

    class Admin(_Admin):
        def __init__(self, bootstrap_servers: str) -> None:
            created.append(("admin", bootstrap_servers))
            super().__init__(bootstrap_servers)

    class Consumer(_Consumer):
        def __init__(self, bootstrap_servers: str, enable_auto_commit: bool = False) -> None:
            created.append(("consumer", bootstrap_servers))
            super().__init__(bootstrap_servers, enable_auto_commit)

    monkeypatch.setattr("kafka.admin.KafkaAdminClient", Admin)
    monkeypatch.setattr("kafka.KafkaConsumer", Consumer)

    lag_main()

    assert created == [("admin", "localhost:9094"), ("consumer", "localhost:9094")]
    captured = capsys.readouterr().out
    assert "committed=4 end=9 lag=5" in captured
    assert "topic=saas.app.organizations" in captured
