"""Print consumer lag and warehouse delay. Numbers come from this run only.

Warehouse delay is max(_loaded_at) minus max(_source_ts) per CDC table, when
both values exist. It is not a benchmark. Copy what you see into docs/cost.md
if you want a record of it.
"""

from __future__ import annotations

import os

from ingest.contract import TABLES
from ingest.load_duckdb import duckdb_path

from streaming.messages import CONSUMER_GROUP
from streaming.topics import APP_TOPICS


def main() -> None:
    _print_warehouse()
    _print_kafka()


def _print_warehouse() -> None:
    target = os.environ.get("WAREHOUSE_TARGET", "duckdb")
    if target != "duckdb":
        print("Warehouse lag SQL is printed for Snowflake; run it in a worksheet.")
        for table in TABLES:
            print(
                "select "
                f"'{table}', count(*), max(_source_ts), max(_loaded_at) "
                f"from raw.{table}_cdc;"
            )
        return
    import duckdb

    path = duckdb_path()
    if not path.exists():
        print(f"No DuckDB file at {path}. Nothing to measure yet.")
        return
    con = duckdb.connect(str(path), read_only=True)
    try:
        for table in TABLES:
            found = con.execute(
                """
                select count(*)
                from information_schema.tables
                where table_schema = 'raw' and table_name = ?
                """,
                [f"{table}_cdc"],
            ).fetchone()[0]
            if not found:
                print(f"{table}_cdc: table is absent")
                continue
            count, source_ts, loaded_at = con.execute(
                f"select count(*), max(_source_ts), max(_loaded_at) from raw.{table}_cdc"
            ).fetchone()
            print(
                f"{table}_cdc rows={count} max_source_ts={source_ts} max_loaded_at={loaded_at}"
            )
            if source_ts is not None and loaded_at is not None:
                print(f"  loaded_minus_source={loaded_at - source_ts}")
    finally:
        con.close()


def _print_kafka() -> None:
    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
    if not servers:
        print("KAFKA_BOOTSTRAP_SERVERS is unset. Broker lag was not queried.")
        return
    try:
        from kafka import KafkaConsumer, TopicPartition
        from kafka.admin import KafkaAdminClient
    except ImportError:
        print("kafka-python is not installed. Broker lag was not queried.")
        return
    admin = KafkaAdminClient(bootstrap_servers=servers)
    try:
        listed = admin.list_consumer_group_offsets(CONSUMER_GROUP)
    except Exception as exc:
        print(f"consumer group {CONSUMER_GROUP} has no readable offsets: {exc}")
        admin.close()
        return
    if not listed:
        print(f"consumer group {CONSUMER_GROUP} has no committed offsets yet.")
        admin.close()
        return
    consumer = KafkaConsumer(bootstrap_servers=servers, enable_auto_commit=False)
    try:
        partitions = [
            TopicPartition(topic_partition.topic, topic_partition.partition)
            for topic_partition in listed
        ]
        ends = consumer.end_offsets(partitions)
        for topic_partition, meta in listed.items():
            end = ends.get(TopicPartition(topic_partition.topic, topic_partition.partition), 0)
            committed = meta.offset
            lag = end - committed
            print(
                f"lag topic={topic_partition.topic} partition={topic_partition.partition} "
                f"committed={committed} end={end} lag={lag}"
            )
    finally:
        consumer.close()
        admin.close()
    print(f"subscribed topics={','.join(APP_TOPICS)}")


if __name__ == "__main__":
    main()
