"""Spark Structured Streaming consumer. Default path when the CDC profile is up.

foreachBatch writes the warehouse first. Spark records the checkpoint, which
is the offset commit, only after that function returns. A failed batch is retried
from the last checkpoint, and the landing dedup token makes the retry a no-op.
"""

from __future__ import annotations

import os
from pathlib import Path

from streaming.apply import apply_records
from streaming.messages import TOPIC_PREFIX, DeadLetter, RawMessage, parse_message
from streaming.topics import APP_TOPICS, dead_letter_producer


def main() -> None:
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise SystemExit(
            "pyspark is not installed. Use the spark-consumer container, "
            "or run python -m streaming.python_consumer."
        ) from exc

    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    duckdb_file = Path(os.environ.get("DUCKDB_PATH", "warehouse/analytics.duckdb"))
    checkpoint = os.environ.get("CDC_CHECKPOINT", str(duckdb_file.parent / "cdc_checkpoints"))
    spark = (
        SparkSession.builder.appName("saas-cdc")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("subscribe", ",".join(APP_TOPICS))
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    def handle(batch_df, epoch_id: int) -> None:
        if batch_df.isEmpty():
            return
        records: list[RawMessage] = []
        for row in batch_df.select("topic", "partition", "offset", "key", "value").collect():
            records.append(
                RawMessage(
                    topic=row.topic,
                    partition=int(row.partition),
                    offset=int(row.offset),
                    key=_text(row.key),
                    value=_text(row.value),
                )
            )
        records.sort(key=lambda record: (record.topic, record.partition, record.offset))
        stats = apply_records(records, commit_offsets=True, batch_id=f"spark-{epoch_id}")
        _publish_dead_letters(records, servers)
        print(f"epoch {epoch_id} landed {stats}")

    query = (
        stream.writeStream.foreachBatch(handle)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="10 seconds")
        .start()
    )
    print(f"spark consumer reading {TOPIC_PREFIX}* from {servers}")
    query.awaitTermination()


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


def _text(raw: object) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, bytes):
        return raw.decode("utf-8")
    return str(raw)


if __name__ == "__main__":
    main()
