"""Streaming config: broker, topics, consumer groups. Env-overridable."""

from __future__ import annotations

import os
from dataclasses import dataclass


def broker() -> str:
    return os.environ.get("REDPANDA_BROKER", "localhost:9092")


READINGS_TOPIC = os.environ.get("READINGS_TOPIC", "sensor.readings")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "sensor.readings.dlq")

ARCHIVER_GROUP = "archiver"
STATS_GROUP = "stats"


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    config: dict[str, str]


# Retention is time-bounded (the Parquet lake is the system of record, not the log) with a
# per-partition size cap so disk stays bounded on a laptop. compression.type=producer keeps
# whatever lz4 the producer sends.
TOPIC_SPECS = [
    TopicSpec(
        READINGS_TOPIC,
        3,
        {
            "cleanup.policy": "delete",
            "retention.ms": "86400000",  # 24h
            "retention.bytes": "536870912",  # 512 MiB/partition (~1.5 GiB total)
            "compression.type": "producer",
        },
    ),
    TopicSpec(
        DLQ_TOPIC,
        1,
        {
            "cleanup.policy": "delete",
            "retention.ms": "604800000",  # 7d: rare, kept long enough to inspect
        },
    ),
]
