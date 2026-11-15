"""Idempotent topic creation via the Kafka Admin API."""

from __future__ import annotations

import logging

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from streaming.config import TOPIC_SPECS, TopicSpec, broker

logger = logging.getLogger(__name__)


def create_topics(
    bootstrap: str | None = None, specs: list[TopicSpec] = TOPIC_SPECS
) -> dict[str, str]:
    """Create topics if absent; existing topics are reported, not errors (idempotent)."""
    admin = AdminClient({"bootstrap.servers": bootstrap or broker()})
    new = [
        NewTopic(s.name, num_partitions=s.partitions, replication_factor=1, config=s.config)
        for s in specs
    ]
    results: dict[str, str] = {}
    for name, fut in admin.create_topics(new).items():
        try:
            fut.result()
            results[name] = "created"
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                results[name] = "exists"
            else:
                raise
    return results


def delete_topics(names: list[str], bootstrap: str | None = None) -> dict[str, str]:
    """Delete topics; absent topics are reported, not errors (used to clean up throwaway topics)."""
    admin = AdminClient({"bootstrap.servers": bootstrap or broker()})
    results: dict[str, str] = {}
    for name, fut in admin.delete_topics(list(names)).items():
        try:
            fut.result()
            results[name] = "deleted"
        except KafkaException as e:
            if e.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                results[name] = "absent"
            else:
                raise
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for topic, status in create_topics().items():
        logger.info("topic %s: %s", topic, status)
