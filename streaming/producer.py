"""Thin confluent_kafka.Producer wrapper with delivery accounting."""

from __future__ import annotations

import logging

from confluent_kafka import Producer

from streaming.config import broker

logger = logging.getLogger(__name__)

Headers = list[tuple[str, bytes]]


class StreamProducer:
    def __init__(self, bootstrap: str | None = None, config: dict | None = None):
        conf = {
            "bootstrap.servers": bootstrap or broker(),
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 50,
            "batch.size": 1048576,
        }
        conf.update(config or {})
        self._producer = Producer(conf)
        self.delivered = 0
        self.failed = 0

    def _on_delivery(self, err: object, msg: object) -> None:
        if err is not None:
            self.failed += 1
            logger.error("delivery failed (topic=%s): %s", msg.topic() if msg else "?", err)
        else:
            self.delivered += 1

    def produce(
        self, topic: str, value: bytes, key: str | None = None, headers: Headers | None = None
    ) -> None:
        while True:
            try:
                self._producer.produce(
                    topic, value=value, key=key, headers=headers, on_delivery=self._on_delivery
                )
                break
            except BufferError:
                # Local queue full: serve delivery callbacks to drain, then retry this message.
                self._producer.poll(0.5)
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        """Block until outstanding messages are delivered; return the count still unacked."""
        remaining = self._producer.flush(timeout)
        if remaining:
            logger.error("flush left %d message(s) unacked after %.1fs", remaining, timeout)
        return remaining
