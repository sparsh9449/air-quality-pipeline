"""Stage-1 smoke test: produce one reading to a throwaway topic and consume it back.

Uses a nonce-named temp topic (created up front, deleted on exit) so it never leaves test
messages on the real sensor.readings topic.
"""

from __future__ import annotations

import logging
import sys
import time
import uuid
from datetime import UTC, datetime

from confluent_kafka import Consumer, Producer

from streaming.admin import create_topics, delete_topics
from streaming.config import TopicSpec, broker
from streaming.serialize import decode_reading, encode_reading

logger = logging.getLogger(__name__)

SAMPLE_READING = {
    "id": 999999,
    "timestamp": "2026-11-15 00:00:00",
    "sampling_rate": None,
    "location": {
        "id": 1,
        "latitude": "43.65",
        "longitude": "-79.38",
        "country": "CA",
        "indoor": 0,
        "exact_location": 0,
    },
    "sensor": {"id": 64138, "pin": "1", "sensor_type": {"id": 23, "name": "PMS5003"}},
    "sensordatavalues": [{"id": 1, "value": "8.0", "value_type": "P2"}],
}


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bootstrap = broker()
    nonce = str(uuid.uuid4())
    topic = f"sensor.readings.roundtrip-{nonce}"
    spec = TopicSpec(topic, 1, {"cleanup.policy": "delete", "retention.ms": "600000"})
    create_topics(bootstrap, [spec])
    logger.info("created throwaway topic %s", topic)
    try:
        reading = {**SAMPLE_READING, "nonce": nonce}
        producer = Producer({"bootstrap.servers": bootstrap})
        producer.produce(
            topic,
            key=str(reading["sensor"]["id"]),
            value=encode_reading(reading, datetime.now(UTC)),
        )
        producer.flush(10)
        logger.info("produced message nonce=%s", nonce)

        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"roundtrip-{nonce}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([topic])
        deadline = time.monotonic() + 20
        try:
            while time.monotonic() < deadline:
                msg = consumer.poll(1.0)
                if msg is None or msg.error():
                    continue
                env = decode_reading(msg.value())
                if env.get("reading", {}).get("nonce") == nonce:
                    logger.info(
                        "consumed match: schema_version=%s ingested_at=%s sensor=%s",
                        env["schema_version"],
                        env["ingested_at"],
                        env["reading"]["sensor"]["id"],
                    )
                    print("ROUNDTRIP OK")
                    return 0
        finally:
            consumer.close()
        print("ROUNDTRIP FAILED: message not seen within timeout", file=sys.stderr)
        return 1
    finally:
        delete_topics([topic], bootstrap)
        logger.info("deleted throwaway topic %s", topic)


if __name__ == "__main__":
    raise SystemExit(main())
