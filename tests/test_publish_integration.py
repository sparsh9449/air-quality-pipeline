import uuid

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def temp_topic():
    # Throwaway topic so integration runs never leave messages on the real sensor.readings.
    from streaming.admin import create_topics, delete_topics
    from streaming.config import TopicSpec

    name = f"sensor.readings.it-{uuid.uuid4()}"
    spec = TopicSpec(name, 1, {"cleanup.policy": "delete", "retention.ms": "600000"})
    create_topics(specs=[spec])
    try:
        yield name
    finally:
        delete_topics([name])


def test_publish_batch_roundtrip(temp_topic):
    import time
    from datetime import UTC, datetime

    from confluent_kafka import Consumer

    from ingestion.poller import publish_readings
    from streaming.config import broker
    from streaming.producer import StreamProducer
    from streaming.serialize import decode_reading

    nonce = str(uuid.uuid4())
    raw = [
        {
            "id": 100 + i,
            "timestamp": "2026-11-14 23:45:00",
            "sampling_rate": None,
            "location": {"id": 1, "latitude": "43.65", "longitude": "-79.38",
                         "country": "CA", "indoor": 0, "exact_location": 0},
            "sensor": {"id": 700 + i, "pin": "1", "sensor_type": {"id": 23, "name": "PMS5003"}},
            "sensordatavalues": [{"id": 1, "value": "8.0", "value_type": "P2"}],
            "nonce": nonce,
        }
        for i in range(3)
    ]

    producer = StreamProducer()
    counts = publish_readings(raw, producer, datetime.now(UTC), readings_topic=temp_topic)
    assert producer.flush(15) == 0
    assert counts["published"] == 3

    consumer = Consumer(
        {
            "bootstrap.servers": broker(),
            "group.id": f"publish-it-{nonce}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([temp_topic])
    seen_keys = set()
    deadline = time.monotonic() + 20
    try:
        while time.monotonic() < deadline and len(seen_keys) < 3:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            env = decode_reading(msg.value())
            if env.get("reading", {}).get("nonce") == nonce:
                seen_keys.add(msg.key().decode())
    finally:
        consumer.close()
    assert seen_keys == {"700", "701", "702"}
