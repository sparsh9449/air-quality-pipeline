import json
from datetime import UTC, datetime

import pytest

from ingestion.poller import publish_once, publish_readings
from streaming.serialize import decode_reading

INGESTED = datetime(2026, 11, 15, 0, 10, tzinfo=UTC)


class FakeProducer:
    def __init__(self, flush_remaining=0, failed=0):
        self.messages = []  # (topic, key, value, headers)
        self.delivered = 0
        self.failed = failed
        self._flush_remaining = flush_remaining

    def produce(self, topic, value, key=None, headers=None):
        self.messages.append((topic, key, value, headers))

    def flush(self, timeout=30.0):
        return self._flush_remaining


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, payload):
        self._payload = payload

    def get(self, url, headers=None, timeout=None):
        return FakeResponse(self._payload)


def _valid(sid=64138, exact=0, lat="43.651234", lon="-79.383215"):
    return {
        "id": sid * 10,
        "timestamp": "2026-11-14 23:45:00",
        "sampling_rate": None,
        "location": {
            "id": 1,
            "latitude": lat,
            "longitude": lon,
            "country": "CA",
            "indoor": 0,
            "exact_location": exact,
        },
        "sensor": {"id": sid, "pin": "1", "sensor_type": {"id": 23, "name": "PMS5003"}},
        "sensordatavalues": [{"id": 1, "value": "8.0", "value_type": "P2"}],
    }


def _invalid_bad_timestamp(sid=99002):
    entry = _valid(sid=sid, exact=1)
    entry["timestamp"] = "not-a-date"
    return entry


def test_exact_location_rounded_before_publish():
    p = FakeProducer()
    counts = publish_readings([_valid(exact=1)], p, INGESTED)
    topic, key, value, _ = p.messages[0]
    env = decode_reading(value)
    assert env["reading"]["location"]["latitude"] == 43.65
    assert env["reading"]["location"]["longitude"] == -79.38
    assert key == "64138"
    assert counts == {"fetched": 1, "published": 1, "dead_lettered": 0}


def test_non_exact_location_untouched():
    p = FakeProducer()
    publish_readings([_valid(exact=0)], p, INGESTED)
    env = decode_reading(p.messages[0][2])
    assert env["reading"]["location"]["latitude"] == "43.651234"


def test_invalid_goes_to_dlq_with_redacted_coords():
    p = FakeProducer()
    counts = publish_readings([_invalid_bad_timestamp()], p, INGESTED)
    topic, key, value, _ = p.messages[0]
    assert topic == "sensor.readings.dlq"
    env = json.loads(value)
    assert env["source"] == "poller" and env["stage"] == "validation"
    assert env["payload"]["location"]["latitude"] == 43.65  # redacted even though invalid
    assert env["payload"]["location"]["longitude"] == -79.38
    assert key == "99002"
    assert counts["dead_lettered"] == 1 and counts["published"] == 0


def test_dlq_error_string_has_loc_type_and_no_coordinates():
    # Offending field is the precise latitude itself; its value must never reach the error string.
    entry = _valid(sid=99003, exact=1, lat="43.651234", lon="-79.383215")
    entry["location"]["latitude"] = "43.651234xyz"  # precise-looking but unparseable
    p = FakeProducer()
    publish_readings([entry], p, INGESTED)
    env = json.loads(p.messages[0][2])
    err = env["error"]
    assert "loc=location.latitude" in err
    assert "type=" in err
    assert "43.651234" not in err
    assert "-79.383215" not in err


def test_keys_are_sensor_id_and_headers_set():
    p = FakeProducer()
    publish_readings([_valid(sid=777)], p, INGESTED)
    topic, key, value, headers = p.messages[0]
    assert topic == "sensor.readings"
    assert key == "777"
    assert ("schema_version", b"1") in headers
    assert ("content-type", b"application/json") in headers


def test_publish_once_ok_returns_counts():
    p = FakeProducer()
    counts = publish_once("http://x", "ua", p, session=FakeSession([_valid(), _valid(sid=2)]))
    assert counts["published"] == 2
    assert counts["delivery_failures"] == 0
    assert counts["unflushed"] == 0


def test_publish_once_raises_on_delivery_failures():
    p = FakeProducer(failed=2)
    with pytest.raises(RuntimeError, match="delivery_failures=2"):
        publish_once("http://x", "ua", p, session=FakeSession([_valid()]))


def test_publish_once_raises_on_unflushed():
    p = FakeProducer(flush_remaining=3)
    with pytest.raises(RuntimeError, match="unflushed=3"):
        publish_once("http://x", "ua", p, session=FakeSession([_valid()]))
