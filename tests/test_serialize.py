import json
from datetime import UTC, datetime

from streaming.serialize import (
    coarsen_reading,
    decode_reading,
    encode_dlq,
    encode_reading,
    redact_coords,
)


def test_reading_round_trip():
    reading = {"id": 1, "sensor": {"id": 7}}
    env = decode_reading(encode_reading(reading, datetime(2026, 11, 15, 0, 10, tzinfo=UTC)))
    assert env["schema_version"] == 1
    assert env["ingested_at"].startswith("2026-11-15T00:10")
    assert env["reading"] == reading


def test_redact_rounds_nested_numeric_and_string_coords():
    payload = {"location": {"latitude": "43.651234", "longitude": -79.383215, "country": "CA"}}
    red = redact_coords(payload)
    assert red["location"]["latitude"] == 43.65
    assert red["location"]["longitude"] == -79.38
    assert red["location"]["country"] == "CA"


def test_dlq_redacts_coords_even_without_exact_location():
    # No exact_location key at all -> coords must still be rounded.
    payload = {"location": {"latitude": "43.651234", "longitude": "-79.383215"}}
    env = json.loads(encode_dlq(source="poller", stage="validation", error="X", payload=payload))
    assert env["payload"]["location"]["latitude"] == 43.65
    assert env["payload"]["location"]["longitude"] == -79.38
    assert env["source"] == "poller"
    assert env["source_offset"] is None


def test_coarsen_rounds_when_exact_location_set():
    loc = {"latitude": "43.651234", "longitude": "-79.383215", "exact_location": 1}
    reading = {"location": loc}
    out = coarsen_reading(reading)
    assert out["location"]["latitude"] == 43.65
    assert out["location"]["longitude"] == -79.38


def test_coarsen_leaves_non_exact_untouched():
    reading = {"location": {"latitude": "43.65", "longitude": "-79.38", "exact_location": 0}}
    assert coarsen_reading(reading) is reading


def test_coarsen_does_not_mutate_input():
    loc = {"latitude": "43.651234", "longitude": "-79.383215", "exact_location": 1}
    reading = {"location": loc}
    coarsen_reading(reading)
    assert reading["location"]["latitude"] == "43.651234"
    assert reading["location"]["longitude"] == "-79.383215"


def test_dlq_carries_source_offset_for_consumer_failures():
    env = json.loads(
        encode_dlq(
            source="archiver",
            stage="processing",
            error="ValueError: bad",
            payload={},
            key="7",
            source_topic="sensor.readings",
            source_partition=2,
            source_offset=42,
        )
    )
    assert (env["source_topic"], env["source_partition"], env["source_offset"]) == (
        "sensor.readings",
        2,
        42,
    )
    assert env["key"] == "7"
