import json
from datetime import UTC, datetime
from pathlib import Path

from ingestion.models import dedup, flatten, parse_readings

FIXTURE = Path(__file__).parent / "data" / "gta_sample.json"
INGESTED_AT = datetime(2026, 11, 15, 0, 10, tzinfo=UTC)


def load_rows():
    raw = json.loads(FIXTURE.read_text())
    return flatten(parse_readings(raw), INGESTED_AT)


def test_flatten_emits_one_row_per_numeric_value():
    rows = load_rows()
    # "not_a_number" is dropped; every other value_type becomes its own row.
    assert any(r["value_type"] == "P2" for r in rows)
    assert all(isinstance(r["value"], float) for r in rows)
    assert not any(r["value"] == "not_a_number" for r in rows)


def test_timestamp_is_utc_aware():
    rows = load_rows()
    ts = rows[0]["measured_at"]
    assert ts.tzinfo is not None
    assert ts.utcoffset().total_seconds() == 0


def test_out_of_range_values_are_kept():
    rows = load_rows()
    p1_values = [r["value"] for r in rows if r["value_type"] == "P1"]
    assert 15000.0 in [r["value"] for r in rows if r["value_type"] == "P2"]
    assert -7.0 in p1_values


def test_exact_location_is_rounded_to_two_decimals():
    rows = load_rows()
    exact = [r for r in rows if r["exact_location"]]
    assert exact
    for r in exact:
        assert r["latitude"] == round(r["latitude"], 2)
        assert r["longitude"] == round(r["longitude"], 2)
    # The crafted precise point (43.651234, -79.383215) becomes (43.65, -79.38).
    assert {(r["latitude"], r["longitude"]) for r in exact} == {(43.65, -79.38)}


def test_non_exact_location_is_untouched():
    rows = load_rows()
    coarse = next(r for r in rows if not r["exact_location"] and r["latitude"] == 43.644)
    assert coarse["longitude"] == -79.438


def _reading_with_values(values):
    return [
        {
            "id": 1,
            "timestamp": "2026-11-14 23:45:00",
            "sampling_rate": None,
            "location": {"id": 1, "latitude": "43.7", "longitude": "-79.3", "country": "CA",
                         "indoor": 0, "exact_location": 0},
            "sensor": {"id": 1, "pin": "1", "sensor_type": {"id": 1, "name": "SDS011"}},
            "sensordatavalues": values,
        }
    ]


def test_non_finite_values_are_dropped():
    raw = _reading_with_values([
        {"value": "nan", "value_type": "P2"},
        {"value": "inf", "value_type": "P1"},
        {"value": "-inf", "value_type": "P0"},
        {"value": "12.5", "value_type": "P2"},
    ])
    rows = flatten(parse_readings(raw), INGESTED_AT)
    # Only the one finite value survives; nan/inf/-inf are all dropped.
    assert [r["value"] for r in rows] == [12.5]


def test_dedup_collapses_repeated_reading_value_pairs():
    rows = load_rows()
    deduped = dedup(rows)
    keys = [(r["reading_id"], r["value_type"]) for r in deduped]
    assert len(keys) == len(set(keys))
    # reading_id 31370748770 / P2 appears twice in the fixture -> one row after dedup.
    assert keys.count((31370748770, "P2")) == 1
