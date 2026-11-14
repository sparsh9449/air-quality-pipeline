from ingestion.poller import in_bbox, to_records

ONTARIO_BBOX = (-95.16, 41.67, -74.34, 56.86)

SAMPLE_PAYLOAD = [
    {
        "sensor": {"id": 42},
        "timestamp": "2026-11-14 12:00:00",
        "location": {"latitude": "43.65", "longitude": "-79.38"},  # Toronto
        "sensordatavalues": [
            {"value_type": "P1", "value": "12.3"},
            {"value_type": "P2", "value": "8.1"},
        ],
    },
    {
        "sensor": {"id": 99},
        "timestamp": "2026-11-14 12:00:00",
        "location": {"latitude": "48.85", "longitude": "2.35"},  # Paris, out of bbox
        "sensordatavalues": [{"value_type": "P1", "value": "5.0"}],
    },
]


def test_to_records_flattens_each_value():
    records = to_records(SAMPLE_PAYLOAD)
    assert len(records) == 3
    toronto = records[0]
    assert toronto["sensor_id"] == 42
    assert toronto["value_type"] == "P1"
    assert toronto["value"] == "12.3"


def test_in_bbox_filters_outside_ontario():
    records = to_records(SAMPLE_PAYLOAD)
    inside = [r for r in records if in_bbox(r, ONTARIO_BBOX)]
    assert len(inside) == 2
    assert all(r["sensor_id"] == 42 for r in inside)


def test_in_bbox_handles_missing_coordinates():
    assert in_bbox({"latitude": None, "longitude": None}, ONTARIO_BBOX) is False
