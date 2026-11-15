from datetime import UTC, date, datetime

import pyarrow.parquet as pq

from ingestion.poller import (
    BOX_BASE,
    BULK_URL,
    SCHEMA,
    source_url,
    write_partitioned,
)

INGESTED_AT = datetime(2026, 11, 15, 0, 10, tzinfo=UTC)


def test_source_url_defaults_to_bulk_feed():
    assert source_url(None) == BULK_URL


def test_source_url_uses_box_filter_when_bbox_set():
    bbox = "43.4,-80.0,44.3,-78.9"
    assert source_url(bbox) == f"{BOX_BASE}{bbox}"


def _row(dt_val, reading_id):
    return {
        "reading_id": reading_id,
        "sensor_id": 1,
        "sensor_type": "SDS011",
        "location_id": 1,
        "latitude": 43.7,
        "longitude": -79.3,
        "altitude": None,
        "country": "CA",
        "indoor": False,
        "exact_location": False,
        "value_type": "P2",
        "value": 12.5,
        "measured_at": datetime.combine(dt_val, datetime.min.time(), tzinfo=UTC),
        "ingested_at": INGESTED_AT,
        "dt": dt_val,
    }


def test_write_partitioned_creates_dt_folders_with_schema(tmp_path):
    rows = [
        _row(date(2026, 11, 14), 1),
        _row(date(2026, 11, 14), 2),
        _row(date(2026, 11, 15), 3),
    ]
    parquet_dir = str(tmp_path / "parquet")

    paths = write_partitioned(rows, parquet_dir, INGESTED_AT)

    # One file per distinct dt, each under a dt=YYYY-MM-DD folder.
    assert len(paths) == 2
    folders = sorted(p.name for p in (tmp_path / "parquet").iterdir())
    assert folders == ["dt=2026-11-14", "dt=2026-11-15"]

    # Both rows for the 14th land in the same file; schema matches exactly.
    for path in paths:
        table = pq.read_table(path)
        assert table.schema.equals(SCHEMA)
    total = sum(pq.read_table(p).num_rows for p in paths)
    assert total == 3


def test_write_partitioned_filename_includes_ingest_stamp(tmp_path):
    parquet_dir = str(tmp_path / "parquet")
    (path,) = write_partitioned([_row(date(2026, 11, 14), 1)], parquet_dir, INGESTED_AT)
    assert path.endswith("measurements_20261115T001000Z.parquet")
