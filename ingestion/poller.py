"""Poll Sensor.Community, validate, and write partitioned Parquet."""

from __future__ import annotations

import logging
import os
import re
import shutil
import sys
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from ingestion.models import dedup, flatten, parse_readings

logger = logging.getLogger(__name__)

BULK_URL = "https://data.sensor.community/static/v2/data.json"
BOX_BASE = "https://data.sensor.community/airrohr/v1/filter/box="
PARTITION_RE = re.compile(r"^dt=\d{4}-\d{2}-\d{2}$")

SCHEMA = pa.schema(
    [
        ("reading_id", pa.int64()),
        ("sensor_id", pa.int64()),
        ("sensor_type", pa.string()),
        ("location_id", pa.int64()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("altitude", pa.float64()),
        ("country", pa.string()),
        ("indoor", pa.bool_()),
        ("exact_location", pa.bool_()),
        ("value_type", pa.string()),
        ("value", pa.float64()),
        ("measured_at", pa.timestamp("us", tz="UTC")),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
        ("dt", pa.date32()),
    ]
)


def source_url(bbox: str | None, bulk_url: str = BULK_URL) -> str:
    return f"{BOX_BASE}{bbox}" if bbox else bulk_url


def fetch(url: str, user_agent: str, session: requests.Session | None = None) -> list[dict]:
    client = session or requests
    response = client.get(url, headers={"User-Agent": user_agent}, timeout=60)
    response.raise_for_status()
    return response.json()


def write_partitioned(rows: list[dict], parquet_dir: str, ingested_at: datetime) -> list[str]:
    """Write rows into dt=YYYY-MM-DD/measurements_<ts>.parquet partitions."""
    by_dt: dict[object, list[dict]] = defaultdict(list)
    for row in rows:
        by_dt[row["dt"]].append(row)
    stamp = ingested_at.strftime("%Y%m%dT%H%M%SZ")
    paths = []
    for dt_val, group in by_dt.items():
        part_dir = os.path.join(parquet_dir, f"dt={dt_val.isoformat()}")
        os.makedirs(part_dir, exist_ok=True)
        path = os.path.join(part_dir, f"measurements_{stamp}.parquet")
        pq.write_table(pa.Table.from_pylist(group, schema=SCHEMA), path, compression="snappy")
        paths.append(path)
    return paths


def prune_partitions(parquet_dir: str, retention_days: int, now: datetime) -> list[str]:
    """Delete only immediate dt=YYYY-MM-DD subdirectories older than the cutoff; nothing else."""
    base = os.path.realpath(parquet_dir)
    if not os.path.isdir(base):
        return []
    cutoff = (now - timedelta(days=retention_days)).date()
    removed = []
    for name in os.listdir(base):
        if not PARTITION_RE.match(name):
            continue
        path = os.path.join(base, name)
        if not os.path.isdir(path):
            continue
        try:
            part_date = datetime.strptime(name[3:], "%Y-%m-%d").date()
        except ValueError:
            continue
        if part_date >= cutoff:
            continue
        resolved = os.path.realpath(path)
        if os.path.commonpath([resolved, base]) != base or resolved == base:
            continue
        shutil.rmtree(resolved)
        removed.append(name)
    if removed:
        logger.info("pruned %d old partition(s)", len(removed))
    return removed


def run_once(
    parquet_dir: str,
    user_agent: str,
    url: str = BULK_URL,
    session: requests.Session | None = None,
    ingested_at: datetime | None = None,
) -> list[str]:
    ingested_at = ingested_at or datetime.now(UTC)
    rows = dedup(flatten(parse_readings(fetch(url, user_agent, session)), ingested_at))
    paths = write_partitioned(rows, parquet_dir, ingested_at)
    logger.info("wrote %d row(s) across %d partition(s)", len(rows), len(paths))
    return paths


def poll_loop(
    parquet_dir: str, user_agent: str, url: str, interval: int, retention_days: int
) -> None:
    while True:
        try:
            run_once(parquet_dir, user_agent, url=url)
            prune_partitions(parquet_dir, retention_days, datetime.now(UTC))
        except Exception:  # noqa: BLE001 - one bad poll must not kill the loop
            logger.exception("poll iteration failed")
        time.sleep(interval)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parquet_dir = os.environ.get("PARQUET_DIR", "./data/parquet")
    user_agent = os.environ.get("USER_AGENT", "air-quality-pipeline")
    url = source_url(os.environ.get("BBOX"), os.environ.get("SENSOR_BULK_URL", BULK_URL))
    if "--loop" in sys.argv:
        interval = int(os.environ.get("POLL_INTERVAL_SECONDS", "300"))
        retention = int(os.environ.get("RETENTION_DAYS", "7"))
        poll_loop(parquet_dir, user_agent, url, interval, retention)
    else:
        print("\n".join(run_once(parquet_dir, user_agent, url=url)))
