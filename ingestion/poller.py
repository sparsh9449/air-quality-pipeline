"""Poll the Sensor.Community API for Ontario air quality readings."""

from __future__ import annotations

import os

import requests

DEFAULT_URL = "https://data.sensor.community/static/v2/data.json"


def to_records(raw: list[dict]) -> list[dict]:
    """Flatten Sensor.Community API payload into measurement records.

    Each sensor reading contains multiple sensordatavalues; emit one record per value.
    """
    records: list[dict] = []
    for entry in raw:
        location = entry.get("location", {})
        for value in entry.get("sensordatavalues", []):
            records.append(
                {
                    "sensor_id": entry.get("sensor", {}).get("id"),
                    "timestamp": entry.get("timestamp"),
                    "latitude": location.get("latitude"),
                    "longitude": location.get("longitude"),
                    "value_type": value.get("value_type"),
                    "value": value.get("value"),
                }
            )
    return records


def in_bbox(record: dict, bbox: tuple[float, float, float, float]) -> bool:
    """Return True if the record falls inside (min_lon, min_lat, max_lon, max_lat)."""
    min_lon, min_lat, max_lon, max_lat = bbox
    try:
        lon = float(record["longitude"])
        lat = float(record["latitude"])
    except (TypeError, ValueError):
        return False
    return min_lon <= lon <= max_lon and min_lat <= lat <= max_lat


def fetch_ontario(bbox: tuple[float, float, float, float], url: str = DEFAULT_URL) -> list[dict]:
    """Fetch the global feed and filter to the Ontario bounding box."""
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    records = to_records(response.json())
    return [r for r in records if in_bbox(r, bbox)]


def _parse_bbox(value: str) -> tuple[float, float, float, float]:
    parts = [float(p) for p in value.split(",")]
    return parts[0], parts[1], parts[2], parts[3]


if __name__ == "__main__":
    bbox = _parse_bbox(os.environ.get("ONTARIO_BBOX", "-95.16,41.67,-74.34,56.86"))
    url = os.environ.get("SENSOR_COMMUNITY_URL", DEFAULT_URL)
    print(f"Fetched {len(fetch_ontario(bbox, url))} Ontario records")
