"""Pydantic models and pure transforms for Sensor.Community readings."""

from __future__ import annotations

import logging
import math
from datetime import UTC, datetime

from pydantic import BaseModel, ValidationError, field_validator

logger = logging.getLogger(__name__)


class SensorType(BaseModel):
    id: int
    name: str
    manufacturer: str | None = None


class Sensor(BaseModel):
    id: int
    pin: str | None = None
    sensor_type: SensorType


class Location(BaseModel):
    id: int
    latitude: float | None = None
    longitude: float | None = None
    altitude: float | None = None
    country: str | None = None
    indoor: bool = False
    exact_location: bool = False

    @field_validator("latitude", "longitude", "altitude", mode="before")
    @classmethod
    def _blank_to_none(cls, v: object) -> object:
        return None if v in ("", None) else v


class SensorDataValue(BaseModel):
    id: int | None = None
    value: str | float | int
    value_type: str


class Reading(BaseModel):
    id: int
    timestamp: datetime
    sampling_rate: float | None = None
    location: Location
    sensor: Sensor
    sensordatavalues: list[SensorDataValue]

    @field_validator("timestamp", mode="before")
    @classmethod
    def _as_utc(cls, v: object) -> datetime:
        dt = datetime.fromisoformat(v) if isinstance(v, str) else v
        if not isinstance(dt, datetime):
            raise ValueError("invalid timestamp")
        return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt


def parse_readings(raw: list[dict]) -> list[Reading]:
    """Validate raw API entries, skipping any that fail validation."""
    readings, skipped = [], 0
    for item in raw:
        try:
            readings.append(Reading.model_validate(item))
        except ValidationError:
            skipped += 1
    if skipped:
        logger.info("skipped %d malformed reading(s)", skipped)
    return readings


def flatten(readings: list[Reading], ingested_at: datetime) -> list[dict]:
    """One row per value. Non-numeric/non-finite dropped; out-of-range values kept."""
    rows, dropped = [], 0
    for r in readings:
        loc = r.location
        lat, lon = loc.latitude, loc.longitude
        # Privacy: coarsen opt-in precise coordinates so homes are never
        # stored/shown at full precision.
        if loc.exact_location and lat is not None and lon is not None:
            lat, lon = round(lat, 2), round(lon, 2)
        for v in r.sensordatavalues:
            try:
                value = float(v.value)
            except (TypeError, ValueError):
                dropped += 1
                continue
            # Drop nan/inf: they are not valid measurements and break downstream aggregation.
            if not math.isfinite(value):
                dropped += 1
                continue
            rows.append(
                {
                    "reading_id": r.id,
                    "sensor_id": r.sensor.id,
                    "sensor_type": r.sensor.sensor_type.name,
                    "location_id": loc.id,
                    "latitude": lat,
                    "longitude": lon,
                    "altitude": loc.altitude,
                    "country": loc.country,
                    "indoor": loc.indoor,
                    "exact_location": loc.exact_location,
                    "value_type": v.value_type,
                    "value": value,
                    "measured_at": r.timestamp,
                    "ingested_at": ingested_at,
                    "dt": r.timestamp.date(),
                }
            )
    if dropped:
        logger.info("dropped %d non-numeric/non-finite value(s)", dropped)
    return rows


def dedup(rows: list[dict]) -> list[dict]:
    """Keep the first row per (reading_id, value_type)."""
    seen, out = set(), []
    for row in rows:
        key = (row["reading_id"], row["value_type"])
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out
