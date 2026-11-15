"""Pure JSON (de)serialization for stream envelopes. No broker imports (unit-testable)."""

from __future__ import annotations

import copy
import json
from datetime import UTC, datetime

SCHEMA_VERSION = 1
_COORD_KEYS = {"latitude", "longitude", "lat", "lon"}


def _iso(ts: datetime | str) -> str:
    return ts.astimezone(UTC).isoformat() if isinstance(ts, datetime) else ts


def encode_reading(reading: dict, ingested_at: datetime) -> bytes:
    env = {"schema_version": SCHEMA_VERSION, "ingested_at": _iso(ingested_at), "reading": reading}
    return json.dumps(env, separators=(",", ":")).encode()


def decode_reading(data: bytes) -> dict:
    return json.loads(data)


def _round_coord(v: object) -> object:
    # The feed sends lat/lon as strings, so round numbers and numeric strings alike.
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return round(v, 2)
    if isinstance(v, str):
        try:
            return round(float(v), 2)
        except ValueError:
            return v
    return v


def _truthy(v: object) -> bool:
    if isinstance(v, str):
        return v.strip().lower() not in ("", "0", "false")
    return bool(v)


def coarsen_reading(reading: dict) -> dict:
    """Round lat/lon to 2 decimals when exact_location is set, so precise coords never leave.

    Non-exact locations are left as the API already coarsened them (matches models.flatten).
    Returns a new object; the input (including its nested location) is never mutated.
    """
    loc = reading.get("location")
    if not isinstance(loc, dict) or not _truthy(loc.get("exact_location")):
        return reading
    new_loc = copy.deepcopy(loc)
    for key in ("latitude", "longitude"):
        if new_loc.get(key) is not None:
            new_loc[key] = _round_coord(new_loc[key])
    return {**reading, "location": new_loc}


def redact_coords(obj: object) -> object:
    """Round every lat/lon field to 2 decimals so precise coordinates never leave the system."""
    if isinstance(obj, dict):
        return {
            k: (_round_coord(v) if k in _COORD_KEYS else redact_coords(v)) for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [redact_coords(v) for v in obj]
    return obj


def encode_dlq(
    *,
    source: str,
    stage: str,
    error: str,
    payload: object,
    key: str | None = None,
    source_topic: str | None = None,
    source_partition: int | None = None,
    source_offset: int | None = None,
    produced_at: datetime | None = None,
) -> bytes:
    """Build a dead-letter envelope. Coordinates in the payload are always redacted."""
    env = {
        "schema_version": SCHEMA_VERSION,
        "source": source,
        "stage": stage,
        "error": error,
        "key": key,
        "source_topic": source_topic,
        "source_partition": source_partition,
        "source_offset": source_offset,
        "payload": redact_coords(payload),
        "produced_at": _iso(produced_at or datetime.now(UTC)),
    }
    return json.dumps(env, separators=(",", ":")).encode()
