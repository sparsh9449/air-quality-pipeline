"""Pure, Streamlit-free helpers for the dashboard (kept importable for tests)."""

from __future__ import annotations

import pandas as pd

BUCKET_FREQ = "5min"

# Time-range selector labels -> lookback duration. "All" means no lower bound.
RANGE_OPTIONS: dict[str, pd.Timedelta | None] = {
    "Last 3 hours": pd.Timedelta(hours=3),
    "Last 12 hours": pd.Timedelta(hours=12),
    "Last 24 hours": pd.Timedelta(hours=24),
    "All": None,
}


def window_start(newest: pd.Timestamp, label: str) -> pd.Timestamp | None:
    """Lower bound for a range label, anchored to the newest reading (None = no bound)."""
    delta = RANGE_OPTIONS[label]
    return None if delta is None else newest - delta


def reindex_5min(series: pd.Series) -> pd.Series:
    """Reindex a bucket->value series onto a complete 5-minute grid, NaN for missing buckets.

    The gaps become NaN so a line chart breaks (rather than interpolating) across periods
    with no data. Empty input is returned unchanged.
    """
    if series.empty:
        return series
    full = pd.date_range(series.index.min(), series.index.max(), freq=BUCKET_FREQ)
    return series.reindex(full)
