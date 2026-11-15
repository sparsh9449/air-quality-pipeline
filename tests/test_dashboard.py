import numpy as np
import pandas as pd

from dashboards.transforms import reindex_5min, window_start


def test_reindex_5min_inserts_nan_for_missing_bucket():
    # Buckets at 00:00 and 00:10 present; 00:05 is missing -> should become NaN.
    idx = pd.to_datetime(["2026-11-15 00:00", "2026-11-15 00:10"])
    series = pd.Series([3.0, 5.0], index=idx)

    out = reindex_5min(series)

    assert list(out.index) == list(
        pd.date_range("2026-11-15 00:00", "2026-11-15 00:10", freq="5min")
    )
    assert out.loc["2026-11-15 00:00"] == 3.0
    assert out.loc["2026-11-15 00:10"] == 5.0
    assert np.isnan(out.loc["2026-11-15 00:05"])


def test_reindex_5min_empty_is_noop():
    empty = pd.Series(dtype=float)
    assert reindex_5min(empty).empty


def test_window_start_last_3_hours():
    newest = pd.Timestamp("2026-11-15 05:00")
    assert window_start(newest, "Last 3 hours") == pd.Timestamp("2026-11-15 02:00")


def test_window_start_all_is_none():
    assert window_start(pd.Timestamp("2026-11-15 05:00"), "All") is None
