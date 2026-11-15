"""Streamlit dashboard: median PM2.5 (P2) over time by country. All times shown in UTC."""

from __future__ import annotations

import glob
import os

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

try:  # `streamlit run` puts dashboards/ on sys.path; pytest puts the repo root on it
    from transforms import BUCKET_FREQ, RANGE_OPTIONS, reindex_5min, window_start
except ModuleNotFoundError:
    from dashboards.transforms import BUCKET_FREQ, RANGE_OPTIONS, reindex_5min, window_start

PARQUET_DIR = os.environ.get("PARQUET_DIR", "./data/parquet")
CLAMP_MAX = 1000
TZ_LABEL = "UTC"


def to_utc_naive(s: pd.Series) -> pd.Series:
    """Normalise a tz-aware series to UTC then drop tz, so display never shifts by viewer tz."""
    return s.dt.tz_convert("UTC").dt.tz_localize(None)


st.set_page_config(page_title="Air Quality", layout="wide")
st.title("Air Quality — PM2.5")
st.caption(
    "Outdoor sensors only (indoor excluded). PM2.5 is clamped to "
    f"0–{CLAMP_MAX} µg/m³ for display; stored values are raw and unmodified."
)

files = glob.glob(os.path.join(PARQUET_DIR, "dt=*", "*.parquet"))
if not files:
    st.info("No data yet. Run `make poll` (or `python -m ingestion.poller`) to collect readings.")
    st.stop()

pattern = os.path.join(PARQUET_DIR, "dt=*", "*.parquet")

last_pull = duckdb.sql(
    f"select max(ingested_at) from read_parquet('{pattern}', hive_partitioning=true)"
).fetchall()[0][0]
last_pull_utc = pd.Timestamp(last_pull).tz_convert("UTC")
st.caption(f"Last updated (newest pull): {last_pull_utc:%Y-%m-%d %H:%M} {TZ_LABEL}")

df = duckdb.sql(
    """
    select reading_id, sensor_id, country, measured_at,
           least(greatest(value, 0), $clamp) as pm25
    from (
        select *, row_number() over (
            partition by reading_id, value_type order by ingested_at desc
        ) as rn
        from read_parquet($pattern, hive_partitioning=true)
        where value_type = 'P2' and indoor = false
    )
    where rn = 1
    """,
    params={"pattern": pattern, "clamp": CLAMP_MAX},
).df()

if df.empty:
    st.info("Data collected, but no PM2.5 (P2) readings yet. Give the poller more time.")
    st.stop()

df["measured_at"] = to_utc_naive(df["measured_at"])

countries = sorted(df["country"].dropna().unique())
default = countries.index("CA") if "CA" in countries else 0
col_country, col_range = st.columns(2)
country = col_country.selectbox("Country", countries, index=default)
range_label = col_range.selectbox("Time range", list(RANGE_OPTIONS), index=0)

sel = df[df["country"] == country].copy()
start = window_start(sel["measured_at"].max(), range_label)
if start is not None:
    sel = sel[sel["measured_at"] >= start]
sel["bucket"] = sel["measured_at"].dt.floor(BUCKET_FREQ)

x_axis = alt.X("bucket:T", title=f"Time ({TZ_LABEL})", axis=alt.Axis(format="%H:%M"))

st.subheader("Median PM2.5 per 5-minute bucket")
median = reindex_5min(sel.groupby("bucket")["pm25"].median())
median_df = median.reset_index()
median_df.columns = ["bucket", "pm25"]
st.altair_chart(
    alt.Chart(median_df).mark_line(point=True).encode(
        x=x_axis,
        y=alt.Y("pm25:Q", title="Median PM2.5 (µg/m³)"),
        tooltip=[alt.Tooltip("bucket:T", title=f"Time ({TZ_LABEL})"), "pm25:Q"],
    ),
    width="stretch",
)

n_sensors = sel["sensor_id"].nunique()
st.caption(f"{n_sensors} sensor(s) in {country}.")
if n_sensors <= 20:
    st.subheader("Per-sensor PM2.5")
    piv = sel.pivot_table(index="bucket", columns="sensor_id", values="pm25", aggfunc="median")
    full = pd.date_range(piv.index.min(), piv.index.max(), freq=BUCKET_FREQ)
    piv = piv.reindex(full)
    long = piv.reset_index(names="bucket").melt("bucket", var_name="sensor_id", value_name="pm25")
    st.altair_chart(
        alt.Chart(long).mark_line(point=True).encode(
            x=x_axis,
            y=alt.Y("pm25:Q", title="PM2.5 (µg/m³)"),
            color=alt.Color("sensor_id:N", title="Sensor"),
            tooltip=[alt.Tooltip("bucket:T", title=f"Time ({TZ_LABEL})"), "sensor_id:N", "pm25:Q"],
        ),
        width="stretch",
    )
else:
    st.info("Per-sensor view hidden: more than 20 sensors in this selection.")
