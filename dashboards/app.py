"""Streamlit dashboard: median PM2.5 (P2) over time by country."""

from __future__ import annotations

import glob
import os

import duckdb
import streamlit as st

PARQUET_DIR = os.environ.get("PARQUET_DIR", "./data/parquet")
CLAMP_MAX = 1000

st.set_page_config(page_title="Ontario Air Quality", layout="wide")
st.title("Ontario Air Quality — PM2.5")
st.caption(
    "Outdoor sensors only (indoor excluded). PM2.5 is clamped to "
    f"0–{CLAMP_MAX} µg/m³ for display; stored values are raw and unmodified."
)

files = glob.glob(os.path.join(PARQUET_DIR, "dt=*", "*.parquet"))
if not files:
    st.info("No data yet. Run `make poll` (or `python -m ingestion.poller`) to collect readings.")
    st.stop()

pattern = os.path.join(PARQUET_DIR, "dt=*", "*.parquet")
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

countries = sorted(df["country"].dropna().unique())
default = countries.index("CA") if "CA" in countries else 0
country = st.selectbox("Country", countries, index=default)

sel = df[df["country"] == country].copy()
sel["bucket"] = sel["measured_at"].dt.floor("5min")

st.subheader("Median PM2.5 per 5-minute bucket")
st.line_chart(sel.groupby("bucket")["pm25"].median())

n_sensors = sel["sensor_id"].nunique()
st.caption(f"{n_sensors} sensor(s) in {country}.")
if n_sensors <= 20:
    st.subheader("Per-sensor PM2.5")
    st.line_chart(
        sel.pivot_table(index="bucket", columns="sensor_id", values="pm25", aggfunc="median")
    )
else:
    st.info("Per-sensor view hidden: more than 20 sensors in this selection.")
