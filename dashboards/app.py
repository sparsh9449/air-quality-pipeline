"""Streamlit dashboard for Ontario air quality. (stub)"""

from __future__ import annotations

import os

import duckdb
import streamlit as st

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "./data/aqi.duckdb")

st.set_page_config(page_title="Ontario Air Quality", layout="wide")
st.title("Ontario Air Quality")

if os.path.exists(DUCKDB_PATH):
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    df = con.execute("select * from stg_measurements limit 100").df()
    st.dataframe(df, use_container_width=True)
else:
    st.info("No data yet. Run the pipeline to populate measurements.")
