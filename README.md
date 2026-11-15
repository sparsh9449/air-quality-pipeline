# ontario-aqi-pipeline

Real-time air quality pipeline for Ontario built on free [Sensor.Community](https://sensor.community)
data. Runs locally with Docker, no paid services.

## Phase 1
Poll the global Sensor.Community v2 feed → validate → write partitioned Parquet → chart median
PM2.5 in Streamlit.

```bash
pip install -r requirements.txt
cp .env.example .env            # adjust USER_AGENT to your own GitHub repo
python -m ingestion.poller      # one pull, writes data/parquet/dt=.../*.parquet
make poll                       # continuous polling every POLL_INTERVAL_SECONDS
streamlit run dashboards/app.py # dashboard
```

## Data & privacy
Measurements come from Sensor.Community's public API. The feed carries no names or emails.
Most locations are already coarsened by the API, but some sensors opt in to exact coordinates
(`exact_location = 1`). For those, this pipeline **rounds latitude/longitude to two decimals at
ingest** (~1 km) and never displays precise coordinates, so individual homes are not pinpointed.
Readings are retained locally for `RETENTION_DAYS` (default 7) and then pruned.
