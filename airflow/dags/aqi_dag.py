"""Airflow DAG orchestrating the AQI pipeline. (stub)

Kept import-light so it loads without a running Airflow install during CI/tests.
"""

from __future__ import annotations

DAG_ID = "ontario_aqi_pipeline"
SCHEDULE = "*/5 * * * *"

# Tasks (to implement): poll_sensor_community -> run_dbt -> refresh_dashboard
TASKS = ["poll_sensor_community", "run_dbt", "refresh_dashboard"]
