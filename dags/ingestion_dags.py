"""DAGs 1-3: scheduled batch ingestion for FIRMS (2 h), EONET (1 h) and Open-Meteo (3 h).

Each run: [select_sites ->] fetch_and_archive (API -> MinIO) -> publish_changes (MinIO -> Kafka, new/changed only).
The code lives in app/ingest.py, shared with the USGS/GDACS producer containers.
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task

import ingest

DEFAULTS = {"retries": 2, "retry_delay": timedelta(minutes=5)}


def ingestion_dag(source, schedule):
    @dag(dag_id=f"ingest_{source}", schedule=schedule, start_date=datetime(2026, 11, 1), catchup=False,
         max_active_runs=1, default_args=DEFAULTS, tags=["ingestion"], doc_md=__doc__)
    def pipeline():
        @task
        def select_sites():
            """Active disaster locations from PostgreSQL (needs at least one Spark ETL run)."""
            return ingest.weather_sites()

        @task
        def fetch_and_archive(sites=None):
            return ingest.fetch_and_archive(source, sites)

        @task
        def publish_changes(ingest_run_id):   # 'run_id' is a reserved Airflow argument name
            return ingest.publish_changes(ingest_run_id)

        sites = select_sites() if source == "openmeteo" else None
        publish_changes(fetch_and_archive(sites))

    pipeline()


ingestion_dag("firms", "0 */2 * * *")
ingestion_dag("eonet", "5 * * * *")
ingestion_dag("openmeteo", "20 */3 * * *")
