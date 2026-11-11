# Implementation Status

Tested on 2026-11-11 (UTC), Windows 11 + Docker Desktop 29.9.3 (24 CPU, 62 GB). Every result below comes from commands that were actually run.

## Components
| Component | Status | Evidence |
|---|---|---|
| Docker | PASS | 10 long-running containers up; `kafka-init` exited 0 after creating 5 topics; about 4.2 GB RAM total |
| USGS | PASS | producer polls every 60 s: 28 runs, 0 failures, 208 versions of 207 earthquakes staged (1 genuine revision) |
| GDACS | PASS | producer every 10 min: 4 runs, 0 failures, 476 versions of 468 events (8 genuine episode/datemodified revisions) |
| FIRMS | PASS | `ingest_firms` DAG: 80,071 global VIIRS NOAA-20 detections (world, 2 days); re-run published 0 |
| EONET | PASS | `ingest_eonet` DAG: 873 events (30 days, open + closed), 1,653 dated geometries; re-run published 0 |
| Open-Meteo | PASS | `ingest_openmeteo` DAG: 50 disaster sites → 3,600 hourly forecast rows; re-run published 0 |
| Kafka | PASS | offsets raw.usgs 208, raw.gdacs 476, raw.firms 80,071, raw.eonet 873, raw.openmeteo 50; consumer group lag 0 on all topics |
| MinIO | PASS | `raw-archive`: usgs 56, gdacs 24, firms 6, eonet 6, openmeteo 6 objects (gzip responses + request.json) |
| Airflow | PASS | 5 DAGs loaded with no import errors; all 16 scheduled + manual runs of the 4 ingestion/ETL DAGs succeeded; daily_validation manual run succeeded |
| Spark | PASS | spark-submit to `spark://spark-master:7077`; executors on spark-worker (`app-2026…` IDs in task logs); 6 `spark_etl` runs succeeded |
| PostgreSQL | PASS | staging, cleaned and analytics tables populated; unique constraints hold; 0 duplicates |
| FastAPI | PASS | all 10 endpoints returned JSON from PostgreSQL with filters; 400 on a bad bbox, 404 on an unknown event |

## Tests performed
- **API preflight**: all 5 live APIs passed (see `API_PREFLIGHT.md`).
- **End-to-end trace** of USGS earthquake `us7000txi0` (M 4.7, Guam): live API feed → MinIO `usgs/2026/11/11/usgs_20261111T091047/page_001.json.gz`
  → Kafka `raw.usgs` partition 0 offset 5 (key `us7000txi0`, same content hash) → `staging.source_records` id 6
  → `disaster_events` + `earthquake_details` (mag 4.7, depth 137.37 km, severity `light`, started 2026-11-11 08:37:15 UTC).
- **Duplicate prevention (overlapping re-ingestion)**: manual re-runs of all five sources published USGS 0/204, FIRMS 0/80,071, EONET 0/873,
  Open-Meteo 0/50. GDACS published 2/467, and both were genuine source revisions (`episodeid` and `datemodified` changed, verified field by field).
- **Revision handling**: GDACS `FL1104253` episodes 24 → 5 → 6 are all kept in `gdacs_alert_history`, and `disaster_events` holds the latest revision.
- **Spark idempotency**: all 81,678 staging rows were reset to unprocessed and re-run. Counts of every final table and analytics sum were identical before and after.
- **Validation DAG**: manual run passed every check (0 stale sources, 0 backlog, staging vs final reconciliation 100% for usgs/gdacs/eonet/firms).
  The scheduled 03:00 run fired at first start-up, before FIRMS/EONET data existed, and correctly **failed** (stale sources, 0% reconciliation).
- **Secrets**: the FIRMS key appears in no container log, Airflow log or MinIO object.

## Live run (09:22:27 – 09:37:48 UTC, 15 min 21 s)
- The producers, consumer, Airflow schedules and Spark (scheduled 09:30 run) kept running. No container restarted and no ingestion run failed.
- New data during the run: 1 new and 1 revised USGS earthquake, 5 GDACS revisions. All were processed by the scheduled Spark run.
- Duplicate checks at the end: 0 duplicates in staging, disaster_events, fire_detections, weather_forecasts and event_geometries.
  There were no future timestamps and no FIRMS timestamps outside the 2-day window.
- **Result: PASS**

## Record counts (end of live run)
| Table | Rows |
|---|---|
| staging.source_records | 81,678 (usgs 208, gdacs 476, firms 80,071, eonet 873, openmeteo 50) |
| disaster_events | 1,548 (usgs 207, gdacs 468, eonet 873) |
| earthquake_details | 207 |
| gdacs_alert_history | 472 |
| event_geometries | 1,653 |
| fire_detections | 80,071 |
| weather_forecasts | 3,600 |
| analytics.daily_event_counts | 129 |
| analytics.daily_fire_grid | 7,806 cells (sum of detections = 80,071) |
| analytics.active_alerts (view) | 296 |
| analytics.latest_event_weather (view) | 50 |

## Not verified / limitations
- No long run beyond 15 minutes. The 2-hourly FIRMS and 3-hourly Open-Meteo schedules were seen firing once at start-up, not repeatedly.
- No source outage happened during the test, so recovery from a real API outage was not observed. Retries exist (3 HTTP attempts, plus Airflow task retries and a producer loop that continues after errors).
- No active GDACS Orange/Red alerts existed during the test (3 Orange alerts, all closed), so `/alerts/active?alert_level=orange` returned an empty list.
- `replay_missing` only re-publishes records the API still returns. FIRMS records older than the 2-day window cannot be replayed from the API (they are still in MinIO).
- Airflow runs in `standalone` mode with login disabled. This is fine for local coursework, not for a shared server.

## Remaining blockers
None.
