"""DAG 5: daily validation / reconciliation.

1. replay_missing: records that were published to Kafka but never reached staging are removed from
   ops.published_records, so the next ingestion run publishes them again (if the API still returns them).
2. quality_checks: freshness, ETL backlog, staging-vs-final reconciliation. Results go to
   ops.validation_results; the task fails if a critical check fails.
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task

import ingest

CHECKS = [
    # (name, SQL returning one number, critical?, pass condition)
    ("stale_sources", "SELECT count(*) FROM analytics.source_freshness "
                      "WHERE last_success_at IS NULL OR last_success_at < now() - 2 * expected_minutes * interval '1 minute'",
     True, lambda v: v == 0),
    ("unprocessed_older_than_1h", "SELECT count(*) FROM staging.source_records "
                                  "WHERE processed_at IS NULL AND ingested_at < now() - interval '1 hour'", True, lambda v: v == 0),
    ("failed_runs_24h", "SELECT count(*) FROM ops.ingestion_runs WHERE status = 'failed' AND started_at > now() - interval '1 day'",
     False, lambda v: v == 0),
    ("events_missing_coordinates", "SELECT count(*) FROM disaster_events WHERE latitude IS NULL", False, lambda v: v == 0),
]
# staged distinct records vs rows in the final table (some may be rejected by validation)
for src, table, cond in [("usgs", "disaster_events", "source = 'usgs'"), ("gdacs", "disaster_events", "source = 'gdacs'"),
                         ("eonet", "disaster_events", "source = 'eonet'"), ("firms", "fire_detections", "TRUE")]:
    CHECKS.append((f"reconcile_{src}_pct", f"""SELECT round(100.0 * (SELECT count(*) FROM {table} WHERE {cond})
                    / greatest(1, (SELECT count(DISTINCT record_id) FROM staging.source_records
                                   WHERE source = '{src}' AND processed_at IS NOT NULL)), 2)""",
                   True, lambda v: v >= 99))


@dag(dag_id="daily_validation", schedule="0 3 * * *", start_date=datetime(2026, 11, 1), catchup=False,
     max_active_runs=1, default_args={"retries": 1, "retry_delay": timedelta(minutes=5)}, tags=["quality"], doc_md=__doc__)
def daily_validation():
    @task
    def replay_missing():
        rows = ingest.sql("""DELETE FROM ops.published_records p
            WHERE published_at < now() - interval '30 minutes'
              AND NOT EXISTS (SELECT 1 FROM staging.source_records s WHERE s.source = p.source
                              AND s.record_id = p.record_id AND s.content_hash = p.content_hash)
            RETURNING source""")
        print(f"{len(rows)} published records missing from staging, marked for re-publishing")
        return len(rows)

    @task
    def quality_checks():
        failed = []
        for name, query, critical, ok in CHECKS:
            value = float(ingest.sql(query)[0][0])
            passed = ok(value)
            ingest.sql("INSERT INTO ops.validation_results (check_name, value, passed) VALUES (%s, %s, %s)", (name, value, passed))
            print(f"{'PASS' if passed else 'FAIL'} {name} = {value}")
            if critical and not passed:
                failed.append(name)
        if failed:
            raise ValueError(f"critical checks failed: {failed}")

    replay_missing() >> quality_checks()


daily_validation()
