"""REST API over the cleaned PostgreSQL tables. Never calls the external APIs. Docs: http://localhost:8000/docs"""
from datetime import date, datetime, timezone

from fastapi import FastAPI, HTTPException, Query
from psycopg2.extras import RealDictCursor

from ingest import db

app = FastAPI(title="Natural Disaster API", version="1.0")


def query(sql, params=()):
    conn = db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


def where(*conditions, bbox=None, lat="latitude", lon="longitude"):
    """Build a WHERE clause from (sql, value) pairs, skipping filters that were not given."""
    conds = [c for c in conditions if c[1] is not None]
    if bbox:
        try:
            w, s, e, n = (float(x) for x in bbox.split(","))
        except ValueError:
            raise HTTPException(400, "bbox must be west,south,east,north")
        conds += [(f"{lat} >= %s", s), (f"{lat} <= %s", n), (f"{lon} >= %s", w), (f"{lon} <= %s", e)]
    return " AND ".join(c[0] for c in conds) or "TRUE", [c[1] for c in conds]


def page(rows, limit, offset):
    return {"count": len(rows), "limit": limit, "offset": offset, "data": rows}


# Shared query parameters
FROM = Query(None, alias="from", description="UTC start, ISO-8601")
TO = Query(None, alias="to", description="UTC end, ISO-8601")
BBOX = Query(None, description="west,south,east,north")
LIMIT = Query(100, ge=1, le=5000)
OFFSET = Query(0, ge=0)


@app.get("/api/v1/health")
def health():
    return {"status": "ok", "database_time": query("SELECT now() AS t")[0]["t"]}


@app.get("/api/v1/events")
def events(source: str = None, disaster_type: str = None, status: str = None, start: datetime = FROM,
           end: datetime = TO, bbox: str = BBOX, limit: int = LIMIT, offset: int = OFFSET):
    clause, params = where(("source = %s", source), ("disaster_type = %s", disaster_type), ("status = %s", status),
                           ("started_at >= %s", start), ("started_at <= %s", end), bbox=bbox)
    rows = query(f"SELECT * FROM disaster_events WHERE {clause} ORDER BY started_at DESC NULLS LAST LIMIT %s OFFSET %s",
                 params + [limit, offset])
    return page(rows, limit, offset)


@app.get("/api/v1/events/{event_id}")
def event(event_id: str):
    rows = query("SELECT * FROM disaster_events WHERE event_id = %s", (event_id,))
    if not rows:
        raise HTTPException(404, "event not found")
    result = rows[0]
    details = query("SELECT magnitude, magnitude_type, depth_km, tsunami_flag, severity FROM earthquake_details WHERE event_id = %s", (event_id,))
    result["earthquake"] = details[0] if details else None
    result["alerts"] = query("SELECT episode_id, alert_level, alert_score, observed_at FROM gdacs_alert_history "
                             "WHERE event_id = %s ORDER BY observed_at", (event_id,))
    result["geometries"] = query("SELECT observed_at, geometry_type, geometry_json, magnitude_value, magnitude_unit "
                                 "FROM event_geometries WHERE event_id = %s ORDER BY observed_at", (event_id,))
    return result


@app.get("/api/v1/earthquakes")
def earthquakes(min_magnitude: float = None, max_magnitude: float = None, max_depth_km: float = None, severity: str = None,
                start: datetime = FROM, end: datetime = TO, bbox: str = BBOX, limit: int = LIMIT, offset: int = OFFSET):
    clause, params = where(("q.magnitude >= %s", min_magnitude), ("q.magnitude <= %s", max_magnitude),
                           ("q.depth_km <= %s", max_depth_km), ("q.severity = %s", severity),
                           ("e.started_at >= %s", start), ("e.started_at <= %s", end), bbox=bbox, lat="e.latitude", lon="e.longitude")
    rows = query(f"""SELECT e.event_id, e.title, e.status, e.started_at, e.updated_at, e.latitude, e.longitude, e.country,
                            q.magnitude, q.magnitude_type, q.depth_km, q.tsunami_flag, q.severity
                     FROM disaster_events e JOIN earthquake_details q USING (event_id)
                     WHERE {clause} ORDER BY e.started_at DESC LIMIT %s OFFSET %s""", params + [limit, offset])
    return page(rows, limit, offset)


@app.get("/api/v1/alerts/active")
def active_alerts(alert_level: str = None, disaster_type: str = None, bbox: str = BBOX, limit: int = LIMIT, offset: int = OFFSET):
    clause, params = where(("alert_level = %s", alert_level.lower() if alert_level else None),
                           ("disaster_type = %s", disaster_type), bbox=bbox)
    rows = query(f"""SELECT * FROM analytics.active_alerts WHERE {clause}
                     ORDER BY array_position(ARRAY['red','orange','green'], alert_level), updated_at DESC
                     LIMIT %s OFFSET %s""", params + [limit, offset])
    return page(rows, limit, offset)


@app.get("/api/v1/fires/heatmap")
def fire_heatmap(day: date = None, start: date = FROM, end: date = TO, min_count: int = None, bbox: str = BBOX,
                 limit: int = Query(5000, ge=1, le=50000), offset: int = OFFSET):
    if not (day or start or end):   # default: most recent day with data
        day = query("SELECT max(day) AS d FROM analytics.daily_fire_grid")[0]["d"]
    clause, params = where(("day = %s", day), ("day >= %s", start), ("day <= %s", end), ("detection_count >= %s", min_count),
                           bbox=bbox, lat="grid_lat", lon="grid_lon")
    rows = query(f"SELECT * FROM analytics.daily_fire_grid WHERE {clause} ORDER BY day DESC, detection_count DESC "
                 f"LIMIT %s OFFSET %s", params + [limit, offset])
    return {"grid_size_deg": 0.5, **page(rows, limit, offset)}


@app.get("/api/v1/fires/detections")
def fire_detections(start: datetime = FROM, end: datetime = TO, confidence: str = None, min_frp: float = None,
                    bbox: str = BBOX, limit: int = LIMIT, offset: int = OFFSET):
    clause, params = where(("acquired_at >= %s", start), ("acquired_at <= %s", end), ("confidence = %s", confidence),
                           ("frp >= %s", min_frp), bbox=bbox)
    rows = query(f"SELECT * FROM fire_detections WHERE {clause} ORDER BY acquired_at DESC, detection_id LIMIT %s OFFSET %s",
                 params + [limit, offset])
    return page(rows, limit, offset)


@app.get("/api/v1/weather")
def weather(event_id: str = None, start: datetime = FROM, end: datetime = TO, limit: int = LIMIT, offset: int = OFFSET):
    if not event_id:   # without an event: next forecast hour for every event
        rows = query("SELECT * FROM analytics.latest_event_weather ORDER BY event_id LIMIT %s OFFSET %s", (limit, offset))
        return page(rows, limit, offset)
    clause, params = where(("forecast_time >= %s", start), ("forecast_time <= %s", end))
    # hourly forecast from the most recent retrieval for this event
    rows = query(f"""SELECT * FROM weather_forecasts
                     WHERE event_id = %s AND retrieved_at = (SELECT max(retrieved_at) FROM weather_forecasts WHERE event_id = %s)
                       AND {clause} ORDER BY forecast_time LIMIT %s OFFSET %s""", [event_id, event_id] + params + [limit, offset])
    return page(rows, limit, offset)


@app.get("/api/v1/analytics/daily")
def daily_counts(source: str = None, disaster_type: str = None, start: date = FROM, end: date = TO,
                 limit: int = Query(1000, ge=1, le=10000), offset: int = OFFSET):
    clause, params = where(("source = %s", source), ("disaster_type = %s", disaster_type), ("day >= %s", start), ("day <= %s", end))
    rows = query(f"SELECT * FROM analytics.daily_event_counts WHERE {clause} ORDER BY day DESC, source, disaster_type "
                 f"LIMIT %s OFFSET %s", params + [limit, offset])
    return page(rows, limit, offset)


@app.get("/api/v1/system/freshness")
def freshness():
    rows = query("""SELECT *, round(extract(epoch FROM now() - last_success_at) / 60) AS minutes_since_success,
                           now() - last_success_at > 2 * expected_minutes * interval '1 minute' AS is_stale
                    FROM analytics.source_freshness""")
    return {"checked_at": datetime.now(timezone.utc), "sources": rows}
