"""REST API over the cleaned PostgreSQL tables. Never calls the external APIs. Docs: http://localhost:8000/docs"""
from datetime import date, datetime, timezone

from fastapi import FastAPI, HTTPException, Query
from psycopg2.extras import RealDictCursor

from ingest import db

app = FastAPI(title="Natural Disaster API", version="1.0")


def query(sql, params=()):
    """Run a read query against PostgreSQL.

    Args:
        sql: SQL statement with placeholders for values.
        params: Values to bind to the placeholders.

    Returns:
        A list of rows keyed by column name.
    """
    conn = db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


def where(*conditions, bbox=None, lat="latitude", lon="longitude"):
    """Build a SQL filter from supplied values and an optional bounding box.

    Args:
        *conditions: Pairs of SQL predicates and values. None values omit the predicate.
        bbox: Comma-separated west, south, east, and north coordinates.
        lat: Latitude column name, optionally qualified with a table alias.
        lon: Longitude column name, optionally qualified with a table alias.

    Returns:
        A tuple of the SQL predicate and its bound values.

    Raises:
        HTTPException: If the bounding box cannot be parsed as four numbers.
    """
    conds = [c for c in conditions if c[1] is not None]
    if bbox:
        try:
            w, s, e, n = (float(x) for x in bbox.split(","))
        except ValueError:
            raise HTTPException(400, "bbox must be west,south,east,north")
        conds += [(f"{lat} >= %s", s), (f"{lat} <= %s", n), (f"{lon} >= %s", w), (f"{lon} <= %s", e)]
    return " AND ".join(c[0] for c in conds) or "TRUE", [c[1] for c in conds]


def page(rows, limit, offset):
    """Wrap one result page in the shared API response format.

    Args:
        rows: Rows returned for this page.
        limit: Maximum number of rows requested.
        offset: Number of rows skipped.

    Returns:
        A dictionary with the rows, their count, and pagination values.
    """
    return {"count": len(rows), "limit": limit, "offset": offset, "data": rows}


# Shared query parameters
FROM = Query(None, alias="from", description="UTC start, ISO-8601")
TO = Query(None, alias="to", description="UTC end, ISO-8601")
BBOX = Query(None, description="west,south,east,north")
LIMIT = Query(100, ge=1, le=5000)
OFFSET = Query(0, ge=0)


@app.get("/api/v1/health")
def health():
    """Check that the API can query PostgreSQL.

    Returns:
        A dictionary with the health status and database time.
    """
    return {"status": "ok", "database_time": query("SELECT now() AS t")[0]["t"]}


@app.get("/api/v1/events")
def events(source: str = None, disaster_type: str = None, status: str = None, start: datetime = FROM,
           end: datetime = TO, bbox: str = BBOX, limit: int = LIMIT, offset: int = OFFSET,
           alert_level: str = None):
    """List disaster events matching the supplied filters.

    Args:
        source: Source feed to include.
        disaster_type: Standardized hazard type.
        status: Source event status.
        start: Inclusive lower bound for the event start time.
        end: Inclusive upper bound for the event start time.
        bbox: Geographic bounds in west, south, east, north order.
        limit: Maximum number of events to return.
        offset: Number of matching events to skip.
        alert_level: Most recent stored GDACS alert level to match.

    Returns:
        A page of events ordered by descending start time.
    """
    # Alert history supplies the level for both open and closed GDACS events.
    latest_level = """(SELECT h.alert_level FROM gdacs_alert_history h
                       WHERE h.event_id = disaster_events.event_id
                       ORDER BY h.observed_at DESC, h.episode_id DESC LIMIT 1)"""
    clause, params = where(("source = %s", source), ("disaster_type = %s", disaster_type), ("status = %s", status),
                           (f"{latest_level} = %s", alert_level.lower() if alert_level else None),
                           ("started_at >= %s", start), ("started_at <= %s", end), bbox=bbox)
    rows = query(f"SELECT * FROM disaster_events WHERE {clause} "
                 "ORDER BY started_at DESC NULLS LAST, event_id LIMIT %s OFFSET %s",
                 params + [limit, offset])
    return page(rows, limit, offset)


@app.get("/api/v1/events/{event_id}")
def event(event_id: str):
    """Look up a disaster event and its source-specific details.

    Args:
        event_id: Event identifier with its source prefix.

    Returns:
        An event with earthquake details, GDACS alert history, and EONET observations.

    Raises:
        HTTPException: If the event does not exist.
    """
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
    """List USGS earthquakes matching the supplied filters.

    Args:
        min_magnitude: Inclusive minimum magnitude.
        max_magnitude: Inclusive maximum magnitude.
        max_depth_km: Inclusive maximum depth in kilometers.
        severity: Magnitude band to include.
        start: Inclusive lower bound for the earthquake time.
        end: Inclusive upper bound for the earthquake time.
        bbox: Geographic bounds in west, south, east, north order.
        limit: Maximum number of earthquakes to return.
        offset: Number of matching earthquakes to skip.

    Returns:
        A page of earthquake records and their event details.
    """
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
    """List current GDACS alerts in descending order of severity.

    Args:
        alert_level: GDACS alert level to include.
        disaster_type: Standardized hazard type.
        bbox: Geographic bounds in west, south, east, north order.
        limit: Maximum number of alerts to return.
        offset: Number of matching alerts to skip.

    Returns:
        A page of current GDACS alerts.
    """
    clause, params = where(("alert_level = %s", alert_level.lower() if alert_level else None),
                           ("disaster_type = %s", disaster_type), bbox=bbox)
    rows = query(f"""SELECT * FROM analytics.active_alerts WHERE {clause}
                     ORDER BY array_position(ARRAY['red','orange','green'], alert_level), updated_at DESC
                     LIMIT %s OFFSET %s""", params + [limit, offset])
    return page(rows, limit, offset)


@app.get("/api/v1/fires/heatmap")
def fire_heatmap(day: date = None, start: date = FROM, end: date = TO, min_count: int = None, bbox: str = BBOX,
                 limit: int = Query(5000, ge=1, le=50000), offset: int = OFFSET):
    """List daily FIRMS hotspot statistics by grid cell.

    Args:
        day: Exact UTC date. The newest available date is used if no dates are given.
        start: First UTC date to include.
        end: Last UTC date to include.
        min_count: Minimum number of detections per grid cell.
        bbox: Geographic bounds applied to cell origins.
        limit: Maximum number of cells to return.
        offset: Number of matching cells to skip.

    Returns:
        A page of daily hotspot cells with the grid size in degrees.
    """
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
    """List individual FIRMS satellite detections.

    Args:
        start: Inclusive lower bound for the detection time.
        end: Inclusive upper bound for the detection time.
        confidence: Confidence category to include.
        min_frp: Minimum fire radiative power in megawatts.
        bbox: Geographic bounds in west, south, east, north order.
        limit: Maximum number of detections to return.
        offset: Number of matching detections to skip.

    Returns:
        A page of satellite detection records.
    """
    clause, params = where(("acquired_at >= %s", start), ("acquired_at <= %s", end), ("confidence = %s", confidence),
                           ("frp >= %s", min_frp), bbox=bbox)
    rows = query(f"SELECT * FROM fire_detections WHERE {clause} ORDER BY acquired_at DESC, detection_id LIMIT %s OFFSET %s",
                 params + [limit, offset])
    return page(rows, limit, offset)


@app.get("/api/v1/weather")
def weather(event_id: str = None, start: datetime = FROM, end: datetime = TO, limit: int = LIMIT, offset: int = OFFSET):
    """List forecasts from the latest available retrieval.

    Args:
        event_id: Event to query. Omit to get one upcoming forecast per event.
        start: Earliest forecast time when an event is specified.
        end: Latest forecast time when an event is specified.
        limit: Maximum number of forecasts to return.
        offset: Number of matching forecasts to skip.

    Returns:
        A page of hourly forecasts for one event or upcoming forecasts across events.
    """
    if not event_id:   # without an event: next forecast hour for every event
        rows = query("SELECT * FROM analytics.latest_event_weather ORDER BY event_id LIMIT %s OFFSET %s", (limit, offset))
        return page(rows, limit, offset)
    clause, params = where(("forecast_time >= %s", start), ("forecast_time <= %s", end))
    # Keep all hours in the response from the same forecast retrieval.
    rows = query(f"""SELECT * FROM weather_forecasts
                     WHERE event_id = %s AND retrieved_at = (SELECT max(retrieved_at) FROM weather_forecasts WHERE event_id = %s)
                       AND {clause} ORDER BY forecast_time LIMIT %s OFFSET %s""", [event_id, event_id] + params + [limit, offset])
    return page(rows, limit, offset)


@app.get("/api/v1/analytics/daily")
def daily_counts(source: str = None, disaster_type: str = None, start: date = FROM, end: date = TO,
                 limit: int = Query(1000, ge=1, le=10000), offset: int = OFFSET):
    """List daily event counts by source and hazard type.

    Args:
        source: Source feed to include.
        disaster_type: Standardized hazard type.
        start: First UTC date to include.
        end: Last UTC date to include.
        limit: Maximum number of summary rows to return.
        offset: Number of matching rows to skip.

    Returns:
        A page of daily event counts.
    """
    clause, params = where(("source = %s", source), ("disaster_type = %s", disaster_type), ("day >= %s", start), ("day <= %s", end))
    rows = query(f"SELECT * FROM analytics.daily_event_counts WHERE {clause} ORDER BY day DESC, source, disaster_type "
                 f"LIMIT %s OFFSET %s", params + [limit, offset])
    return page(rows, limit, offset)


@app.get("/api/v1/system/freshness")
def freshness():
    """Report ingestion and processing status for the source feeds.

    Returns:
        A dictionary with the check time and freshness details for each feed.
    """
    rows = query("""SELECT *, round(extract(epoch FROM now() - last_success_at) / 60) AS minutes_since_success,
                           now() - last_success_at > 2 * expected_minutes * interval '1 minute' AS is_stale
                    FROM analytics.source_freshness""")
    return {"checked_at": datetime.now(timezone.utc), "sources": rows}
