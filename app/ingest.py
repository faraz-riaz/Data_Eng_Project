"""Ingestion for all five sources: request API -> archive in MinIO -> parse -> publish changed records to Kafka.

Used by the USGS/GDACS producer containers (python ingest.py usgs 60) and by the Airflow DAGs
(fetch_and_archive / publish_changes as separate tasks).
"""
import csv, gzip, hashlib, io, json, os, sys, time
from datetime import datetime, timedelta, timezone

import psycopg2
import requests
from confluent_kafka import Producer
from minio import Minio
from psycopg2.extras import execute_values

BUCKET = "raw-archive"
FIRMS_KEY = os.getenv("FIRMS_MAP_KEY", "")
# Development volume limits (documented in README); defaults keep the full live feeds
FIRMS_AREA = os.getenv("FIRMS_AREA", "world")
FIRMS_DAYS = os.getenv("FIRMS_DAYS", "2")          # 2 = today + yesterday (UTC), overlap is deduplicated
EONET_DAYS = os.getenv("EONET_DAYS", "30")
GDACS_DAYS = int(os.getenv("GDACS_DAYS", "7"))
WEATHER_MAX_SITES = int(os.getenv("WEATHER_MAX_SITES", "50"))


def db():
    return psycopg2.connect(host=os.getenv("POSTGRES_HOST", "postgres"), dbname=os.environ["POSTGRES_DB"],
                            user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"])


def sql(query, params=None):
    conn = db()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall() if cur.description else None
    finally:
        conn.close()


def minio():
    return Minio("minio:9000", os.environ["MINIO_ROOT_USER"], os.environ["MINIO_ROOT_PASSWORD"], secure=False)


def get(url, params=None):
    """GET with 3 attempts. The FIRMS key is part of the URL, so it is masked in errors."""
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=60)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            if attempt == 2:
                raise RuntimeError(str(e).replace(FIRMS_KEY, "***") if FIRMS_KEY else str(e)) from None
            time.sleep(5)


# ---------------------------------------------------------------- 1. request

def request_source(source, sites):
    """Return the raw response bodies (bytes) for one run. GDACS and Open-Meteo may need several requests."""
    today = datetime.now(timezone.utc).date()
    if source == "usgs":
        return [get("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson").content]
    if source == "gdacs":
        pages = []
        for page in range(1, 51):
            r = get("https://www.gdacs.org/gdacsapi/api/Events/geteventlist/search",
                    {"eventlist": "EQ;TC;FL;VO;DR;WF", "alertlevel": "Green;Orange;Red", "pageSize": 100, "pageNumber": page,
                     "fromDate": str(today - timedelta(days=GDACS_DAYS)), "toDate": str(today + timedelta(days=1))})
            if r.status_code == 204 or not r.content:   # past the last page
                break
            pages.append(r.content)
            if len(r.json()["features"]) < 100:
                break
        return pages
    if source == "firms":
        url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{FIRMS_KEY}/VIIRS_NOAA20_NRT/{FIRMS_AREA}/{FIRMS_DAYS}"
        body = get(url).content
        if not body.startswith(b"latitude,"):   # FIRMS returns plain-text errors with HTTP 200
            raise RuntimeError(f"Unexpected FIRMS response: {body[:200]!r}")
        return [body]
    if source == "eonet":
        return [get("https://eonet.gsfc.nasa.gov/api/v3/events", {"status": "all", "days": EONET_DAYS}).content]
    if source == "openmeteo":
        bodies = []
        for i in range(0, len(sites), 50):   # Open-Meteo accepts many coordinates per request
            batch = sites[i:i + 50]
            bodies.append(get("https://api.open-meteo.com/v1/forecast", {
                "latitude": ",".join(str(s["latitude"]) for s in batch),
                "longitude": ",".join(str(s["longitude"]) for s in batch),
                "hourly": "temperature_2m,precipitation,wind_speed_10m,relative_humidity_2m",
                "forecast_days": 3, "timezone": "UTC"}).content)
        return bodies
    raise ValueError(source)


def weather_sites():
    """Locations of current disasters (from the cleaned tables) to sample with Open-Meteo."""
    rows = sql("""
        SELECT e.event_id, round(e.latitude::numeric, 3), round(e.longitude::numeric, 3)
        FROM disaster_events e LEFT JOIN earthquake_details q USING (event_id)
        WHERE e.latitude IS NOT NULL AND (
              (e.source IN ('gdacs', 'eonet') AND e.status = 'active')
           OR (e.source = 'usgs' AND q.magnitude >= 4.5 AND e.started_at > now() - interval '2 days'))
        ORDER BY e.updated_at DESC LIMIT %s""", (WEATHER_MAX_SITES,))
    return [{"event_id": r[0], "latitude": float(r[1]), "longitude": float(r[2])} for r in rows]


# ---------------------------------------------------------------- 2+3. check and archive

def fetch_and_archive(source, sites=None):
    """Request the API and store the original responses in MinIO. Returns the run_id."""
    now = datetime.now(timezone.utc)
    run_id = f"{source}_{now:%Y%m%dT%H%M%S}"
    prefix = f"{source}/{now:%Y/%m/%d}/{run_id}/"
    sql("INSERT INTO ops.ingestion_runs (run_id, source, started_at, status) VALUES (%s, %s, %s, 'running')",
        (run_id, source, now))
    try:
        if source == "openmeteo" and not sites:
            sql("UPDATE ops.ingestion_runs SET status='skipped', finished_at=now(), error='no active event locations yet' "
                "WHERE run_id=%s", (run_id,))
            return run_id
        bodies = request_source(source, sites)
        mc = minio()
        if not mc.bucket_exists(BUCKET):
            mc.make_bucket(BUCKET)
        objects = [("request.json", json.dumps({"source": source, "fetched_at": now.isoformat(), "sites": sites}).encode())]
        ext = "csv" if source == "firms" else "json"
        objects += [(f"page_{i:03d}.{ext}.gz", gzip.compress(b)) for i, b in enumerate(bodies, 1)]
        for name, data in objects:
            mc.put_object(BUCKET, prefix + name, io.BytesIO(data), len(data))
        sql("UPDATE ops.ingestion_runs SET status='archived', archive_prefix=%s, pages=%s, bytes=%s WHERE run_id=%s",
            (prefix, len(bodies), sum(len(b) for b in bodies), run_id))
        return run_id
    except Exception as e:
        sql("UPDATE ops.ingestion_runs SET status='failed', finished_at=now(), error=%s WHERE run_id=%s", (str(e)[:500], run_id))
        raise


# ---------------------------------------------------------------- 4. parse

def parse(source, bodies, sites):
    """Split raw responses into (record_id, payload) pairs with stable source identifiers."""
    if source == "usgs":
        return {f["id"]: f for f in json.loads(bodies[0])["features"]}
    if source == "gdacs":
        feats = [f for b in bodies for f in json.loads(b)["features"]]
        return {f"{f['properties']['eventtype']}{f['properties']['eventid']}": f for f in feats}
    if source == "firms":
        rows = csv.DictReader(io.StringIO(bodies[0].decode()))
        # sensor + acquisition time + pixel location identifies one detection
        return {f"{r['satellite']}_{r['acq_date']}T{int(r['acq_time']):04d}_{r['latitude']}_{r['longitude']}": r for r in rows}
    if source == "eonet":
        return {e["id"]: e for e in json.loads(bodies[0])["events"]}
    if source == "openmeteo":
        results = []
        for b in bodies:
            data = json.loads(b)
            results += data if isinstance(data, list) else [data]   # a single location returns an object
        # responses come back in request order; generationtime_ms is dropped because it changes every call
        return {s["event_id"]: {"event_id": s["event_id"], "latitude": r["latitude"], "longitude": r["longitude"],
                                "hourly_units": r["hourly_units"], "hourly": r["hourly"]}
                for s, r in zip(sites, results)}
    raise ValueError(source)


# ---------------------------------------------------------------- 5+6. publish and record

def publish_changes(run_id):
    """Read the archived responses, publish only new or changed records to Kafka, record the result."""
    source, status, prefix, fetched_at = sql(
        "SELECT source, status, archive_prefix, started_at FROM ops.ingestion_runs WHERE run_id=%s", (run_id,))[0]
    if status == "skipped":
        return 0
    try:
        mc = minio()
        names = sorted(o.object_name for o in mc.list_objects(BUCKET, prefix))
        read = lambda name: mc.get_object(BUCKET, name).read()
        sites = json.loads(read(prefix + "request.json"))["sites"]
        bodies = [gzip.decompress(read(n)) for n in names if "/page_" in n]
        records = parse(source, bodies, sites)

        hashes = {rid: hashlib.sha256(json.dumps(p, sort_keys=True).encode()).hexdigest() for rid, p in records.items()}
        known = dict(sql("SELECT record_id, content_hash FROM ops.published_records WHERE source=%s AND record_id = ANY(%s)",
                         (source, list(records))))
        changed = [rid for rid in records if known.get(rid) != hashes[rid]]

        errors = []
        producer = Producer({"bootstrap.servers": "kafka:9092", "acks": "all", "enable.idempotence": True,
                             "linger.ms": 50, "queue.buffering.max.messages": 500000})
        for rid in changed:
            message = {"source": source, "record_id": rid, "content_hash": hashes[rid], "fetched_at": fetched_at.isoformat(),
                       "run_id": run_id, "archive_prefix": prefix, "payload": records[rid]}
            producer.produce(f"raw.{source}", key=rid, value=json.dumps(message),
                             on_delivery=lambda err, msg: err and errors.append(str(err)))
            producer.poll(0)
        if producer.flush(120) > 0 or errors:
            raise RuntimeError(f"Kafka delivery failed: {errors[:3]}")

        # remember what was published only after Kafka confirmed delivery
        conn = db()
        with conn, conn.cursor() as cur:
            execute_values(cur, """INSERT INTO ops.published_records (source, record_id, content_hash) VALUES %s
                ON CONFLICT (source, record_id) DO UPDATE SET content_hash = EXCLUDED.content_hash, published_at = now()""",
                           [(source, rid, hashes[rid]) for rid in changed], page_size=5000)
        conn.close()
        sql("UPDATE ops.ingestion_runs SET status='success', finished_at=now(), records_parsed=%s, records_published=%s "
            "WHERE run_id=%s", (len(records), len(changed), run_id))
        return len(changed)
    except Exception as e:
        sql("UPDATE ops.ingestion_runs SET status='failed', finished_at=now(), error=%s WHERE run_id=%s", (str(e)[:500], run_id))
        raise


def run_once(source):
    sites = weather_sites() if source == "openmeteo" else None
    run_id = fetch_and_archive(source, sites)
    published = publish_changes(run_id)
    print(f"{run_id}: published {published} new/changed records", flush=True)
    return run_id


if __name__ == "__main__":
    # Continuous producer: python ingest.py <source> <interval_seconds>   (interval 0 = run once)
    source, interval = sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 0
    while True:
        started = time.time()
        try:
            run_once(source)
        except Exception as e:
            print(f"{source} run failed: {e}", flush=True)
            if not interval:
                sys.exit(1)
        if not interval:
            break
        time.sleep(max(0, interval - (time.time() - started)))
