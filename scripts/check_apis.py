"""Preflight check of the five live APIs. Usage: python scripts/check_apis.py"""
import csv, io, os, time
from datetime import datetime, timedelta, timezone
import requests

# Use .env to fill in settings absent from the shell environment.
if os.path.exists(".env"):
    for line in open(".env"):
        if "=" in line and not line.startswith("#"):
            k, v = line.strip().split("=", 1)
            os.environ.setdefault(k, v)

KEY = os.environ.get("FIRMS_MAP_KEY", "")
today = datetime.now(timezone.utc).date()


def fetch(url, params=None):
    """Check an upstream endpoint with at most two HTTP attempts.

    Args:
        url: Endpoint URL.
        params: Optional query parameters.

    Returns:
        A tuple of the response, elapsed seconds, and error text. On failure, the
        response is None and the FIRMS key is masked in the error.
    """
    err = None
    for _ in range(2):
        t = time.time()
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 200:
                return r, time.time() - t, None
            err = f"HTTP {r.status_code}: {r.text[:200]}"
        except requests.RequestException as e:
            err = type(e).__name__
        time.sleep(2)
    return None, 0, err.replace(KEY, "***") if KEY else err


def check_usgs(r):
    """Check the shape of a USGS earthquake feed response.

    Args:
        r: HTTP response containing the GeoJSON feed.

    Returns:
        The event count and representative fields from the first event.
    """
    feats = r.json()["features"]
    f = feats[0]
    fields = {"id": f.get("id"), "time": f["properties"].get("time"), "updated": f["properties"].get("updated"),
              "mag": f["properties"].get("mag"), "coordinates": f["geometry"].get("coordinates")}
    return len(feats), fields


def check_gdacs(r):
    """Check the shape of a GDACS event feed response.

    Args:
        r: HTTP response containing the GeoJSON feed.

    Returns:
        The event count and sample alert, date, and coordinate fields.
    """
    feats = r.json()["features"]
    p = feats[0]["properties"]
    fields = {k: p.get(k) for k in ["eventtype", "eventid", "episodeid", "alertlevel", "fromdate", "todate", "datemodified", "country"]}
    fields["coordinates"] = feats[0]["geometry"].get("coordinates")
    return len(feats), fields


def check_firms(r):
    """Check the FIRMS CSV header and sample detection fields.

    Args:
        r: HTTP response containing the CSV feed.

    Returns:
        The detection count and sample values keyed by CSV column.
    """
    rows = list(csv.DictReader(io.StringIO(r.text)))
    header = r.text.splitlines()[0].split(",")
    fields = {h: (rows[0][h] if rows else "(no rows)") for h in header}
    return len(rows), fields


def check_eonet(r):
    """Check the shape of an EONET event feed response.

    Args:
        r: HTTP response containing the JSON feed.

    Returns:
        The event count and sample category, closure, and geometry fields.
    """
    events = r.json()["events"]
    e = events[0]
    fields = {"id": e.get("id"), "title": e.get("title"), "categories": [c["id"] for c in e.get("categories", [])],
              "closed": e.get("closed"), "geometry": [(g.get("date"), g.get("type")) for g in e.get("geometry", [])][:2]}
    return len(events), fields


def check_meteo(r):
    """Check the expected hourly arrays in an Open-Meteo forecast.

    Args:
        r: HTTP response containing a single-location forecast.

    Returns:
        The forecast hour count and sample values for the expected arrays.
    """
    h = r.json()["hourly"]
    fields = {k: (h[k][:2] if k in h else None) for k in ["time", "temperature_2m", "precipitation", "wind_speed_10m", "relative_humidity_2m"]}
    return len(h["time"]), fields


SOURCES = [
    ("USGS", "GeoJSON", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson", None, check_usgs),
    ("GDACS", "GeoJSON", "https://www.gdacs.org/gdacsapi/api/Events/geteventlist/search",
     {"eventlist": "EQ;TC;FL;VO;DR;WF", "fromDate": str(today - timedelta(days=7)), "toDate": str(today), "pageSize": 100}, check_gdacs),
    # Small 5x4 degree box over the central African savanna, 2 days (1 day = current UTC date only) of VIIRS NOAA-20 data
    ("FIRMS", "CSV", f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{KEY}/VIIRS_NOAA20_NRT/20,8,25,12/2", None, check_firms),
    ("EONET", "JSON", "https://eonet.gsfc.nasa.gov/api/v3/events", {"status": "all", "days": 30, "limit": 5}, check_eonet),
    ("Open-Meteo", "JSON", "https://api.open-meteo.com/v1/forecast",
     {"latitude": 35.68, "longitude": 139.69, "forecast_days": 1,
      "hourly": "temperature_2m,precipitation,wind_speed_10m,relative_humidity_2m"}, check_meteo),
]

if __name__ == "__main__":
    if not KEY:
        print("FIRMS_MAP_KEY missing"); raise SystemExit(1)
    failed = False
    for name, fmt, url, params, check in SOURCES:
        r, secs, err = fetch(url, params)
        if err:
            failed = True
            print(f"\n[FAIL] {name}: {err}")
            continue
        try:
            n, fields = check(r)
            missing = [k for k, v in fields.items() if v in (None, "", [])]
            print(f"\n[PASS] {name}: HTTP {r.status_code}, {fmt} ({r.headers.get('content-type')}), "
                  f"{n} records, {secs:.2f}s, rate-limit headers: "
                  f"{ {k: v for k, v in r.headers.items() if 'limit' in k.lower()} or 'none'}")
            print(f"  missing/empty fields: {missing or 'none'}")
            for k, v in fields.items():
                print(f"  {k}: {str(v)[:90]}")
        except (ValueError, KeyError, IndexError) as e:
            failed = True
            print(f"\n[FAIL] {name}: HTTP 200 but parsing failed: {type(e).__name__} {e}")
    raise SystemExit(1 if failed else 0)
