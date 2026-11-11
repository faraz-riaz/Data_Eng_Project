# API Preflight Results

Run: 2026-11-11 (UTC) with `python scripts/check_apis.py` (real requests, 20 s timeout, max 2 attempts). All five passed.

| Source | Endpoint tested | HTTP | Format | Parsed | Records | Time | Auth / rate limit |
|---|---|---|---|---|---|---|---|
| USGS | `summary/all_day.geojson` | 200 | GeoJSON | yes | 200 | 0.8 s | none |
| GDACS | `gdacsapi/api/Events/geteventlist/search` (last 7 days) | 200 | GeoJSON | yes | 3 (default) / 100+ with all levels | 0.7–1.0 s | none |
| FIRMS | `api/area/csv/{key}/VIIRS_NOAA20_NRT/20,8,25,12/2` | 200 | CSV | yes | 5168 | 1.4–8 s | MAP_KEY valid; 5000 transactions / 10 min |
| EONET | `api/v3/events?status=all&days=30&limit=5` | 200 | JSON | yes | 5 | 2–4 s | `X-RateLimit-Limit: 60` |
| Open-Meteo | `v1/forecast` (Tokyo, 24 h) | 200 | JSON | yes | 24 hourly rows | 0.6 s | `X-RateLimit-Limit: 600 / 60 s` |

No expected fields were missing.

## Fields verified
- **USGS**: `id`, `properties.time` / `updated` (epoch ms), `mag`, `geometry.coordinates` [lon, lat, depth].
- **GDACS**: `eventtype`, `eventid`, `episodeid`, `alertlevel`, `fromdate`, `todate`, `datemodified`, `country`, Point geometry. The endpoint comes from the official Swagger spec (`/gdacsapi/swagger/v1/swagger.json`).
- **FIRMS**: CSV header `latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,satellite,instrument,confidence,version,bright_ti5,frp,daynight`.
- **EONET**: `id`, `title`, `categories[].id`, `closed`, `geometry[].date` / `type` / `coordinates`.
- **Open-Meteo**: `hourly.time`, `temperature_2m`, `precipitation`, `wind_speed_10m`, `relative_humidity_2m` as parallel arrays.

## Limitations and quirks found
- **GDACS** `search` returns only Orange/Red alerts by default. Pass `alertlevel=Green;Orange;Red` to get all alerts (100 in the last 7 days, one full page), so ingestion pages through the results.
- **FIRMS** `days=1` covers only the current UTC date. Early in the UTC day a small box can return a valid empty CSV (header only), so ingestion uses `days=2` and deduplicates. The global `world/2` request returned about 80k rows (6.7 MB) in 6.5 s.
- **FIRMS** `acq_time` is HHMM without zero padding (`13` means 00:13 UTC).
- **EONET** once answered with header `content-type: application/rss+xml` even though the body was JSON. Parsing ignores the header.
- The FIRMS key is read from `FIRMS_MAP_KEY` and never printed. Error messages mask it as `***`.
