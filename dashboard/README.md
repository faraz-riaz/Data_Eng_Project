# World in Motion: Global Hazard Activity

This local dashboard uses the existing Natural Disaster Pipeline FastAPI backend. Use the switch on the map to choose events or hotspots. Click an event or alert to view details and available forecasts.

## Run

Start the main project's pipeline using its root README. Then run this command from the repository root with Python 3.10 or later.

```sh
python3 dashboard/server.py
```

Open [http://127.0.0.1:5173](http://127.0.0.1:5173) in your browser. The dashboard forwards its GET requests to `http://127.0.0.1:8000/api/v1` by default. You can set a different backend with `DASHBOARD_API_URL` or `--api-url`, and change the local port with `--port`. The Python server provides a same-origin connection with no CORS configuration or extra Python packages.

```sh
python3 dashboard/server.py --port 5173 --api-url http://127.0.0.1:8000
```

Use the refresh button to update immediately. The dashboard refreshes once a minute while the tab is visible. The data source bar reports upstream freshness, which follows each feed's ingestion schedule.

## How to read the dashboard

- Time filters use inclusive UTC calendar dates. The trend and hazard charts use daily analytics globally and the filtered event records for a specific region. Trends count records stored by the pipeline on each day. Feed coverage varies by date and source.
- Region options use approximate geographic bounds. The Reported Regions card counts the distinct location labels supplied by the feeds. Sources can report the same incident.
- Active Alerts shows the current GDACS count for the selected hazard and region. Its sparkline ends today and earlier counts are estimated from the stored event dates. The estimated comparison uses the active count at the end of the preceding period.
- Hotspots shows the relative density of FIRMS detections over the selected date range and region. Click a heat area to inspect its nearest 0.5° cell. Large results use the 8,000 busiest cells.
- Event details include the newest available hourly forecast for eligible sites. The dashboard displays unavailable data when a request fails and indicates when it retains previously loaded data.

## Assets

- World basemap, Natural Earth I shaded relief with water. [Natural Earth raster](https://www.naturalearthdata.com/downloads/50m-raster-data/50m-natural-earth-1/)
- Map library, Leaflet 1.9.4. [Leaflet](https://leafletjs.com/) · [BSD license](public/assets/leaflet.LICENSE)
- Heatmap layer, Leaflet.heat 1.0.0. [Leaflet.heat](https://github.com/Leaflet/Leaflet.heat) · [BSD license](public/assets/leaflet-heat.LICENSE)
- Typography, Manrope. [SIL Open Font License](public/assets/manrope.LICENSE)
