# World in Motion presentation guide

This guide is context for writing a spoken script for **World in Motion**, the DSAI5102 project on global natural hazard monitoring. It follows the presentation and records the questions we worked through, with plain-language answers and details from the repository. The attached presentation reviewed here is `World_in_Motion_Presentation (3).html`. The Vanuatu earthquake is a worked example from the slides. The dashboard screenshots are a dated snapshot.

> **Prompt to use with this guide**
> I’m attaching my presentation and the notes I used to understand the project. Help me write a natural, slide-by-slide script in simple language. Start with what each stage does and why we chose it. Use the Vanuatu example to connect stages where it helps. Put the deeper implementation details in a separate professor Q&A at the end. Use the dated screenshot figures for the dashboard slides and follow the verified slide corrections in this guide.

## 1. What problem are we solving?

Earthquake reports, disaster alerts, satellite heat detections, and weather forecasts come from different services. They use different field names, formats, and update times. Looking at them together requires collecting them repeatedly, handling revisions, and giving the records a consistent shape for analysis.

World in Motion automates that work. It saves the original API responses, identifies new and revised records, prepares the data, and shows recent activity in an interactive dashboard. Users can explore where events were reported, what hazard categories make up the records, where satellite heat detections cluster, and what the forecast is near selected events.

**What does one count mean?** An event is a report from one of three event feeds. Two feeds may report the same incident, and the project counts those as two source records. A FIRMS detection is a satellite observation of a heat signal. One physical fire can produce several detections. An Open-Meteo row is a forecast for an event location and hour. Each of these records answers a different question.

## 2. What are the five sources and how often do we call them?

**What does it mean that USGS publishes a report?** A report is a machine-readable earthquake record. For example, it has an ID, an estimated magnitude, an occurrence time, a location, a depth, an update time, and a review status. An API is how another program requests records like this. A dataset is the collection of records it receives.

| Source | Format and data we request | Our polling interval | How the source changes |
|---|---|---|---|
| **USGS** | GeoJSON for all events in the past 24 hours. One feature holds an ID, properties such as magnitude, and coordinates in longitude, latitude, depth order. | Every **60 seconds** in a Python loop. | USGS documents a one-minute feed refresh. A refresh can show a new report, a revision, or the same records. |
| **GDACS** | Paginated GeoJSON of hazard alerts, with hazard type, event and episode IDs, alert level and score, dates, and location. The default search looks back seven days. | Every **10 minutes** in a Python loop. | Alerts change as GDACS receives updates and assessments. The 10-minute interval is our schedule. |
| **NASA FIRMS** | CSV of VIIRS NOAA-20 near-real-time detections. By default we ask for worldwide data covering today and yesterday in UTC. Rows include location, acquisition time, confidence, and fire radiative power. | Every **2 hours** in Airflow. | NASA describes global availability for this product in terms of latency, typically within about three hours of observation. Detections arrive with satellite passes and processing. |
| **NASA EONET** | JSON of open and closed curated natural events in the requested past 30 days by default. One event can contain several dated location observations. | Every **hour** in Airflow. | NASA describes EONET as continuously updated and curated. New events and observations appear as they are added. |
| **Open-Meteo** | JSON with three forecast days in UTC for selected event sites. Each response has hourly arrays for temperature, precipitation, wind, and humidity. | Every **3 hours** in Airflow. | The model and location affect the update cadence. Many global models update every six hours and some regional models update every one to three hours. Hourly values describe when the forecast applies. |

[Ingestion configuration](app/ingest.py) · [Airflow ingestion schedules](dags/ingestion_dags.py) · [USGS feed documentation](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php) · [GDACS API guide](https://www.gdacs.org/Documents/2025/GDACS_API_quickstart_v2.pdf) · [NASA FIRMS product overview](https://www.earthdata.nasa.gov/s3fs-public/2023-03/FIRMS_OnePager_2022_Prnt-Web.pdf) · [NASA EONET](https://eonet.gsfc.nasa.gov/) · [Open-Meteo model updates](https://open-meteo.com/en/docs/model-updates)

**Did the sources force us to choose these intervals?** We chose the schedules and lookback windows. We balanced how quickly we wanted to see new reports or revisions with the cost of repeated calls, the amount of data returned, and how much work each run does. USGS refreshes quickly, GDACS alert changes matter to users, FIRMS pulls a large area of satellite data, EONET is curated, and weather changes with its own model schedule and needs a site selection step.

**Why fetch USGS every minute if most records overlap?** The source documentation tells us the feed refreshes every minute and covers a rolling 24 hours. So two requests one minute apart cover almost the same time window. We check frequently to pick up changes promptly, and the hash check keeps unchanged records out of Kafka. The 24-hour window, the one-minute feed refresh, and our one-minute polling interval are three separate facts. Spark has its own 15-minute schedule, which also affects when a new report reaches the dashboard.

**Are these live streams?** They are live, changing data services that we pull through HTTP requests. The upstream services add records or revise them over time according to their own processes. Kafka is the event stream inside our system. It receives records after our producer fetches them and checks for changes.

## 3. How does data get from the source to the dashboard?

```text
Public APIs → Python ingestion → MinIO raw archive
→ Parse and check for changes → Kafka → Python consumer
→ PostgreSQL staging → Spark → PostgreSQL cleaned tables and summaries
→ FastAPI → Dashboard
```

**What does the Python producer do technically?** For USGS, the container runs `python ingest.py usgs 60`. The script calls `requests.get` on the USGS GeoJSON endpoint and checks the HTTP status. Each attempt has a configured 60-second timeout, and it tries up to three times with a five-second pause between failed attempts. A successful run archives the response, then reads it back to parse, hash, and publish changed records. The loop subtracts the time spent on the run from its target interval before sleeping. GDACS uses the same framework and a 600-second interval. It requests successive pages when needed. [HTTP requests and producer loop](app/ingest.py) · [Service commands](docker-compose.yml)

**What is Airflow, and does it replace the Python program?** Airflow is workflow orchestration software. It decides when a workflow should run, starts its tasks in dependency order, and records task logs and failures. A **DAG** is the definition of one workflow, including its tasks and the arrows that say which task must finish before another can start. Airflow calls the same shared Python ingestion functions to fetch, archive, and publish. The main difference is who starts the work. The two fast collectors use simple Python loops. Airflow gives the slower or multi-step jobs their own schedules, dependency graphs, logs, and retries. [Ingestion DAG definitions](dags/ingestion_dags.py)

**Are the three ingestion schedules one big DAG?** One Airflow service runs three independent ingestion DAGs. `ingest_firms` runs every two hours and `ingest_eonet` every hour. Each does **fetch and archive → publish changes**. `ingest_openmeteo` runs every three hours and does **select event sites → fetch and archive → publish changes**. The Spark and validation workflows are two more DAGs, described below.

**Why does Open-Meteo need coordinates first?** Weather forecasts are requested by location. At the start of its ingestion DAG, Open-Meteo site selection reads events that an earlier Spark run already cleaned. It selects active GDACS or EONET events with locations, plus USGS earthquakes of magnitude at least 4.5 that occurred in the past two days. It sorts by update time, rounds the coordinates to three decimal places, and uses up to 50 sites by default. The project associates the forecast responses with our event IDs by request order. After the forecasts are staged, a later Spark run prepares the hourly rows. This is why the weather ingestion DAG needs data from an earlier event transformation. [Site selection and ingest handling](app/ingest.py)

**What does “Open-Meteo Last run skipped” mean?** The workflow ran and found zero eligible event sites, so it recorded a skipped run and had no forecast request to make. That can happen during startup while `disaster_events` is empty. Once Spark loads eligible events, a later scheduled weather run can fetch forecasts for them. The source bar reports the ingestion outcome.

## 4. Why do we save the raw API response in MinIO?

**What is MinIO?** It is S3-compatible object storage. Think of a bucket as a container for files and their paths. In our project, MinIO stores the original API responses in the `raw-archive` bucket. This archive serves the raw data lake role. PostgreSQL later stores rows and columns we can query. We chose MinIO so the original responses stay available for inspection and traceability, and so the publish step can read the archived body, including when it runs as a separate Airflow task.

**What are `.gz` and `request.json`?** A `.gz` file uses gzip, short for GNU zip, to compress the response without losing the original bytes. Each response body becomes one `page_NNN.json.gz` file, or `.csv.gz` for FIRMS. Each successfully archived ingestion run also gets a `request.json` with the source, the run's collection start time in UTC, and the requested sites for weather when applicable. The site list lets the publisher match forecasts back to the original requests. USGS creates one response file per poll. GDACS can create multiple page files in one run. [Archive and read-back code](app/ingest.py)

**What is the difference between archive and staging?** MinIO keeps the whole response from a run, including overlapping records that appeared on previous calls. The PostgreSQL staging table holds individual record versions that passed through Kafka. The full response file is the source evidence. A staging row is one piece of that response plus the metadata needed to process and trace it.

## 5. How do change detection and Kafka work?

**Do we hash the ID or the content?** The producer hashes the parsed record payload with SHA-256 after sorting its JSON keys. Sorting keeps a difference in object key order from creating a new fingerprint. It looks up the last successfully published hash for the same `(source, record_id)`. For USGS the payload itself also contains the original `id`, so it is included in those hashed bytes. Our Kafka envelope metadata, such as `run_id` and `fetched_at`, is added after hashing. The ID is how we find the previous version, and the hash tells us whether the payload matches. [Hashing and publishing](app/ingest.py)

| What arrives | Producer decision |
|---|---|
| A record ID with no last-published hash | Publish the payload as a new record. |
| A known ID whose hash matches the last published hash | Skip publishing that unchanged payload. |
| A known ID with a different hash | Publish the full revised payload. The fingerprint identifies a difference at the record level. A field-by-field comparison would identify which values changed. |

**What exactly is Kafka?** Apache Kafka stores messages in named streams called topics. A producer writes messages, and a consumer reads them at its own pace. There is one topic per source in this project. The topics are `raw.usgs`, `raw.gdacs`, `raw.firms`, `raw.eonet`, and `raw.openmeteo`. Every USGS poll uses `raw.usgs` for its new and revised records. Kafka buffers those messages until the consumer can process them.

**What would I actually find in a `raw.usgs` Kafka message?** Its key is the record ID. Its JSON value includes `source`, `record_id`, `content_hash`, `fetched_at`, `run_id`, `archive_prefix`, and `payload`. The nested `payload` holds the source report, including the earthquake’s magnitude, dates, and geometry. The surrounding fields are our pipeline metadata. After Kafka confirms delivery, the producer saves the new hash in `ops.published_records`. [Message format and acknowledgments](app/ingest.py)

**If the magnitude goes from A to B and back to A, should we publish again?** Yes, when the provider issues a new revision that changes the magnitude back. For example, a USGS magnitude can go `5.1 → 5.3 → 5.1`. The third report has the same magnitude as the first but a later source update timestamp, so it is a new payload version. The producer publishes it, staging stores it, and Spark can make `5.1` current again. If an exact copy of the old payload reappears with the original old timestamp, the producer can still publish it because it differs from the last hash. Staging recognizes the already-saved `(source, record_id, content_hash)`, and the older revision does not replace the current event.

## 6. What does the Kafka consumer do with PostgreSQL?

**Does Kafka send data to PostgreSQL in batches?** The Python consumer script subscribes to the five Kafka topics and asks for up to 2,000 messages at a time. It checks the incoming fields and topic, then inserts valid rows in bulk into `staging.source_records`. Kafka supplies the messages and the consumer decides when and how to write them.

**What is an offset?** It’s the position of a message in a Kafka partition. Each topic in this local deployment has one partition. The consumer saves its progress with Kafka after PostgreSQL has committed the batch. For example, if a partition’s messages 20, 21, and 22 have been saved, the committed position tells the consumer to start at 23 next time.

**How can duplicates happen if the producer checks changes?** Suppose PostgreSQL successfully writes the batch and the consumer crashes before it tells Kafka its new position. After restart, the consumer may read those same messages again. The database has a unique key on `(source, record_id, content_hash)`, so an already saved version produces no second staging row. A producer can also encounter the exact old version example above, or resend after Kafka accepts a message and a later step fails to record the hash. Change detection reduces repeated publishes, while the staging key protects the database when a version arrives again. [Consumer batch and offset logic](app/consumer.py) · [Staging schema](sql/01_schema.sql)

**What does `processed_at = NULL` mean?** The consumer already saved the row and PostgreSQL already applied the duplicate constraint. `NULL` means it is pending Spark handling. Spark reads pending rows, writes the clean tables, then records its completion time in `processed_at` for the handled staging batch. A handled batch can include records that were filtered out during validation. `ingested_at` is when PostgreSQL saved the staging row. An event's `updated_at` describes source update time or, for EONET, the latest observation or closure time used by this project.

**Why do we use PostgreSQL twice?** The staging and cleaned tables live in the same project database. The original JSON stays in `staging.source_records`, whose progress flag Spark updates. Spark writes the cleaned event into a separate table such as `disaster_events`. After the Vanuatu example changes from 5.1 to 5.3, there are two distinct raw versions in staging and one current event row. The linked current `earthquake_details` row lists 5.3. GDACS alert history and EONET geometries are cleaned history tables with their own rules about what each row represents.

## 7. What does Spark transform and where does it save the result?

Spark runs **ETL**, which means **Extract, Transform, Load**. It extracts pending records from staging, transforms the incoming fields into standard columns, and loads the results into cleaned PostgreSQL tables. The event, fire, and weather jobs each read the sources they are responsible for. They bound a batch by the highest staging ID at the start so records arriving later can go into a subsequent batch. Spark writes batches through JDBC to intermediate `tmp` tables and uses SQL inserts or updates for the final tables. The six main cleaned tables and two summary tables are PostgreSQL tables. The archived response files live in MinIO.

| Source | Spark transformation in simple terms |
|---|---|
| **USGS** | Turn milliseconds since the Unix epoch into a timestamp, separate longitude, latitude, and depth, standardize the hazard type, check magnitude and depth ranges, convert the tsunami flag to a Boolean, and derive a magnitude band. The project uses the last part of the source’s place label for `country`, which can be a region. |
| **GDACS** | Translate a hazard code such as `TC` into `tropical_cyclone`, clean the country and alert labels, set active or closed status, and save one alert-history row per event, episode, and level. |
| **EONET** | Expand the dated geometry list into individual observations. Use the earliest observation as the start date and the latest for the representative map location. A point supplies its coordinates. For a polygon the project uses the center of the box enclosing that shape. Keep the underlying observation geometry too. |
| **FIRMS** | Combine the acquisition date and time, parse numbers, discard invalid locations or times, expand confidence labels, and assign each detection to a half-degree grid cell. Summarize detections for each cell and date. |
| **Open-Meteo** | Turn arrays of forecast times and measurements into one row per hour for each event and retrieval. Convert wind from km/h to m/s. Set out-of-range temperature or humidity and negative precipitation to `NULL`. Keep forecasts associated with known events. |

**Worked example for the event transformation slide.** The slides use Vanuatu report `us7000r1b7`, with magnitude revised to `5.3`. The input timestamp `1795409880000` becomes **23 November 2026 at 04:58 UTC**. The coordinates `[168.21, -17.74, 35.0]` give longitude `168.21`, latitude `-17.74`, and depth `35.0 km`. Spark builds `usgs:us7000r1b7`, saves the common event fields in `disaster_events`, and puts the earthquake measurements and `moderate` band in `earthquake_details`. For an existing event, the write checks `updated_at` so an older revision cannot replace a newer one. [Spark ETL](spark/etl.py)

## 8. Relational model and field definitions

A **primary key** identifies one row in a table. A **foreign key** links a row to an existing row in another table. `event_id` is the primary key of `disaster_events`. In `earthquake_details`, the same field is both a foreign key and the primary key, so a source event can have at most one earthquake detail row. GDACS history, EONET geometries, and forecasts can each have multiple rows linked to one event. [Full PostgreSQL schema](sql/01_schema.sql)

### `staging.source_records`

This is the waiting area between Kafka and Spark. The consumer stores one row per distinct source record version. The table supports traceability and lets Spark find pending work.

| Field | What it represents and why we keep it |
|---|---|
| `id` | PostgreSQL generates a row number that Spark can use to bound a batch. |
| `source` | The original feed name tells Spark which transformation to apply. |
| `record_id` | The ingestion record ID identifies the source record across versions. |
| `content_hash` | The SHA-256 fingerprint distinguishes versions of that record. |
| `payload` | The record content is stored as JSONB so Spark can read the source fields. |
| `fetched_at` | The ingestion run’s collection start time tells us when this version was collected. |
| `ingested_at` | PostgreSQL sets the insert time so we can measure how long the row has waited. |
| `processed_at` | Spark sets this timestamp after handling the row, and `NULL` marks pending work. |
| `run_id` | The ingestion run identifier links the record back to the run and its archive path. |
| `kafka_partition` | The Kafka partition tells us which part of the topic held the message. |
| `kafka_offset` | The Kafka message position helps trace the row back to the message. |

### `disaster_events`

This is the main table for source event reports. It gives **USGS, GDACS, and NASA EONET** a common format so the dashboard can map, filter, and count their events together. Each row comes from one source. FIRMS heat detections have their own table, and Open-Meteo forecasts add weather to events already in this table.

| Field | What it represents and why we keep it |
|---|---|
| `event_id` | Spark builds the project-wide event key from the source and source event ID, such as `usgs:us7000r1b7`. |
| `source` | Spark sets the name of the feed that supplied the report. |
| `source_event_id` | This links back to the provider’s identifier, using USGS or EONET’s ID or a GDACS hazard-code-and-ID combination. |
| `disaster_type` | Spark maps the source category to a common hazard type used by the dashboard filters and charts. |
| `title` | The source’s human-readable name labels the event in the dashboard. |
| `status` | The source-specific state describes USGS review status or whether the GDACS or EONET event is open or closed. |
| `started_at` | The event start time, or earliest dated EONET observation, places it on the trend chart. |
| `ended_at` | A reported closure time helps show when a GDACS or EONET event ended, and the project leaves this field empty for USGS. |
| `latitude` | The event’s north–south coordinate places its marker on the map. |
| `longitude` | The event’s east–west coordinate places its marker on the map. |
| `country` | The source location label helps describe the event, with a derived label for USGS and an empty value for EONET. |
| `updated_at` | The time of the source revision, or the latest observation or closure for EONET, helps choose which version to keep current. |
| `processed_at` | The database sets the time when Spark saves the cleaned event row. |

**Do all the sources provide all these fields?** The event table takes three sources, and Spark maps what each one provides. For example, USGS calls its title `properties.title`, GDACS calls it `properties.name`, and EONET calls it `title`. These all fill the same table column. Spark calculates some values, such as the event key or EONET's start date, and leaves unavailable optional fields as `NULL`.

**How did we derive `status`?** For USGS, Spark lowercases `properties.status`. A value like `reviewed` means a scientist reviewed the report. For GDACS, Spark maps `properties.iscurrent = "true"` to `active` and other values to `closed`. For EONET, a `NULL` closure date means `active`, and a supplied closure date means `closed`. Read `status` together with `source` to understand what it describes. [Source mappings](spark/etl.py) · [USGS reviewed-report example](https://earthquake.usgs.gov/earthquakes/eventpage/ci41530471/origin/phase)

### `earthquake_details`

This holds the additional seismic fields the project saves for USGS reports. FastAPI looks it up by `event_id` when loading an event’s details. The dashboard uses it to show the **Seismic details** section.

| Field | What it represents and why we keep it |
|---|---|
| `event_id` | The project’s event ID links the seismic measurements to exactly one main event row. |
| `magnitude` | The reported numerical size of the earthquake appears in its details. |
| `magnitude_type` | The reported scale, such as `mb`, describes how to interpret the magnitude. |
| `depth_km` | The third USGS geometry coordinate gives the estimated depth in kilometers. |
| `tsunami_flag` | Spark converts the USGS tsunami flag to a Boolean for the detail view. |
| `severity` | Spark calculates a readable magnitude band from the cleaned magnitude. |

**Did we engineer `severity`?** Yes. USGS supplies a magnitude such as `5.3`. Spark assigns a band such as `moderate`, which covers cleaned magnitudes from 5.0 up to 6.0. The other bands are `micro` below 2, `minor` from 2, `light` from 4, `strong` from 6, `major` from 7, and `great` from 8. The dashboard labels this field **Magnitude band** in the event details. Programmatic filtering is available through `/api/v1/earthquakes?severity=moderate`. [Magnitude calculation](spark/etl.py) · [API routes and filters](app/api.py)

### `gdacs_alert_history`

The main disaster table stores one current event row. This table holds the alert levels the pipeline observed for a GDACS event, including its episodes. Imagine a cyclone is green, then orange during the same episode. There is one cyclone row in `disaster_events` and two history rows for it. The dashboard can use the latest stored level to display the alert badge and apply the **International Aid** filter, while the details show recent stored history. One row is kept per event, episode, and level. If the same color returns during the same episode, that color shares the existing row.

| Field | What it represents and why we keep it |
|---|---|
| `alert_id` | PostgreSQL generates an ID for this alert history row. |
| `event_id` | Our event key links the alert history to its disaster event. |
| `episode_id` | GDACS supplies the episode identifier so we can group levels within a source episode. |
| `alert_level` | GDACS supplies green, orange, or red, and Spark trims and lowercases the label for display and filtering. |
| `alert_score` | GDACS supplies a numerical score that the dashboard can show in the event’s alert history. |
| `observed_at` | Spark stores the earliest source update time seen for this event, episode, and level. |

The aid labels in the dashboard are **Green · Unlikely**, **Orange · Possible**, and **Red · Likely**. These use GDACS alert levels. The earthquake magnitude band describes earthquake size and is a different field. [Alert history writes](spark/etl.py) · [Alert selection and event details](app/api.py)

### `event_geometries`

EONET can report several dated positions or shapes for one event. This table allows us to keep those observations and show the recent ones in event details. For example, an EONET wildfire with observations on three different dates has one `disaster_events` row and three `event_geometries` rows, linked by `event_id`.

| Field | What it represents and why we keep it |
|---|---|
| `geometry_id` | PostgreSQL generates an identifier for the observation row. |
| `event_id` | The project event key links the observation to its EONET event. |
| `observed_at` | EONET’s observation date tells us when the location or shape was reported. |
| `geometry_type` | The source shape type tells us how to interpret coordinates such as a point or polygon. |
| `geometry_json` | Spark assembles the shape and coordinates into a GeoJSON object for that observation. |
| `magnitude_value` | An optional EONET measurement adds detail about the observation. |
| `magnitude_unit` | The source unit tells us how to read that optional measurement. |

### `fire_detections`

Each row is one FIRMS satellite heat detection. The individual detections support daily hotspot counts and can also be queried through the API. A detection is an independent observation with its own key.

| Field | What it represents and why we keep it |
|---|---|
| `detection_id` | Ingestion builds this text key from the satellite, acquisition date and time, latitude, and longitude to identify a detection. |
| `acquired_at` | Spark combines the source date and time so the detection can be grouped by UTC day. |
| `latitude` | FIRMS supplies the north–south position used to locate the detection. |
| `longitude` | FIRMS supplies the east–west position used to locate the detection. |
| `satellite` | The feed identifies the satellite, and Spark labels `N20` as `NOAA-20`. |
| `instrument` | The source identifies the instrument that made the observation. |
| `confidence` | FIRMS supplies a detection confidence code that Spark expands to a readable level. |
| `frp` | The reported fire radiative power in megawatts helps describe the detection’s intensity. |
| `bright_ti4` | The sensor’s I-4 channel brightness temperature in kelvin describes the observed thermal signal. |
| `daynight` | The source flag tells us whether the observation was taken in daytime or nighttime. |
| `grid_lat` | Spark calculates the latitude of the cell’s southwest corner for grouping. |
| `grid_lon` | Spark calculates the longitude of the cell’s southwest corner for grouping. |

**Did we engineer `confidence`?** FIRMS gives each detection a confidence code. Spark expands `l` to `low`, `n` to `nominal`, and `h` to `high`. We calculate the separate `high_confidence_count` summary by counting detections with the `high` label. If a box has five detections and two are high-confidence, the Hotspots popup reports both numbers. The color uses the total of five. [FIRMS transformations](spark/etl.py) · [NASA VIIRS product guide](https://www.earthdata.nasa.gov/s3fs-public/2024-07/VIIRS_C2_AF-375m_User_Guide_1.0.pdf)

### `weather_forecasts`

This table keeps the forecast tied to an existing event by `event_id`. Its rows support the **Open-Meteo forecast** section in the event window. One retrieval can have many hourly rows, and a later retrieval can give a different forecast for the same hour.

| Field | What it represents and why we keep it |
|---|---|
| `forecast_id` | PostgreSQL generates a row ID for this forecast hour. |
| `event_id` | Our event key attaches the forecast to the event whose coordinates were requested. |
| `latitude` | Open-Meteo supplies the latitude of the weather grid point used in the forecast. |
| `longitude` | Open-Meteo supplies the longitude of the weather grid point used in the forecast. |
| `retrieved_at` | We copy the collection time from staging so the user can tell how old the forecast is. |
| `forecast_time` | The source timestamp gives the hour for which the prediction is valid. |
| `temperature` | The forecast air temperature in degrees Celsius describes conditions at that hour. |
| `precipitation` | The forecast precipitation in millimeters describes the total for the preceding hour. |
| `wind_speed` | Spark converts the source’s ten-meter wind speed from km/h to m/s for the dashboard. |
| `humidity` | The forecast relative humidity percentage describes moisture in the air. |

**Why can “Retrieved” be two days earlier than “Valid hour”?** A forecast retrieved on 23 November at 06:20 UTC can predict the weather for 25 November at 05:00 UTC. `retrieved_at` is when we collected it. `forecast_time` is when the prediction applies. The event window asks for up to six upcoming hours from the latest saved retrieval for that event. Weather coverage depends on site selection and whether a forecast is available. [Forecast API query](app/api.py) · [Open-Meteo hourly variables](https://open-meteo.com/en/docs)

### `analytics.daily_event_counts`

Spark counts events from `disaster_events` by UTC day, source, and hazard type. These precomputed totals supply the global **Event Trend** and **Events by Hazard** charts when there is no aid-level filter. The dashboard summarizes individual filtered event rows for regional and aid-level views.

| Field | What it represents and why we keep it |
|---|---|
| `day` | Spark derives the UTC calendar date from the event start time. |
| `source` | The feed name lets us track each source’s contribution to a daily total. |
| `disaster_type` | The standardized hazard type lets the dashboard count and filter categories. |
| `event_count` | Spark calculates how many current source event rows belong to that day, source, and type. |

### `analytics.daily_fire_grid`

Spark turns individual FIRMS detections into one summary row per occupied cell and UTC day. The dashboard reads those summaries to draw the **Hotspots** map for the selected dates.

| Field | What it represents and why we keep it |
|---|---|
| `day` | Spark derives the UTC calendar date from detection acquisition times. |
| `grid_lat` | The latitude of the cell’s southwest corner identifies its position. |
| `grid_lon` | The longitude of the cell’s southwest corner identifies its position. |
| `detection_count` | Spark counts all detections in that cell and day for the heatmap. |
| `high_confidence_count` | Spark counts detections labeled high-confidence in that cell and day. |
| `avg_frp` | Spark calculates the average fire radiative power of valid readings in that cell and day. |
| `max_frp` | Spark calculates the highest fire radiative power reading in that cell and day. |

### Supporting tables and views

| Name | What it does |
|---|---|
| `ops.ingestion_runs` | Logs each poll’s time, outcome, archive location, and record totals for debugging and freshness. |
| `ops.published_records` | Tracks the latest successfully published content hash per source record for change detection. |
| `ops.validation_results` | Stores daily quality check results so the team can inspect and troubleshoot them. |
| `tmp` tables | Hold Spark's transfer batches before SQL loads the final tables. |
| `analytics.active_alerts` | A saved query combines active GDACS events and their latest stored levels for the Active Alerts panel. |
| `analytics.latest_event_weather` | A saved query supports requesting one upcoming forecast hour per event through the API. |
| `analytics.source_freshness` | A saved query joins ingestion and staging information so the API can report source status. |

## 9. How do individual FIRMS detections turn into colored patches?

**What is a `0.5°` cell?** Imagine invisible boxes across the map. Each one spans half a degree of latitude and half a degree of longitude. Degrees are the units used for those coordinates. For example, a box can run from `10.0°` to `10.5°` north and `20.0°` to `20.5°` east. It is about 56 km tall. Its width depends on how far it is from the equator.

1. **Put each detection in a box.** FIRMS supplies coordinates and an acquisition time. Spark finds the southwest corner of the right box. The slide example uses `(10.12, 20.31)`, `(10.34, 20.07)`, and `(10.40, 20.22)`. They all fit in the cell whose southwest corner is `(10.0, 20.0)`. Spark drops the example detection with latitude `95.00` because valid latitudes are between −90° and 90°.
2. **Count detections by cell and day.** Those three detections on the same day produce one summary row with a count of three. With FRP readings of `12`, `30`, and `9` MW, the mean is `17` MW and the maximum is `30` MW. If the box has two detections on the next day, the next day gets its own row with a count of two.
3. **Add the daily counts for the dates selected.** If the user selects both days, the dashboard reads the grid through `/api/v1/fires/heatmap` and combines `3 + 2` into `5`. It also sums high-confidence counts and keeps the highest FRP from the selected days.
4. **Draw the heat layer.** The map draws soft patches centered on the boxes. A higher count gives a cell a stronger patch, and nearby patches can blend. Leaflet.heat uses the square root of each count for display weight, which reduces how much the busiest cells dominate the colors. Clicking near a patch opens a popup with the count, high-confidence count, and peak FRP of a nearby cell. The colors represent concentrations of satellite detections.

[Spark grid aggregation](spark/etl.py) · [Dashboard date-range totals](dashboard/public/data.js) · [Hotspots drawing and click behavior](dashboard/public/map.js)

**Simple line to say aloud.** “Spark counts satellite heat detections by area and day. The dashboard adds up the selected days and uses color to show where detections are concentrated.”

## 10. The five Airflow DAGs

| DAG | When it runs | What it does |
|---|---|---|
| `ingest_firms` | Every two hours at minute `00` | Fetches and archives the FIRMS CSV, then publishes changes. |
| `ingest_eonet` | Hourly at minute `05` | Fetches and archives EONET events, then publishes changes. |
| `ingest_openmeteo` | Every three hours at minute `20` | Selects sites, fetches and archives forecasts, then publishes changes. |
| `spark_etl` | Every 15 minutes | Launches the Spark jobs for events, fires, and weather. |
| `daily_validation` | Daily at 03:00 UTC | Makes missing versions eligible for a later republish and runs data quality checks. |

**How do the Spark jobs depend on each other?** The event job must finish before the weather job because clean forecasts link to event IDs. The independent fires job is eligible to run in parallel. Airflow launches the jobs with `spark-submit` on the local Spark cluster and manages retries. PostgreSQL stores the input and outputs. [Spark DAG](dags/spark_etl.py) · [Spark jobs](spark/etl.py)

**What does the fifth DAG check?** It checks whether sources ran recently, whether staging has rows that have waited over an hour, and whether cleaned row counts reach at least 99% of distinct handled staging IDs for USGS, GDACS, EONET, and FIRMS. It also records failed ingestion runs and events with missing coordinates. It saves the results in `ops.validation_results`. The count comparison is a broad completeness check. The weather table has a different unit of observation and is outside this percentage check.

**Does the fifth DAG directly resend missing Kafka records?** It finds published versions that have been absent from staging for over 30 minutes and removes their tracking hashes. A later source poll can publish them again if the provider still returns them. The archived responses in MinIO remain available for inspection. A workflow that replays directly from the archive is a possible extension. [Daily validation code](dags/daily_validation.py)

## 11. How do FastAPI and the dashboard serve the data?

**Why use another API after collecting from five APIs?** The upstream APIs supply the original data. FastAPI is our own API for the cleaned project data. The browser requests a resource with filters, like an event list for a date range. The local dashboard server forwards the request to FastAPI, which validates query parameters, queries PostgreSQL with parameterized SQL, and returns JSON. The browser uses that JSON to render the dashboard. [Dashboard server](dashboard/server.py) · [FastAPI routes](app/api.py)

| Dashboard feature | What it reads or calculates |
|---|---|
| **Events map** | `/api/v1/events` gives matching event rows. The map puts a marker at each valid location and colors it by hazard type. |
| **Total Events and Event Trend** | Count events by UTC start date. The default global summary uses `/api/v1/analytics/daily`. Region and aid-level filtering uses matching `/api/v1/events` rows instead. |
| **Events by Hazard** | Groups selected event reports by hazard and shows each group’s share of the total. |
| **Reported Regions** | Counts distinct normalized location labels supplied or derived from source reports. A label can be a country, a region, or another place description. |
| **Active Alerts** | `/api/v1/alerts/active` supplies current GDACS alerts for the selected hazard, region, and aid level, ordered by alert level and update time. This count includes older events that are still active. Its small trend estimates past counts from recorded event start and end times. |
| **International aid filter** | Green, orange, or red filters event rows and active alerts using their latest stored GDACS level. The filtered event rows also drive the event charts. Hotspots uses the date and region filters. |
| **Event details** | `/api/v1/events/{event_id}` gets the general event and any source-specific details. `/api/v1/weather` adds upcoming forecast hours when available. |
| **Hotspots map** | `/api/v1/fires/heatmap` supplies daily grid rows. The dashboard merges them across days and renders up to the 8,000 busiest cells. |
| **Data Sources bar** | `/api/v1/system/freshness` gives the latest run status and timing for each source. A skipped or failed run can be shown alongside persisted dashboard data. |

**Why does Last 7 days request 14 days in the Data Serving slide?** The current seven days drive the displayed total. The dashboard fetches the previous seven too, so it can calculate the percentage change. The slide’s walkthrough uses 17–23 November for the current period and 10–16 November for comparison. The actual event request sends full UTC start and end timestamps. [Date and event request logic](dashboard/public/data.js) · [Dashboard refresh logic](dashboard/public/app.js)

**Where is the Seismic details panel?** Set **Hazard** to **Earthquake**, keep **International aid** on **All aid levels**, and select **Events** on the map. Click a marker whose popup says **USGS**, then click **Event details** within the small popup. Inside the new window, below the general event information, **Seismic details** shows magnitude, depth, magnitude band, and the USGS tsunami flag. `Magnitude band` is the display name for `severity`. [Marker popup](dashboard/public/map.js) · [Detailed view](dashboard/public/app.js)

**Why did the China earthquake window show GDACS alert history?** The subtitle in the screenshot says **GDACS · Earthquake**. That window displays the event fields, the stored green GDACS alert and score for episode `1748238`, and its available Open-Meteo forecast. A USGS marker opens the same kind of window with the seismic fields if its linked `earthquake_details` row is present. These views illustrate how the same event interface uses source-specific tables.

**What does the crosshair below the zoom buttons do?** It resets the map’s position and zoom to the selected region. Under **All regions** it goes back to the world view. Under **Asia**, for example, it fits the map to Asia. [Map controls](dashboard/public/map.js)

## 12. What results should I describe from the slides?

The **embedded dashboard screenshot** was refreshed on **24 November 2026 at 10:16 UTC**. The selected seven days shown are 18–24 November UTC, with the 24th still in progress. Use the numbers below when speaking about those screenshots.

| Screenshot observation | Simple interpretation |
|---|---|
| **406 event records** and **72 reported regions** | The selected dates contain 406 source records and 72 distinct reported location labels. |
| **483 active alerts** | This is the current GDACS active count for the displayed filters, including ongoing events that started before the selected seven-day window. |
| **82.3% earthquake** and **15.0% wildfire** | Earthquakes were the largest category of event reports in this snapshot. The share reflects which feeds cover and report each category. |
| **6,116 hotspot cells** | The Hotspots view shows occupied grid cells from FIRMS across the selected dates. Cells are areas containing one or more detections. |

The separate [technical report](report.md) uses a **23 November 2026** measurement and records 532 events, 204 active alerts, and 7,150 distinct hotspot cells. Use its date and figures together when discussing report evidence. For the Vanuatu transformation and the Data Serving walkthrough, introduce the values as teaching examples from the slides.

## 13. Verified corrections to give to the slide editor

| Visible slide title | What is wrong and how to fix it |
|---|---|
| **Data Storage · Three Layers** | The USGS archive example shows a second page file, but a USGS poll produces one body. Show `request.json` and `page_001.json.gz`, and remove `page_002.json.gz` from the example. |
| **Data Storage · Relational Model** | The `fire_detections.detection_id` type is wrong. Change it from `BIGSERIAL` to `TEXT`. The diagram’s `VARCHAR` and `NUMERIC` labels also differ from the schema. Use `TEXT` and `DOUBLE PRECISION` for the respective labels shown. |
| **Workflow Orchestration · Apache Airflow** | “Replay missing records” sounds like the validation task immediately sends them again. Change the label to **“Allow republishing on a later poll.”** The next poll can publish a missing version if the source still returns it. |
| **Technical Deep Dive · Reliability & Quality** | Change “lost messages re-published” to **“Missing records can be republished if a later poll returns them.”** In the **Idempotent updates** diagram, show **`disaster_events` · 1 event row** linked to **`earthquake_details` · M 5.3**. The magnitude field belongs to `earthquake_details`. |
| **Technical Deep Dive · Design Choices** | “The database never slows ingestion down” overstates the separation. Replace it with **“Kafka buffers incoming records so the consumer can load them into PostgreSQL at its own pace.”** The producer uses PostgreSQL for change detection and run tracking. |

[Ingestion code](app/ingest.py) · [PostgreSQL schema](sql/01_schema.sql) · [Validation workflow](dags/daily_validation.py)

The original deck also contains an unfinished **The Problem** hook. A spoken opening could be “When a natural hazard happens, the information we need may be scattered across earthquake feeds, disaster alerts, satellite detections, and weather services. We built World in Motion to bring those pieces into one place so we can explore them together.”

## 14. What limitations and extensions can I explain?

| Limitation | Possible extension |
|---|---|
| Several sources can report the same real incident, and the dashboard counts their records separately. | Match reports by time, location, and hazard type while keeping source IDs for traceability. |
| The sources cover different time windows, and a new local instance only has the data it has had time to collect. | Show source coverage beside the charts and collect longer history for fairer comparisons. |
| FIRMS reports heat detections and can observe the same location several times. | Combine detections with independently verified incident records when estimating confirmed wildfire counts. |
| Weather covers selected sites and can show model predictions from an earlier retrieval. | Show forecast age and site coverage alongside the forecast. |
| Reconciliation relies on the source still returning a missing version, and checks use aggregate row counts. | Add replay by archive run ID and compare record identities as well as totals. |
| The project runs locally with limited deployment resources. | Test larger workloads and longer runs, and design a cloud deployment for wider access. |

## 15. Live demo and practical questions

Run the backend from the repository root, following the environment setup in the [main README](README.md). Start Docker Desktop first if applicable. After the backend starts, keep the dashboard Python server running in its own terminal.

```bash
docker compose up -d --build
curl -fsS http://127.0.0.1:8000/api/v1/health
python3 dashboard/server.py
```

Open the [dashboard](http://127.0.0.1:5173). The [Airflow UI](http://127.0.0.1:8080), [FastAPI docs](http://127.0.0.1:8000/docs), [Spark UI](http://127.0.0.1:8081), and [MinIO console](http://127.0.0.1:9001) can show individual parts of the pipeline in a demo. The backend can be ready before Spark has populated the dashboard data. Check that the charts have data before presenting.

| What I saw | What it means and what to check |
|---|---|
| `no configuration file provided: not found` | Compose was run outside the folder containing `docker-compose.yml`. Change into the project directory and retry. |
| A Spark or PostgreSQL image pull reports `context deadline exceeded` | Docker timed out reaching the image registry. Retry the affected download with `docker compose pull spark-master` or `docker compose pull postgres`, then repeat the startup command. |
| **API offline** on a dashboard that loads visually | The browser loaded the dashboard site, but its requests to the local API failed. Check `docker compose ps`, the health endpoint, and `docker compose logs --tail=50 api`. |
| **Open-Meteo Last run skipped** | The forecast workflow had no eligible event sites in the cleaned tables at that run. Check Spark’s event job and wait for or trigger another weather run after eligible events are available. |

**A simple click-through for the presentation.** Show the date and hazard filters, open a USGS event to point out **Magnitude band**, open a GDACS event to show its alert history, and switch to **Hotspots** to show how a colored patch relates to a cell count. Explain the three record types as you go. A source event is a reported incident, a FIRMS row is a satellite detection, and a weather row is a prediction for a particular hour.
