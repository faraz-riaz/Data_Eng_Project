-- Runs once on first PostgreSQL start (database POSTGRES_DB).
CREATE SCHEMA staging;    -- raw records written by the Kafka consumer
CREATE SCHEMA ops;        -- ingestion metadata
CREATE SCHEMA analytics;  -- Spark aggregates + views
CREATE SCHEMA tmp;        -- Spark writes batches here before SQL upserts

---------------------------------------------------------------- ops
CREATE TABLE ops.ingestion_runs (
    run_id            TEXT PRIMARY KEY,           -- <source>_<UTC timestamp>
    source            TEXT NOT NULL,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),  -- = API request time
    finished_at       TIMESTAMPTZ,
    status            TEXT NOT NULL,              -- running / archived / success / skipped / failed
    archive_prefix    TEXT,                       -- MinIO folder with the original responses
    pages             INT,
    bytes             BIGINT,
    records_parsed    INT,
    records_published INT,
    error             TEXT
);
CREATE INDEX ON ops.ingestion_runs (source, started_at DESC);

-- Last content hash sent to Kafka for every source record (producer-side dedup)
CREATE TABLE ops.published_records (
    source       TEXT NOT NULL,
    record_id    TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, record_id)
);

CREATE TABLE ops.validation_results (
    id         BIGSERIAL PRIMARY KEY,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    check_name TEXT NOT NULL,
    value      NUMERIC,
    passed     BOOLEAN NOT NULL
);

---------------------------------------------------------------- staging
-- One row per distinct version of a source record (same content is never stored twice)
CREATE TABLE staging.source_records (
    id              BIGSERIAL PRIMARY KEY,
    source          TEXT NOT NULL,
    record_id       TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    payload         JSONB NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,              -- when the producer called the API
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(), -- when the consumer stored it
    processed_at    TIMESTAMPTZ,                       -- set by Spark
    run_id          TEXT,
    kafka_partition INT,
    kafka_offset    BIGINT,
    UNIQUE (source, record_id, content_hash)
);
CREATE INDEX ON staging.source_records (source, id) WHERE processed_at IS NULL;
CREATE INDEX ON staging.source_records (source, ingested_at);

---------------------------------------------------------------- cleaned tables
CREATE TABLE disaster_events (
    event_id        TEXT PRIMARY KEY,               -- '<source>:<source_event_id>'
    source          TEXT NOT NULL CHECK (source IN ('usgs', 'gdacs', 'eonet')),
    source_event_id TEXT NOT NULL,
    disaster_type   TEXT NOT NULL,
    title           TEXT,
    status          TEXT,
    started_at      TIMESTAMPTZ,
    ended_at        TIMESTAMPTZ,
    latitude        DOUBLE PRECISION CHECK (latitude BETWEEN -90 AND 90),
    longitude       DOUBLE PRECISION CHECK (longitude BETWEEN -180 AND 180),
    country         TEXT,
    updated_at      TIMESTAMPTZ NOT NULL,           -- source revision time
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, source_event_id)
);
CREATE INDEX ON disaster_events (started_at);
CREATE INDEX ON disaster_events (disaster_type, started_at);
CREATE INDEX ON disaster_events (latitude, longitude);

CREATE TABLE earthquake_details (
    event_id       TEXT PRIMARY KEY REFERENCES disaster_events ON DELETE CASCADE,
    magnitude      DOUBLE PRECISION,
    magnitude_type TEXT,
    depth_km       DOUBLE PRECISION,
    tsunami_flag   BOOLEAN,
    severity       TEXT
);
CREATE INDEX ON earthquake_details (magnitude);

CREATE TABLE gdacs_alert_history (
    alert_id    BIGSERIAL PRIMARY KEY,
    event_id    TEXT NOT NULL REFERENCES disaster_events ON DELETE CASCADE,
    episode_id  INT NOT NULL,
    alert_level TEXT NOT NULL CHECK (alert_level IN ('green', 'orange', 'red')),
    alert_score DOUBLE PRECISION,
    observed_at TIMESTAMPTZ NOT NULL,              -- first time this level was seen
    UNIQUE (event_id, episode_id, alert_level)
);

CREATE TABLE event_geometries (
    geometry_id     BIGSERIAL PRIMARY KEY,
    event_id        TEXT NOT NULL REFERENCES disaster_events ON DELETE CASCADE,
    observed_at     TIMESTAMPTZ NOT NULL,
    geometry_type   TEXT NOT NULL,
    geometry_json   JSONB NOT NULL,                -- GeoJSON geometry (Point or Polygon)
    magnitude_value DOUBLE PRECISION,
    magnitude_unit  TEXT,
    UNIQUE (event_id, observed_at, geometry_type)
);

-- Satellite thermal anomalies; NOT confirmed wildfire events, so no link to disaster_events
CREATE TABLE fire_detections (
    detection_id TEXT PRIMARY KEY,                 -- date+time+lat+lon+satellite
    acquired_at  TIMESTAMPTZ NOT NULL,
    latitude     DOUBLE PRECISION NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude    DOUBLE PRECISION NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    satellite    TEXT,
    instrument   TEXT,
    confidence   TEXT,
    frp          DOUBLE PRECISION,                 -- fire radiative power, MW
    bright_ti4   DOUBLE PRECISION,
    daynight     TEXT,
    grid_lat     DOUBLE PRECISION NOT NULL,
    grid_lon     DOUBLE PRECISION NOT NULL
);
CREATE INDEX ON fire_detections (acquired_at);
CREATE INDEX ON fire_detections (latitude, longitude);

CREATE TABLE weather_forecasts (
    forecast_id   BIGSERIAL PRIMARY KEY,
    event_id      TEXT NOT NULL REFERENCES disaster_events ON DELETE CASCADE,
    latitude      DOUBLE PRECISION NOT NULL,       -- Open-Meteo grid point
    longitude     DOUBLE PRECISION NOT NULL,
    retrieved_at  TIMESTAMPTZ NOT NULL,            -- when the forecast was requested
    forecast_time TIMESTAMPTZ NOT NULL,            -- hour the forecast is valid for
    temperature   DOUBLE PRECISION,                -- deg C
    precipitation DOUBLE PRECISION,                -- mm
    wind_speed    DOUBLE PRECISION,                -- m/s
    humidity      DOUBLE PRECISION,                -- %
    UNIQUE (event_id, retrieved_at, forecast_time)
);
CREATE INDEX ON weather_forecasts (event_id, forecast_time);

---------------------------------------------------------------- analytics (filled by Spark)
CREATE TABLE analytics.daily_event_counts (
    day           DATE NOT NULL,
    source        TEXT NOT NULL,
    disaster_type TEXT NOT NULL,
    event_count   INT NOT NULL,
    PRIMARY KEY (day, source, disaster_type)
);

CREATE TABLE analytics.daily_fire_grid (
    day                   DATE NOT NULL,
    grid_lat              DOUBLE PRECISION NOT NULL,   -- south-west corner of a 0.5 degree cell
    grid_lon              DOUBLE PRECISION NOT NULL,
    detection_count       INT NOT NULL,
    high_confidence_count INT NOT NULL,
    avg_frp               DOUBLE PRECISION,
    max_frp               DOUBLE PRECISION,
    PRIMARY KEY (day, grid_lat, grid_lon)
);

---------------------------------------------------------------- analytics views
-- Current GDACS events with their latest alert level
CREATE VIEW analytics.active_alerts AS
SELECT e.event_id, e.disaster_type, e.title, e.country, e.latitude, e.longitude,
       e.started_at, e.updated_at, h.episode_id, h.alert_level, h.alert_score,
       h.observed_at AS alert_observed_at
FROM disaster_events e
JOIN LATERAL (SELECT * FROM gdacs_alert_history h
              WHERE h.event_id = e.event_id
              ORDER BY h.observed_at DESC, h.episode_id DESC LIMIT 1) h ON true
WHERE e.source = 'gdacs' AND e.status = 'active';

-- Next forecast hour from the most recent forecast of every event
CREATE VIEW analytics.latest_event_weather AS
SELECT DISTINCT ON (w.event_id)
       w.event_id, e.title, e.disaster_type, w.latitude, w.longitude, w.retrieved_at,
       w.forecast_time, w.temperature, w.precipitation, w.wind_speed, w.humidity
FROM weather_forecasts w
JOIN disaster_events e USING (event_id)
WHERE w.forecast_time >= date_trunc('hour', now())
ORDER BY w.event_id, w.retrieved_at DESC, w.forecast_time;

CREATE VIEW analytics.source_freshness AS
SELECT s.source, s.expected_minutes,
       (SELECT max(started_at) FROM ops.ingestion_runs r WHERE r.source = s.source AND r.status IN ('success', 'skipped')) AS last_success_at,
       (SELECT status FROM ops.ingestion_runs r WHERE r.source = s.source ORDER BY started_at DESC LIMIT 1) AS last_run_status,
       (SELECT max(ingested_at) FROM staging.source_records r WHERE r.source = s.source) AS last_staged_at,
       (SELECT max(processed_at) FROM staging.source_records r WHERE r.source = s.source) AS last_processed_at,
       (SELECT count(*) FROM staging.source_records r WHERE r.source = s.source) AS staged_records,
       (SELECT count(*) FROM staging.source_records r WHERE r.source = s.source AND r.processed_at IS NULL) AS unprocessed_records
FROM (VALUES ('usgs', 1), ('gdacs', 10), ('firms', 120), ('eonet', 60), ('openmeteo', 180)) AS s(source, expected_minutes);
