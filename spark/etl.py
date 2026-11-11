"""Spark batch ETL: staging.source_records -> cleaned PostgreSQL tables + analytics.

    spark-submit etl.py events    USGS + GDACS + EONET -> disaster_events, earthquake_details,
                                  gdacs_alert_history, event_geometries, analytics.daily_event_counts
    spark-submit etl.py fires     FIRMS -> fire_detections, analytics.daily_fire_grid
    spark-submit etl.py weather   Open-Meteo -> weather_forecasts

Each step reads unprocessed staging rows (id <= max id at start), writes the cleaned batch to tmp.*
with Spark JDBC, upserts it with SQL, then marks the staging rows processed. Re-running is safe.
"""
import os, sys

import psycopg2
from pyspark.sql import SparkSession, Window, functions as F

PG = dict(host="postgres", dbname=os.environ["POSTGRES_DB"], user=os.environ["POSTGRES_USER"],
          password=os.environ["POSTGRES_PASSWORD"])
JDBC_URL = f"jdbc:postgresql://postgres:5432/{PG['dbname']}"
JDBC = {"user": PG["user"], "password": PG["password"], "driver": "org.postgresql.Driver"}
GRID = 0.5  # fire grid cell size in degrees

spark = SparkSession.builder.appName(f"disaster-etl-{sys.argv[1]}").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")


def sql(*statements):
    """Run SQL statements in one PostgreSQL transaction (driver side)."""
    conn = psycopg2.connect(**PG, options="-c timezone=UTC")
    with conn, conn.cursor() as cur:
        for s in statements:
            cur.execute(s)
        result = cur.fetchall() if cur.description else None
    conn.close()
    return result


def read(query):
    return spark.read.jdbc(JDBC_URL, f"({query}) q", properties=JDBC)


def write_tmp(df, name):
    df.write.jdbc(JDBC_URL, f"tmp.{name}", mode="overwrite", properties=JDBC)


def read_staging(sources):
    """New staging rows for the given sources, plus the max id so we can mark exactly these rows later."""
    max_id = sql("SELECT coalesce(max(id), 0) FROM staging.source_records")[0][0]
    src = ",".join(f"'{s}'" for s in sources)
    df = read(f"SELECT id, source, record_id, fetched_at, payload::text AS payload FROM staging.source_records "
              f"WHERE processed_at IS NULL AND id <= {max_id} AND source IN ({src})")
    return df.cache(), max_id, src


def mark_processed(max_id, src):
    sql(f"UPDATE staging.source_records SET processed_at = now() "
        f"WHERE processed_at IS NULL AND id <= {max_id} AND source IN ({src})")


def latest(df, key, order):
    """Keep only the newest version of each key (a batch can contain several revisions)."""
    w = Window.partitionBy(key).orderBy(F.desc(order), F.desc("id"))
    return df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


def valid_coords(lat, lon):
    return F.col(lat).between(-90, 90) & F.col(lon).between(-180, 180)


def mapping(d):
    return F.create_map(*[F.lit(x) for kv in d.items() for x in kv])


def lookup(d, key):
    return F.element_at(mapping(d), key)


# Source categories -> one shared vocabulary
GDACS_TYPES = {"EQ": "earthquake", "TC": "tropical_cyclone", "FL": "flood", "VO": "volcano", "DR": "drought", "WF": "wildfire"}
EONET_TYPES = {"wildfires": "wildfire", "severeStorms": "severe_storm", "volcanoes": "volcano", "floods": "flood",
               "earthquakes": "earthquake", "drought": "drought", "landslides": "landslide", "seaLakeIce": "sea_lake_ice",
               "snow": "snow", "dustHaze": "dust_haze", "tempExtremes": "extreme_temperature",
               "waterColor": "water_color", "manmade": "manmade"}

USGS_SCHEMA = ("id STRING, properties STRUCT<mag: DOUBLE, magType: STRING, place: STRING, time: LONG, updated: LONG, "
               "status: STRING, tsunami: INT, type: STRING, title: STRING>, geometry STRUCT<coordinates: ARRAY<DOUBLE>>")
GDACS_SCHEMA = ("properties STRUCT<eventtype: STRING, eventid: LONG, episodeid: INT, name: STRING, alertlevel: STRING, "
                "alertscore: DOUBLE, iscurrent: STRING, country: STRING, fromdate: STRING, todate: STRING, "
                "datemodified: STRING>, geometry STRUCT<type: STRING, coordinates: ARRAY<DOUBLE>>")
# coordinates kept as raw JSON text because Point and Polygon have different shapes
EONET_SCHEMA = ("id STRING, title STRING, closed STRING, categories ARRAY<STRUCT<id: STRING>>, "
                "geometry ARRAY<STRUCT<date: STRING, type: STRING, coordinates: STRING, magnitudeValue: DOUBLE, magnitudeUnit: STRING>>")
FIRMS_FIELDS = ["latitude", "longitude", "bright_ti4", "acq_date", "acq_time", "satellite", "instrument", "confidence", "frp", "daynight"]
METEO_SCHEMA = ("event_id STRING, latitude DOUBLE, longitude DOUBLE, hourly STRUCT<time: ARRAY<STRING>, "
                "temperature_2m: ARRAY<DOUBLE>, precipitation: ARRAY<DOUBLE>, wind_speed_10m: ARRAY<DOUBLE>, "
                "relative_humidity_2m: ARRAY<DOUBLE>>")

EVENT_COLS = ["id", "event_id", "source", "source_event_id", "disaster_type", "title", "status", "started_at", "ended_at",
              "latitude", "longitude", "country", "updated_at", "magnitude", "magnitude_type", "depth_km", "tsunami_flag", "severity"]


def usgs_events(raw):
    p = raw.filter("source = 'usgs'").select("id", F.from_json("payload", USGS_SCHEMA).alias("j"))
    mag = F.col("j.properties.mag")
    depth = F.col("j.geometry.coordinates")[2]
    df = p.select(
        "id",
        F.concat(F.lit("usgs:"), "j.id").alias("event_id"),
        F.lit("usgs").alias("source"),
        F.col("j.id").alias("source_event_id"),
        F.when(F.col("j.properties.type") == "earthquake", "earthquake").otherwise("seismic_other").alias("disaster_type"),
        F.col("j.properties.title").alias("title"),
        F.lower("j.properties.status").alias("status"),            # USGS review status: automatic / reviewed
        F.timestamp_millis("j.properties.time").alias("started_at"),
        F.lit(None).cast("timestamp").alias("ended_at"),
        F.col("j.geometry.coordinates")[1].alias("latitude"),
        F.col("j.geometry.coordinates")[0].alias("longitude"),
        F.trim(F.element_at(F.split("j.properties.place", ","), -1)).alias("country"),  # region text after last comma
        F.timestamp_millis("j.properties.updated").alias("updated_at"),
        F.when(mag.between(-2, 10), F.round(mag, 2)).alias("magnitude"),
        F.col("j.properties.magType").alias("magnitude_type"),
        F.when(depth.between(-10, 800), F.round(depth, 2)).alias("depth_km"),
        (F.col("j.properties.tsunami") == 1).alias("tsunami_flag"),
    )
    severity = (F.when(F.col("magnitude") >= 8, "great").when(F.col("magnitude") >= 7, "major")
                .when(F.col("magnitude") >= 6, "strong").when(F.col("magnitude") >= 5, "moderate")
                .when(F.col("magnitude") >= 4, "light").when(F.col("magnitude") >= 2, "minor")
                .when(F.col("magnitude").isNotNull(), "micro"))
    return df.withColumn("severity", severity)


def gdacs_parsed(raw):
    p = raw.filter("source = 'gdacs'").select("id", F.from_json("payload", GDACS_SCHEMA).alias("j")).select("id", "j.*")
    return p.select(
        "id",
        F.concat(F.lit("gdacs:"), "properties.eventtype", F.col("properties.eventid").cast("string")).alias("event_id"),
        F.concat("properties.eventtype", F.col("properties.eventid").cast("string")).alias("source_event_id"),
        lookup(GDACS_TYPES, F.col("properties.eventtype")).alias("disaster_type"),
        F.col("properties.name").alias("title"),
        F.when(F.col("properties.iscurrent") == "true", "active").otherwise("closed").alias("status"),
        F.to_timestamp("properties.fromdate").alias("started_at"),   # GDACS dates are UTC without offset
        F.to_timestamp("properties.todate").alias("todate"),
        F.when(F.col("geometry.type") == "Point", F.col("geometry.coordinates")[1]).alias("latitude"),
        F.when(F.col("geometry.type") == "Point", F.col("geometry.coordinates")[0]).alias("longitude"),
        F.regexp_replace("properties.country", r"[,\s]+$", "").alias("country"),
        F.to_timestamp("properties.datemodified").alias("updated_at"),
        F.col("properties.episodeid").alias("episode_id"),
        F.lower(F.trim("properties.alertlevel")).alias("alert_level"),
        F.col("properties.alertscore").alias("alert_score"),
    )


def gdacs_events(g):
    return (g.withColumn("source", F.lit("gdacs"))
            .withColumn("ended_at", F.when(F.col("status") == "closed", F.col("todate")))
            .withColumn("magnitude", F.lit(None).cast("double")).withColumn("magnitude_type", F.lit(None).cast("string"))
            .withColumn("depth_km", F.lit(None).cast("double")).withColumn("tsunami_flag", F.lit(None).cast("boolean"))
            .withColumn("severity", F.lit(None).cast("string")))


def eonet_geometries(raw):
    p = raw.filter("source = 'eonet'").select("id", F.from_json("payload", EONET_SCHEMA).alias("j"))
    g = p.select("id", F.col("j.id").alias("eonet_id"), F.explode("j.geometry").alias("g")).select(
        "id", F.concat(F.lit("eonet:"), "eonet_id").alias("event_id"),
        F.to_timestamp("g.date").alias("observed_at"), F.col("g.type").alias("geometry_type"),
        F.col("g.coordinates").alias("coordinates"),
        F.col("g.magnitudeValue").alias("magnitude_value"), F.col("g.magnitudeUnit").alias("magnitude_unit"))
    point = F.from_json("coordinates", "array<double>")
    ring = F.from_json("coordinates", "array<array<array<double>>>")[0]
    lons, lats = F.transform(ring, lambda c: c[0]), F.transform(ring, lambda c: c[1])
    return g.select(
        "*",
        # polygon location = centre of its bounding box
        F.when(F.col("geometry_type") == "Point", point[1]).otherwise((F.array_min(lats) + F.array_max(lats)) / 2).alias("lat"),
        F.when(F.col("geometry_type") == "Point", point[0]).otherwise((F.array_min(lons) + F.array_max(lons)) / 2).alias("lon"),
        F.concat(F.lit('{"type":"'), "geometry_type", F.lit('","coordinates":'), "coordinates", F.lit("}")).alias("geometry_json"),
    )


def eonet_events(raw, geoms):
    p = raw.filter("source = 'eonet'").select("id", F.from_json("payload", EONET_SCHEMA).alias("j"))
    loc = geoms.groupBy("id").agg(F.min("observed_at").alias("started_at"), F.max("observed_at").alias("last_geometry_at"),
                                  F.max_by("lat", "observed_at").alias("latitude"), F.max_by("lon", "observed_at").alias("longitude"))
    df = p.join(loc, "id", "left").select(
        "id",
        F.concat(F.lit("eonet:"), "j.id").alias("event_id"),
        F.lit("eonet").alias("source"),
        F.col("j.id").alias("source_event_id"),
        F.coalesce(lookup(EONET_TYPES, F.col("j.categories")[0]["id"]), F.lit("other")).alias("disaster_type"),
        F.col("j.title").alias("title"),
        F.when(F.col("j.closed").isNull(), "active").otherwise("closed").alias("status"),
        "started_at",
        F.to_timestamp("j.closed").alias("ended_at"),
        "latitude", "longitude",
        F.lit(None).cast("string").alias("country"),
        F.greatest("last_geometry_at", F.to_timestamp("j.closed")).alias("updated_at"),
    )
    for c, t in [("magnitude", "double"), ("magnitude_type", "string"), ("depth_km", "double"),
                 ("tsunami_flag", "boolean"), ("severity", "string")]:
        df = df.withColumn(c, F.lit(None).cast(t))
    return df


def run_events():
    raw, max_id, src = read_staging(["usgs", "gdacs", "eonet"])
    gdacs = gdacs_parsed(raw).cache()
    geoms = eonet_geometries(raw).cache()

    events = (usgs_events(raw).select(EVENT_COLS)
              .unionByName(gdacs_events(gdacs).select(EVENT_COLS))
              .unionByName(eonet_events(raw, geoms).select(EVENT_COLS))
              .filter(F.col("disaster_type").isNotNull() & F.col("updated_at").isNotNull()))
    # missing or invalid coordinates become NULL instead of dropping the event
    ok = valid_coords("latitude", "longitude")
    events = events.withColumn("latitude", F.when(ok, F.col("latitude"))).withColumn("longitude", F.when(ok, F.col("longitude")))
    write_tmp(latest(events, "event_id", "updated_at"), "events")

    # every GDACS revision is kept: first time each (episode, alert level) was seen
    alerts = (gdacs.filter(F.col("alert_level").isin("green", "orange", "red"))
              .groupBy("event_id", "episode_id", "alert_level")
              .agg(F.min("updated_at").alias("observed_at"), F.max("alert_score").alias("alert_score")))
    write_tmp(alerts, "alerts")
    write_tmp(geoms.filter(F.col("observed_at").isNotNull())
              .dropDuplicates(["event_id", "observed_at", "geometry_type"])
              .select("event_id", "observed_at", "geometry_type", "geometry_json", "magnitude_value", "magnitude_unit"), "geoms")

    sql("""INSERT INTO disaster_events (event_id, source, source_event_id, disaster_type, title, status, started_at,
               ended_at, latitude, longitude, country, updated_at, processed_at)
           SELECT event_id, source, source_event_id, disaster_type, title, status, started_at, ended_at,
                  latitude, longitude, country, updated_at, now() FROM tmp.events
           ON CONFLICT (event_id) DO UPDATE SET disaster_type = EXCLUDED.disaster_type, title = EXCLUDED.title,
               status = EXCLUDED.status, started_at = EXCLUDED.started_at, ended_at = EXCLUDED.ended_at,
               latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, country = EXCLUDED.country,
               updated_at = EXCLUDED.updated_at, processed_at = now()
           WHERE EXCLUDED.updated_at >= disaster_events.updated_at""",   # never overwrite with an older revision
        """INSERT INTO earthquake_details (event_id, magnitude, magnitude_type, depth_km, tsunami_flag, severity)
           SELECT t.event_id, t.magnitude, t.magnitude_type, t.depth_km, t.tsunami_flag, t.severity
           FROM tmp.events t JOIN disaster_events e ON e.event_id = t.event_id AND e.updated_at = t.updated_at
           WHERE t.source = 'usgs'
           ON CONFLICT (event_id) DO UPDATE SET magnitude = EXCLUDED.magnitude, magnitude_type = EXCLUDED.magnitude_type,
               depth_km = EXCLUDED.depth_km, tsunami_flag = EXCLUDED.tsunami_flag, severity = EXCLUDED.severity""",
        """INSERT INTO gdacs_alert_history (event_id, episode_id, alert_level, alert_score, observed_at)
           SELECT a.event_id, a.episode_id, a.alert_level, a.alert_score, a.observed_at
           FROM tmp.alerts a JOIN disaster_events e USING (event_id)
           ON CONFLICT (event_id, episode_id, alert_level)
           DO UPDATE SET observed_at = LEAST(gdacs_alert_history.observed_at, EXCLUDED.observed_at)""",
        """INSERT INTO event_geometries (event_id, observed_at, geometry_type, geometry_json, magnitude_value, magnitude_unit)
           SELECT g.event_id, g.observed_at, g.geometry_type, g.geometry_json::jsonb, g.magnitude_value, g.magnitude_unit
           FROM tmp.geoms g JOIN disaster_events e USING (event_id)
           ON CONFLICT (event_id, observed_at, geometry_type) DO UPDATE SET geometry_json = EXCLUDED.geometry_json,
               magnitude_value = EXCLUDED.magnitude_value, magnitude_unit = EXCLUDED.magnitude_unit""")
    mark_processed(max_id, src)

    # full refresh of the daily counts from the cleaned table
    counts = (read("SELECT source, disaster_type, started_at FROM disaster_events WHERE started_at IS NOT NULL")
              .groupBy(F.to_date("started_at").alias("day"), "source", "disaster_type").agg(F.count("*").alias("event_count")))
    write_tmp(counts, "daily_counts")
    sql("DELETE FROM analytics.daily_event_counts",
        "INSERT INTO analytics.daily_event_counts SELECT day, source, disaster_type, event_count FROM tmp.daily_counts")
    print("events batch (events, alert levels, geometries):",
          sql("SELECT (SELECT count(*) FROM tmp.events), (SELECT count(*) FROM tmp.alerts), (SELECT count(*) FROM tmp.geoms)"))


def run_fires():
    raw, max_id, src = read_staging(["firms"])
    j = raw.select("id", "record_id", F.from_json("payload", ", ".join(f"{c} STRING" for c in FIRMS_FIELDS)).alias("j"))
    fires = j.select(
        "id", F.col("record_id").alias("detection_id"),
        # acq_time is HHMM without leading zeros, e.g. "13" = 00:13 UTC
        F.to_timestamp(F.concat_ws(" ", "j.acq_date", F.lpad("j.acq_time", 4, "0")), "yyyy-MM-dd HHmm").alias("acquired_at"),
        F.col("j.latitude").cast("double").alias("latitude"), F.col("j.longitude").cast("double").alias("longitude"),
        F.when(F.col("j.satellite") == "N20", "NOAA-20").otherwise(F.col("j.satellite")).alias("satellite"),
        F.col("j.instrument").alias("instrument"),
        lookup({"l": "low", "n": "nominal", "h": "high"}, F.lower("j.confidence")).alias("confidence"),
        F.col("j.frp").cast("double").alias("frp"), F.col("j.bright_ti4").cast("double").alias("bright_ti4"),
        F.col("j.daynight").alias("daynight"),
    )
    fires = (fires.filter(F.col("acquired_at").isNotNull() & valid_coords("latitude", "longitude"))
             .withColumn("frp", F.when(F.col("frp") >= 0, F.col("frp")))
             .withColumn("grid_lat", F.floor(F.col("latitude") / GRID) * GRID)
             .withColumn("grid_lon", F.floor(F.col("longitude") / GRID) * GRID))
    fires = latest(fires, "detection_id", "acquired_at").drop("id")
    write_tmp(fires, "fires")
    sql("""INSERT INTO fire_detections SELECT detection_id, acquired_at, latitude, longitude, satellite, instrument,
               confidence, frp, bright_ti4, daynight, grid_lat, grid_lon FROM tmp.fires
           ON CONFLICT (detection_id) DO UPDATE SET frp = EXCLUDED.frp, confidence = EXCLUDED.confidence""")
    # recompute grid statistics only for the days that received new detections
    days = [str(r[0]) for r in sql("SELECT DISTINCT acquired_at::date FROM tmp.fires")]
    mark_processed(max_id, src)
    if days:
        day_list = ",".join(f"'{d}'" for d in days)
        detections = read(f"SELECT acquired_at, grid_lat, grid_lon, frp, confidence FROM fire_detections "
                          f"WHERE (acquired_at AT TIME ZONE 'UTC')::date IN ({day_list})")
        grid = detections.groupBy(F.to_date("acquired_at").alias("day"), "grid_lat", "grid_lon").agg(
            F.count("*").alias("detection_count"),
            F.sum(F.when(F.col("confidence") == "high", 1).otherwise(0)).alias("high_confidence_count"),
            F.round(F.avg("frp"), 2).alias("avg_frp"), F.max("frp").alias("max_frp"))
        write_tmp(grid, "fire_grid")
        sql(f"DELETE FROM analytics.daily_fire_grid WHERE day IN ({day_list})",
            "INSERT INTO analytics.daily_fire_grid SELECT day, grid_lat, grid_lon, detection_count, "
            "high_confidence_count, avg_frp, max_frp FROM tmp.fire_grid")
    print(f"fires: {sql('SELECT count(*) FROM tmp.fires')[0][0]} detections in batch, days refreshed: {days}")


def run_weather():
    raw, max_id, src = read_staging(["openmeteo"])
    j = raw.select("id", "fetched_at", F.from_json("payload", METEO_SCHEMA).alias("j"))
    # hourly arrays -> one row per forecast hour
    rows = j.select("fetched_at", "j.event_id", "j.latitude", "j.longitude", "j.hourly",
                    F.posexplode("j.hourly.time").alias("i", "t"))
    weather = rows.select(
        "event_id", "latitude", "longitude",
        F.col("fetched_at").alias("retrieved_at"),
        F.to_timestamp("t", "yyyy-MM-dd'T'HH:mm").alias("forecast_time"),
        F.element_at("hourly.temperature_2m", F.col("i") + 1).alias("temperature"),
        F.element_at("hourly.precipitation", F.col("i") + 1).alias("precipitation"),
        F.round(F.element_at("hourly.wind_speed_10m", F.col("i") + 1) / 3.6, 2).alias("wind_speed"),   # km/h -> m/s
        F.element_at("hourly.relative_humidity_2m", F.col("i") + 1).alias("humidity"),
    )
    weather = (weather.withColumn("temperature", F.when(F.col("temperature").between(-90, 60), F.col("temperature")))
               .withColumn("precipitation", F.when(F.col("precipitation") >= 0, F.col("precipitation")))
               .withColumn("humidity", F.when(F.col("humidity").between(0, 100), F.col("humidity")))
               .filter(F.col("forecast_time").isNotNull()))
    # join to the disaster the forecast was requested for (drops forecasts for unknown events)
    events = read("SELECT event_id FROM disaster_events")
    weather = weather.join(events, "event_id").dropDuplicates(["event_id", "retrieved_at", "forecast_time"])
    write_tmp(weather, "weather")
    sql("""INSERT INTO weather_forecasts (event_id, latitude, longitude, retrieved_at, forecast_time,
               temperature, precipitation, wind_speed, humidity)
           SELECT event_id, latitude, longitude, retrieved_at, forecast_time, temperature, precipitation, wind_speed, humidity
           FROM tmp.weather ON CONFLICT (event_id, retrieved_at, forecast_time) DO NOTHING""")
    mark_processed(max_id, src)
    print(f"weather: {sql('SELECT count(*) FROM tmp.weather')[0][0]} forecast rows in batch")


{"events": run_events, "fires": run_fires, "weather": run_weather}[sys.argv[1]]()
spark.stop()
