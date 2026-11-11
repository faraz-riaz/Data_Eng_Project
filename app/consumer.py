"""Kafka -> PostgreSQL staging. Offsets are committed only after the database commit (at-least-once);
replayed messages are ignored by the UNIQUE (source, record_id, content_hash) constraint."""
import json

from confluent_kafka import Consumer
from psycopg2.extras import execute_values

from ingest import db

SOURCES = ["usgs", "gdacs", "firms", "eonet", "openmeteo"]
REQUIRED = ["source", "record_id", "content_hash", "fetched_at", "payload"]

consumer = Consumer({"bootstrap.servers": "kafka:9092", "group.id": "staging-writer",
                     "enable.auto.commit": False, "auto.offset.reset": "earliest"})
consumer.subscribe([f"raw.{s}" for s in SOURCES])
conn = db()
print("consumer started", flush=True)

while True:
    messages = consumer.consume(num_messages=2000, timeout=2.0)
    if not messages:
        continue
    rows, rejected = [], 0
    for m in messages:
        if m.error():
            print("kafka error:", m.error(), flush=True)
            continue
        try:
            v = json.loads(m.value())
        except ValueError:
            v = {}
        # validate the envelope: all required fields present and source matches the topic
        if any(not v.get(k) for k in REQUIRED) or m.topic() != f"raw.{v['source']}":
            rejected += 1
            continue
        rows.append((v["source"], v["record_id"], v["content_hash"], json.dumps(v["payload"]),
                     v["fetched_at"], v.get("run_id"), m.partition(), m.offset()))
    with conn, conn.cursor() as cur:
        inserted = execute_values(cur, """INSERT INTO staging.source_records
            (source, record_id, content_hash, payload, fetched_at, run_id, kafka_partition, kafka_offset)
            VALUES %s ON CONFLICT (source, record_id, content_hash) DO NOTHING RETURNING id""", rows, page_size=1000, fetch=True)
    consumer.commit(asynchronous=False)   # only after the rows are safely in PostgreSQL
    print(f"batch: {len(messages)} messages, {len(inserted)} inserted, {rejected} rejected", flush=True)
