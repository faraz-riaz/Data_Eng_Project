# Image for the USGS/GDACS producers, the Kafka consumer and FastAPI
FROM python:3.10-slim
RUN pip install --no-cache-dir requests==2.34.2 confluent-kafka==2.16.1 minio==7.2.20 \
    psycopg2-binary==2.9.13 fastapi==0.162.4 uvicorn==0.54.0
WORKDIR /app
