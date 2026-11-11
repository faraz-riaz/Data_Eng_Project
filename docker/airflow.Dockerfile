# Airflow + Java + PySpark (same version as the Spark cluster) so tasks can run spark-submit
FROM apache/airflow:3.3.2-python3.10
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/jars \
    && curl -fsSL -o /opt/jars/postgresql.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
USER airflow
RUN pip install --no-cache-dir "apache-airflow==3.3.2" pyspark==3.5.8 confluent-kafka==2.16.1 minio==7.2.20
