"""DAG 4: Spark ETL + analytics refresh every 15 minutes.

Runs spark/etl.py on the Spark standalone cluster (spark://spark-master:7077) with spark-submit.
The driver runs inside the Airflow container; executors run on the spark-worker container.
max_active_runs=1 prevents overlapping Spark jobs.
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

SPARK_SUBMIT = (
    "spark-submit --master spark://spark-master:7077 --jars /opt/jars/postgresql.jar "
    "--conf spark.driver.host=airflow --conf spark.driver.bindAddress=0.0.0.0 "
    "--conf spark.cores.max=1 --conf spark.executor.memory=1g "
    "--conf spark.driver.extraJavaOptions=-Duser.timezone=UTC "
    "--conf spark.executor.extraJavaOptions=-Duser.timezone=UTC "
    "/opt/airflow/spark/etl.py "
)

with DAG("spark_etl", schedule="*/15 * * * *", start_date=datetime(2026, 11, 1), catchup=False,
         max_active_runs=1, default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
         tags=["spark"], doc_md=__doc__):
    events = BashOperator(task_id="spark_events", bash_command=SPARK_SUBMIT + "events")
    weather = BashOperator(task_id="spark_weather", bash_command=SPARK_SUBMIT + "weather")
    fires = BashOperator(task_id="spark_fires", bash_command=SPARK_SUBMIT + "fires")
    events >> weather   # weather rows reference disaster_events; fires run in parallel
