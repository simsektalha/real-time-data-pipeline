from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
BASE = "/opt/app"
BATCH_JOB = f"{BASE}/src/jobs/spark_batch_backfill.py"
PKGS = ",".join([
  "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
  "org.postgresql:postgresql:42.7.3"
])

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(minutes=30),
}

with DAG(
    "gbfs_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    tags=["gbfs", "spark", "kafka"],
) as dag:

    daily_batch = SparkSubmitOperator(
        task_id="daily_batch_backfill",
        application=BATCH_JOB,
        master=SPARK_MASTER,
        packages=PKGS,
        application_args=["--date", "{{ ds }}"],
        spark_binary="spark-submit",
        name="gbfs_daily_backfill",
        env_vars={
            "KAFKA_BOOTSTRAP": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB", "postgres"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER", "postgres"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "postgres"),
        },
    )

    soda_scan_gold = BashOperator(
        task_id="soda_scan_gold",
        bash_command="soda scan -d postgres -c /opt/airflow/soda/configuration.yml /opt/airflow/soda/checks/checks_gold.yml",
        cwd="/opt/airflow",
        trigger_rule="all_done"
    )

    housekeeping = BashOperator(
        task_id="housekeeping_bronze",
        bash_command="find /opt/app/data/bronze/station_status -type f -mtime +7 -delete || true"
    )

    daily_batch >> [soda_scan_gold, housekeeping]


