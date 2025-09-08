from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable

SPARK_MASTER = Variable.get("spark_master", default_var="spark://spark-master:7077")
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
        application_args=[
            "--date", "{{ ds }}",
            "--data-base-path", "{{ var.value.data_base_path | default('/opt/app/data') }}",
            "--pg-host", "{{ var.value.pg_host | default('postgres') }}",
            "--pg-port", "{{ var.value.pg_port | default('5432') }}",
            "--pg-db", "{{ var.value.pg_db | default('postgres') }}",
            "--pg-user", "{{ var.value.pg_user | default('postgres') }}",
            "--pg-password", "{{ var.value.pg_password | default('postgres') }}",
            "--gold-table", "{{ var.value.gold_table | default('station_availability_15m') }}",
        ],
        spark_binary="spark-submit",
        name="gbfs_daily_backfill",
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


