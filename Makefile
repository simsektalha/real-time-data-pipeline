SHELL := /bin/bash

export COMPOSE_CMD := docker compose

.PHONY: up down logs airflow-init producer submit-stream test ps

up:
	$(COMPOSE_CMD) up -d

down:
	$(COMPOSE_CMD) down -v

ps:
	$(COMPOSE_CMD) ps

logs:
	$(COMPOSE_CMD) logs -f --tail=200 | cat

airflow-init:
	$(COMPOSE_CMD) run --rm airflow-webserver bash -lc "pip install -r /requirements.txt || true; airflow db migrate; airflow users create --role Admin --username $${AIRFLOW_USER:-admin} --password $${AIRFLOW_PASSWORD:-admin} --firstname Admin --lastname User --email admin@example.com"

producer:
	$(COMPOSE_CMD) run --rm producer

submit-stream:
	docker exec -it spark-master bash -lc "spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 /opt/app/src/jobs/spark_streaming_job.py"

test:
	pytest -q || true


