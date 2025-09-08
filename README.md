# GBFS Kafka + Spark + Airflow (Parquet + Soda)

A portfolio-ready, end-to-end data pipeline that ingests NYC Citi Bike GBFS station status into Apache Kafka (JSON), processes with Spark Structured Streaming into Bronze/Silver/Gold Parquet layers, and orchestrates batch/backfill with Airflow (SparkSubmitOperator). Data quality uses Soda Core on the Gold table in Postgres.

## Quickstart

Requirements: Docker Desktop (6–8 GB RAM), Make, Git.

```bash
cp .env.example .env
make up
make airflow-init
# start streaming job (keeps running)
make submit-stream
# in another terminal, run the producer
make producer
```

UIs:
- Airflow: http://localhost:8088 (admin/admin)
- Spark: http://localhost:8080
- Postgres: localhost:5432 (postgres/postgres)

Example SQL in Postgres (gold, 15-min aggregates):
```sql
SELECT station_id, window_start, window_end, avg_pct_bikes_available
FROM station_availability_15m
ORDER BY window_start DESC, station_id
LIMIT 50;
```

## Structure
```
.
├─ docker-compose.yml
├─ Makefile
├─ src/                   # ingestion and Spark jobs
├─ dags/                  # Airflow
├─ soda/                  # Soda Core config & checks
├─ tests/                 # unit/integration tests
├─ data/                  # mounted bronze/silver/gold/checkpoints
└─ .github/workflows/ci.yml
```

## Notes
- Kafka JSON, Parquet bronze/silver, gold -> Postgres
- Airflow SparkSubmitOperator + Soda scan
- MIT license
# real-time-data-pipeline
