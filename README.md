# NYC Citi Bike GBFS Data Pipeline (Kafka + Spark + Airflow)

Production-focused streaming and batch pipeline for NYC Citi Bike GBFS station status. Ingests JSON from the GBFS HTTP feed into Apache Kafka, processes with Spark Structured Streaming into Parquet Bronze/Silver layers, and publishes 15-minute Gold aggregates to Postgres. Airflow orchestrates daily backfills and runs Soda Core data quality checks.

## Components
- Kafka (OSS) for messaging
- Spark 3.5 (Structured Streaming + batch)
- Airflow 2.9 (SparkSubmitOperator)
- Postgres (analytics sink)
- Parquet data lake (Bronze/Silver)
- Soda Core (data quality checks)

## Architecture
```mermaid
flowchart LR
  GBFS[NYC Citi Bike GBFS\nstation_status.json]
  PROD[Python Producer\nHTTP -> Kafka (JSON)]
  KAFKA[(Kafka)]
  SPARK[Spark Structured Streaming]
  BRZ[[Bronze\nParquet]]
  SLV[[Silver\nParquet]]
  GOLD[(Postgres\nstation_availability_15m)]
  AF[Airflow DAGs]
  SODA[Soda Core\nChecks]

  GBFS --> PROD --> KAFKA
  KAFKA -->|consume| SPARK
  SPARK --> BRZ
  SPARK --> SLV
  SPARK --> GOLD
  AF -->|spark-submit backfill| SPARK
  AF -->|soda scan| SODA
  SODA -.checks.-> GOLD
  AF -->|housekeeping| BRZ
```

## Prerequisites
- Docker Desktop (allocate 6–8 GB RAM)
- Make, Git

## Configuration
Copy and edit environment defaults:
```bash
cp .env.example .env
```
Key variables:
- KAFKA_BOOTSTRAP (default: localhost:19092)
- KAFKA_TOPIC (default: gbfs.station_status.json)
- POSTGRES_USER/POSTGRES_PASSWORD/POSTGRES_DB
- DATA_BASE_PATH (default mounted: /opt/app/data)

## Deploy locally (Docker Compose)
```bash
make up
make airflow-init
# Run streaming Spark job (keeps running)
make submit-stream
# Start ingestion producer (publishes JSON to Kafka)
make producer
```

UIs
- Airflow: http://localhost:8088 (admin/admin)
- Spark: http://localhost:8080
- Postgres: localhost:5432 (postgres/postgres)

## Data model
- Bronze (Parquet): decoded Kafka JSON records, append-only
- Silver (Parquet): typed/cleaned, deduplicated on (station_id, last_reported), derived columns
- Gold (Postgres): 15-minute window aggregates per station:
  - avg_pct_bikes_available, avg_bikes, avg_docks, window_start, window_end

Example query (Gold):
```sql
SELECT station_id, window_start, window_end, avg_pct_bikes_available
FROM station_availability_15m
ORDER BY window_start DESC, station_id
LIMIT 50;
```

## Operations
Common commands:
```bash
make up                # start stack
make down              # stop & remove volumes
make airflow-init      # init Airflow DB and admin user
make submit-stream     # run Spark streaming job
make producer          # run GBFS -> Kafka producer
make logs              # tails all container logs
```

Scheduling (Airflow):
- DAG `gbfs_pipeline` performs daily backfill and then runs Soda checks.

## Data quality (Soda)
- Config: `soda/configuration.yml`
- Checks: `soda/checks/checks_gold.yml`
- Run via Airflow task or inside the webserver container:
```bash
soda scan -d postgres -c /opt/airflow/soda/configuration.yml /opt/airflow/soda/checks/checks_gold.yml
```

## Observability
- Spark UI shows batch durations, input rates (http://localhost:8080)
- Airflow UI shows task durations, retries, and failures (http://localhost:8088)

## Security and secrets
- All credentials are provided via environment variables. Do not commit .env.
- For production, enable TLS/auth for Kafka, secure Airflow, and use a secrets manager.

## Troubleshooting
- Port conflicts: change host ports in `docker-compose.yml`
- Kafka client connectivity:
  - In containers: `kafka:9092`
  - From host: `localhost:19092`
- Memory: reduce Spark worker memory with `SPARK_WORKER_MEMORY=1g`

## Repository layout
```
.
├─ docker-compose.yml
├─ Makefile
├─ src/                   # ingestion and Spark jobs
├─ dags/                  # Airflow
├─ soda/                  # Soda config & checks
├─ tests/                 # unit/integration tests
├─ data/                  # bronze/silver/gold/checkpoints (mounted)
└─ .github/workflows/ci.yml
```

## License
MIT
# real-time-data-pipeline
