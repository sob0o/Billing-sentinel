# Billing Sentinel

> A local simulation of a GCP-native billing anomaly detection pipeline for Orange subcontractors, built with Apache Kafka, Spark, MinIO, PostgreSQL, and Grafana — fully containerized with Docker Compose.

---

## Table of Contents

- [Overview](#overview)
- [GCP Mapping](#gcp-mapping)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Running the Jobs](#running-the-jobs)
- [Known Issues & Fixes](#known-issues--fixes)
- [Grafana Dashboard](#grafana-dashboard)
- [CI/CD Testing (Planned)](#cicd-testing-planned)

---

## Overview

Billing Sentinel detects anomalies in invoices submitted by Orange subcontractors. It simulates a real-time data pipeline that ingests invoices, scores them for fraud patterns (overbilling, wrong rates, duplicates), aggregates KPIs per subcontractor, and exposes the results in a Grafana dashboard.

The pipeline is split into two jobs:

- **Streaming Job** — reads invoices from Kafka in real-time, scores each invoice, and writes Parquet files to MinIO
- **Batch Job** — reads all accumulated Parquet files, aggregates KPIs per subcontractor, detects duplicate invoices, and writes results to PostgreSQL

Anomaly detection is based on three rules:

| Rule | Condition | Score |
|---|---|---|
| Overbilling | `billed_duration > real_duration × 1.15` | +0.5 |
| Wrong rate | `rate_applied > contract_rate × 1.10` | +0.5 |
| Both | Both conditions true | 1.0 |
| None | No anomaly | 0.0 |

---

## GCP Mapping

| Local Component | GCP Equivalent |
|---|---|
| Docker Compose | GCP Infrastructure (VPC, GKE) |
| Apache Kafka | Google Cloud Pub/Sub |
| Apache Spark (Scala) | Dataproc + Spark |
| Spark MLlib | Vertex AI |
| MinIO | Google Cloud Storage (GCS) |
| Parquet on MinIO | BigLake |
| Hive Metastore | Dataproc Hive Metastore |
| PostgreSQL | Cloud SQL / BigQuery |
| Grafana | Looker Studio |
| Apache Airflow | Cloud Composer |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Network                           │
│                                                                 │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────────┐  │
│  │  producer   │────▶│    Kafka    │────▶│  Spark          │  │
│  │ (Python     │     │  + ZooKeep  │     │  Streaming Job  │  │
│  │  Faker)     │     │             │     │  (StreamingJob  │  │
│  └─────────────┘     └─────────────┘     │   .scala)       │  │
│                                          └───────┬─────────┘  │
│                                                  │             │
│                                                  ▼             │
│                                        ┌─────────────────┐    │
│                                        │      MinIO      │    │
│                                        │  (s3a://billing │    │
│                                        │  -sentinel/raw/)│    │
│                                        └───────┬─────────┘    │
│                                                │               │
│                                                ▼               │
│                                        ┌─────────────────┐    │
│                                        │  Spark          │    │
│                                        │  Batch Job      │    │
│                                        │  (BatchJob      │    │
│                                        │   .scala)       │    │
│                                        └───────┬─────────┘    │
│                                                │               │
│                                                ▼               │
│  ┌─────────────┐                      ┌─────────────────┐    │
│  │   Grafana   │◀─────────────────────│   PostgreSQL    │    │
│  │  Dashboard  │                      │ billing_sentinel│    │
│  └─────────────┘                      └─────────────────┘    │
│                                                                 │
│  ┌─────────────────────────────────────┐                      │
│  │  Hive Metastore + Postgres (meta)   │                      │
│  └─────────────────────────────────────┘                      │
└─────────────────────────────────────────────────────────────────┘
```

**Data Flow:**

```
Python Producer
    │
    │  JSON invoices (80% normal, 20% anomalies)
    ▼
Kafka topic: "invoices"
    │
    │  Spark Structured Streaming
    ▼
Anomaly Scoring (is_overbilling, is_wrong_rate, anomaly_score)
    │
    │  Parquet files
    ▼
MinIO  →  s3a://billing-sentinel/raw/
    │
    │  Spark Batch (run daily / on demand)
    ▼
Aggregated KPIs + Duplicate Detection
    │
    │  JDBC
    ▼
PostgreSQL → subcontractor_kpis, duplicate_invoices
    │
    ▼
Grafana Dashboard
```

---

## Project Structure

```
billing-sentinel/
├── docker-compose.yml
├── postgres/
│   └── init.sql                          # Table definitions (auto-run on first start)
├── generator/
│   └── producer.py                       # Faker-based invoice generator
└── spark-jobs/
    ├── build.sbt                         # Scala dependencies + assembly config
    ├── project/
    │   ├── build.properties
    │   └── plugins.sbt                   # sbt-assembly plugin
    └── src/main/scala/billing/
        ├── StreamingJob.scala            # Phase 3 — Kafka → MinIO streaming
        └── BatchJob.scala               # Phase 4 — MinIO → PostgreSQL batch
```

---

## Quick Start

### Prerequisites

- Docker Desktop with WSL2 enabled
- Java 17 (Eclipse Adoptium Temurin)
- sbt 1.12.x installed locally
- Python 3.x with `faker` and `kafka-python` installed

### 1. Start all services

```powershell
docker compose up -d
```

Verify all containers are running:

```powershell
docker compose ps
```

You should see: `zookeeper`, `kafka`, `minio`, `hive-metastore-db`, `hive-metastore`, `spark-master`, `spark-worker`, `postgres`, `grafana`

### 2. Create PostgreSQL tables

> This only needs to be done once. If your postgres volume is fresh, `init.sql` runs automatically.
> If the volume already existed, run this manually:

```powershell
docker exec -it postgres psql -U sentinel -d billing_sentinel -c "
CREATE TABLE IF NOT EXISTS subcontractor_kpis (
    subcontractor_id    VARCHAR(50),
    total_invoices      BIGINT,
    total_billed        DOUBLE PRECISION,
    total_expected      DOUBLE PRECISION,
    avg_anomaly_score   DOUBLE PRECISION,
    anomaly_count       BIGINT,
    overbilling_count   BIGINT,
    wrong_rate_count    BIGINT,
    batch_timestamp     TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS duplicate_invoices (
    intervention_id     VARCHAR(50),
    invoice_count       BIGINT,
    subcontractor_ids   TEXT,
    total_billed        DOUBLE PRECISION,
    batch_timestamp     TIMESTAMP DEFAULT NOW()
);
"
```

### 3. Fix Spark Ivy cache permissions

> Run this once after every fresh container start. Without it, Spark cannot download packages.

```powershell
docker exec -u root spark-master mkdir -p /home/spark/.ivy2/cache
docker exec -u root spark-master chmod -R 777 /home/spark/.ivy2
```

### 4. Build the fat JAR

```powershell
cd spark-jobs
sbt assembly
```

The JAR is written to `spark-jobs/target/billing-sentinel-assembly.jar` and is automatically mounted into the Spark containers via the volume defined in `docker-compose.yml`.

### 5. Start the data generator

```powershell
cd generator
python producer.py
```

This sends invoices to the Kafka `invoices` topic continuously (80% normal, 20% anomalies).

---

## Running the Jobs

> **Important:** The local Spark cluster has limited resources (1 worker, 2 cores, 2GB RAM).
> Run the streaming job and batch job **separately**, not at the same time.
> In production on GCP, each job would run on its own dedicated Dataproc cluster.

### Streaming Job (Phase 3)

Reads from Kafka, scores invoices, writes Parquet to MinIO.

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --class billing.StreamingJob `
  --master spark://spark-master:7077 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 `
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 `
  --conf spark.hadoop.fs.s3a.access.key=minioadmin `
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin `
  --conf spark.hadoop.fs.s3a.path.style.access=true `
  /opt/spark-jobs/billing-sentinel-assembly.jar
```

Let it run for a few minutes, then press `Ctrl+C` to stop it before running the batch job.

### Batch Job (Phase 4)

Reads all Parquet files from MinIO, computes KPIs, detects duplicates, writes to PostgreSQL.

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --class billing.BatchJob `
  --master spark://spark-master:7077 `
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 `
  --conf spark.hadoop.fs.s3a.access.key=minioadmin `
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin `
  --conf spark.hadoop.fs.s3a.path.style.access=true `
  /opt/spark-jobs/billing-sentinel-assembly.jar
```

### Verify data in PostgreSQL

```powershell
docker exec -it postgres psql -U sentinel -d billing_sentinel -c "SELECT * FROM subcontractor_kpis LIMIT 10;"
docker exec -it postgres psql -U sentinel -d billing_sentinel -c "SELECT * FROM duplicate_invoices LIMIT 10;"
```

---

## Known Issues & Fixes

### 1. `No main class set in JAR`

**Cause:** The JAR contains two entry points (`StreamingJob` and `BatchJob`) so Spark doesn't know which to run.

**Fix:** Always specify `--class` in your spark-submit command:
```
--class billing.StreamingJob
--class billing.BatchJob
```

---

### 2. `Partition offset was changed from X to Y, some data may have been missed`

**Cause:** Kafka restarted and lost its old messages, but the Spark checkpoint in MinIO still remembers the old offset.

**Fix:** Add `failOnDataLoss=false` to the Kafka read options in `StreamingJob.scala`:
```scala
.option("failOnDataLoss", "false")
```

---

### 3. `Initial job has not accepted any resources`

**Cause:** The Spark worker is fully occupied by the streaming job. The batch job cannot get any CPU or RAM.

**Fix:** Stop the streaming job first (`Ctrl+C`), then run the batch job. Restart the streaming job afterward.

---

### 4. Spark cannot download packages (Ivy permission error)

**Cause:** The `/home/spark/.ivy2` directory inside the container has wrong permissions.

**Fix:** Run these two commands after every fresh container start:
```powershell
docker exec -u root spark-master mkdir -p /home/spark/.ivy2/cache
docker exec -u root spark-master chmod -R 777 /home/spark/.ivy2
```

---

### 5. `pq: SSL is not enabled on the server` in Grafana

**Cause:** Grafana tries to connect to PostgreSQL with SSL enabled by default.

**Fix:** In the Grafana PostgreSQL datasource settings, set **TLS/SSL Mode** to `disable`.

---

### 6. PostgreSQL tables missing after container restart

**Cause:** The `init.sql` script only runs once on the very first container initialization. If the volume already existed before you added the file, it is skipped.

**Fix:** Run the `CREATE TABLE` commands manually as shown in the Quick Start section above.

---

## Grafana Dashboard

### Connect to PostgreSQL

1. Open `http://localhost:3000` — login: `admin / admin`
2. Go to **Connections → Add new connection → PostgreSQL**
3. Fill in:

| Field | Value |
|---|---|
| Host | `postgres:5432` |
| Database | `billing_sentinel` |
| User | `sentinel` |
| Password | `sentinel` |
| TLS/SSL Mode | `disable` |
| Version | `15` |

4. Click **Save & test**

### Suggested Panels

**Panel 1 — Anomaly Rate per Subcontractor (Bar chart)**
```sql
SELECT subcontractor_id, avg_anomaly_score
FROM subcontractor_kpis
ORDER BY avg_anomaly_score DESC;
```

**Panel 2 — Total Billed vs Expected (Bar chart)**
```sql
SELECT subcontractor_id, total_billed, total_expected
FROM subcontractor_kpis
ORDER BY total_billed DESC;
```

**Panel 3 — Anomaly Breakdown (Bar chart)**
```sql
SELECT subcontractor_id, overbilling_count, wrong_rate_count, anomaly_count
FROM subcontractor_kpis
ORDER BY anomaly_count DESC;
```

**Panel 4 — Duplicate Invoices (Table)**
```sql
SELECT intervention_id, invoice_count, subcontractor_ids, total_billed
FROM duplicate_invoices
ORDER BY invoice_count DESC;
```

**Panel 5 — Total Invoices Processed (Stat)**
```sql
SELECT SUM(total_invoices) as total FROM subcontractor_kpis;
```

**Panel 6 — High Risk Subcontractors (Table)**
```sql
SELECT subcontractor_id, total_invoices, anomaly_count, avg_anomaly_score
FROM subcontractor_kpis
WHERE avg_anomaly_score >= 0.5
ORDER BY avg_anomaly_score DESC;
```

---

## CI/CD Testing (Planned)

A GitHub Actions workflow will be added to automatically test the Scala code on every push.

### Planned test cases

- Unit tests for anomaly scoring logic (`is_overbilling`, `is_wrong_rate`, `anomaly_score`)
- Schema validation — ensure Parquet output matches expected schema
- Integration test — verify batch job correctly counts duplicates
- Build verification — confirm `sbt assembly` succeeds on CI

### Planned workflow file: `.github/workflows/ci.yml`

```yaml
name: CI

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Java 17
        uses: actions/setup-java@v3
        with:
          java-version: '17'
          distribution: 'temurin'

      - name: Run tests
        working-directory: spark-jobs
        run: sbt test

      - name: Build assembly JAR
        working-directory: spark-jobs
        run: sbt assembly
```

> Tests will be written using **ScalaTest** with **Spark's local mode** so no Docker is needed in CI.

---

## Services & Ports

| Service | URL | Credentials |
|---|---|---|
| Spark Master UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Grafana | http://localhost:3000 | admin / admin |
| Kafka | localhost:9092 | — |
| PostgreSQL | localhost:5432 | sentinel / sentinel |

---

## Tech Stack

![Scala](https://img.shields.io/badge/Scala-2.12-red)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-orange)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.4.0-black)
![MinIO](https://img.shields.io/badge/MinIO-latest-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Grafana](https://img.shields.io/badge/Grafana-10.0.0-orange)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
