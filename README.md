# Real-Time E-Commerce Clickstream & Revenue Loss Analytics Pipeline

A production-style, end-to-end data lakehouse pipeline that ingests real-time user behavior events, identifies high-risk sessions where items were added to cart but never purchased, and transforms raw clickstream data into actionable revenue-loss insights.

## Business Problem

Cart abandonment is one of the largest sources of lost revenue in e-commerce. Without real-time visibility into abandoned sessions, teams can't intervene while the opportunity is still live — whether through targeted discounts, retargeting, or inventory reprioritization. This pipeline closes that gap by streaming user events as they happen, isolating abandonment patterns in near real time, and landing the results in a queryable analytical store.

## Architecture

```
Kafka (event stream) → PySpark Structured Streaming → Apache Iceberg (MinIO/S3) → Analytics
```

| Layer | Technology | Purpose |
|---|---|---|
| Infrastructure | Docker & Docker Compose | Local orchestration of all services |
| Message Broker | Apache Kafka (KRaft mode) | Durable, ordered ingestion of clickstream events |
| Stream Processing | PySpark Structured Streaming | Real-time parsing, watermarking, and session logic |
| Storage & Table Format | Apache Iceberg on MinIO (S3-compatible) | ACID-compliant, partition-aware lakehouse storage |
| Data Simulation | Python (Faker) | Realistic synthetic user behavior for local development |

## Getting Started

### Prerequisites

- Python 3.9 or later
- Java 17 (JDK 11 also supported) — required for the local PySpark runtime
- Docker Desktop

> **Note:** PySpark's Hadoop dependencies are incompatible with very new JDK releases (21+). If `python init_tables.py` fails with a Java gateway error, confirm `java -version` reports 17.x before proceeding.

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start Infrastructure

```bash
docker compose up -d
```

Confirm MinIO is running by visiting `http://localhost:9001` (user: `your_username`, password: `your_password`).

### 3. Initialize Iceberg Tables

Creates the lakehouse database and table schemas:

```bash
python init_tables.py
```

### 4. Start the Stream Processor

Launches the PySpark job that consumes events and writes to Iceberg:

```bash
python spark_stream.py
```

### 5. Generate Simulated Traffic

In a separate terminal, start producing mock user behavior events:

```bash
python producer.py
```

## Design Highlights

- **Hidden partitioning:** Iceberg's `days(timestamp)` partition transform accelerates time-range queries without requiring analysts to manually filter or restructure upstream columns.
- **Bounded streaming state:** A 10-minute watermark (`withWatermark`) caps how long late or out-of-order events are held in memory, keeping the Spark cluster's resource footprint predictable under continuous load.
- **S3-compatible storage parity:** Using MinIO locally mirrors the object-storage architecture of production AWS S3 / GCS deployments, so the pipeline can move to a cloud environment with minimal changes.
