# Real-Time E-Commerce Clickstream & Revenue Loss Analytics Pipeline

A production-style, end-to-end data lakehouse pipeline that ingests real-time user behavior events, identifies high-risk sessions where items were added to cart but never purchased, and transforms raw clickstream data into actionable revenue-loss insights — from real-time ingestion through to ad-hoc SQL analytics and automated table maintenance.

## Business Problem

Cart abandonment is one of the largest sources of lost revenue in e-commerce. Without real-time visibility into abandoned sessions, teams can't intervene while the opportunity is still live — whether through targeted discounts, retargeting, or inventory reprioritization. This pipeline closes that gap by streaming user events as they happen, isolating abandonment patterns in near real time, and landing the results in a queryable analytical store that supports both operational monitoring and ad-hoc business analysis.

## Architecture

```
Kafka (event stream) → PySpark Structured Streaming → Apache Iceberg (MinIO/S3)
                                                              │
                                        ┌─────────────────────┴─────────────────────┐
                                        ▼                                           ▼
                                  Trino (SQL analytics)                Airflow (scheduled compaction)
```

The ingestion path (Kafka → Spark → Iceberg) and the analytics/orchestration path (Trino + Airflow) run as separate Docker Compose stacks, so the streaming pipeline can be developed, deployed, and scaled independently of the BI and maintenance layer.

| Layer | Technology | Purpose |
|---|---|---|
| Infrastructure | Docker & Docker Compose | Local orchestration of all services |
| Message Broker | Apache Kafka (KRaft mode) | Durable, ordered ingestion of clickstream events |
| Stream Processing | PySpark Structured Streaming | Real-time parsing, watermarking, and session logic |
| Storage & Table Format | Apache Iceberg on MinIO (S3-compatible) | ACID-compliant, partition-aware lakehouse storage |
| Query Engine | Trino | Distributed SQL analytics directly over the lakehouse |
| Orchestration | Apache Airflow | Scheduled maintenance jobs (file compaction, snapshot expiry) |
| Data Simulation | Python (Faker) | Realistic synthetic user behavior for local development |

## Project Structure

```
ecommerce-analytics-pipeline/
│
├── docker-compose.yml         # Kafka, MinIO — core ingestion infrastructure
├── docker-compose-bi.yml      # Trino, Airflow — analytics and orchestration layer
├── init_tables.py             # Creates the Iceberg database and table schemas
├── producer.py                # Simulates real-time clickstream events
├── spark_stream.py            # Consumes events, writes to Iceberg
├── compact_iceberg.py         # Scheduled table maintenance (compaction, snapshot expiry)
├── trino/catalog/lakehouse.properties   # Trino → Iceberg/MinIO catalog config
└── requirements.txt
```

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

### 2. Start Core Infrastructure

```bash
docker compose up -d
```

Confirm MinIO is running by visiting `http://localhost:9001` (credentials as configured in `docker-compose.yml`).

### 3. Initialize Iceberg Tables

```bash
python init_tables.py
```

### 4. Start the Stream Processor

```bash
python spark_stream.py
```

### 5. Generate Simulated Traffic

In a separate terminal:

```bash
python producer.py
```

### 6. (Optional) Start the Analytics & Orchestration Layer

The Trino and Airflow stack is optional — it adds SQL query access and automated table maintenance on top of the core pipeline above.

Before starting Trino for the first time, create the catalog config it needs to connect to the lakehouse:

```bash
mkdir -p trino/catalog
```

Save the following as `trino/catalog/lakehouse.properties`:

```properties
connector.name=iceberg
iceberg.catalog.type=hadoop
iceberg.warehouse=s3a://lakehouse/
hive.s3.endpoint=http://minio:9000
hive.s3.aws-access-key=admin
hive.s3.aws-secret-key=password123
hive.s3.path-style-access=true
```

Then start the analytics stack:

```bash
docker compose -f docker-compose-bi.yml up -d
```

Connect a SQL client (Trino CLI, DBeaver, etc.) to `localhost:8080`, catalog `lakehouse`, schema `ecommerce`.

## Table Maintenance

Streaming writes naturally produce many small files over time, which degrades query performance — a well-known issue in lakehouse architectures known as the "small files problem." `compact_iceberg.py` addresses this by rewriting small data files into larger, query-optimized files and expiring old snapshots to reclaim storage:

```python
from pyspark.sql import SparkSession

def compact_lakehouse_tables():
    spark = SparkSession.builder \
        .appName("IcebergTableCompaction") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hadoop") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

    print("Compacting data files for 'raw_events'...")
    spark.sql("""
        CALL lakehouse.system.rewrite_data_files(
            table => 'lakehouse.ecommerce.raw_events',
            options => map('max-file-size-bytes', '536870912')
        )
    """)

    print("Expiring old snapshots to free storage...")
    spark.sql("CALL lakehouse.system.expire_snapshots('lakehouse.ecommerce.raw_events')")

    print("Maintenance run complete.")
    spark.stop()

if __name__ == "__main__":
    compact_lakehouse_tables()
```

In production, this would run on a schedule via an Airflow DAG (e.g., daily) rather than manually. A minimal DAG definition under `dags/` can trigger this script through a `BashOperator` or `SparkSubmitOperator`.

## Sample Analytical Queries

Once the Trino layer is running, these queries can be run directly against the lakehouse:

**Financial exposure from abandoned carts:**

```sql
SELECT
    abandoned_products_id AS product,
    COUNT(*) AS total_abandonments,
    SUM(potential_revenue_loss) AS total_lost_revenue_usd
FROM lakehouse.ecommerce.abandoned_carts
WHERE is_recovered = false
GROUP BY abandoned_products_id
ORDER BY total_lost_revenue_usd DESC
LIMIT 10;
```

**Funnel conversion rate by category:**

```sql
SELECT
    category,
    COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS total_views,
    COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) AS total_cart_adds,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS total_purchases,
    ROUND(
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) * 100.0
        / NULLIF(COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END), 0), 2
    ) AS cart_conversion_rate_percentage
FROM lakehouse.ecommerce.raw_events
GROUP BY category;
```

## Design Highlights

- **Hidden partitioning:** Iceberg's `days(timestamp)` partition transform accelerates time-range queries without requiring analysts to manually filter or restructure upstream columns.
- **Bounded streaming state:** A 10-minute watermark (`withWatermark`) caps how long late or out-of-order events are held in memory, keeping the Spark cluster's resource footprint predictable under continuous load.
- **Small-files mitigation:** Scheduled compaction via Iceberg's `rewrite_data_files` procedure consolidates the many small files produced by streaming writes into larger, query-efficient files, with `expire_snapshots` reclaiming storage from superseded data.
- **Decoupled compute and storage:** Kafka, Spark, Trino, and MinIO each operate independently, so ingestion, querying, and storage can scale or be replaced separately — the same pattern used in production big-data platforms.
- **Cloud portability:** Built entirely on S3-compatible interfaces (S3A adapters) via MinIO, the stack can move to AWS S3/EMR or GCP equivalents with configuration changes rather than architectural rework.
