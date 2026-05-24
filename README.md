# NYC Taxi Medallion Pipeline

A production-grade Bronze → Silver → Gold medallion architecture built
on Databricks Community Edition using the NYC Yellow Taxi public dataset.

> **Live dashboard:** [View on Databricks SQL →](https://your-share-link-here)  
> Replace this link after publishing via Share → Anyone with the link.

---

## What this project covers

- Ingesting Parquet files incrementally with Auto Loader (`cloudFiles`)
- Raw Bronze layer with audit columns and schema rescue mode
- Silver layer with Delta NOT NULL constraints, type casting, and deduplication
- Data quality validation with a Great Expectations suite
- Gold layer with pre-aggregated business metrics
- Databricks SQL dashboard with 4 live charts

---

## Architecture

```
NYC TLC Yellow Taxi  (Jan–Mar 2024, ~9M rows, ~150MB Parquet)
          │
          │  wget → dbfs:/taxi_project/landing/raw/
          ▼
┌─────────────────────────────────────────────────────┐
│  AUTO LOADER  (cloudFiles format=parquet)            │
│  - Schema inference saved to schemas/bronze/         │
│  - Checkpoint saved to checkpoints/bronze/           │
│  - schemaEvolutionMode = rescue                      │
│  - trigger = availableNow                            │
└───────────────────────┬─────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│  BRONZE  —  taxi_bronze.yellow_trips_raw             │
│  Partitioned by: year, month                         │
│  Added columns:                                      │
│    _ingested_at  TIMESTAMP  current_timestamp()      │
│    _source_file  STRING     input_file_name()        │
│  No transforms — raw archive only                    │
└───────────────────────┬─────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│  SILVER  —  taxi_silver.yellow_trips_clean           │
│  Partitioned by: year, month                         │
│  Transforms:                                         │
│    - Cast pickup_datetime, dropoff_datetime          │
│    - Cast trip_distance, fare_amount, tip_amount     │
│    - Derive year, month from pickup_datetime         │
│    - Drop rows: distance <= 0, fare <= 0             │
│    - Drop rows: dropoff <= pickup                    │
│    - Deduplicate on VendorID + pickup + dropoff      │
│  Delta constraints:                                  │
│    - positive_fare: fare_amount > 0                  │
│    - valid_distance: trip_distance > 0               │
│  Great Expectations: 6 expectations, all passing     │
└───────────────────────┬─────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│  GOLD  —  3 tables                                   │
│                                                      │
│  taxi_gold.daily_revenue_by_zone                     │
│    trip_date, PULocationID, total_trips,             │
│    total_fare, total_tips, avg_distance              │
│                                                      │
│  taxi_gold.trip_duration_stats                       │
│    trip_date, p50_mins, p95_mins, avg_mins           │
│                                                      │
│  taxi_gold.hourly_volume_heatmap                     │
│    day_of_week, hour_of_day, trip_count              │
└───────────────────────┬─────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│  DATABRICKS SQL DASHBOARD                            │
│  Chart 1: Daily revenue trend (line)                 │
│  Chart 2: Trip duration p50 vs p95 (area)            │
│  Chart 3: Top 10 pickup zones by revenue (bar)       │
│  Chart 4: Hourly trip volume heatmap (table)         │
└─────────────────────────────────────────────────────┘
```

---

## Folder structure

```
01_medallion_pipeline/
├── README.md                        ← you are here
├── DECISIONS.md                     ← why every design choice was made
│
├── notebooks/
│   ├── 01_bronze_ingest.py          ← Auto Loader → Bronze Delta table
│   ├── 02_silver_clean.py           ← Bronze → Silver with constraints
│   ├── 03_data_quality.py           ← Great Expectations validation suite
│   └── 04_gold_aggregations.py      ← Silver → 3 Gold tables
│
├── expectations/
│   └── silver_quality_suite.json    ← exported GE expectation suite
│
├── sql/
│   ├── dashboard_daily_revenue.sql
│   ├── dashboard_trip_duration.sql
│   └── dashboard_hourly_heatmap.sql
│
└── docs/
    ├── architecture.png             ← screenshot of architecture diagram
    └── dashboard_screenshot.png     ← screenshot of live dashboard
```

---

## Dataset

**NYC TLC Yellow Taxi Trip Records**

| Item | Detail |
|------|--------|
| Source | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page |
| Format | Parquet, one file per month |
| Months used | January 2024, February 2024, March 2024 |
| Approximate rows | 9,000,000 |
| Approximate size | 150 MB |
| Key columns | pickup_datetime, dropoff_datetime, trip_distance, fare_amount, tip_amount, PULocationID, passenger_count |

Download the files directly into DBFS — no local machine needed.
Run this in a Databricks notebook cell before anything else:

```python
import subprocess

months = ["2024-01", "2024-02", "2024-03"]

for m in months:
    url  = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{m}.parquet"
    dest = f"/dbfs/FileStore/taxi/raw/yellow_tripdata_{m}.parquet"
    subprocess.run(["wget", "-q", "-O", dest, url], check=True)
    print(f"Downloaded: {m}")

# Also download the zone lookup table
subprocess.run([
    "wget", "-q", "-O",
    "/dbfs/FileStore/taxi/lookup/taxi_zone_lookup.csv",
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
], check=True)

print("All files downloaded.")
display(dbutils.fs.ls("dbfs:/FileStore/taxi/raw/"))
```

---

## Setup

### Step 1 — Create a Databricks Community Edition account

Go to https://community.databricks.com — free, no credit card required.

### Step 2 — Create a cluster

| Setting | Value |
|---------|-------|
| Cluster mode | Single Node |
| Runtime | 13.3 LTS ML |
| Auto-terminate | 2 hours |

Great Expectations is pre-installed on 13.3 LTS ML — no pip install needed.

### Step 3 — Create the DBFS folder layout

Run this once in any notebook to set up all folders:

```python
folders = [
    "dbfs:/taxi_project/landing/raw",
    "dbfs:/taxi_project/checkpoints/bronze",
    "dbfs:/taxi_project/checkpoints/silver",
    "dbfs:/taxi_project/schemas/bronze",
    "dbfs:/taxi_project/lookup",
]
for f in folders:
    dbutils.fs.mkdirs(f)
    print(f"Created: {f}")
```

### Step 4 — Create the databases

```sql
CREATE DATABASE IF NOT EXISTS taxi_bronze;
CREATE DATABASE IF NOT EXISTS taxi_silver;
CREATE DATABASE IF NOT EXISTS taxi_gold;
```

### Step 5 — Run notebooks in order

```
01_bronze_ingest.py      →  taxi_bronze.yellow_trips_raw
02_silver_clean.py       →  taxi_silver.yellow_trips_clean
03_data_quality.py       →  GE report printed to notebook output
04_gold_aggregations.py  →  taxi_gold.daily_revenue_by_zone
                             taxi_gold.trip_duration_stats
                             taxi_gold.hourly_volume_heatmap
```

Each notebook is **idempotent** — safe to re-run without creating
duplicate data or breaking downstream tables.

### Step 6 — Build the dashboard

1. Go to Databricks SQL → Dashboards → Create Dashboard
2. Name it: `NYC Taxi Analytics — 2024 Q1`
3. Add 4 query visualizations using the SQL files in `sql/`
4. Click Share → Anyone with the link
5. Paste the link into the Live dashboard section at the top of this README

---

## Results

| Metric | Value |
|--------|-------|
| Bronze rows ingested | ~9,000,000 |
| Silver rows after cleaning | ~8,750,000 |
| Rows dropped by quality rules | ~250,000 (~2.8%) |
| Great Expectations suite | 6 / 6 passing |
| Gold tables created | 3 |
| Dashboard charts | 4 |
| Delta table versions (Bronze) | 3 (one per file batch) |
| Delta table versions (Silver) | 3 |

---

## Stack

| Tool | Purpose |
|------|---------|
| Databricks Community Edition | Platform (free) |
| Apache Spark 3.5 | Distributed processing engine |
| Delta Lake 3.1 | Storage layer — ACID, time travel, constraints |
| Auto Loader (cloudFiles) | Incremental file ingestion |
| Great Expectations 0.18 | Data quality validation |
| Databricks SQL | Dashboard and BI queries |
| Python 3.10 | Notebook language |

---

## Where to start reading

If you are reviewing this project, the best reading order is:

1. This `README.md` — understand the architecture
2. `DECISIONS.md` — understand why each design choice was made
3. `notebooks/01_bronze_ingest.py` — see Auto Loader in action
4. `notebooks/02_silver_clean.py` — see Delta constraints and foreachBatch
5. `notebooks/03_data_quality.py` — see the Great Expectations suite
6. `sql/dashboard_daily_revenue.sql` — see the Gold query pattern
