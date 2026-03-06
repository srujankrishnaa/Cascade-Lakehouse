# User Manual — Lakehouse Medallion Pipeline

> A complete guide to set up, understand, and run a production-grade Data Lakehouse using the Medallion Architecture on a local Kubernetes cluster.

---

## Table of Contents

1. [System Requirements](#1-system-requirements)
2. [Prerequisites Installation](#2-prerequisites-installation)
3. [Architecture Overview](#3-architecture-overview)
4. [Project File Reference](#4-project-file-reference)
5. [Infrastructure Setup](#5-infrastructure-setup)
6. [Build the Spark Docker Image](#6-build-the-spark-docker-image)
7. [Pipeline Execution — Step by Step](#7-pipeline-execution--step-by-step)
8. [Querying with Trino](#8-querying-with-trino)
9. [How Iceberg Writes Work](#9-how-iceberg-writes-work)
10. [Troubleshooting Guide](#10-troubleshooting-guide)

---

## 1. System Requirements

### Minimum Specs

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| **RAM** | 8 GB | 16 GB |
| **CPU Cores** | 4 | 8 |
| **Disk** | 20 GB free | 40 GB free |
| **OS** | Windows 10/11 or macOS or Linux | — |

### ⚠️ Performance Warning

This pipeline runs **multiple distributed systems** (Spark, MinIO, Nessie, Trino) simultaneously on your local machine. On lower-spec machines:

- **8 GB RAM**: You **must** run pipeline jobs one at a time (wave-by-wave). Running 2+ Spark jobs simultaneously will cause Kubernetes to OOM-kill executor pods.
- **16 GB RAM**: You can run 2 jobs in parallel safely. Still avoid running all jobs at once.
- **Silver Transform (page_views)** is the most memory-intensive job because it runs 6 Python UDF calls per row. If it keeps failing, increase executor memory to `2g` in its YAML.
- **Gold Facts** run file compaction (`rewrite_data_files`), which is CPU and I/O intensive. Give it time on slower hardware.

### Docker Desktop Settings

Go to **Docker Desktop → Settings → Resources** and configure:
- **Memory**: At least 6 GB (8+ GB recommended)
- **CPUs**: At least 4
- **Disk**: At least 20 GB

---

## 2. Prerequisites Installation

### Step 1: Install Docker Desktop

Download from [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop) and install.

### Step 2: Enable Kubernetes

1. Open **Docker Desktop → Settings → Kubernetes**
2. Check ✅ **Enable Kubernetes**
3. Click **Apply & Restart**
4. Wait for the Kubernetes indicator to turn green (2-3 minutes)

Verify:
```bash
kubectl get nodes
# Expected:
# NAME             STATUS   ROLES           AGE   VERSION
# docker-desktop   Ready    control-plane   ...   v1.x.x
```

### Step 3: Install Helm

```bash
# Windows (PowerShell)
winget install Helm.Helm

# macOS
brew install helm

# Linux
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

Verify:
```bash
helm version
```

### Step 4: Install the Spark Operator

The Spark Operator lets you submit Spark jobs as Kubernetes resources (`SparkApplication` CRDs).

```bash
# Add the Spark Operator Helm chart repository
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update

# Install the operator
helm install spark-operator spark-operator/spark-operator \
  --set webhook.enable=true \
  --set sparkJobNamespace=default

# Verify the operator is running
kubectl get pods | grep spark-operator
# Expected: spark-operator-...   1/1   Running
```

### Step 5: Create the Spark Service Account

Spark needs a Kubernetes service account with permissions to create executor pods:

```bash
kubectl create serviceaccount spark
kubectl create clusterrolebinding spark-role \
  --clusterrole=edit \
  --serviceaccount=default:spark
```

---

## 3. Architecture Overview

```
┌───────────────────────────────────────────────────────────────────┐
│                      Kubernetes Cluster                          │
│                                                                   │
│   ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌────────────────┐  │
│   │  MinIO   │   │ Nessie  │   │  Trino  │   │ Spark Operator │  │
│   │ Storage  │   │ Catalog │   │ Query   │   │  Job Scheduler │  │
│   │ (S3)     │   │ (Git)   │   │ Engine  │   │                │  │
│   └────┬─────┘   └────┬────┘   └────┬────┘   └───────┬────────┘  │
│        │              │             │                 │           │
│        └──────────────┴─────────────┴─────────────────┘           │
│                           │                                       │
│              ┌────────────┴────────────┐                          │
│              │     Apache Iceberg      │                          │
│              │   Open Table Format     │                          │
│              └────────────┬────────────┘                          │
│                           │                                       │
│    ┌──────────┐     ┌───────────┐     ┌──────────┐               │
│    │  BRONZE  │ ──► │  SILVER   │ ──► │   GOLD   │               │
│    │ Raw Data │     │ Enriched  │     │  Facts   │               │
│    └──────────┘     └───────────┘     └──────────┘               │
└───────────────────────────────────────────────────────────────────┘
```

### What Each Component Does

| Component | Role | Analogy |
|-----------|------|---------|
| **MinIO** | S3-compatible object storage. Stores all Parquet data files and Iceberg metadata. | Your hard drive in the cloud |
| **Project Nessie** | Iceberg catalog with git-like branching. Tracks which tables exist and where their metadata lives. | Git for your data tables |
| **Apache Iceberg** | Open table format. Manages snapshots, manifests, schema evolution, and ACID transactions on top of Parquet files. | The file system that makes Parquet files behave like database tables |
| **Apache Spark** | Distributed compute engine. Reads/writes data, runs transformations, executes SQL. | The CPU that processes your data |
| **Spark Operator** | Kubernetes controller. Manages Spark driver and executor pods from YAML definitions. | The scheduler that launches Spark jobs |
| **Trino** | SQL query engine. Connects to Iceberg tables for interactive analytics. | The SQL client you use to explore results |

---

## 4. Project File Reference

### Infrastructure (`infrastructure/`)

These YAMLs deploy the core platform services as Kubernetes pods:

| File | What It Deploys | Key Config |
|------|----------------|------------|
| `minio-deployment.yaml` | MinIO server pod | Stores data at `/data/`, credentials: `admin`/`password` |
| `minio-service.yaml` | MinIO K8s Service | Exposes ports 9000 (API) and 9001 (Console) |
| `nessie-deployment.yaml` | Nessie catalog pod | **In-memory storage** — resets on pod restart |
| `nessie-service.yaml` | Nessie K8s Service | Exposes API at port 19120 |
| `trino-deployment.yaml` | Trino query engine pod | Configured via ConfigMap for Iceberg |
| `trino-configmap.yaml` | Trino catalog config | Connects Trino to Nessie catalog at `http://nessie:19120/api/v2` |
| `trino-service.yaml` | Trino K8s Service | Exposes port 8080 |

> ⚠️ **Nessie uses in-memory storage.** If the Nessie pod restarts, all table definitions are lost. You must rerun DDL jobs to recreate them. The actual data in MinIO is not affected.

---

### DDL Setup (`src/ddl/`)

These files create the Iceberg table schemas in each layer:

#### `src/ddl/bronze/setup.py` — BronzeSetup

Creates the `nessie.bronze` namespace and two tables:

| Table | Columns | Partitioning |
|-------|---------|-------------|
| `bronze.page_views` | `event_type`, `event_id`, `created_ts`, `session_id`, `user_id`, `page_url` | `bucket(3, event_id)` |
| `bronze.click_events` | Same as above + `element_id`, `element_text`, `element_tag` | `bucket(3, event_id)` |

**Why `bucket(3, event_id)`?** Distributes data across 3 buckets based on a hash of `event_id`. This ensures MERGE INTO queries only scan 1 of 3 buckets when matching by `event_id`, reducing I/O by ~67%.

#### `src/ddl/silver/setup.py` — SilverSetup

Creates 4 Silver tables:

| Table | Purpose | Extra Columns vs Bronze |
|-------|---------|----------------------|
| `silver.page_views` | Enriched + deduplicated page views | `product_id`, `product_name`, `product_segment`, `user_location`, `user_gender` |
| `silver.click_events` | Enriched + deduplicated click events | `product_id`, `product_name`, `product_segment` |
| `silver.page_views_agg` | Direct copy of Silver page_views (for streaming to Gold) | Same as `silver.page_views` |
| `silver.click_events_agg` | Direct copy of Silver click_events (for streaming to Gold) | Same as `silver.click_events` |

#### `src/ddl/gold/setup.py` — GoldSetup

Creates 1 Gold table:

| Table | Columns | Purpose |
|-------|---------|---------|
| `gold.fact_product_metrics` | `minute_ts`, `product_id`, `product_name`, `product_segment`, `counts`, `count_type`, `click` (struct) | Per-minute product metrics (views + clicks) |

---

### Data Generation (`src/ingestion/`)

#### `src/ingestion/pageviews.py` — PageViewsIngestion

**What it does:** Generates 10 synthetic page view events per batch, simulating users browsing an e-commerce site.

**How it works:**
```
1. Generate 10 random page views:
   - Random user (user_1 to user_5)
   - Random session (correlated to user)
   - Random product URL from 25 products across 5 categories
   - UUID event_id + current timestamp

2. Inject ~5% duplicates (simulates real-world data quality issues)

3. Deduplicate within the batch:
   row_number().over(Window.partitionBy("event_id")
                           .orderBy(col("created_ts").desc()))
   .filter(col("row_num") == 1)

4. Write to Iceberg:
   df.writeTo("nessie.bronze.page_views").append()
```

**The runner** (`runners/Ingestion.py`) calls `ingest_page_views()` in a `while True` loop with a 1-second sleep, creating continuous streaming ingestion.

#### `src/ingestion/clickevents.py` — ClickEventsIngestion

**What it does:** Generates 30 click events per batch (3x the rate of page views).

**Extra fields vs page views:**
- `element_id`: What was clicked (`add-to-cart`, `buy-now`, `wishlist`, `product-image`, `product-description`)
- `element_text`: Display text (`Add to Cart`, `Buy Now`, etc.)
- `element_tag`: HTML tag (`button`, `img`, `a`)

---

### Silver Transformation (`src/transformation/`)

#### `src/transformation/pageviews.py` — PageViewsTransformation

Contains two methods:

##### `transform()` — Bronze → Silver Enrichment

**What it does:** Reads Bronze page_views as a stream, enriches each event with product and user details via UDFs, and writes to Silver using MERGE INTO for cross-batch deduplication.

**How it works step by step:**

```
1. Start Iceberg streaming read:
   spark.readStream.format("iceberg")
     .option("streaming-max-files-per-micro-batch", "1")
     .load("nessie.bronze.page_views")

   → Iceberg tracks "which snapshots have I already read?" via checkpoint
   → Each new append to Bronze triggers a new micro-batch

2. For each micro-batch, the _upsert function runs:

   a. Register the batch as a temp view: "page_views_input"
   
   b. Execute MERGE INTO with UDF enrichment:
      MERGE INTO nessie.silver.page_views AS target
      USING (
          SELECT event_type, event_id, created_ts, session_id, page_url,
                 get_product_id(page_url)                     as product_id,
                 get_product_name(get_product_id(page_url))   as product_name,
                 get_product_segment(get_product_id(page_url)) as product_segment,
                 user_id,
                 get_user_location(user_id)  as user_location,
                 get_user_gender(user_id)    as user_gender
          FROM page_views_input
      ) AS source
      ON target.event_id = source.event_id
      WHEN NOT MATCHED THEN INSERT *
   
   → The MERGE ensures no duplicate event_ids across batches
   → UDFs enrich each row with product/user details
   → Iceberg's bucket partitioning prunes the JOIN efficiently

3. Checkpoint saves progress to MinIO:
   s3a://warehouse/checkpoint/silver_page_views_transf/
```

##### `aggregate()` — Silver → Silver Agg (Pass-through)

**What it does:** Reads Silver page_views as a stream and writes directly to `silver.page_views_agg` (an append-only copy used for Gold layer streaming).

**Why a separate table?** Iceberg streaming can only read from tables that receive `append()` writes. The MERGE INTO in the transform step creates `overwrite` snapshots, which aren't visible to a downstream streaming reader. The agg table receives pure appends.

#### `src/transformation/clickevents.py` — ClickEventsTransformation

Identical pattern to page views, but:
- Reads from `bronze.click_events`
- Enriches with 3 UDFs (no user enrichment): `get_product_id`, `get_product_name`, `get_product_segment`
- Writes to `silver.click_events` and `silver.click_events_agg`

---

### Gold Facts (`src/facts/productfacts.py`) — ProductFacts

#### `create_product_facts_from_page_view()`

**What it does:** Reads `silver.page_views_agg` as a stream, aggregates by product + minute, writes to Gold, then compacts small files.

**How it works:**

```
1. Read Silver agg as stream
   
2. For each micro-batch:
   a. Add minute_ts column: date_trunc("minute", "created_ts")
   
   b. GROUP BY minute_ts, product_segment, product_name, product_id
      COUNT(*) as counts, 'views' as count_type
   
   c. Write grouped data:
      grouped_df.writeTo("nessie.gold.fact_product_metrics").append()
   
   d. Run file compaction:
      CALL nessie.system.rewrite_data_files(
          table => 'nessie.gold.fact_product_metrics',
          where => "minute_ts = ... AND product_segment = ...",
          options => map(
              'target-file-size-bytes', '536870912'  -- 512 MB
          )
      )
```

**Why compact?** Streaming creates many tiny Parquet files (one per micro-batch). Compaction merges them into 512 MB target files for efficient query performance.

#### `create_product_facts_from_click_events()`

Same pattern but aggregates click events with element-level granularity (adds `element_id`, `element_text`).

---

### UDFs and Services (`src/utils/`, `src/services/`)

#### `src/utils/sparkudfs.py` — SparkUDFs

Registers 6 Spark SQL UDFs used in Silver transformation:

| UDF Name | Input | Output | Source |
|----------|-------|--------|--------|
| `get_product_id(url)` | Page URL | Product ID (e.g., `E001-LAPTOP`) | Strips URL prefix |
| `get_product_name(url)` | Page URL | Product name (e.g., `MacBook Pro`) | ProductService lookup |
| `get_product_segment(url)` | Page URL | Category (e.g., `Electronics`) | ProductService lookup |
| `get_user_location(user_id)` | User ID | City name (e.g., `Mumbai`) | UserService (Faker) |
| `get_user_age(user_id)` | User ID | Age (18-80) | UserService |
| `get_user_gender(user_id)` | User ID | `Male` or `Female` | UserService |

> ⚠️ These are Python UDFs, which are slow because data crosses the JVM ↔ Python boundary for every row. In production, use **Pandas UDFs** (vectorized) or native Spark SQL functions.

#### `src/utils/products.py` — Products

A product catalog of 25 products across 5 categories: Electronics, Clothing, Books, Sports, Food. Used to generate realistic page URLs and look up product details.

#### `src/services/productservice.py` — ProductService

Wraps the Products catalog to return product details (name, segment) given a product ID.

#### `src/services/userservice.py` — UserService

Generates deterministic user attributes using `Faker` library with `hash(user_id)` as seed. This means the same `user_id` always gets the same location, age, and gender.

---

### Runner Scripts (`runners/`)

These are the **entry points** that the Docker container executes:

| File | SparkApplication YAML | What It Runs |
|------|----------------------|--------------|
| `DDL.py` | `spark-jobs/ddl/*.yaml` | Creates schemas + tables. Arg: `--ddl_type=bronze\|silver\|gold` |
| `Ingestion.py` | `spark-jobs/bronze/*.yaml` | Continuous ingestion loop. Arg: `--ingestion_type=page_views\|click_events` |
| `Transformation.py` | `spark-jobs/silver/silver-transform-*.yaml` | MERGE INTO enrichment. Arg: `--transformation_type=page_views\|click_events` |
| `Aggregation.py` | `spark-jobs/silver/silver-aggregation-*.yaml` | Silver → Silver agg streaming. Arg: `--aggregation_type=page_views\|click_events` |
| `Facts.py` | `spark-jobs/gold/*.yaml` | Gold aggregation + compaction. Arg: `--facts_type=page_views\|click_events` |
| `TestRunner.py` | `spark-jobs/test-run-sampler.yaml` | Runs a single batch of all stages for quick validation |

---

### SparkApplication YAMLs (`spark-jobs/`)

Each YAML defines a `SparkApplication` Kubernetes resource with:

```yaml
spec:
  type: Python
  image: "webclickstream-events"        # Docker image
  mainApplicationFile: local:///opt/application/Ingestion.py  # Runner script
  arguments: ["--ingestion_type=page_views"]  # CLI args
  
  sparkConf:
    # Nessie catalog configuration
    "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog"
    "spark.sql.catalog.nessie.uri": "http://nessie:19120/api/v2"
    "spark.sql.catalog.nessie.warehouse": "s3://warehouse/"
    "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog"
    
    # S3/MinIO configuration
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000"
    "spark.hadoop.fs.s3a.access.key": "admin"
    "spark.hadoop.fs.s3a.secret.key": "password"
    
  driver:
    memory: "1g"       # 2g for silver-transform-pageviews
  executor:
    instances: 1
    memory: "1g"       # 2g for silver-transform-pageviews
```

**Key YAML per pipeline stage:**

| YAML | Runner | Purpose |
|------|--------|---------|
| `ddl/bronze-ddl.yaml` | DDL.py `--ddl_type=bronze` | Create Bronze namespace + tables |
| `ddl/silver-ddl.yaml` | DDL.py `--ddl_type=silver` | Create Silver namespace + tables |
| `ddl/gold-ddl.yaml` | DDL.py `--ddl_type=gold` | Create Gold namespace + tables |
| `bronze/bronze-ingest-pageviews.yaml` | Ingestion.py `--ingestion_type=page_views` | Continuous page view generation |
| `bronze/bronze-ingest-clickevents.yaml` | Ingestion.py `--ingestion_type=click_events` | Continuous click event generation |
| `silver/silver-transform-pageviews.yaml` | Transformation.py `--transformation_type=page_views` | Bronze → Silver enrichment + dedup (page views) |
| `silver/silver-transform-clickevents.yaml` | Transformation.py `--transformation_type=click_events` | Bronze → Silver enrichment + dedup (click events) |
| `silver/silver-aggregation-pageviews.yaml` | Aggregation.py `--aggregation_type=page_views` | Silver → Silver agg (page views) |
| `silver/silver-aggregation-clickevents.yaml` | Aggregation.py `--aggregation_type=click_events` | Silver → Silver agg (click events) |
| `gold/gold-fact-productmetrics-pageview.yaml` | Facts.py `--facts_type=page_views` | Silver agg → Gold facts (page views) |
| `gold/gold-fact-productmetrics-clickevents.yaml` | Facts.py `--facts_type=click_events` | Silver agg → Gold facts (click events) |

---

## 5. Infrastructure Setup

### Step 1: Deploy All Services

```bash
kubectl apply -f infrastructure/
```

This creates 3 pods: MinIO, Nessie, Trino.

### Step 2: Wait for Pods to Be Ready

```bash
kubectl get pods
# Wait until all show:  1/1   Running
```

### Step 3: Create the MinIO Warehouse Bucket

```bash
kubectl exec <minio-pod-name> -- mkdir -p /data/warehouse
```

### Step 4: Verify Trino Can See the Catalog

```bash
kubectl exec -it <trino-pod-name> -- trino --execute "SHOW SCHEMAS IN iceberg"
# Expected: information_schema, system
```

---

## 6. Build the Spark Docker Image

```bash
docker build -t webclickstream-events .
```

This creates a Docker image based on `apache/spark:3.5.3-python3` with your Python source code and dependencies installed.

The image contains:
```
/opt/application/
├── DDL.py, Ingestion.py, Transformation.py, Aggregation.py, Facts.py
└── src/ (all Python modules)
```

---

## 7. Pipeline Execution — Step by Step

### ⚠️ Critical: One Wave at a Time

On a local machine, **never run more than 2 Spark jobs simultaneously**. Follow this wave-by-wave order:

---

### Wave 0: Create Tables (DDL)

Run each DDL job **one at a time**, waiting for `COMPLETED` before starting the next:

```bash
kubectl apply -f spark-jobs/ddl/bronze-ddl.yaml
kubectl get sparkapplications -w
# Wait for: bronze-ddl  COMPLETED

kubectl delete sparkapplication bronze-ddl
kubectl apply -f spark-jobs/ddl/silver-ddl.yaml
# Wait for: silver-ddl  COMPLETED

kubectl delete sparkapplication silver-ddl
kubectl apply -f spark-jobs/ddl/gold-ddl.yaml
# Wait for: gold-ddl  COMPLETED

kubectl delete sparkapplication gold-ddl
```

Verify in Trino:
```bash
kubectl exec -it <trino-pod> -- trino --execute "SHOW SCHEMAS IN iceberg"
# Expected: bronze, gold, information_schema, silver, system
```

---

### Wave 1: Bronze Ingestion

Start the ingestion jobs. These run continuously, generating data every second:

```bash
kubectl apply -f spark-jobs/bronze/bronze-ingest-pageviews.yaml
kubectl apply -f spark-jobs/bronze/bronze-ingest-clickevents.yaml
```

Monitor data flowing in:
```bash
kubectl exec -it <trino-pod> -- trino --execute \
  "SELECT COUNT(*) FROM iceberg.bronze.page_views"
# Watch count grow: 10 → 20 → 30 → ...
```

**Let Bronze run for 2-3 minutes** to accumulate data, then stop both:
```bash
kubectl delete sparkapplication bronze-ingestion-pageviews bronze-ingestion-clickevents
```

> The data is permanently stored in MinIO. Stopping ingestion doesn't delete anything.

---

### Wave 2: Silver Transform

Start transformation jobs (safe to run both together on 16GB RAM, on 8GB run one at a time):

```bash
kubectl apply -f spark-jobs/silver/silver-transform-pageviews.yaml
```

Monitor Silver table growing:
```bash
kubectl exec -it <trino-pod> -- trino --execute \
  "SELECT COUNT(*) FROM iceberg.silver.page_views"
# Watch: 0 → 50 → 100 → ... → plateaus at ~Bronze count
```

**When the count plateaus** (stops growing for 2-3 checks), the job has processed all Bronze data. Then:

```bash
kubectl apply -f spark-jobs/silver/silver-transform-clickevents.yaml
```

Once both plateau, stop them:
```bash
kubectl delete sparkapplication silver-transform-pageviews silver-transform-clickevents
```

---

### Wave 3: Silver Aggregation

```bash
kubectl apply -f spark-jobs/silver/silver-aggregation-pageviews.yaml
kubectl apply -f spark-jobs/silver/silver-aggregation-clickevents.yaml
```

Monitor:
```bash
kubectl exec -it <trino-pod> -- trino --execute \
  "SELECT COUNT(*) FROM iceberg.silver.page_views_agg"
```

When plateaued:
```bash
kubectl delete sparkapplication silver-aggregation-pageviews silver-aggregation-clickevents
```

---

### Wave 4: Gold Facts

```bash
kubectl apply -f spark-jobs/gold/gold-fact-productmetrics-pageview.yaml
```

Monitor:
```bash
kubectl exec -it <trino-pod> -- trino --execute \
  "SELECT COUNT(*) FROM iceberg.gold.fact_product_metrics"
```

When plateaued:
```bash
kubectl delete sparkapplication gold-fact-productmetrics-pageview
kubectl apply -f spark-jobs/gold/gold-fact-productmetrics-clickevents.yaml
```

When done:
```bash
kubectl delete sparkapplication gold-fact-productmetrics-clickevents
```

---

## 8. Querying with Trino

Connect to Trino:
```bash
kubectl exec -it <trino-pod> -- trino
```

### Explore Schemas
```sql
SHOW SCHEMAS IN iceberg;
SHOW TABLES IN iceberg.bronze;
SHOW TABLES IN iceberg.silver;
SHOW TABLES IN iceberg.gold;
```

### Row Counts Across Layers
```sql
SELECT 'bronze.page_views' as tbl, COUNT(*) FROM iceberg.bronze.page_views
UNION ALL
SELECT 'silver.page_views', COUNT(*) FROM iceberg.silver.page_views
UNION ALL
SELECT 'gold.fact_product_metrics', COUNT(*) FROM iceberg.gold.fact_product_metrics;
```

### Compare Bronze vs Silver (See Enrichment)
```sql
-- Bronze: raw columns only
SELECT * FROM iceberg.bronze.page_views LIMIT 3;

-- Silver: enriched with product_name, product_segment, user_location, user_gender
SELECT * FROM iceberg.silver.page_views LIMIT 3;
```

### Business Query: Top Products
```sql
SELECT product_name, product_segment, SUM(counts) as total
FROM iceberg.gold.fact_product_metrics
WHERE count_type = 'views'
GROUP BY product_name, product_segment
ORDER BY total DESC;
```

### Iceberg Time Travel
```sql
-- See table snapshots
SELECT * FROM iceberg.bronze."page_views$snapshots";

-- Query data as of a specific snapshot
SELECT COUNT(*) FROM iceberg.bronze.page_views
FOR VERSION AS OF <snapshot_id>;
```

Exit Trino: type `quit` or press `Ctrl+C`.

---

## 9. How Iceberg Writes Work

### File Structure on MinIO

```
s3://warehouse/
└── bronze/page_views/
    ├── metadata/
    │   ├── v1.metadata.json     ← Table schema, partition spec
    │   ├── v2.metadata.json     ← After write: points to snap-001
    │   ├── snap-001.avro        ← Manifest list: "data is in manifest-A"
    │   ├── snap-002.avro        ← Manifest list: "manifest-A + manifest-B"
    │   ├── manifest-A.avro      ← File list: file1.parquet, file2.parquet
    │   └── manifest-B.avro      ← File list: file3.parquet
    └── data/
        ├── file1.parquet        ← Actual row data
        ├── file2.parquet
        └── file3.parquet
```

### The 3 Write Patterns Used in This Project

**1. `append()` — Bronze Ingestion**
```python
df.writeTo("nessie.bronze.page_views").append()
```
- Creates new Parquet files + new manifest + new snapshot
- Old data is untouched

**2. `MERGE INTO` — Silver Transform**
```sql
MERGE INTO silver.page_views AS target
USING (...) AS source
ON target.event_id = source.event_id
WHEN NOT MATCHED THEN INSERT *
```
- JOINs source batch against entire target table
- Only inserts rows where `event_id` doesn't already exist
- Creates an `overwrite` snapshot (copy-on-write)

**3. `rewrite_data_files` — Gold Compaction**
```sql
CALL nessie.system.rewrite_data_files(
    table => 'nessie.gold.fact_product_metrics',
    where => "...",
    options => map('target-file-size-bytes', '536870912')
)
```
- Reads many small files → writes fewer large files
- Atomically swaps manifest entries (old files → new files)
- Old snapshots still reference old files (time travel still works)

### Key Insight

**Iceberg never modifies existing files.** It only adds new files and swaps metadata pointers atomically. This is why:
- ✅ Concurrent reads never see partial writes
- ✅ Time travel works (old snapshots still valid)
- ✅ MERGE INTO is ACID-safe
- ✅ Failed jobs don't corrupt data

---

## 10. Troubleshooting Guide

### Job Shows FAILED

```bash
# Check the driver logs for the actual error:
kubectl logs <job-name>-driver --tail=50
```

### Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `NoSuchBucketException` | MinIO `warehouse` bucket doesn't exist | `kubectl exec <minio-pod> -- mkdir -p /data/warehouse` |
| `Schema 'bronze' does not exist` | Nessie pod restarted, catalog lost | Rerun DDL jobs |
| `Connection refused` to `kubernetes.default.svc` | Too many jobs running, K8s API overloaded | Delete some jobs, run one at a time |
| `Max number of executor failures (3) reached` | Executor OOM-killed by K8s | Increase executor memory in YAML to `2g` |
| `BroadcastExchangeExec TimeoutException` | Table too large to broadcast on local cluster | Add `spark.sql.autoBroadcastJoinThreshold: "-1"` to sparkConf |
| `Initial job has not accepted any resources` | Cluster ran out of memory for new executor | Delete other running jobs to free resources |
| `TLS handshake timeout` | Docker Desktop K8s API overloaded | Wait 30s and retry, or restart Docker Desktop |

### Job Stuck at Same Count

1. Check if the job is still RUNNING: `kubectl get sparkapplications`
2. If RUNNING but count frozen: check logs for "no resources" warnings
3. Delete other jobs to free cluster resources

### After PC Reboot

```bash
# 1. Wait for Docker Desktop + K8s to start (green indicator)
kubectl get nodes   # Should show: Ready

# 2. Check pods restarted
kubectl get pods    # MinIO, Nessie, Trino should be Running

# 3. Recreate MinIO bucket (if lost)
kubectl exec <minio-pod> -- mkdir -p /data/warehouse

# 4. Rerun DDL jobs (Nessie catalog reset)
kubectl apply -f spark-jobs/ddl/bronze-ddl.yaml
# ... wait for each to complete
```

---

*Built with Apache Spark 3.5 • Apache Iceberg 1.8 • Project Nessie 0.103 • MinIO • Trino • Kubernetes*
