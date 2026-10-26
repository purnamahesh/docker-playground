# Spark Lakehouse Lab

Five projects built on one realistic mini data platform:

- **PostgreSQL** simulates the operational (OLTP) source systems.
- **MinIO** simulates the object-store data lake (S3 API).
- **Apache Iceberg** is the lakehouse table format for bronze, silver and gold.
- **Spark 4.x standalone** (1 master + 4 workers, 2 cores / 2 GB each) with **JupyterLab** as the driver.

This guide contains no Dockerfiles, compose files or code. Every project is a specification: what to build, what to observe, and what you should be able to explain afterwards.

| # | Project | You learn |
|---|---|---|
| 1 | Lakehouse medallion ETL | JDBC extraction, incremental loads, Iceberg writes, MERGE, SCD2, time travel, schema/partition evolution, maintenance |
| 2 | Joins, shuffles and skew | join strategies, AQE, skew fixes, storage-partitioned joins, federated JDBC joins |
| 3 | Catalyst and query planning | plan reading, Iceberg/JDBC pushdown, pruning, statistics, runtime filters, layout |
| 4 | Executors, memory and I/O performance | executor sizing, memory, Python UDFs, S3A tuning, JDBC concurrency, commit conflicts, failures |
| 5 | Streaming and CDC into Iceberg | triggers, watermarks, state, CDC apply with MERGE, small files, recovery |
| ref | Configuration map | every important config by category, now including JDBC, S3A and Iceberg |

**Suggested order:** setup, then 1, 3, 2, 4, 5. Keep the reference open throughout.

---

## 1. Target architecture

Data flows left to right:

1. **Sources:** PostgreSQL (orders, customers, products, payments) plus a web clickstream delivered as JSON files.
2. **Landing:** Spark JDBC extracts and the clickstream files arrive as raw objects in a MinIO `landing` bucket.
3. **Bronze -> silver -> gold:** Iceberg tables in a MinIO `warehouse` bucket, organized into `bronze`, `silver` and `gold` namespaces.
4. **Consumers:** gold tables are queried from Jupyter, and a reverse-ETL export writes selected aggregates back into Postgres.
5. **Operations:** Spark event logs, streaming checkpoints and an optional Iceberg catalog database live outside the compute cluster.

### Service inventory

| Service | Role | Notes |
|---|---|---|
| Spark master + 4 workers | compute | as you have today |
| JupyterLab | driver and notebooks | workers must be able to resolve its hostname |
| History server | compare runs | reads event logs from a bucket |
| PostgreSQL | OLTP source (+ catalog DB if you choose a JDBC catalog) | default port 5432; use separate databases for source and catalog |
| MinIO | object storage | S3 API on 9000, console on 9001 |
| Optional: Iceberg REST catalog | production-like catalog | stretch goal |

### Buckets (suggested)

| Bucket | Contents |
|---|---|
| `landing` | raw extracts and clickstream files, organized by source/table/date |
| `warehouse` | Iceberg tables (data and metadata) |
| `checkpoints` | streaming checkpoints |
| `spark-events` | Spark event logs |
| `archive` | processed landing files moved out by cleanup |

---

## 2. Decision gates (do these before Project 1)

### Gate A: Spark and Iceberg version compatibility

Iceberg ships a **separate Spark runtime artifact per Spark minor version** (Scala 2.13 for Spark 4.x), and a mismatched runtime fails at extension load time. Check Maven Central for the runtime that matches your Spark version. Spark 4.2 support has been merged into Iceberg and listed in the 1.12.0 release notes, but a bug report said the 4.2 runtime artifact was missing from Maven, so verify it exists before relying on it. If it doesn't, pin Spark to the newest 4.x version with a published runtime (Iceberg 1.11 publishes a Spark 4.1 runtime). Version choice is part of the lab: record what you picked and why.

### Gate B: classpath dependencies

Decide how each of these reaches **both the driver and every executor** (baked into the image vs resolved at runtime, with trade-offs for reproducibility, build time and offline use):

- Iceberg Spark runtime matching your Spark version
- Object-store access: either the Hadoop S3A connector plus a matching AWS SDK bundle (versions must line up with the Hadoop version inside your Spark distribution), or Iceberg's own AWS bundle with its S3 file I/O. Project 4 compares them.
- PostgreSQL JDBC driver (also needed by the catalog if you use a JDBC catalog)

### Gate C: catalog choice

| Catalog | Pros | Cons |
|---|---|---|
| Hadoop (path-based) | zero extra services | not safe on object stores without atomic rename; single-writer only |
| JDBC (Postgres-backed) | atomic commits, concurrent writers, reuses Postgres | one more database to manage |
| REST | closest to production, engine-neutral | extra container and config |

Recommended: start with **JDBC on Postgres** (separate database from your source data), and treat REST as a stretch goal.

### Gate D: MinIO specifics

MinIO needs path-style addressing, a custom endpoint, and no TLS in a local lab. Create the buckets up front, and pick credentials you will reuse everywhere. Decide where event logs go (bucket vs shared volume).

---

## 3. Source systems specification

### 3.1 PostgreSQL OLTP schema

| Table | Key columns | Notes |
|---|---|---|
| `customers` | `customer_id` (PK), email, country, segment, `created_at`, `updated_at`, `is_deleted` | soft deletes; 1M rows at medium scale |
| `products` | `product_id`, category, price, `updated_at` | 100k rows; price changes over time |
| `orders` | `order_id`, `customer_id` (FK), status, `order_ts`, `updated_at` | status lifecycle: created, paid, shipped, cancelled, refunded |
| `order_items` | `order_item_id`, `order_id`, `product_id`, qty, unit_price | 2-5 items per order |
| `payments` | `payment_id`, `order_id`, amount, method, status, `paid_at` | some orders have retries and partial refunds |
| `change_log` | `seq`, table name, op (I/U/D), key, before/after image, `changed_at` | filled by triggers; used by the CDC work in Project 5 |

Add realistic indexes (PK, FK, `updated_at`) and **deliberately omit one** (for example, nothing on `order_ts`) so you can see its effect on extraction.

### 3.2 Scale tiers

| Tier | Orders | Items | Clickstream events | Use for |
|---|---|---|---|---|
| S | 2M | ~6M | 10M | functional work, quick iteration |
| M | 20M | ~60M | 50M | most experiments |
| L | 50M+ | 150M+ | 100M+ | spill and I/O-bound experiments (check disk space first) |

### 3.3 Distribution and quality traps (build these in on purpose)

- **Hot customer:** one reseller account owns about 5% of all orders. A few "power" customers own another large share.
- **Hot product:** a promotional product appears in a disproportionate share of items.
- **Seasonality:** order volume varies by hour and weekday.
- **Dirty clickstream (files):** malformed JSON lines, string amounts such as "N/A", null user ids, duplicate event ids, very late events, schema drift (a new field halfway through).
- **Dirty OLTP:** status typos, future-dated orders, negative quantities, orphan-like rows created by app bugs (documented as known defects).

### 3.4 Change simulator

A long-running process that mutates Postgres at a configurable rate:

- inserts new orders and items
- updates status transitions and prices, always bumping `updated_at`
- soft-deletes customers
- occasionally hard-deletes rows (detectable only by CDC or full-key reconciliation)
- sends late updates to old orders (to break naive high-watermark extraction)

### 3.5 Clickstream producer

Writes JSON files into the MinIO `landing` bucket continuously and in bulk (historical backfill), including out-of-order and replayed files.

---

## 4. Helpers to write yourself

Short, reusable utilities. Specify them first, then build them:

| Helper | Behavior |
|---|---|
| Session factory | one place for master URL, driver host settings, Iceberg catalog, S3 settings, event-log location; accepts per-experiment overrides; stops the previous session |
| Timer and job labeler | labels Spark jobs and prints elapsed time |
| No-op executor | forces full plan execution without a real write |
| Plan reporter | extracts scan filters, join types and exchange counts from plan text |
| Iceberg inspector | summarizes files, sizes, snapshots and manifests from the metadata tables |
| Source generator | builds the Postgres schema and data at a chosen scale tier |
| Reconciler | compares row counts and checksums across source, bronze, silver and gold |

### Static vs runtime configuration

- Runtime SQL settings can be changed on a live session.
- Resource settings (executor cores/memory, `spark.cores.max`, dynamic allocation, serializer) and catalog definitions need a **new session**.
- Driver JVM settings need a **kernel restart**.

---

## 5. Measurement protocol

1. Change one setting at a time; run each variant twice and record the second.
2. Force execution with a no-op sink when you only care about read/compute.
3. Record every run in a table:

| Experiment | Config | Time | Tasks | Shuffle R/W | Spill | GC % | Bytes read | Notes |
|---|---|---|---|---|---|---|---|---|

4. Look at **four places**, not one:
   - **Spark UI:** SQL, Stages, Executors, Storage, Environment tabs.
   - **Iceberg metadata tables:** files, manifests, partitions, snapshots.
   - **MinIO:** object counts, request rates, throughput.
   - **PostgreSQL:** active connections, running queries, slow-query log, query plans.
5. You understand an experiment when you can **predict** what all four will show before you run it.

### Capacity notes

- Postgres and MinIO share the host with 8 Spark cores. Give each container explicit CPU/memory limits so the source database or storage cannot silently starve the workers, or deliberately remove the limits to study contention.
- A single-node MinIO on one disk is often the bottleneck for scan-heavy experiments. Treat that as a finding, not a defect.
- Postgres connection limits matter once Spark parallelism rises (Project 4).
