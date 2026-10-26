# Spark Lakehouse Lab (Instacart Edition)

Five projects built on one realistic mini data platform and **one real dataset**:

- **Dataset:** the Instacart Online Grocery Shopping dataset (3.4M real orders, 33.8M order lines), extended with the OLTP and operational data a real grocery platform would also have.
- **PostgreSQL** simulates the operational (OLTP) source systems.
- **RustFS** simulates the object-store data lake (S3 API).
- **Apache Iceberg** is the lakehouse table format for bronze, silver and gold.
- **Spark 4.x standalone** (1 master + 4 workers, 2 cores / 2 GB each) with **JupyterLab** as the driver.

This guide contains no Dockerfiles, compose files or code. Every project is a specification: what to build, what to observe, and what you should be able to explain afterwards.

| # | Project | You learn |
|---|---|---|
| 1 | Lakehouse medallion ETL | profiling a real source, enriching it into an OLTP model, JDBC extraction, incremental loads, Iceberg, MERGE, SCD2, time travel, maintenance |
| 2 | Joins, shuffles and skew | join strategies, AQE, natural and injected skew, pair-explosion self-joins, range joins, storage-partitioned joins, federated joins |
| 3 | Catalyst and query planning | plan reading, Iceberg/JDBC pushdown, pruning, statistics, runtime filters, layout |
| 4 | Executors, memory and I/O performance | executor sizing, memory, Python UDFs, S3A tuning, JDBC concurrency, commit conflicts, failures |
| 5 | Streaming and CDC into Iceberg | triggers, watermarks, state, CDC apply with MERGE, small files, recovery |
| ref | Configuration map | every important config by category |

**Suggested order:** setup, then 1, 3, 2, 4, 5. Keep the reference open throughout.

---

## 1. Target architecture

Data flows left to right:

1. **Sources:** PostgreSQL (the Instacart tables loaded as an operational model, plus synthetic operational tables) and a clickstream delivered as JSON files.
2. **Landing:** Spark JDBC extracts and clickstream files arrive as raw objects in a RustFS `landing` bucket.
3. **Bronze -> silver -> gold:** Iceberg tables in a RustFS `warehouse` bucket, in `bronze`, `silver` and `gold` namespaces.
4. **Consumers:** gold tables are queried from Jupyter, and a reverse-ETL export writes selected aggregates back into Postgres.
5. **Operations:** Spark event logs, streaming checkpoints and an optional catalog database live outside the compute cluster.

### Service inventory

| Service | Role | Notes |
|---|---|---|
| Spark master + 4 workers | compute | as you have today |
| JupyterLab | driver and notebooks | workers must resolve its hostname |
| History server | compare runs | reads event logs from a bucket |
| PostgreSQL | OLTP source (+ catalog DB if you choose a JDBC catalog) | use separate databases for source and catalog |
| RustFS | object storage | S3 API on port 9000, web console on 9001 |
| Optional: standalone Iceberg REST catalog | production-like catalog | stretch goal |

### Buckets (suggested)

| Bucket | Contents |
|---|---|
| `landing` | raw extracts and clickstream files, by source/table/date |
| `warehouse` | Iceberg tables (data and metadata) |
| `checkpoints` | streaming checkpoints |
| `spark-events` | Spark event logs |
| `archive` | processed landing files moved out by cleanup |

---

## 2. The dataset: Instacart

Download it from Kaggle (the "Instacart Market Basket Analysis" competition). You need a free Kaggle account and must accept the competition rules before the files unlock. Read the dataset terms and use it for learning only; keep your own copy out of any shared repository.

### 2.1 Real files and relationships

| File | Rows | Key columns | Role in the lab |
|---|---|---|---|
| `orders` | 3.42M | `order_id`, `user_id`, `eval_set` (prior / train / test), `order_number`, `order_dow`, `order_hour_of_day`, `days_since_prior_order` | the order header; roughly 206k users |
| `order_products__prior` | 32.4M | `order_id`, `product_id`, `add_to_cart_order`, `reordered` | order lines for all prior orders |
| `order_products__train` | 1.38M | same columns | order lines for each user's train order |
| `products` | ~50k | `product_id`, `product_name`, `aisle_id`, `department_id` | product dimension |
| `aisles` | 134 | `aisle_id`, `aisle` | taxonomy level 2 |
| `departments` | 21 | `department_id`, `department` | taxonomy level 1 |

Relationships to map (and test) in M0 of Project 1:

- `users` do not have their own table: the user is only an id on `orders`.
- `orders` (many per user, numbered by `order_number`) -> order lines (many per order). Lines are **split across two files** by `eval_set`, and the test-set orders have **no lines at all**.
- Order lines -> `products` -> `aisles` -> `departments` (a three-level taxonomy).
- `reordered` is a derived flag: it should be 1 exactly when the user bought that product in an earlier order. You will verify that claim.

### 2.2 Real-world issues to find (verify each by profiling; this is part of the lab)

- Orders with `eval_set` = test have no order lines.
- Order lines are split across two files that must be unioned, and the split depends on the order's `eval_set`.
- `days_since_prior_order` is null for each user's first order, and values appear capped at 30 days (censored data).
- There are **no timestamps**: only a day-of-week, an hour, and the gap since the previous order. Day-of-week may not match the gaps if you rebuild a calendar.
- A placeholder "missing" aisle and "missing" department that a block of products points at.
- Product names with commas, quotes and inconsistent capitalization that break naive CSV parsing.
- Very different basket sizes (a long tail of huge orders).
- Possible inconsistencies in the `reordered` flag, and orders with a user history that doesn't line up with `order_number`.
- No prices, payments, stores or user attributes exist at all.

Keep a **defect register** (issue, table, count, rule you used to handle it). It is a deliverable of Project 1.

### 2.3 Scaling

The real data is already large enough for most experiments.

| Tier | What you load | Orders | Order lines | Use for |
|---|---|---|---|---|
| S | a random sample of ~10% of users (all of their orders) | ~340k | ~3.4M | functional work, iteration |
| M | the full dataset | 3.42M | ~33.8M | most experiments |
| L | the full dataset replicated ~3 times with new user ids and shifted time | ~10M | ~100M | spill and I/O-bound experiments (check disk space first) |

Replication rules for L: new ids derived deterministically from original id and copy number; share a small fraction of users across copies to make longer histories; shift the reconstructed timeline per copy with trend and seasonality; keep the dirt rates of the original.

### 2.4 Skew in the data

Natural skew to measure: a few products dominating line counts (bananas and other produce staples), a few aisles and departments dominating the taxonomy (produce, dairy and eggs), uneven basket sizes, and uneven order volume by hour of day and day of week (low-cardinality skew).

Users are bounded in how many orders they have, so user-level skew is **weak** in the real data. Injected skew (at M and L tiers) gives Spark a true hot key: one **bulk buyer** account with about 5% of orders, one **promotional product** with a big share of lines, and one **popular store** (see 2.5).

### 2.5 Derived and synthetic data (so one dataset drives everything)

The real files lack most of what an OLTP platform stores, so you add it deterministically and document each rule.

| Source | How it is built | Used in |
|---|---|---|
| **Rebuilt timeline** | assign each user a start date, then turn `days_since_prior_order`, the hour and the weekday into order timestamps; record every inconsistency | all projects |
| **Users table** | one row per user: signup date (first order), region, membership tier that changes over time | SCD2, MERGE |
| **Stores** | a small set of partner retailers; each user shops mostly in one or two | joins, skew |
| **Price book** | a price per product by taxonomy level, with periodic effective-dated price changes (and a few promotions) | range joins, SCD |
| **Order totals and payments** | priced lines plus fees and tip, split over one to three tenders, with a small rate of failures and refunds | reconciliation |
| **Order lifecycle** | placed, accepted, picking, delivered or cancelled timestamps, plus item-level substitutions and out-of-stock removals | incremental extraction, CDC |
| **Erasure requests** | occasional privacy deletions: hard deletes plus anonymized rows | MERGE, delete handling |
| **Clickstream** | JSON events generated around real baskets: cart-add events in the real `add_to_cart_order` sequence, searches and views, plus sessions that never convert; with duplicates, schema drift, malformed lines and late events | files, streaming |
| **Campaign calendar** | small table of promotion windows | range joins |
| **Change log** | trigger-filled table in Postgres (operation, key, before/after image, sequence, changed_at) | CDC in Project 5 |

---

## 3. Decision gates (do these before Project 1)

### Gate A: Spark and Iceberg version compatibility
Iceberg ships a separate Spark runtime artifact **per Spark minor version** (Scala 2.13 for Spark 4.x), and a mismatch fails at extension load. Check Maven Central for the runtime matching your Spark version. Spark 4.2 support was merged into Iceberg, but a bug report noted the 4.2 runtime artifact missing from Maven, so verify it exists; if not, pin Spark to the newest 4.x with a published runtime (Iceberg 1.11 publishes a Spark 4.1 runtime). Record your choice and why.

### Gate B: RustFS compatibility smoke test
RustFS is an Apache-2.0, S3-compatible object store. Version 1.0.0 was announced as generally available in September 2026, so it is young compared with older stores. Before building on it, prove each of these with **both** access paths you plan to use (Hadoop S3A and Iceberg's own S3 file I/O):

| Check | Why |
|---|---|
| Write and read a Parquet file | basic path |
| Write an object larger than the multipart threshold | multipart upload |
| List a prefix with 10k+ objects | listing performance and pagination |
| Delete many objects | cleanup, orphan removal |
| Two Spark applications writing at once | concurrency |
| Read an Iceberg table's metadata tables | metadata-heavy queries |
| Kill RustFS during a write, restart, verify | durability behavior |

Practical notes: pin an explicit release tag instead of `latest`; change the default credentials; the S3 API is on port 9000 and the console on 9001 (the console may live under its own URL path); Spark needs **path-style access** and a custom endpoint. The RustFS documentation has Spark (S3A) and Iceberg guides. The S3A guide was verified on an older Spark/Hadoop pair, so re-verify on yours.

### Gate C: classpath dependencies
Decide how each reaches **both the driver and every executor** (baked into the image vs resolved at runtime; trade-offs: reproducibility, build time, offline use):

- Iceberg Spark runtime matching your Spark version
- Object-store access: Hadoop S3A plus a matching AWS SDK bundle (versions must line up with the Hadoop version in your Spark distribution), or Iceberg's own AWS bundle with its S3 file I/O. Project 4 compares them.
- PostgreSQL JDBC driver (also needed by the catalog if you choose a JDBC catalog)

### Gate D: catalog choice

| Catalog | Pros | Cons |
|---|---|---|
| Hadoop (path-based) | no extra services | unsafe on object stores without atomic rename; single writer only |
| JDBC (Postgres-backed) | atomic commits, concurrent writers, reuses Postgres | one more database to manage |
| Standalone REST catalog | production-like, engine-neutral | extra container |
| RustFS built-in S3 Tables REST catalog | no extra service | marked as a preview feature; Spark support is validated by a manual harness only, and an open issue reports Spark metadata-table lookups failing on a request-signing mismatch (check its status) |

Recommended: **JDBC on Postgres** (separate database from your source data) for everything. Treat the RustFS-built-in catalog as an optional comparison experiment in Project 4.

### Gate E: Postgres capacity
At the M tier Postgres holds about 34M order lines plus the synthetic tables. Plan disk, memory and load time (bulk load first, indexes after), and decide what stays in Postgres versus what only ever exists as files.

---

## 4. Helpers to write yourself

| Helper | Behavior |
|---|---|
| Dataset loader | stages the Instacart CSVs into Postgres as text, then types them, applies constraints in stages, and records violations in the defect register |
| Timeline builder | rebuilds timestamps from gaps, hours and weekdays, and reports inconsistencies |
| Enricher | generates users, stores, the price book, order totals, payments and lifecycle rows by the rules in 2.5 |
| Upscaler / sampler | builds the S, M or L tier |
| Lifecycle replayer | the change simulator, with adjustable speed and late-change probability |
| Clickstream generator | writes JSON sessions to `landing`, continuously and in bulk |
| Session factory | one place for master URL, driver host, Iceberg catalog, S3 settings, event-log location; accepts per-experiment overrides; stops the previous session |
| Timer and job labeler | labels Spark jobs and prints elapsed time |
| No-op executor | forces full execution without a real write |
| Plan reporter | extracts scan filters, join types and exchange counts from plan text |
| Iceberg inspector | summarizes files, sizes, snapshots and manifests from metadata tables |
| Reconciler | compares counts and checksums across source, bronze, silver and gold |

**Static vs runtime configuration:** runtime SQL settings change on a live session; resource settings and catalog definitions need a **new session**; driver JVM settings need a **kernel restart**.

---

## 5. Measurement protocol

1. Change one setting at a time; run each variant twice and record the second.
2. Force execution with a no-op sink when you only care about read/compute.
3. Record every run:

| Experiment | Config | Time | Tasks | Shuffle R/W | Spill | GC % | Bytes read | Notes |
|---|---|---|---|---|---|---|---|---|

4. Look at **four places**, not one:
   - **Spark UI:** SQL, Stages, Executors, Storage, Environment.
   - **Iceberg metadata tables:** files, manifests, partitions, snapshots.
   - **RustFS:** object counts, request rates, throughput (console and metrics).
   - **PostgreSQL:** active connections, running queries, slow-query log, query plans.
5. You understand an experiment when you can **predict** what all four will show before you run it.

### Capacity notes

- Postgres and RustFS share the host with 8 Spark cores. Set explicit CPU and memory limits on both so they can't silently starve the workers, or remove the limits on purpose to study contention.
- A single-node, single-disk RustFS is often the bottleneck for scan-heavy experiments. Treat that as a finding.
- Postgres connection limits matter once Spark parallelism rises (Project 4).
- Stretch: add a second real dataset with a different shape (for example, a retail transactions file with customers and articles) to compare join behavior across schemas.
