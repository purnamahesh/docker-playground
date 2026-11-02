# Spark Lakehouse Lab (Instacart + Delta Lake Edition)

Five projects on one realistic mini data platform and one real dataset:

- **Dataset:** Instacart Online Grocery Shopping (about 3.4M real orders and 33.8M order lines), extended with the operational data a real grocery platform would also have.
- **PostgreSQL:** the operational (OLTP) source systems.
- **RustFS:** the object-store data lake (S3 API).
- **Delta Lake:** the lakehouse table format for bronze, silver and gold.
- **Spark 4.x standalone:** 1 master + 4 workers (2 cores / 2 GB each), with JupyterLab as the driver.

There is no code, Dockerfile or compose content anywhere in this guide. Each project says **what to do at each point, in what order, which options to explore, why, and how to know you're done**. Each milestone also ends with "Read more" links.

| # | Project | You learn |
|---|---|---|
| 1 | Lakehouse medallion ETL | profiling, enrichment, JDBC extraction, incremental loads, Delta tables, MERGE, SCD2, versions and time travel, maintenance, reconciliation |
| 2 | Joins, shuffles and skew | join strategies, AQE, skew, pair explosion, range joins, federated joins, file layout |
| 3 | Catalyst and query planning | plan reading, Delta data skipping, pushdown, statistics, layout (Z-order, clustering) |
| 4 | Executors, memory and I/O performance | topology, memory, Python UDFs, S3A, JDBC concurrency, writer rules, failures |
| 5 | Streaming and CDC into Delta | triggers, watermarks, state, change-log CDC, Delta change data feed, small files, recovery, SLOs |
| ref | Configuration map (`06`) | important configs by category |

Companion files: `00-start-here.md` (self-check, hello world, FAQ), `07-learning-companion.md` (minimal paths, time-boxes, concept checks, rubrics), `08-debugging-playbook.md` (symptom trees, error table, known version issues), `09-reference-answers.md` (worked experiments, expected plans, magnitude calibration).

---

## 0. How to use the projects

**First:** read `00-start-here.md` and complete its hello world before the bring-up below. Keep `08-debugging-playbook.md` open while you work.

### Milestone template
Every milestone has the same parts:

- **Objective:** what you'll understand or have built.
- **Why it matters and terms:** the idea in plain language, with any new vocabulary defined.
- **Do, in order:** numbered steps, each with the reason for it and what a good result looks like.
- **Options to explore:** a table of alternatives. Try at least the first two rows of each table and note when each wins.
- **Record / observe:** exactly what to capture and where.
- **Traps:** known ways this goes wrong.
- **Done when:** concrete exit criteria.
- **Read more:** links to official documentation.

### Three living documents (keep them from day one)

| Document | Contents |
|---|---|
| **Defect register** | id, table, column(s), description, row count, share of rows, severity, how detected, handling rule, layer where handled, test that proves it |
| **Experiment log** | experiment id, prediction, setting changed, baseline, result, Spark UI / Delta log / RustFS / Postgres evidence, conclusion |
| **Decision log** | decision, options considered, evidence, choice, date, what would make you revisit it |

### Naming and conventions (decide once)
- A **run id** and an **extract id** on every load, carried into every layer.
- Layers: `bronze`, `silver`, `gold`, plus a quarantine area per entity.
- Path convention in the `landing` bucket: source / table / extract date / extract id.
- Path convention in the `warehouse` bucket: layer / table (for example `warehouse/silver/orders`).
- Fixed random seeds for every generator, so any tier can be rebuilt identically.
- Time zone: store everything in UTC, and document the assumed local zone for the rebuilt timeline.

---

## 1. Glossary: words that are easy to mix up

| Term | Plain-language meaning |
|---|---|
| **Landing zone** | a folder area in object storage where raw copies of source data arrive, untouched |
| **Bronze / silver / gold** | three quality layers: bronze = faithful raw copy; silver = cleaned, conformed, deduplicated; gold = business-ready aggregates |
| **Manifest** | a small file that says what an extraction contained (table, row count, time, extract id); written last, so its presence means the extract is complete |
| **Checksum** | a number computed from the data (for example a sum of ids) that lets you check two copies match without comparing every row |
| **Idempotent** | running the same step twice leaves the same result as running it once |
| **Snapshot (database sense)** | a frozen, consistent view of the database as of one moment. This is the meaning used in Project 1 M1. |
| **Version (Delta sense)** | every commit to a Delta table creates a new numbered version (0, 1, 2, ...). People often call these snapshots too, but this guide says **version** for Delta tables and **snapshot** only for the database. |
| **Time travel** | reading a Delta table as of an older version or timestamp |
| **Transaction log** | the `_delta_log` folder inside every Delta table: ordered JSON commit files (plus periodic checkpoint files) recording which data files are in the table |
| **Partition (four meanings)** | (1) a Spark partition = a slice of data one task processes; (2) a shuffle partition = a slice after a shuffle; (3) a JDBC partition = one slice of a source table that one connection reads; (4) a table partition = a folder layout by column value. Always say which one. |
| **Watermark (two meanings)** | (1) in incremental extraction: the highest `updated_at` already extracted (also called a high-water mark); (2) in streaming: how late data may arrive before it is dropped |
| **Shuffle** | moving data across the network between tasks so rows with the same key meet |
| **Skew** | uneven distribution of work; one task or partition much bigger than the others |
| **Hot key** | a key value (for example one product or one user) that owns a large share of the rows |
| **AQE** | adaptive query execution: Spark adjusts the plan at runtime using real statistics |
| **CDC (change data capture)** | capturing row-level changes (insert, update, delete) as a stream of events. In this guide CDC appears **only in Project 5**. |

---

## 2. Master sequence (what happens when)

| Step | Work | Depends on | Output |
|---|---|---|---|
| 0 | Environment bring-up and smoke tests (section 3) | nothing | verified platform |
| 1 | Acquire Instacart; store untouched copy in RustFS | 0 | raw dump with checksums |
| 2 | P1 M0: load, profile, constrain | 1 | profile report, defect register v1 |
| 3 | P1 M0: rebuilt timeline and synthetic enrichment (S tier first) | 2 | enriched source in Postgres |
| 4 | Build the M tier (and later L) | 3 | scaled source |
| 5 | P1 M1: full extraction to landing | 4 | landing extracts and manifests |
| 6 | P1 M3: bronze | 5 | bronze Delta tables |
| 7 | P1 M4: silver | 6 | silver tables, quarantine, SCD2 |
| 8 | P1 M5: gold | 7 | gold marts |
| 9 | P3 (planning study) and P2 (joins/skew) | 7 or 8 | experiment log entries |
| 10 | P1 M2: incremental extraction (updates in Postgres replayed by the simulator) | simulator built | incremental pipeline |
| 11 | P1 M6 to M7: versions, evolution, maintenance | 8 | maintenance runbook |
| 12 | P4 performance study | 8 | capacity model |
| 13 | P5 streaming and CDC (change-log triggers are installed here) | 10, 11 | streaming pipeline, SLOs |
| 14 | Failure drills and final reconciliation | all | reconciliation report, lessons |

Suggested pacing if you work in evenings: week 1 steps 0 to 3; week 2 steps 4 to 8; week 3 step 9; week 4 steps 10 to 12; week 5 steps 13 to 14.

---

## 3. Environment bring-up (verify each step before the next)

| # | Step | Verify by |
|---|---|---|
| 1 | RustFS running with a pinned release tag and changed credentials | log in to the console; create the five buckets; list them with any S3 client |
| 2 | PostgreSQL running with one source database (and CPU/memory limits) | connect from your laptop and from the Jupyter container |
| 3 | Spark cluster running | 4 workers visible in the master UI; each reports 2 cores and 2 GB |
| 4 | Jars resolved (Gate C) | a Spark session starts with the Delta package, S3 access and the JDBC driver on the driver and executors |
| 5 | Delta enabled on the session | creating a Delta table works without the "requires the DeltaSparkSessionExtension and the DeltaCatalog" error |
| 6 | Delta smoke test | create a table, insert, read back, show the table history, and see data files plus a `_delta_log` folder in the RustFS console |
| 7 | S3A smoke test (Gate B) | write and read Parquet at an s3a path |
| 8 | JDBC smoke test | read a million-row test table with several partitions; see the connections in Postgres |
| 9 | History server | a finished application appears with its event log in the bucket |
| 10 | End-to-end hello pipeline | Postgres table -> landing Parquet -> Delta bronze -> a query, run twice, second run idempotent |

### Gate A: Spark and Delta version compatibility
Delta Lake ships a **separate Spark artifact per Spark minor version**, and a mismatch fails when the session starts. As of Delta Lake 4.4.0 (and the 4.4.1 patch), Delta Spark is built and tested against **Spark 4.2.0, 4.1.0 and 4.0.1**, so your Spark 4.2.0 works with Delta without pinning an older Spark. The artifact name carries the Spark version (for Spark 4.2 it is `delta-spark_4.2_2.13`). Delta 4.1 and later require **Java 17 or newer**. Write the exact artifact and version into the Environment tab.

### Gate B: RustFS compatibility smoke test
RustFS is an Apache-2.0, S3-compatible object store, announced generally available (1.0.0) in September 2026, so it is young. Prove each check with the access path Delta uses (Hadoop S3A):

| Check | Why |
|---|---|
| Write and read a Parquet file | basic path |
| Write an object larger than the multipart threshold | multipart upload |
| List a prefix with 10k+ objects | listing and pagination |
| Delete many objects | `VACUUM` and cleanup |
| Create a Delta table and read it back | Delta commit protocol on this store |
| Two writers to one **scratch** table | to *see* the single-writer limitation (Gate F); never on real tables |
| A conditional PUT (create only if the object does not exist) with an S3 client | tells you whether the store could ever support safe multi-writer commits |
| Kill RustFS during a write, restart, verify the table | durability behavior |

Practical notes: pin a release tag instead of `latest`; change default credentials; S3 API on 9000, console on 9001; Spark needs path-style access and a custom endpoint.

### Gate C: classpath dependencies
Decide how each reaches **the driver and every executor** (baked into the image vs resolved at runtime; trade-offs: reproducibility, build time, offline use):
- The Delta Spark artifact matching your Spark version (and the Delta storage artifact it needs).
- Object-store access: the Hadoop S3A connector plus a matching AWS SDK bundle (versions must line up with the Hadoop version in your Spark distribution).
- The PostgreSQL JDBC driver.

### Gate D: how will tables be named?
Delta tables are just folders: each is read by **path**. A catalog is optional.

| Option | Pros | Cons |
|---|---|---|
| Path-only (recommended to start) | no extra service; nothing to break | you track paths yourself; keep a registry in your helper |
| External tables in Spark's built-in session catalog, re-registered at session start | readable names | metadata lives only for the session |
| A standalone Hive metastore backed by Postgres | persistent names, standard | extra container and configuration |
| Unity Catalog OSS (catalog-managed tables, new in Delta 4.1) | modern, multi-engine | newer; extra service; stretch goal |

### Gate E: Postgres capacity
At the M tier Postgres holds about 34M order lines plus the synthetic tables. Plan disk, memory and load time (bulk load first, indexes after), and decide what stays in Postgres and what only ever exists as files.

### Gate F: the single-writer rule (Delta on S3-style storage)
By default Delta Lake's S3 commit logic only guarantees correctness when **all concurrent writes to a table come from one Spark driver**. Writes to the same table from several drivers (separate applications) can lose data. The old workaround needs an AWS DynamoDB table; newer conditional-write support on S3 exists, but the Spark connector's support is not confirmed, and whether RustFS supports conditional writes is something you test in Gate B. **Rule for this lab: at any time, only one application writes to a given Delta table.** Several threads inside one application are fine (Delta's optimistic concurrency handles them). Orchestration must serialize jobs that write the same table. Project 4 M7 studies this deliberately, on scratch tables only.

---

## 4. The dataset: Instacart

### 4.1 Acquisition checklist
1. Create a free Kaggle account and accept the rules of the "Instacart Market Basket Analysis" competition so the files unlock.
2. Read the dataset terms; use it for learning only and keep your own copy out of shared repositories.
3. Download all files, record checksums, and store an **immutable copy** in `landing/raw_dump`.
4. Note file sizes and plain line counts; they become your first reconciliation numbers.

### 4.2 Files and relationships

| File | Rows | Key columns | Role |
|---|---|---|---|
| `orders` | 3.42M | `order_id`, `user_id`, `eval_set` (prior / train / test), `order_number`, `order_dow`, `order_hour_of_day`, `days_since_prior_order` | order header (about 206k users) |
| `order_products__prior` | 32.4M | `order_id`, `product_id`, `add_to_cart_order`, `reordered` | lines for prior orders |
| `order_products__train` | 1.38M | same | lines for each user's train order |
| `products` | ~50k | `product_id`, `product_name`, `aisle_id`, `department_id` | product dimension |
| `aisles` | 134 | `aisle_id`, `aisle` | taxonomy level 2 |
| `departments` | 21 | `department_id`, `department` | taxonomy level 1 |

Relationships to map and test:
- There is **no user table**; the user is only an id on `orders`.
- Orders (many per user, numbered by `order_number`) -> lines (many per order). Lines are **split across two files** by `eval_set`, and test-set orders have **no lines**.
- Lines -> products -> aisles -> departments.
- `reordered` is a derived flag: it should be 1 exactly when the user bought that product earlier. You will verify that.

### 4.3 Real-world issues to find (verify each by profiling)
- Test-set orders without lines; train and prior lines in separate files.
- `days_since_prior_order` null for each user's first order and capped near 30 days (censored data).
- No timestamps: only weekday, hour and the gap since the previous order.
- A placeholder "missing" aisle and department that a block of products points at.
- Product names with commas, quotes and inconsistent capitalization.
- A long tail of very large baskets.
- Possible disagreements in the `reordered` flag, and user histories that don't line up with `order_number`.
- No prices, payments, stores or user attributes exist.

### 4.4 Scaling tiers

| Tier | Build | Orders | Lines | Use for |
|---|---|---|---|---|
| S | ~10% of users, with **all** their orders | ~340k | ~3.4M | functional work, fast iteration |
| M | full dataset | 3.42M | ~33.8M | most experiments |
| L | full dataset replicated about 3 times | ~10M | ~100M | spill and I/O-bound studies (check disk first) |

Sampling options for S: by user (keeps histories intact, **recommended**), by order (breaks histories, so reorder logic fails). Replication rules for L: deterministic new ids from original id and copy number; share a small fraction of users across copies; shift the timeline per copy with trend and seasonality; preserve the dirt rates. After building each tier, **re-profile** and compare distributions with M.

### 4.5 Skew in the data
Natural: a few products dominate lines (bananas and other produce staples); a few aisles and departments dominate (produce, dairy and eggs); basket sizes have a long tail; order volume varies by hour and weekday (low-cardinality skew). User-level skew is **weak** (users have bounded order counts), so inject: one **bulk buyer** with ~5% of orders, one **promotional product** with a large share of lines, and one **popular store** (M and L tiers).

### 4.6 Synthetic data design (decisions to make, with options)

| Synthetic table | Design decisions and options | Validity tests |
|---|---|---|
| **Rebuilt timeline** | per-user start date: random in a window / cohort-based / fixed; first-order gap; capped gap: keep / sample a longer tail; weekday mismatch: honor / ignore / derive; minute and second: uniform / realistic peaks | gaps preserved within a day; weekday agreement rate reported; per-user monotonic |
| **Users** | region, signup date = first order, membership tier that changes over time | one row per user; tier history non-overlapping |
| **Stores** | number of retailers; sticky vs random assignment; a popular store | each order has a store; share per store matches design |
| **Price book** | base price by aisle distribution; change cadence; effective-dated rows; promotions overlapping base periods (a deliberate trap) | no gaps; overlaps only where intended; positive prices |
| **Order totals and payments** | priced lines + fees + tip; one to three tenders; failure and refund rates | payments sum to total except injected defects |
| **Order lifecycle** | state machine (placed, accepted, picking, delivered or cancelled); stage durations; cancellation rate; substitutions; late updates | timestamps monotonic; legal transitions |
| **Erasure requests** | schedule and share; hard delete vs anonymize | no orphans after cascade, except designed ones |
| **Clickstream** | sessions around real baskets; duplicates, schema drift, malformed lines, late events | every converting session maps to a real order |
| **Campaign calendar** | a few promotion windows | no unintended overlaps |
| **Change log (Project 5 only)** | trigger-filled table (operation, key, before/after image, sequence, changed_at), created in Project 5 M0 | one entry per row change, strictly ordered |

---

## 5. Helpers to write yourself

| Helper | Behavior |
|---|---|
| Dataset loader | stages CSVs into Postgres as text, types them, applies constraints in stages, logs violations to the defect register |
| Timeline builder | rebuilds timestamps from gaps, hours and weekdays; reports inconsistencies |
| Enricher | generates users, stores, price book, totals, payments and lifecycle by the rules above |
| Sampler / upscaler | builds the S, M or L tier reproducibly |
| Change simulator | applies realistic inserts, updates and deletes to Postgres at an adjustable speed (used from Project 1 M2 on; **no triggers, no change log**) |
| Clickstream generator | writes JSON sessions to `landing`, continuously and in bulk |
| Session factory | one place for master, driver host, Delta settings, S3 settings, event-log location; per-experiment overrides; stops the previous session |
| Timer and job labeler | labels Spark jobs and prints elapsed time |
| No-op executor | forces full execution without a write |
| Plan reporter | extracts scan filters, join types and exchange counts from plan text |
| Delta inspector | summarizes table history (versions, operations, metrics), detail (file count, size, partition columns), and per-file statistics read from the transaction log |
| Reconciler | compares counts and checksums across source, bronze, silver and gold |

**Static vs runtime configuration:** runtime SQL settings change on a live session; resource settings and extension settings need a **new session**; driver JVM settings need a **kernel restart**.

---

## 6. Measurement protocol

1. Change one setting at a time; run each variant at least twice and record the second (first run warms caches).
2. Force execution with a no-op sink when you only care about read and compute.
3. Record every run in the experiment log: time, tasks, shuffle read/write, spill, GC share, bytes read, plan fingerprint (join types and exchange counts), notes.
4. Look at **four places**, not one:
   - **Spark UI:** SQL, Stages, Executors, Storage, Environment.
   - **Delta log and history:** versions, operation metrics, file counts and sizes, per-file statistics.
   - **RustFS:** object counts, request rates, throughput.
   - **PostgreSQL:** active connections, running queries, slow-query log, plans.
5. Before each experiment, write a **prediction**. You understand it when your prediction matches all four views.

### Capacity notes
- Postgres and RustFS share the host with 8 Spark cores; set CPU and memory limits on both, or remove them deliberately to study contention.
- A single-node, single-disk RustFS is often the bottleneck for scan-heavy work; treat that as a finding.
- Postgres connection limits matter once Spark parallelism rises (Project 4).

---

## 7. Further reading (official documentation)

Links point to official documentation. Sections occasionally move; if a link breaks, search the page title on the project's site.

| Topic | Link |
|---|---|
| Spark documentation home | https://spark.apache.org/docs/latest/ |
| Spark SQL performance tuning (joins, hints, AQE) | https://spark.apache.org/docs/latest/sql-performance-tuning.html |
| Spark configuration reference | https://spark.apache.org/docs/latest/configuration.html |
| Spark tuning guide (memory, GC, serialization) | https://spark.apache.org/docs/latest/tuning.html |
| Spark standalone mode | https://spark.apache.org/docs/latest/spark-standalone.html |
| Spark monitoring and history server | https://spark.apache.org/docs/latest/monitoring.html |
| Spark web UI guide | https://spark.apache.org/docs/latest/web-ui.html |
| Spark job scheduling and dynamic allocation | https://spark.apache.org/docs/latest/job-scheduling.html |
| Spark JDBC data source | https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html |
| Spark Structured Streaming guide | https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html |
| Delta Lake documentation (latest) | https://docs.delta.io/latest/index.html |
| Delta Lake releases and compatibility | https://github.com/delta-io/delta/releases |
| Delta Lake storage configuration (S3, single vs multi-cluster) | https://docs.delta.io/latest/delta-storage.html |
| Delta Lake protocol specification | https://github.com/delta-io/delta/blob/master/PROTOCOL.md |
| Hadoop S3A connector | https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html |
| RustFS documentation | https://docs.rustfs.com/ |
| PostgreSQL documentation | https://www.postgresql.org/docs/current/ |
| Instacart competition page (Kaggle) | https://www.kaggle.com/c/instacart-market-basket-analysis |
| Medallion architecture overview | https://www.databricks.com/glossary/medallion-architecture |
