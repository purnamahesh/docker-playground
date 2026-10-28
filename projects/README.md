# Spark Lakehouse Lab (Instacart Edition)

Five projects on one realistic mini data platform and one real dataset:

- **Dataset:** Instacart Online Grocery Shopping (about 3.4M real orders and 33.8M order lines), extended with the operational data a real grocery platform would also have.
- **PostgreSQL:** the operational (OLTP) source systems.
- **RustFS:** the object-store data lake (S3 API).
- **Apache Iceberg:** the lakehouse table format for bronze, silver and gold.
- **Spark 4.x standalone:** 1 master + 4 workers (2 cores / 2 GB each), with JupyterLab as the driver.

There is no code, Dockerfile or compose content anywhere in this guide. Each project says **what to do at each point, in what order, which options to explore, and how to know you're done**.

| # | Project | You learn |
|---|---|---|
| 1 | Lakehouse medallion ETL | profiling, enrichment, JDBC extraction, incremental loads, Iceberg, MERGE, SCD2, evolution, maintenance, reconciliation |
| 2 | Joins, shuffles and skew | join strategies, AQE, skew, pair explosion, range joins, storage-partitioned joins, federated joins |
| 3 | Catalyst and query planning | plan reading, pruning, pushdown, statistics, runtime filters, layout |
| 4 | Executors, memory and I/O performance | topology, memory, Python UDFs, S3A, JDBC concurrency, commit conflicts, failures |
| 5 | Streaming and CDC into Iceberg | triggers, watermarks, state, CDC apply, small files, recovery, SLOs |
| ref | Configuration map | important configs by category |

---

## 0. How to use the projects

### Milestone template
Every milestone has the same parts:

- **Objective:** what you'll understand or have built.
- **Do, in order:** numbered steps, with the reasoning for the order.
- **Options to explore:** a table of alternatives. Try at least the first two rows of each table, and note when each wins.
- **Record / observe:** exactly what to capture and where.
- **Traps:** known ways this goes wrong.
- **Done when:** concrete exit criteria.

### Three living documents (keep them from day one)

| Document | Contents |
|---|---|
| **Defect register** | id, table, column(s), description, row count, share of rows, severity, how detected, handling rule, layer where handled, test that proves it |
| **Experiment log** | experiment id, hypothesis, setting changed, baseline, result, Spark UI / Iceberg / RustFS / Postgres evidence, conclusion |
| **Decision log** | decision, options considered, evidence, choice, date, what would make you revisit it |

### Naming and conventions (decide once)
- Run identifiers on every load (a run id and an extract id), carried into every layer.
- Namespaces `bronze`, `silver`, `gold`, plus `quarantine` or a `quarantine_` prefix.
- Path convention in `landing` (source / table / extract date / extract id).
- Fixed random seeds for every generator, so any tier can be rebuilt identically.
- Time zone: store everything in UTC, and document the assumed local zone for the rebuilt timeline.

---

## 1. Master sequence (what happens when)

| Step | Work | Depends on | Output |
|---|---|---|---|
| 0 | Environment bring-up and smoke tests (section 2) | nothing | verified platform, version decisions |
| 1 | Acquire Instacart; store untouched copy in RustFS | 0 | raw dump with checksums |
| 2 | P1 M0: load, profile, constrain | 1 | profile report, defect register v1 |
| 3 | P1 M0: rebuilt timeline and synthetic enrichment (S tier first) | 2 | enriched source in Postgres |
| 4 | Build the M tier (and later L) | 3 | scaled source |
| 5 | P1 M1: full extraction to landing | 4 | landing extracts and manifests |
| 6 | P1 M3: bronze | 5 | bronze tables |
| 7 | P1 M4: silver | 6 | silver tables, quarantine, SCD2 |
| 8 | P1 M5: gold | 7 | gold marts |
| 9 | P3 (planning study) and P2 (joins/skew) | 7 or 8 | experiment log entries |
| 10 | P1 M2: incremental extraction, lifecycle replay | change simulator built | incremental pipeline |
| 11 | P1 M6 to M7: evolution, maintenance | 8 | maintenance runbook |
| 12 | P4 performance study | 8 | capacity model |
| 13 | P5 streaming and CDC | 10, 11 | streaming pipeline, SLOs |
| 14 | Failure drills and final reconciliation | all | reconciliation report, lessons |

Suggested pacing if you work in evenings: week 1 steps 0 to 3; week 2 steps 4 to 8; week 3 step 9 (P3, P2); week 4 steps 10 to 12; week 5 steps 13 to 14.

---

## 2. Environment bring-up (verify each step before the next)

| # | Step | Verify by |
|---|---|---|
| 1 | RustFS running with a pinned release tag and changed credentials | log in to the console; create the five buckets; list them with any S3 client |
| 2 | PostgreSQL running with two databases (source, catalog) and explicit CPU/memory limits | connect from your laptop and from the Jupyter container |
| 3 | Spark cluster running | 4 workers visible in the master UI; each reports 2 cores and 2 GB |
| 4 | Jars resolved (Gate C) | a Spark session starts with the Iceberg runtime, S3 access and the JDBC driver on both driver and executors |
| 5 | Catalog configured (Gate D) | create and drop a test namespace |
| 6 | Iceberg smoke test | create a table, insert rows, read them back, list snapshots, see the data and metadata objects in the RustFS console |
| 7 | S3A smoke test (Gate B) | write and read a Parquet file at an s3a path; compare with Iceberg's own file access |
| 8 | JDBC smoke test | read a million-row test table with several partitions; see the connections in Postgres |
| 9 | History server | a finished application appears with its event log in the bucket |
| 10 | End-to-end hello pipeline | Postgres table -> landing Parquet -> Iceberg bronze -> a query, run twice, second run idempotent |

### Gate A: Spark and Iceberg version compatibility
Iceberg ships a separate Spark runtime artifact **per Spark minor version** (Scala 2.13 for Spark 4.x); a mismatch fails when the extension loads. Check Maven Central for the runtime matching your Spark version. Spark 4.2 support was merged into Iceberg, but a bug report noted the 4.2 runtime artifact missing from Maven. If it still isn't published, pin Spark to the newest 4.x with a published runtime (Iceberg 1.11 publishes a Spark 4.1 runtime). Write the decision into the decision log.

### Gate B: RustFS compatibility smoke test
RustFS is an Apache-2.0, S3-compatible object store, announced generally available (1.0.0) in September 2026, so it is young. Prove each check with **both** access paths you plan to use (Hadoop S3A and Iceberg's own S3 file I/O):

| Check | Why |
|---|---|
| Write and read a Parquet file | basic path |
| Write an object larger than the multipart threshold | multipart upload |
| List a prefix with 10k+ objects | listing and pagination |
| Delete many objects | cleanup and orphan removal |
| Two applications writing at once | concurrency |
| Read an Iceberg table's metadata tables | metadata-heavy queries |
| Kill RustFS during a write, restart, verify | durability behavior |

Practical notes: pin a release tag instead of `latest`; change the default credentials; S3 API on 9000, console on 9001; Spark needs path-style access and a custom endpoint. The RustFS docs have Spark and Iceberg guides, but the S3A guide was validated on an older Spark/Hadoop pair, so re-verify on yours.

### Gate C: classpath dependencies
Decide how each reaches **the driver and every executor** (baked into the image vs resolved at runtime; trade-offs: reproducibility, build time, offline use):
- Iceberg Spark runtime matching your Spark version.
- Object-store access: Hadoop S3A plus a matching AWS SDK bundle (versions must line up with Hadoop inside your Spark distribution), or Iceberg's own AWS bundle with its S3 file I/O. Project 4 compares them.
- PostgreSQL JDBC driver (also needed by a JDBC catalog).

### Gate D: catalog choice

| Catalog | Pros | Cons |
|---|---|---|
| Hadoop (path-based) | no extra services | unsafe on object stores without atomic rename; single writer only |
| JDBC (Postgres-backed) | atomic commits, concurrent writers, reuses Postgres | one more database |
| Standalone REST catalog | production-like, engine-neutral | extra container |
| RustFS built-in S3 Tables REST catalog | no extra service | preview feature; Spark support only validated by a manual harness; an open issue reports Spark metadata-table lookups failing on a request-signing mismatch (check its status) |

Recommended: **JDBC on Postgres** (separate database) for everything; treat the RustFS-built-in catalog as an optional comparison in Project 4.

### Gate E: Postgres capacity
At the M tier Postgres holds about 34M order lines plus the synthetic tables. Plan disk, memory and load time (bulk load first, indexes after), and decide what stays in Postgres and what only ever exists as files.

---

## 3. The dataset: Instacart

### 3.1 Acquisition checklist
1. Create a free Kaggle account and accept the rules of the "Instacart Market Basket Analysis" competition so the files unlock.
2. Read the dataset terms; use it for learning only and keep your own copy out of shared repositories.
3. Download all files, record their checksums, and store an **immutable copy** in a `landing/raw_dump` prefix.
4. Note the file sizes and row counts from a plain line count; they become your first reconciliation numbers.

### 3.2 Files and relationships

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

### 3.3 Real-world issues to find (verify each by profiling)
- Test-set orders without lines; train and prior lines in separate files.
- `days_since_prior_order` null for each user's first order and capped near 30 days (censored).
- No timestamps: only weekday, hour and the gap since the previous order.
- A placeholder "missing" aisle and department that a block of products points at.
- Product names with commas, quotes and inconsistent capitalization.
- A long tail of very large baskets.
- Possible disagreements in the `reordered` flag, and user histories that don't line up with `order_number`.
- No prices, payments, stores or user attributes exist.

### 3.4 Scaling tiers

| Tier | Build | Orders | Lines | Use for |
|---|---|---|---|---|
| S | ~10% of users, with **all** their orders | ~340k | ~3.4M | functional work, fast iteration |
| M | full dataset | 3.42M | ~33.8M | most experiments |
| L | full dataset replicated about 3 times | ~10M | ~100M | spill and I/O-bound studies (check disk first) |

Sampling options for S: by user (keeps histories intact, **recommended**), by order (breaks histories, so reorder logic fails), by time (not possible before the timeline exists). Replication rules for L: deterministic new ids from original id and copy number; share a small fraction of users across copies; shift the timeline per copy with trend and seasonality; preserve the dirt rates. After building each tier, **re-profile** and compare distributions (basket size, orders per user, product frequency) with M.

### 3.4b Skew in the data
Natural: a few products dominate lines (bananas and other produce staples); a few aisles and departments dominate (produce, dairy and eggs); basket sizes have a long tail; order volume varies by hour and weekday (low-cardinality skew). User-level skew is **weak** (users have bounded order counts), so inject: one **bulk buyer** with ~5% of orders, one **promotional product** with a large share of lines, and one **popular store** (M and L tiers).

### 3.5 Synthetic data design (decisions to make, with options)

| Synthetic table | Design decisions and options | Validity tests |
|---|---|---|
| **Rebuilt timeline** | per-user start date: random in a window / cohort-based / fixed; first-order gap: none / sampled; capped gap: keep as is / sample a longer tail; weekday mismatch: honor the weekday by nudging / ignore / derive from timeline; minute and second: uniform / realistic peaks | gaps preserved within a day; weekday agreement rate reported; per-user monotonic |
| **Users** | attributes (region, signup date = first order, membership tier); tier changes over time (SCD source); fraction who churn | one row per user; tier history non-overlapping |
| **Stores** | number of retailers; sticky assignment vs random per order; skew (popular store) | each order has a store; share per store matches design |
| **Price book** | base price by aisle distribution; change cadence (monthly / quarterly / event-driven); effective-dated rows; promotions overlapping base periods (a deliberate trap) | no gaps in coverage; overlaps only where intended; positive prices |
| **Order totals and payments** | priced lines + service fee + delivery fee + tip; one to three tenders; failure and refund rates; rounding rule | payments sum to total except injected defects |
| **Order lifecycle** | state machine (placed, accepted, picking, delivered or cancelled); stage durations from distributions; cancellation rate; item substitutions and removals; late-arriving updates | timestamps monotonic per order; state transitions legal |
| **Erasure requests** | schedule and share; hard delete vs anonymize; cascade scope | no orphans after cascade, except designed ones |
| **Clickstream** | sessions around real baskets: cart adds in real add-to-cart order, searches, views, non-converting sessions; duplicates, schema drift, malformed lines, late events | every converting session maps to a real order; defect rates as designed |
| **Campaign calendar** | a few promotion windows | no unintended overlaps |
| **Change log** | trigger-filled table (operation, key, before/after image, sequence, changed_at) | one entry per row change, strictly ordered sequence |

---

## 4. Helpers to write yourself

| Helper | Behavior |
|---|---|
| Dataset loader | stages CSVs into Postgres as text, types them, applies constraints in stages, records violations in the defect register |
| Timeline builder | rebuilds timestamps from gaps, hours and weekdays; reports inconsistencies |
| Enricher | generates users, stores, price book, totals, payments and lifecycle by the rules above |
| Sampler / upscaler | builds the S, M or L tier reproducibly |
| Lifecycle replayer | the change simulator; adjustable speed and late-change probability |
| Clickstream generator | writes JSON sessions to `landing`, continuously and in bulk |
| Session factory | one place for master, driver host, catalog, S3 settings, event-log location; per-experiment overrides; stops the previous session |
| Timer and job labeler | labels Spark jobs and prints elapsed time |
| No-op executor | forces full execution without a write |
| Plan reporter | extracts scan filters, join types and exchange counts from plan text |
| Iceberg inspector | summarizes files, sizes, snapshots and manifests from metadata tables |
| Reconciler | compares counts and checksums across source, bronze, silver and gold |

**Static vs runtime configuration:** runtime SQL settings change on a live session; resource settings and catalog definitions need a **new session**; driver JVM settings need a **kernel restart**.

---

## 5. Measurement protocol

1. Change one setting at a time; run each variant at least twice and record the second (first run warms caches).
2. Force execution with a no-op sink when you only care about read and compute.
3. Record every run in the experiment log with these fields: time, tasks, shuffle read/write, spill, GC share, bytes read, plan fingerprint (join types and exchange counts), notes.
4. Look at **four places**, not one:
   - **Spark UI:** SQL, Stages, Executors, Storage, Environment.
   - **Iceberg metadata tables:** files, manifests, partitions, snapshots.
   - **RustFS:** object counts, request rates, throughput.
   - **PostgreSQL:** active connections, running queries, slow-query log, plans.
5. Before each experiment, write a **prediction**. You understand it when your prediction matches all four views.

### Capacity notes
- Postgres and RustFS share the host with 8 Spark cores; set CPU and memory limits on both, or remove them deliberately to study contention.
- A single-node, single-disk RustFS is often the bottleneck for scan-heavy work; treat that as a finding.
- Postgres connection limits matter once Spark parallelism rises (Project 4).
- Stretch: add a second real dataset of a different shape to compare join behavior across schemas.
