# Project 1: Lakehouse Medallion ETL (Instacart)

## Goal

Profile and load the **Instacart** data into Postgres as an operational model, extract it with Spark into **RustFS landing**, and build **Iceberg bronze, silver and gold** tables with data quality handling, incremental loads, time travel, evolution and maintenance.

## Concepts covered

Profiling a large real source, constraint staging, enriching a source with synthetic operational data, JDBC parallel reads at 34M rows, full vs incremental extraction, raw landing zones, Iceberg table design, MERGE INTO, SCD Type 2, delete handling, ANSI mode and `try_*` functions, window functions over user history, write-audit-publish, time travel, schema and partition evolution, compaction, snapshot expiry, orphan cleanup, reconciliation.

## Sources and targets

| Layer | Location | Format | Notes |
|---|---|---|---|
| Source 1 | Postgres OLTP | rows | the Instacart tables plus synthetic users, stores, prices, payments, lifecycle |
| Source 2 | RustFS `landing` clickstream | JSON files | built around real baskets; dirty, duplicated, late |
| Landing | RustFS `landing` | Parquet/CSV/JSON | raw extracts plus extract metadata |
| Bronze | Iceberg `bronze.*` | Iceberg/Parquet | append-only, faithful, with lineage |
| Silver | Iceberg `silver.*` | Iceberg/Parquet | current state, quarantine, SCD2, conformed dimensions |
| Gold | Iceberg `gold.*` | Iceberg/Parquet | analytics marts |

## Milestones

### M0. Load, profile and constrain the source

- Download the Instacart files. Load each into a Postgres **staging schema as text**, then into typed tables. Bulk-load first and build indexes afterwards; compare load times.
- Parse `products` with care (commas and quotes in names). Compare a naive loader with a proper CSV parser.
- **Profile every table:** row counts, null rates, distinct counts, duplicate keys, value ranges. Write each defect into the **defect register**. Include: test orders with no lines, the null and capped `days_since_prior_order`, the placeholder aisle and department, basket-size distribution, products that never sell.
- **Union the two order-line files** into one `order_items` table and keep the source file as a column. Decide how test-set orders (no lines) are modeled.
- Add constraints in stages and see which relationships hold: primary keys (orders; the composite `order_id` + `product_id` on lines), foreign keys from lines to orders and products, products to aisles and departments. Failures are **findings**, not errors.
- **Verify the `reordered` flag** against each user's history. How many rows disagree?
- Build the **rebuilt timeline**: turn gaps, hours and weekdays into timestamps. Report how often the weekday doesn't fit.
- Add the synthetic operational tables from the README (users, stores, price book, order totals and payments, lifecycle). Document each generation rule.
- Add OLTP columns (`updated_at`, `is_deleted`), indexes (PK, FK, `updated_at`), and **leave one useful index out on purpose** (for example on the order timestamp).
- Build the S, M and L tiers. Which defect rates stayed constant?

### M1. Full-snapshot extraction over JDBC

- Read each table in parallel. Decide the partition strategy:
  - `orders`: numeric `order_id`; check that ids are evenly spread before assuming a range split is balanced.
  - `order_items` (34M rows): partition on `order_id` ranges, and compare with partitioning on a hash expression and on explicit predicates. Large baskets make equal id ranges carry **unequal row counts**. Measure the imbalance.
  - small dimensions: a single read.
- Compare on the large table: single partition vs several `numPartitions` values; different `fetchsize` values; the table option vs a query with a subquery.
- Land the extract as Parquet in RustFS with extract metadata (extract id, extracted_at, source table, row count). Compare Parquet, CSV and JSON for size and time.
- **Observe in Postgres:** active connections, the exact queries Spark sent, index vs sequential scans, and the effect of the missing index.
- Skew trap: partition on a column with uneven values (for example the hour of day or the department). Which JDBC task is slowest and why?

### M2. Incremental extraction

- Design a high-watermark extraction on `updated_at`, driven by the **lifecycle replay**. Decide where the watermark state lives and how a failed run is retried idempotently.
- Handle the traps: late status updates, identical timestamps (ties), item substitutions that arrive after the order is delivered, clock skew (use an overlap window and dedupe), and **hard deletes** (privacy erasures) a watermark can never see.
- Add a periodic **full key reconciliation** to detect deletes and document its cost.
- Compare with the change-log table (preview of Project 5): what does each approach catch and miss?

### M3. Bronze tables in Iceberg

- Create bronze tables append-only, with lineage columns (extract id, source system, ingested_at, source file).
- Choose properties deliberately: format version, Parquet compression, target file size. Compare two compression codecs.
- Try three partition designs for `order_items` (34M rows): by month of the rebuilt order date, by a bucket transform on `order_id`, and by department. Predict the file counts first.
- After each write, inspect the Iceberg metadata tables and correlate them with the objects in the RustFS console.
- Clickstream: read JSON with an explicit schema vs an inferred one (note the extra scan), capture corrupt records, handle the schema drift you built in, write to a bronze events table.

### M4. Silver: conform, dedupe, quarantine

- **Order lines:** union prior and train lines (already unioned in M0); deduplicate; validate `add_to_cart_order` as a gap-free sequence per order and flag exceptions.
- **Orders:** validate the user history (`order_number` sequence, no gaps, no duplicates), route violations to `silver.quarantine_orders` with a reason code, and keep the raw `eval_set`.
- **Products and taxonomy:** conform the three-level taxonomy. Replace the placeholder "missing" aisle and department with an explicit unknown value, and normalize product names.
- **Users:** build the user dimension from the synthetic users table. Track the membership tier over time as **SCD Type 2** with MERGE (valid_from, valid_to, is_current). Verify with an as-of query.
- **Prices:** build an effective-dated price table and price each order line using the price valid at the order timestamp. Compare doing this with a range join vs an expanded daily price table.
- **Payments:** reconcile payments against priced lines plus fees and tip. Classify mismatches (rounding, refunds, failed tenders).
- **Reorder flag:** rebuild `reordered` from each user's history with a window or a self-join, and compare it with the source flag. Quarantine disagreements.
- **Current state via MERGE** from bronze changes, using `updated_at` and sequence. Apply soft deletes as flags and hard deletes from reconciliation.
- **Copy-on-write vs merge-on-read:** switch the merge/update/delete modes. Measure write time, read time and extra delete files.
- With ANSI mode on, use `try_*` casts. Every rejected row goes to quarantine with a reason.

### M5. Gold marts

Build these and record plan and runtime for each:

1. Reorder rate by product, aisle and department.
2. Basket analytics: items per order distribution, department mix per order, add-to-cart position vs reorder probability.
3. Order cadence: gap between orders by user, hour-by-weekday order heatmap.
4. User 360: order count, recency/frequency/monetary on priced orders, favorite products, department mix vector.
5. Cohorts: retention by first-order month and by order number.
6. Top-N products per department per month.
7. **Product co-occurrence** (pairs bought together): the expensive one. Record the row count before and after the pair join, and revisit it in Project 2.
8. Next-basket features: each user's last N orders (window), plus recently bought products not yet re-bought.

Materialize with partitioning by date and a bucket on a key where useful. Compare three rewrite strategies for a reprocessed day: dynamic partition overwrite, MERGE, delete+insert. Prove each is **idempotent** by running twice and diffing.

### M6. Time travel, evolution and write-audit-publish

- Query past snapshots by id and timestamp. Roll back a table after a deliberately bad load (for example, a wrong price-book version).
- Evolve the schema: add, rename, drop a column; promote a type. Confirm old files are untouched.
- **Partition evolution:** change a table from monthly to daily partitioning. Confirm existing data isn't rewritten and look at plans scanning both specs.
- **Branches and tags:** load into a staging branch, run quality checks, publish to main. Tag month-end snapshots.

### M7. Table maintenance

Create a deliberate small-file mess (many tiny appends), then measure scan time and object counts before and after each step:

| Task | Question to answer |
|---|---|
| Compaction: bin-pack vs sort vs z-order | how much faster are filtered queries? |
| Manifest rewrite | does planning time drop? |
| Snapshot expiry | what time-travel ability did you give up, and how much storage came back? |
| Orphan file removal | why is it dangerous with in-flight writes, and what age cutoff is safe? |
| Delete-file compaction | how do accumulated delete files slow reads? |

### M8. Orchestration, failures and reconciliation

- Define run order and dependencies, then re-run one day end to end.
- **Inject failures:** kill a worker mid-write; stop RustFS mid-commit; stop Postgres mid-extract. For each, verify that readers never see a partial table and record leftover files.
- Produce a **reconciliation report** across the raw files, Postgres, landing, bronze, silver and gold, explaining every legitimate difference (filtered, quarantined, deleted, deduplicated). Tie each difference to a defect-register entry.

## Config and property experiments

| Area | Knobs |
|---|---|
| JDBC | `numPartitions`, `fetchsize`, partition strategy, pushdown options |
| Spark | shuffle partitions, AQE, `spark.sql.sources.partitionOverwriteMode` |
| Iceberg write | target file size, distribution mode, compression, merge/update/delete mode |
| Iceberg layout | partition transforms, sort order, format version |
| Storage | S3A or Iceberg S3 file I/O settings against RustFS (Project 4) |

## Stretch goals

- Move the catalog from JDBC to a REST catalog and note what changes.
- Package the pipeline as a parameterized application and run it with `spark-submit` in client mode; try cluster mode and see what standalone allows for Python apps.
- Add a reverse-ETL step writing a gold aggregate back to a Postgres reporting table with controlled parallelism.

## Done when you can answer

1. Which Instacart defects did you find, and which rule handled each?
2. Why do test-set orders have no lines, and how did you model them?
3. Why can equal id ranges give very unequal JDBC partitions here?
4. When is merge-on-read better than copy-on-write, and what does it cost readers?
5. Why can readers never see half a write, even when you killed a worker mid-commit?
