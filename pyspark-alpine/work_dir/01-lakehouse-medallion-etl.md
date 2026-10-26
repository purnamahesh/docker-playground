# Project 1: Lakehouse Medallion ETL

## Goal

Build an idempotent, incremental pipeline from **PostgreSQL and clickstream files** through **MinIO landing** into **Iceberg bronze, silver and gold** tables, with data quality handling, time travel, schema and partition evolution, and table maintenance.

## Concepts covered

JDBC parallel reads, full vs incremental extraction, raw landing zones, Iceberg table design (partition transforms, file format, target file size), MERGE INTO, SCD Type 2, soft and hard deletes, ANSI mode and `try_*` functions, write-audit-publish with branches/tags, time travel, schema and partition evolution, compaction, snapshot expiry, orphan cleanup, reconciliation.

## Sources and targets

| Layer | Location | Format | Notes |
|---|---|---|---|
| Source 1 | Postgres OLTP | rows | customers, products, orders, order_items, payments |
| Source 2 | MinIO `landing` clickstream | JSON files | dirty, duplicated, late |
| Landing | MinIO `landing` | Parquet/CSV/JSON | raw extracts plus extract metadata |
| Bronze | Iceberg `bronze.*` | Iceberg/Parquet | append-only, faithful, with lineage |
| Silver | Iceberg `silver.*` | Iceberg/Parquet | current-state tables, quarantine, SCD2 |
| Gold | Iceberg `gold.*` | Iceberg/Parquet | marts for analytics |

## Milestones

### M1. Full-snapshot extraction over JDBC

- Read each Postgres table in parallel. Decide the **partition column** (numeric or date, indexed, evenly distributed), its bounds (from a min/max query), and the partition count.
- Compare these on a large table: single partition vs several `numPartitions` values; different `fetchsize` values; the `dbtable` option vs a `query` with a subquery; explicit predicates per partition.
- Land the extract in MinIO as Parquet with extract metadata (extract id, extracted_at, source table, row count). Compare Parquet vs CSV vs JSON for size and time.
- **Observe in Postgres:** number of active connections during the extract, the queries Spark sent, whether they used index scans or sequential scans, and the effect of the **missing index** you left on purpose.
- Skew trap: partition on a column that is not evenly spread (for example, by `customer_id` ranges with a hot customer). Which JDBC task runs longest and why?

### M2. Incremental extraction

- Design a high-watermark extraction on `updated_at`. Decide where the watermark state lives (a control table in Postgres or Iceberg) and how a failed run is retried idempotently.
- Handle the traps: late updates to old rows, identical timestamps (ties), clock skew (use an overlap window and dedupe), and hard deletes that a watermark can never see.
- Add a periodic **full key reconciliation** to detect deletes, and document its cost.
- Compare against the change-log table (preview of Project 5): what does each approach catch and miss?

### M3. Bronze tables in Iceberg

- Create bronze tables as append-only, with lineage columns (extract id, source system, ingested_at, source file).
- Choose table properties deliberately: format version, Parquet compression, target file size. Compare at least two compression codecs.
- Try three partition designs for the large table: daily on the load timestamp, hourly, and a bucket transform on the key. Predict the file counts first.
- After each write, inspect the Iceberg metadata tables (snapshots, files, manifests, partitions) and correlate them with the objects visible in the MinIO console.
- Bring in the clickstream: read JSON with an explicit schema vs an inferred one (note the extra scan), capture corrupt records, and write to a bronze events table.

### M4. Silver: MERGE, SCD2, quality

- **Current-state tables:** MERGE the latest change per key into silver using `updated_at` (and sequence when available). Deduplicate deterministically.
- **Deletes:** apply soft deletes as flags and hard deletes from reconciliation.
- **Copy-on-write vs merge-on-read:** switch the merge/update/delete modes. Measure write time, read time, and the extra delete files produced. When does each win?
- **SCD Type 2** for `customers` (valid_from, valid_to, is_current) using MERGE. Verify with an as-of query for a past date.
- **Quality:** with ANSI mode on, use `try_*` casts and route failures to a `silver.quarantine_*` table with a reason code. Add checks for nulls, ranges, referential integrity, and duplicate payments.
- Clickstream silver: deduplicate by event id, handle late events, quarantine malformed ones.

### M5. Gold marts

Build these and record the plan and runtime for each:

1. Daily revenue by country and category.
2. Customer 360 (lifetime value, last order, recency/frequency/monetary).
3. Funnel conversion (view, cart, purchase) per day, comparing exact and approximate distinct counts.
4. Cohort retention by signup month.
5. Top 10 products per category per month.

Materialize them with partitioning by date and a bucket on customer where useful. Compare three rewrite strategies for a reprocessed day: dynamic partition overwrite, MERGE, and delete+insert. Prove each is **idempotent** by running twice and diffing.

### M6. Time travel, evolution and write-audit-publish

- Query a past snapshot by id and by timestamp. Roll a table back after a deliberately bad load.
- Evolve the schema: add, rename, drop a column; promote a type. Confirm old files are untouched and old snapshots still read correctly.
- **Partition evolution:** change a table from daily to hourly partitioning. Confirm existing data is not rewritten, and look at plans that scan both specs.
- **Branches and tags:** load into a staging branch, run quality checks, then fast-forward/publish to main. Tag month-end snapshots for audits.

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

- Define the run order and dependencies, then re-run one day end to end.
- **Inject failures:** kill a worker mid-write; stop MinIO mid-commit; stop Postgres mid-extract. For each, verify that readers never see a partial table, and record what leftover files remain.
- Produce a **reconciliation report** comparing counts and checksums across Postgres, landing, bronze, silver and gold, explaining every legitimate difference (filtered, quarantined, deleted).

## Config and property experiments

| Area | Knobs |
|---|---|
| JDBC | `numPartitions`, `fetchsize`, partition column and bounds, pushdown options |
| Spark | `spark.sql.shuffle.partitions`, AQE, `spark.sql.sources.partitionOverwriteMode` |
| Iceberg write | target file size, distribution mode, compression codec, merge/update/delete mode |
| Iceberg layout | partition transforms, sort order, format version |
| Storage | S3A or S3 file I/O connection settings (details in Project 4) |

## Stretch goals

- Move the catalog from JDBC to a REST catalog and note what changes.
- Package the pipeline as a parameterized application and run it with `spark-submit` in client mode; try cluster mode and see what standalone allows for Python apps.
- Add a reverse-ETL step that writes a gold aggregate back to a Postgres reporting table with controlled parallelism.

## Done when you can answer

1. Why can a high-watermark extract miss data, and which three mitigations did you use?
2. When is merge-on-read better than copy-on-write, and what does it cost readers?
3. What does partition evolution change in the metadata, and what does it leave untouched?
4. Which maintenance step improved performance most, and which reduced storage?
5. Why can readers never see half a write, even when you killed a worker mid-commit?
