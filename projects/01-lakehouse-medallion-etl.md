# Project 1: Lakehouse Medallion ETL (Instacart, Delta Lake)

> **Before you start:** read `00-start-here.md` once. Use `07-learning-companion.md` (P1 section) for prerequisites, the minimal path, time-boxes, prediction prompts, concept checks, rubric and teach-back. Use `08-debugging-playbook.md` when stuck and `09-reference-answers.md` for expected plans and a worked experiment.

## Goal

Turn the raw Instacart files into a governed lakehouse: profile and enrich them into an operational model in Postgres, copy them with Spark into RustFS landing, and build Delta Lake bronze, silver and gold tables with data-quality handling, incremental loads, versioning and time travel, maintenance and reconciliation.

**This project has no change-data-capture and no database triggers.** Incremental loading here uses `updated_at` watermarks and key comparisons only. CDC is introduced in Project 5.

## Work plan (what happens when)

| Phase | Milestone | Core? | Starts when | Produces |
|---|---|---|---|---|
| A | M0 Load, profile, enrich | Core | raw files stored in landing | typed source in Postgres, defect register, rebuilt timeline |
| B | M1 Full extraction | Core | S tier exists in Postgres | landing extracts, manifests |
| C | M3 Bronze | Core | extraction works | bronze Delta tables |
| D | M4 Silver | Core | bronze exists | silver, quarantine, SCD2 |
| E | M5 Gold | Core (3 marts) | silver exists | marts |
| F | M2 Incremental extraction | Core if you will do Project 5 | change simulator exists | incremental pipeline |
| G | M6 Versions, evolution, publish checks | Core (steps 1-3) | gold exists | versioning and evolution results |
| H | M7 Maintenance | Core (OPTIMIZE and VACUUM) | tables have history | runbook |
| I | M8 Orchestration, failure drills, reconciliation | Core (reruns, 2 drills, reconciliation) | everything exists | final report |

Work on the **S tier** until each phase works end to end, then re-run on M. Only touch L in Projects 2 and 4.

**If time is short, do the core path:** M0 (steps 1 to 10), M1, M3, M4 (orders, lines, products, SCD2, MERGE idempotency, quarantine), M5 (three marts), M6 (steps 1 to 3), M7 (OPTIMIZE and VACUUM), M8 (reruns, two drills, reconciliation). Everything marked *optional* is depth.

## Terms used in this project

| Term | Meaning here |
|---|---|
| **Landing zone** | raw copies of source data in object storage, never edited |
| **JDBC partition** | one slice of a source table that one Spark task reads over its own database connection |
| **Snapshot (database)** | a frozen, consistent view of Postgres as of one moment |
| **Manifest** | a small file written last that describes a finished extract |
| **Checksum** | a number computed from data to prove two copies match |
| **Idempotent** | safe to run twice; same result as once |
| **Watermark (extraction)** | the highest `updated_at` already extracted |
| **Control table** | a small bookkeeping table recording every extraction run (explained in M2) |
| **Version (Delta)** | a numbered commit of a Delta table; time travel reads an older version |
| **Quarantine** | a table holding rejected rows with the reason they were rejected |
| **SCD2** | slowly changing dimension type 2: keep history by closing the old row and adding a new one |

---

## M0. Load, profile, enrich and constrain the source

**Objective:** a trustworthy operational database whose defects you know and have recorded.

**Why it matters:** every later layer inherits whatever you misunderstand here. Profiling first turns "the data is weird" into a list of named, counted, testable defects.

**Do, in order:**
1. **Preserve the raw files.** Keep the untouched copy in `landing/raw_dump`. Everything later must be reproducible from it. *Good result:* checksums recorded and stored beside the files.
2. **Stage as text.** Create a staging schema where every column is text so loading can't fail on bad values. Decide how to load and measure rows per second for each way you try. *Good result:* row counts equal the line counts of the files (minus headers).
3. **Fix parsing early.** Product names contain commas and quotes. Load once with a naive reader, count the broken rows, then with a proper quoted-CSV reader. *Good result:* the difference is your first defect entry.
4. **Type the data.** Cast to proper types in a second step and capture every row that fails to cast (there should be few or none). Choose the smallest sensible types (weekday and hour fit small integers). *Good result:* a table of cast failures, even if empty.
5. **Profile every table.** For each: row count, null rate per column, distinct counts, min/max, length of text, top values, duplicate rate on candidate keys. Then add **relationship profiling** (orphan counts for every candidate foreign key, cardinality of each relationship), **distribution profiling** (basket size, orders per user, product frequency, hour and weekday), and **temporal profiling** (distribution of `days_since_prior_order`, size of the spike at the cap, share of nulls by `order_number`). *Good result:* one profile page per table that a colleague could read.
6. **Union the line files.** Combine prior and train lines into one table with a source-file column. Decide how test-set orders (no lines) are represented: keep with no lines, flag, or move aside. Write the decision in the decision log.
7. **Stage constraints.** Add them in order and record the result of each: primary keys on small tables and on orders, then the composite key on lines; foreign keys from lines to orders and to products; products to aisles and departments. Use "add without validating, then validate" so you can count violations instead of failing. *Good result:* a constraint status table (constraint, result, violation count).
8. **Verify `reordered`.** Rule: it should be 1 when the same user bought this product in any earlier order. Check against the data and interpret mismatches (wrong rule, or wrong flag?).
9. **Build the rebuilt timeline.** Follow the design decisions in the README (start date, first order, capped gaps, weekday mismatch). Output a timestamp and a quality flag per order, plus a report of how often the weekday doesn't fit.
10. **Generate the synthetic tables** in dependency order: users and stores first, then the price book, then totals, then payments, then lifecycle columns, erasures, campaign calendar. Test each with its validity rules.
11. **Add OLTP columns and indexes.** Add `updated_at` and `is_deleted`. Create keys and foreign-key indexes. **Deliberately skip one useful index** (for example on the order timestamp) so Projects 1 to 3 can show its effect.
12. **Build tiers.** S first (sample by user), then M, later L. Re-profile each tier and compare distributions with M.

**Example of a profile finding (illustrative numbers):**

| Table | Column | Finding | Rows | Share |
|---|---|---|---|---|
| orders | days_since_prior_order | null | ~206k | ~6% (first orders) |
| orders | days_since_prior_order | exactly 30.0 | a visible spike | the cap |
| orders | eval_set | test | ~75k | no lines exist for them |

**Options to explore** *(try the first three rows; the rest are optional)*:

| Decision | Options | What to compare |
|---|---|---|
| Loading method | server-side bulk copy / client-side copy / Spark JDBC write | rows per second, ease, error visibility |
| Index and constraint timing | before load / after load | total load time |
| Postgres lines table design | single table / partitioned by order date | extract speed (M1), maintenance |
| Test-set orders | keep / flag / separate table | complexity downstream |
| Surrogate keys | natural keys only / surrogate on lines | JDBC partitioning ease (M1) |
| Statistics | analyze after load / skip | quality of Postgres plans in M1 |

**Record / observe:** load times, database size with and without indexes, per-table profile, defect register v1, timeline quality report, constraint status table.

**Traps:**
- Loading with constraints on hides defects behind errors.
- Sampling S by order instead of by user breaks reorder logic.
- A timeline rebuilt without keeping the original gap and hour can't be audited.

**Done when:** every table loaded and typed; the defect register has an entry for every issue in README 4.3 (or a note that you didn't find it); constraint table complete; timeline report written; S and M row counts match the files.

**Read more:** PostgreSQL COPY https://www.postgresql.org/docs/current/sql-copy.html | ALTER TABLE (NOT VALID, VALIDATE) https://www.postgresql.org/docs/current/sql-altertable.html | Spark CSV options https://spark.apache.org/docs/latest/sql-data-sources-csv.html | Instacart competition https://www.kaggle.com/c/instacart-market-basket-analysis

---

## M1. Full extraction: copy every Postgres table into the landing zone

**Objective:** a complete, consistent, verified raw copy of every table as Parquet files in RustFS, produced quickly and without hurting Postgres.

**What this milestone is, in plain words:** Spark reads each Postgres table and writes it as files under `landing/`. It sounds trivial, but there are three real problems: the big tables must be read in parallel (JDBC partitions), the data in Postgres may change while you read (you need a *snapshot*), and you must be able to tell a finished extract from a half-written one (the *manifest*).

### Concepts you need before the steps

**1. What is a snapshot, and why do I need one?**

A **database snapshot** is a frozen, consistent picture of the database *as of one moment*. Why it matters, with an example:

- At 10:00:00 Postgres has 3,421,083 orders.
- You extract `orders` first (takes 1 minute).
- While you do that, the change simulator adds 500 new orders and their lines.
- At 10:02 you extract `order_items`. It now contains lines for orders that are **not in your orders extract**.

Those lines look like orphans (a defect), but they are just a timing problem. A snapshot prevents it: every table is read as of the *same* moment. (Do not confuse this with a Delta table "version", which comes in M3. Here "snapshot" always means the database's consistent view.)

**2. Three ways to get that consistency** (pick one per extract and write it down):

| Approach | How it works (plain words) | Pros | Cons | Use it when |
|---|---|---|---|---|
| **A. Quiet window** | pause the change simulator and any writers for the duration of the extract | simplest; fully consistent | needs the ability to pause writers | your first full extract (recommended) |
| **B. Shared database snapshot (advanced, optional)** | open a transaction in Postgres at a strict isolation level and export its snapshot so every Spark connection can attach to the same view | consistent even while writers run | needs each JDBC connection to join the snapshot (via a session setup statement); fiddly; the exporting transaction must stay open for the whole extract | you can't pause writers and want true consistency |
| **C. Cutoff timestamp** | record the current time as a cutoff at the start; read only rows with `updated_at` at or before the cutoff | easy to do | rows committed late with older timestamps may be missed, so reconcile afterward | later reruns and the incremental work in M2 |

**3. What is a JDBC partition?** Spark can open several connections to Postgres at once. Each connection reads one slice (a "JDBC partition") of the table, defined by a range of a numeric column. Four settings define it:

| Setting | Meaning | Common misunderstanding |
|---|---|---|
| partition column | the numeric column used to slice | must be numeric (or date); text keys need a different approach |
| lower bound / upper bound | the min and max of that column, used to compute slice boundaries | **they do not filter rows**; rows outside the bounds fall into the first or last slice |
| number of partitions | how many slices (and so how many connections at most) | more is not always faster |
| fetch size | rows pulled per round trip | too small = chatty; too large = memory pressure |

*Worked example (illustrative):* `orders` has ids 1 to 3,421,083 and you choose 8 partitions. Each slice covers about 427,635 ids. For `order_items` the same id ranges can hold very different **row counts**, because some orders have 100+ lines and others 1.

| Slice | Id range of orders | Orders | Lines (illustrative) |
|---|---|---|---|
| 1 | 1 to 427,635 | 427,635 | 4.3M |
| 2 | 427,636 to 855,270 | 427,635 | 4.0M |
| ... | ... | ... | ... |
| 8 | last range | 427,641 | 4.6M |

If one slice has twice the rows of the others, the extract finishes only when that slice does.

**4. What is a manifest, and why last?** Object storage can't rename a folder atomically. So you write all data files to a temporary folder first, and the **very last** thing you write is a small manifest file (table, row count, byte count, number of files, time, extract id, as-of moment). Rule for every later step: *an extract without a manifest is incomplete and must be ignored.*

**5. What is a checksum?** A number computed from the data, such as the sum of all `order_id` values and the row count. If source and copy give the same numbers, you can be confident they match without comparing every row.

### Do, in order

1. **List what to extract and how big it is.** Make a table: each Postgres table, row count, table size, whether it has an index on its primary key. Small dimensions (aisles, departments, stores) are read in one piece. *Good result:* a one-page plan.
2. **Choose the as-of approach** (A, B or C above) and write it in the decision log. For the first run, use **A**: pause the change simulator. Record the moment extraction starts as the **as-of time**.
3. **Choose a slicing strategy for each large table** (below), and read the bounds from Postgres just before the run (`min` and `max` of the column).
4. **Check balance before the big run.** For your chosen slices, ask Postgres for the row count per slice (a simple count grouped by slice). Compute max divided by average. *Good result:* ratio under about 1.5. If it's higher, change the slicing. *Why:* the slowest slice sets the total time.
5. **Run on the S tier first**, then M. Extract small tables first, big tables last. Use a modest number of partitions at first (for example 4), then increase.
6. **Write to a temporary location** under the landing prefix, named by extract id. *Why:* a failed run leaves only temporary files, never a half-published extract.
7. **Write the manifest last.** Include: table, extract id, row count, byte count, number of files, partition count, start and end time, the as-of time, the approach used (A, B or C), and checksums. *Good result:* the manifest appears only after every data file.
8. **Validate.** Compare row counts and checksums with Postgres, table by table. They must match exactly in approach A. In approach C, account for rows with `updated_at` after the cutoff.
9. **Run the experiment grid below,** logging each variant.
10. **Rerun the same extract** and confirm it produces the same row count and checksum (the idempotency test). Decide what happens to the old folder (overwrite by extract id or keep both).

### What you should have at the end (example of a landing layout)

| Path (illustrative) | Contents |
|---|---|
| `landing/postgres/orders/extract_dt=2026-11-02/extract_id=E0007/` | Parquet part files + the manifest |
| `landing/postgres/order_items/extract_dt=2026-11-02/extract_id=E0007/` | Parquet part files + the manifest |
| `landing/postgres/_tmp/E0007/` | empty after publishing (temporary area) |

### Slicing options per table

| Table | Options | Notes |
|---|---|---|
| orders (3.4M) | numeric id ranges / hash predicates / per-user-bucket predicates | check ids are evenly spread (gaps?) |
| order_items (34M) | ranges on order id / hash of order id / month of order timestamp (needs an index) / a surrogate numeric key | equal ranges give **unequal row counts**; measure |
| small dimensions | one read | not worth splitting |

### Options to explore

| Knob | Values | Watch |
|---|---|---|
| Number of partitions | 1, 4, 8, 16, 32 | total time, Postgres connections, slowest task |
| Fetch size | small, default, large | executor memory, round trips |
| Table read vs query with a subquery | both | what Postgres actually executes |
| Landing format | Parquet, CSV, JSON | size, write time, later read speed |
| Compression | snappy, zstd, none | size, time |
| Output file size | many small vs few large | later scan cost on RustFS |
| As-of approach | A vs C | consistency and effort |
| Slicing on the unindexed timestamp | try it | effect of the missing index (sequential scans) |

**Record / observe:** per-slice row counts and task durations; Postgres active connections, sequential vs index scans, and the queries Spark actually sent; RustFS object counts.

**Traps:**
- Thinking the lower and upper bounds filter rows (they don't).
- A text key can't be range-partitioned numerically.
- Slicing on a column with no index turns every slice into a full table scan.
- Too many partitions can exceed Postgres's connection limit.
- Publishing without a manifest leaves you unable to tell complete extracts from partial ones.

**Done when:** every table extracted with matching counts and checksums; a manifest per extract; you can explain, with evidence, why you chose each slicing strategy and which as-of approach you used; rerunning gives identical results.

**Read more:** Spark JDBC options https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html | PostgreSQL transaction isolation https://www.postgresql.org/docs/current/transaction-iso.html | PostgreSQL snapshot synchronization (exporting a snapshot) https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION | PostgreSQL EXPLAIN https://www.postgresql.org/docs/current/using-explain.html | Hadoop S3A https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html

---

## M2. Incremental extraction (copy only what changed)

**Objective:** keep landing and bronze current by extracting only changed rows, correctly, including late updates and deletes.

**Why it matters:** re-copying 34M lines every time is wasteful, but copying only "new" rows is wrong if rows are updated or deleted. You need a rule for "what changed since last time" and a record of what you already did. That record is the **control table**.

**No CDC and no triggers here.** You will detect changes only with the `updated_at` column and key comparisons.

### Concepts

**Watermark (high-water mark).** The largest `updated_at` value you have already extracted. Next run extracts rows with `updated_at` above it.

**Overlap window.** Some rows commit late: a transaction starts at 10:00:00, the row gets `updated_at = 10:00:00`, but the transaction commits at 10:00:09. If your run happened at 10:00:05, you passed the timestamp before the row became visible. Fix: each run starts a bit *before* the watermark (for example 5 minutes), and you deduplicate. The overlap should be derived from measured lateness, not guessed.

**Ties.** Several rows can have exactly the same `updated_at`. If you use "greater than" you can skip tied rows; use "greater than or equal" plus deduplication.

**Hard deletes.** If a row is deleted, there is no row left to carry a new timestamp, so a watermark can never see it. You need a separate check (below).

### What the control table records

The **control table** is a small table that stores one row for every extraction attempt, so the pipeline always knows *what it has done, what it is doing, and where to start next*. Think of it as the pipeline's notebook.

**Columns, with example rows (illustrative):**

| run_id | table_name | strategy | lower_bound (from) | upper_bound (to) | status | rows_extracted | started_at | finished_at | note |
|---|---|---|---|---|---|---|---|---|---|
| R0001 | orders | full | n/a | 2026-11-02 10:00:00 | succeeded | 3,421,083 | 10:00:05 | 10:03:41 | first full load |
| R0002 | orders | watermark | 2026-11-02 09:55:00 | 2026-11-02 10:30:00 | succeeded | 1,204 | 10:30:02 | 10:30:19 | overlap 5 min |
| R0003 | orders | watermark | 2026-11-02 10:25:00 | 2026-11-02 11:00:00 | failed | null | 11:00:02 | null | worker lost |
| R0004 | orders | watermark | 2026-11-02 10:25:00 | 2026-11-02 11:30:00 | succeeded | 2,310 | 11:30:01 | 11:30:40 | reran the failed window |

| Column | What it is for |
|---|---|
| run_id | unique id of this attempt; also used in the landing folder name |
| table_name | which source table |
| strategy | full, watermark, key comparison |
| lower_bound / upper_bound | the window of changes this run covers (watermark minus overlap, up to the as-of time) |
| status | started, succeeded or failed |
| rows_extracted | how many rows landed (for reconciliation) |
| started_at / finished_at | for auditing and spotting slow runs |
| note | free text such as the reason for a failure |

**How a run uses the table (in order):**
1. Read the latest **succeeded** row for this table; its `upper_bound` is the watermark.
2. Compute the new window: lower = watermark minus overlap; upper = now (the as-of time).
3. Insert a new row with status **started**.
4. Extract rows in the window, land them under a folder named with the run id, write the manifest last.
5. Update the row to **succeeded** with the row count and finish time (or **failed**).
6. If a run is found in status started with no finish (a crash), treat it as failed and **rerun the same window**. Because landing paths include the run id and the manifest is written last, rerunning is safe.

**Where should it live?**

| Option | Pros | Cons |
|---|---|---|
| A Postgres schema (separate from source tables) | transactional; easy updates; queryable | extra load on the source database |
| A small Delta table | versioned; fits the lakehouse | updates need MERGE; remember the single-writer rule |
| A JSON file per run in landing | no database needed | no atomic updates; hard to query |

For the lab, a Postgres control schema is the simplest correct choice.

### Do, in order

1. **Start the change simulator** so Postgres changes continuously: new orders, status moves, substitutions, tier changes, erasures, all with `updated_at` bumped. (It writes to the normal tables only; there are no triggers and no change log.)
2. **Choose a strategy per table** from the options below and justify each.
3. **Create the control table** and decide where it lives.
4. **Measure real lateness:** compare each row's `updated_at` with the time it became visible to a reader (you can sample by polling). Set the overlap window from the maximum you observe, plus a margin.
5. **Build the run loop** following "How a run uses the table".
6. **Land the window** under the run id, with the manifest last; deduplicate on key keeping the latest change.
7. **Implement delete detection** (below).
8. **Prove idempotency:** rerun the same window and show nothing changes downstream; then kill a run midway and show the next run recovers.
9. **Reconcile** at intervals: counts and key sets between source and landing.

### Strategy options by table

| Strategy | Good for | Weakness |
|---|---|---|
| High watermark on `updated_at` | orders, payments, users | misses hard deletes; needs overlap for late rows; ties |
| Monotonic key (new rows only) | append-only data | unsafe if rows can change (substitutions are updates) |
| Key comparison (compare all keys in source with all keys downstream) | small tables; delete detection | expensive on big tables |
| Full re-extract | small dimensions | too slow for large tables |

### Delete detection options

| Option | How | Cost |
|---|---|---|
| Soft-delete flag | the application sets `is_deleted` and bumps `updated_at`; the watermark sees it | none extra; requires soft deletes only |
| Periodic key reconciliation | extract the set of all current keys; keys downstream but not in source were deleted (in Delta this is a MERGE with "when not matched by source" or an anti-join) | proportional to table size |
| Both | soft flag for normal deletes, reconciliation as a safety net for hard deletes | recommended |

**Record / observe:** the control table over a day, lateness measurements, number of rows per window, reconciliation differences.

**Traps:**
- Equal timestamps at the boundary: use inclusive lower bounds plus deduplication.
- Setting overlap by guess instead of measurement.
- Reviewing only counts: updates that change values without changing counts slip through (compare a checksum of changed columns).
- Forgetting that a hard delete never appears in a watermark extract.

**Done when:** a day of replayed changes arrives complete; a reconciliation shows no missed late updates; hard deletes are detected within a documented lag; every table's strategy has a justification and a known weakness; a crashed run recovers by rerunning its window.

**Read more:** Postgres transaction isolation https://www.postgresql.org/docs/current/transaction-iso.html | Idempotence https://en.wikipedia.org/wiki/Idempotence | Delta MERGE (including "not matched by source") https://docs.delta.io/latest/delta-update.html

---

## M3. Bronze tables in Delta Lake

**Objective:** a faithful, append-only, queryable copy of every source with full lineage, stored as Delta tables.

**What a Delta table is (plain words):** a folder of Parquet data files plus a folder `_delta_log` of numbered JSON commit files. Each commit lists "add this file" and "remove that file". Reading a table means replaying the log to learn which files are current. Every commit is a **version** (0, 1, 2, ...). Every 10 commits (default), Delta writes a checkpoint file to speed up replay. The log also stores per-file statistics (row count, min and max of the first 32 columns), which Spark uses to skip files that can't match a filter ("data skipping").

**Do, in order:**
1. **Define the bronze contract:** raw values as received, no business cleaning, plus lineage columns (run id, extract id, source system, ingested time, source file, operation).
2. **Create one bronze Delta table per source table** under `warehouse/bronze/<table>`. Snapshot loads and incremental loads go into the same table, distinguished by lineage columns.
3. **Choose types:** keep source types, or keep everything as strings and cast in silver (decide, record).
4. **Choose table properties** (names to review: checkpoint interval, data-skipping columns, log and file retention, deletion vectors off for bronze) and Parquet compression.
5. **Choose partitioning** for the lines table (below). Delta tables partition by **column values**, fixed at creation; changing it later means rewriting the table. Predict the number of partitions and files first.
6. **Load** from landing with an append; then load the clickstream with an explicit schema, corrupt-record capture and the planned schema drift handled.
7. **Inspect after every write:** table history (version, operation, metrics such as files added and rows written), table detail (number of files, total size, partition columns), and a few `add` entries of the transaction log (path, size, statistics). Compare file counts with the objects in the RustFS console.
8. **Sanity-check:** counts equal the landing manifests; trace a few rows back to the source.

**Partitioning options for the lines table:**

| Option | Pros | Cons |
|---|---|---|
| Month of order date (a generated column derived from the timestamp) | prunes date filters | needs the timeline joined early; generated column must be defined at creation |
| Day of load (ingestion date) | simple, matches loads | doesn't prune business-date filters |
| Department | prunes department queries | skewed (produce) |
| Unpartitioned, then compact and Z-order | fewer small files; flexible | no partition pruning; Z-order maintenance |

**Rule of thumb:** partition only on low-cardinality columns (tens to a few thousand values), because every partition has its own files; many tiny partitions produce many tiny files.

**Options to explore** *(try the first three rows; the rest are optional)*: two compression codecs; two target file sizes (affects OPTIMIZE); typed vs all-string bronze; one table for snapshots and increments vs two; number of columns with statistics.

**Traps:** too many partitions create many small files; partition columns can't be changed without a rewrite; no lineage columns makes later debugging impossible; running two writers on the same table (Gate F).

**Done when:** all bronze tables reconciled with landing; each partition choice evaluated with file counts and sizes; lineage complete; you can read a table's history and explain every version.

**Read more:** Delta batch reads and writes https://docs.delta.io/latest/delta-batch.html | Delta utility commands (history, detail) https://docs.delta.io/latest/delta-utility.html | Delta protocol (log, checkpoints, statistics) https://github.com/delta-io/delta/blob/master/PROTOCOL.md | Medallion architecture https://www.databricks.com/glossary/medallion-architecture

---

## M4. Silver: conform, deduplicate, quarantine, version

**Objective:** trusted current-state and historical tables, with every rejected row explained.

**Do, in order, per table:**

| Table | Rules and decisions |
|---|---|
| **Order lines** | deduplicate on order + product; validate `add_to_cart_order` as a gap-free sequence per order; flag orders with no lines; join the price valid at order time |
| **Orders** | validate user history (order numbers contiguous and unique); keep raw `eval_set`; apply the timeline; flag cancelled; quarantine impossible lifecycles |
| **Products and taxonomy** | conform the three levels; replace the placeholder aisle/department with an explicit unknown; normalize names |
| **Users (SCD2)** | derive from synthetic users; track membership tier history with valid_from, valid_to, is_current; prove with as-of queries |
| **Prices (optional)** | effective-dated prices; detect overlaps and gaps; resolve promotions by precedence |
| **Payments (optional)** | reconcile against priced lines + fees + tip; classify mismatches (rounding, refunds, failed tenders, missing rows) |
| **Reorder flag** | rebuild from history; compare with the source flag; quarantine or flag disagreements |
| **Clickstream (optional)** | deduplicate on event id; handle late events; map sessions to orders; quarantine malformed events |

**MERGE design (do this for every current-state table):**
1. Define the match key and the "newer wins" rule (`updated_at`).
2. **Collapse the incoming batch to one row per key first.** Delta's MERGE fails if several source rows match one target row, so this is required, not optional.
3. Decide how deletes arrive (soft flag, reconciliation) and how each is applied.
4. Prove idempotency: apply the same batch twice and show nothing changes.

**SCD2 example (illustrative):** user 42 upgrades from free to plus on 2026-03-01.

| user_id | tier | valid_from | valid_to | is_current |
|---|---|---|---|---|
| 42 | free | 2025-06-10 | 2026-03-01 | false |
| 42 | plus | 2026-03-01 | 9999-12-31 | true |

An as-of query for 2026-02-15 must return `free`; for 2026-03-02, `plus`. Build it with MERGE: close the old row and insert the new one.

**Quarantine design:** one quarantine table per entity holding the original row, a reason code from a catalog (example codes: `CAST_FAILURE`, `LIFECYCLE_VIOLATION`, `MISSING_PARENT`, `DUPLICATE_KEY`, `OUT_OF_RANGE`), the rule version and the run id. For each rule decide: **reject** (row goes only to quarantine), **flag** (row continues with a flag column), or **count** (only metrics).

**Two ways to enforce quality in Delta (compare them):**

| Mechanism | Behavior | Good for |
|---|---|---|
| Table constraints (NOT NULL, CHECK) | the whole write **fails** if any row violates | invariants that must never be violated (for example price > 0 in gold) |
| Quarantine routing | bad rows are diverted; good rows continue | silver ingestion where some bad rows are expected |

**Data-quality rule catalog (build and run these):** completeness (required fields), validity (ranges and formats), uniqueness (keys), referential integrity (parent exists), consistency (cross-table totals), timeliness (late arrivals). Store the metrics per run so you can chart them.

**Options to explore** *(try the first three rows; the rest are optional)*:

| Experiment | Options | Measure |
|---|---|---|
| Update mechanism | rewrite files (default) vs **deletion vectors** (mark rows deleted without rewriting the file) at 0.1%, 1%, 10% updates | write time, read time, number of files rewritten, extra files |
| SCD2 construction | MERGE in steps vs build the changed rows then insert | complexity and speed |
| Price join | range join vs daily-expanded price table | time and plan |
| Dedup | arbitrary row vs deterministic latest | plan and result stability |
| Casting | strict (fail) vs `try_*` with quarantine | rows rejected, behavior |
| Enforcement | CHECK constraint vs quarantine for the same rule | failure behavior |

**Traps:** double-counting test orders; silently dropping rows instead of quarantining; non-deterministic dedup; merging without collapsing the batch first; turning on deletion vectors without realizing it upgrades the table protocol irreversibly (older readers can't read it).

**Done when:** each silver table has a documented rule list, a quarantine reason catalog, and row accounting that balances (bronze rows = silver rows + quarantined + deduplicated); SCD2 as-of queries return correct history; merges are idempotent.

**Read more:** Delta MERGE, UPDATE, DELETE https://docs.delta.io/latest/delta-update.html | Delta constraints https://docs.delta.io/latest/delta-constraints.html | Delta deletion vectors https://docs.delta.io/latest/delta-deletion-vectors.html | Spark ANSI mode https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html | SCD overview https://en.wikipedia.org/wiki/Slowly_changing_dimension

---

## M5. Gold marts

**Objective:** analytics-ready tables with verified numbers and a refresh strategy.

**Do, in order, for each mart:** write its grain (what one row means) and purpose; list inputs; define the logic and pitfalls; choose a refresh strategy; test with invariants; record plan and runtime.

| Mart | Grain | Logic and pitfalls | Invariant to test |
|---|---|---|---|
| Reorder rate | product, aisle, department | exclude orders with no lines; decide the denominator (all purchases of the product) | rates between 0 and 1; totals match lines |
| Basket analytics | order | items per order, department mix, position vs reorder | sum of items equals line count |
| Order cadence | user, hour by weekday | gap distribution; treat capped gaps as censored | counts match orders |
| User 360 | user | recency, frequency, monetary on priced orders; favorites; department vector | user counts match |
| Cohorts | first-order month by order number | careful with incomplete months | cohort sizes sum to users |
| Top-N (optional) | department by month | ties and deterministic ordering | N rows per group |
| Co-occurrence (optional here; done in Project 2) | product pair | the expensive one (Project 2); decide pruning and basket caps | symmetry; counts reproduce on a sample |
| Next-basket features (optional) | user, as-of date | window over the last N orders; leakage control | no data after the as-of date used |

**Refresh strategy options:**

| Strategy | How it works in Delta | Watch |
|---|---|---|
| Full rebuild (overwrite) | replace the whole table | simplest; creates a new version; old files remain until VACUUM |
| Overwrite by predicate (replaceWhere) | atomically replace only rows matching a condition such as one date | the condition must match the data written |
| Dynamic partition overwrite | replace only the partitions present in the new data | easy to overwrite more than intended |
| MERGE | update changed groups, insert new ones | needs a key; collapse the source first |
| Delete + insert | two steps | not atomic unless wrapped; prefer replaceWhere or MERGE |

**Test each strategy for idempotency:** run it twice and diff the outputs.

**Options to explore** *(try the first three rows; the rest are optional)*: partition by date vs department; Z-order on the dominant filter column after OPTIMIZE; pre-aggregate before joining; cache an input used by several marts.

**Done when:** every mart has a documented grain and passing invariants; three refresh strategies tried on one mart; reruns identical.

**Read more:** Delta selective overwrite https://docs.delta.io/latest/delta-batch.html | Spark window functions https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html

---

## M6. Versions, time travel, evolution and publish checks

**Objective:** use Delta's versioning safely.

**Concepts:** every commit is a version. You can read an old version (time travel), roll the table back to an old version (RESTORE), or copy a table cheaply at a version (shallow clone: a new table that points at the same data files). Old versions are readable only while both the log entries *and* the data files still exist (see M7).

**Do, in order:**
1. **Time travel:** read past versions by version number and by timestamp; compare a gold number before and after a load.
2. **Bad-load recovery:** load wrong data on purpose (for example a mis-versioned price book), detect it with a check, and restore the previous version. Record what was rolled back and what remained.
3. **Schema evolution matrix:** try each and record the outcome for old and new readers:

| Change | Delta behavior to test |
|---|---|
| Add a nullable column | allowed with schema merge on write (or schema auto-merge for MERGE) |
| Rename or drop a column | requires **column mapping** to be enabled (a protocol upgrade); without it, rewrite the table |
| Widen a type (int to long) | **type widening** feature (no rewrite) |
| Change a type narrower | not allowed; rewrite |
| Add a derived column | **generated column** (computed on write; also enables partition pruning on a derived date) |
| Reorder columns | usually harmless; verify downstream readers |

4. **(Optional) Changing the partition layout:** unlike some formats, a Delta table's partition columns are fixed. Change them by rewriting the table (overwrite with the new layout), and record the cost. Then read about **liquid clustering**, which lets you change clustering keys without rewriting (depth topic).
5. **(Optional) Publish checks without branches:** Delta open source has no branches or tags. Use one of: a **staging table** you validate and then publish with MERGE; a **shallow clone** you test on, then apply the same change to the real table; or record **version bookmarks** (a small audit table mapping a label such as "month_end_2026_03" to a version number). Run the data-quality suite on the staging copy and publish only if it passes.
6. **Define a retention policy:** how long you keep versions and why (ties to M7).

**Traps:** forgetting that VACUUM removes the old files, ending time travel; renames that downstream readers depend on; publishing without re-running checks after later changes; enabling a table feature (column mapping, deletion vectors) that older readers can't read.

**Done when:** a restore recovered a clean state; every evolution type has a recorded outcome; a failing check blocked a publish.

**Read more:** Delta utility commands (RESTORE, history, clone) https://docs.delta.io/latest/delta-utility.html | Delta column mapping https://docs.delta.io/latest/delta-column-mapping.html | Delta type widening https://docs.delta.io/latest/delta-type-widening.html | Delta liquid clustering https://docs.delta.io/latest/delta-clustering.html

---

## M7. Table maintenance

**Objective:** a runbook that keeps tables fast and storage bounded.

**Concepts (plain words):**
- **Small files:** many tiny appends create many tiny files; every file costs a request to RustFS and planning time.
- **OPTIMIZE:** rewrites many small files into fewer, larger ones (bin-packing). With **Z-order** it also sorts data by chosen columns so that file statistics can skip more files.
- **VACUUM:** physically deletes data files that are no longer referenced by the table *and* older than a retention period (default 7 days). It does not change the logical table, but it ends time travel to versions that needed those files.
- **Log retention:** separately, old log entries are cleaned up after a retention period (default 30 days), which also limits how far back you can travel.
- **Deletion vectors:** if enabled, deletes and updates mark rows as removed in a small side file instead of rewriting the data file; a later purge (REORG ... APPLY PURGE) or OPTIMIZE rewrites files to apply them.

**Do, in order:**
1. **Baseline:** table detail (file count, size), history, and the scan time of three reference queries.
2. **Create the mess:** many tiny appends; many MERGEs with deletion vectors on; many versions. Re-measure and note the damage.
3. **Apply each action separately and measure:**

| Action | Parameters to explore | Question to answer |
|---|---|---|
| OPTIMIZE (bin-pack) | target file size; limit to recent partitions | how much faster are filtered queries? |
| OPTIMIZE with Z-order | one vs two columns | does file skipping improve? |
| Purge deletion vectors | after many merges | does read time recover? |
| VACUUM | retention hours; dry run first | what time travel did you lose; how much storage came back? |
| (Optional) Checkpoint settings | interval | does log replay get faster? |
| (Optional) Auto compaction / optimized writes | check availability in your Delta version | do small files stop accumulating? |

4. **Order the actions** (OPTIMIZE, then purge, then VACUUM) and test the schedule while a writer is running. Remember Gate F: only one application may write the table; maintenance counts as a writer.
5. **Write the runbook:** cadence per table type, triggers (file count or small-file ratio), safety rules (never set retention below your time-travel and consumer needs), and alerts.

**Traps:** VACUUM with a very short retention can delete files that a long-running query or a stream still needs (Delta blocks retention under 7 days unless you override the safety check; don't); maintenance from a second application while a job writes; assuming OPTIMIZE reduces storage (it creates new files; VACUUM reclaims the old ones).

**Done when:** each action has a measured benefit; the schedule survives a concurrent writer inside one application; runbook written.

**Read more:** Delta optimizations (OPTIMIZE, Z-order, auto compaction) https://docs.delta.io/latest/optimizations-oss.html | Delta VACUUM https://docs.delta.io/latest/delta-utility.html | Delta deletion vectors https://docs.delta.io/latest/delta-deletion-vectors.html

---

## M8. Orchestration, failure drills and reconciliation

**Objective:** prove the pipeline is repeatable, recoverable and accountable.

**Do, in order:**
1. **Draw the dependency graph** (extract, bronze, silver, gold, maintenance), choose run ordering and retry rules, and design a **writer lock**: a simple mechanism (for example a row in the control schema) that ensures only one application writes a given table at a time (Gate F).
2. **Rerun tests:** rerun one day end to end; rerun a failed middle step; backfill a past day. Show identical results each time.
3. **Failure drill matrix:**

| Inject | Expect | Verify |
|---|---|---|
| Kill a worker mid-write | task retry, success | no duplicate or partial data; table version history looks sane |
| Stop RustFS mid-commit | failed or retried commit | the table is unchanged (the last good version still reads); list leftover data files |
| (Optional) Stop Postgres mid-extract | failed extract | no half-published extract; manifest absent; control table shows failed |
| Kill the driver mid-merge | failed run | rerun applies exactly once |
| (Optional) Corrupt a landing file | quarantine path | pipeline continues, defect recorded |

4. **Reconciliation report** across the raw files, Postgres, landing, bronze, silver and gold: row counts, key-set differences, and totals (lines, items, priced amounts). Explain **every** difference, linking each to a defect-register entry (filtered, quarantined, deleted, deduplicated).
5. **Lessons:** list what you would automate, monitor and alert on.

**Done when:** every drill's outcome matches your prediction (or the surprise is explained); the reconciliation has zero unexplained differences.

**Read more:** Delta concurrency control https://docs.delta.io/latest/concurrency-control.html | Delta storage configuration https://docs.delta.io/latest/delta-storage.html

---

## Config and property experiments (summary)

| Area | Knobs |
|---|---|
| JDBC | partitions, fetch size, partition strategy, pushdown options |
| Spark | shuffle partitions, AQE, dynamic partition overwrite mode |
| Delta write | partition columns, compression, OPTIMIZE target size, deletion vectors, schema merge |
| Delta layout | Z-order columns, statistics columns, checkpoint interval |
| Storage | S3A settings against RustFS (Project 4) |

## Stretch goals
- Add a small Hive metastore (backed by Postgres) so tables have persistent names.
- Package the pipeline as a parameterized application and run it with `spark-submit` (client mode, then cluster mode on standalone).
- Add a reverse-ETL step writing a gold aggregate to a Postgres reporting table with controlled parallelism.

## Exit criteria for Project 1
1. You can state each Instacart defect, how you found it, and which rule handles it.
2. Every layer reconciles to the one before it.
3. Rerunning any step is safe, and you can show it.
4. Your maintenance runbook has measured effects.
