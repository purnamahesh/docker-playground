# Project 3: Catalyst and Query Planning (Instacart, Delta Lake)

> **Before you start:** read `00-start-here.md` once. Use `07-learning-companion.md` (P3 section) for prerequisites, the minimal path, time-boxes, prediction prompts, concept checks, rubric and teach-back. Use `08-debugging-playbook.md` when stuck and `09-reference-answers.md` for expected plans.

## Goal

Read plans fluently, predict what the optimizer will do, and prove each prediction with evidence from four places: the Spark plan and UI, the Delta transaction log, RustFS request activity, and Postgres. You cover two kinds of scans: Delta tables and JDBC tables.

**Simplified scope:** seven core milestones (M0 to M6) plus an optional Depth block. No CDC in this project.

## Work plan

| Phase | Milestone | Core? | Produces |
|---|---|---|---|
| A | M0 Variants and the query suite | Core | test tables, 8 reference queries |
| B | M1 Plan literacy | Core | annotated plans |
| C | M2 Delta pruning and data skipping | Core | pruning evidence |
| D | M3 Statistics and join order | Core | estimate-vs-actual notes |
| E | M4 JDBC pushdown | Core | pushdown matrix |
| F | M5 Optimizer blockers | Core | pitfall catalog |
| G | M6 Layout choices | Core | layout recommendations |
| H | Depth: AQE, runtime filters, codegen | Optional | extra notes |
| I | M7 Cheat sheet | Core | one page |

## Terms used in this project

| Term | Meaning |
|---|---|
| **Logical plan / physical plan** | what you asked for vs how Spark will actually do it |
| **Catalyst** | Spark's query optimizer; rewrites the logical plan using rules |
| **Predicate pushdown** | moving a filter as close to the data source as possible |
| **Partition pruning** | skipping whole table partitions (folders) because the filter excludes them |
| **Data skipping** | skipping individual files using per-file min/max statistics stored in the Delta log |
| **Generated column** | a column Delta computes from other columns on write (for example the date of a timestamp); lets filters on the timestamp prune date partitions |
| **Z-order** | a way of sorting data across several columns so file statistics skip more files |
| **Sargable** | a filter written so an index or statistics can be used (no function wrapped around the column) |
| **CBO** | cost-based optimizer: uses statistics to choose join order and strategy |

---

## M0. Variants and the query suite

**Objective:** a controlled set of tables and queries that differ in only one respect at a time.

**Do, in order:**
1. **Build variants of the lines table** (about 34M rows at the **M tier**; see the README scaling table):
   - V1: partitioned by month (via a generated date column), not sorted;
   - V2: same partitioning, **Z-ordered** by product id after OPTIMIZE;
   - V3: unpartitioned, Z-ordered by product id;
   - V4: a plain Parquet copy in landing (for comparison with a non-Delta scan).
2. **Make files comparable:** run OPTIMIZE on every Delta variant with the same target size.
3. **Define eight reference queries** and keep them fixed:

| Id | Query | Shows |
|---|---|---|
| Q1 | lines for one product in one month | partition pruning plus file skipping |
| Q2 | lines for one department (low selectivity) | limits of pruning |
| Q3 | orders in a date range with an hour filter | partition filter plus pushed filter |
| Q4 | five-way join with a selective filter on one dimension | join order and broadcast |
| Q5 | the same join written in a bad order | cost-based reordering |
| Q6 | count, min and max over the whole lines table | metadata-only answers |
| Q7 | filter and aggregate over a live Postgres table | JDBC pushdown |
| Q8 | Q4 with a Python UDF inside a filter | optimizer blocker |

4. **Record baselines:** time, bytes read, files read, plan fingerprint (scan type, join types, exchanges).

**Done when:** all variants exist and are compacted; the suite runs and is logged.

**Read more:** Delta generated columns and partitioning https://docs.delta.io/latest/delta-batch.html | Delta OPTIMIZE and Z-order https://docs.delta.io/latest/optimizations-oss.html

---

## M1. Plan literacy

**Objective:** read any plan top-down and explain it.

**Why it matters:** a plan shows the decisions Spark made on your behalf. Learn to read three things: the scan (what was read and filtered), the joins (type and order), and the exchanges (where data moved).

**Do, in order:**
1. For Q1 to Q4, **write a prediction first**: scan type, pushed filters, join types, number of exchanges.
2. Print the plan in stages (parsed, analyzed, optimized, physical) and compare with your prediction.
3. Find these in the plans: **constant folding** (an expression like 1+1 computed once), **column pruning** (only the columns you use are read), **predicate pushdown** (filters moved to the scan), **limit pushdown**.
4. Learn every field on a scan node: partition filters (which partitions), pushed/data filters (what the file reader applies), read schema (columns read), and what remains for Spark to filter afterward.
5. Learn the plan formats: formatted (readable), cost (with estimates) and the SQL tab graph.
6. Compare a Delta scan with a JDBC scan in the same plan.

**Traps:** reading the initial plan when AQE changed it; assuming "pushed" means "applied at the source".

**Done when:** at least 80% of your plan predictions are right.

**Read more:** EXPLAIN https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html | Spark SQL performance tuning https://spark.apache.org/docs/latest/sql-performance-tuning.html

---

## M2. Delta pruning and data skipping

**Objective:** prove how many files each query skipped and why.

**Concepts (plain words):**
- **Partition pruning:** if the table is partitioned by month and you filter on one month, Spark never opens other months' folders. It works when the filter is on the partition column, or on a column from which a **generated** partition column is derived.
- **Data skipping:** every Delta data file has statistics in the log (row count, min and max for the first 32 columns by default). If a filter value is outside a file's min/max range, the file is skipped without reading it. It works best when similar values are stored together, which is what sorting and Z-order do.

**Do, in order:**
1. Run Q1 on V1 (partitioned, unsorted), V2 (partitioned and Z-ordered) and V3 (unpartitioned, Z-ordered). For each record: partition filters in the plan, files read versus total files (compare with table detail), bytes read.
2. Confirm that a filter on the **timestamp** prunes month partitions even though you never mention the generated date column.
3. Explain the difference between V1 and V2 for the product filter (statistics become useful only after clustering).
4. Run Q2 (low-selectivity filter) and explain why pruning helps little.
5. Run Q6 (count, min, max): is it answered from the log or by scanning? (Check in your version; the SQL tab shows rows read.)
6. **Prove it with the log:** read a few of the table's `add` entries (file path, size, min/max of product id) and show why a given file was or wasn't read.

**Worked example (illustrative numbers):**

| Variant | Files total | Files read for Q1 | Bytes read |
|---|---|---|---|
| V1 | 240 | 18 (one month) | 380 MB |
| V2 | 240 | 3 | 45 MB |
| V3 | 90 | 4 | 60 MB |

**Options to explore:**

| Variable | Values | Watch |
|---|---|---|
| Partitioning | month / none | files read, number of files |
| Clustering | none / Z-order one column / two columns | files skipped |
| File size | small / target / large | planning time and request count on RustFS |
| Filter shape | equality / range / IN list / OR | what is pruned |

**Done when:** for each query you can state files skipped, which statistic made that possible, and how to improve it.

**Read more:** Delta optimizations (data skipping, Z-order) https://docs.delta.io/latest/optimizations-oss.html | Delta protocol (statistics) https://github.com/delta-io/delta/blob/master/PROTOCOL.md

---

## M3. Statistics and join order

**Objective:** see how estimates drive join order, and where they fail.

**Why it matters:** with many joins, the order matters a lot. Spark can choose the order using cost estimates, but only if it has statistics and the cost-based optimizer is on (off by default).

**Do, in order:**
1. Run Q5 (bad order) with the cost-based optimizer **off**. Record the join order and time.
2. Collect table and column statistics for the tables involved, then turn on the cost-based optimizer and join reordering. Re-run Q5.
3. Print the **cost** plan and read estimated sizes and row counts per operator.
4. Compare estimates with actuals in the SQL tab. Find the worst misestimate and explain it (correlated columns, skew, expressions, stale statistics).
5. Add a day of data and see how stale statistics change the plan.
6. Check which statistics Delta tables contribute in your versions (per-file statistics in the log feed data skipping; whether `ANALYZE` records column statistics for Delta tables is worth confirming in your version).

**Options to explore:** cost-based optimizer on/off; with and without column statistics; stale vs refreshed statistics.

**Done when:** you can say when cost-based planning helped, when it hurt, and what upkeep it needs.

**Read more:** Spark SQL performance tuning (CBO) https://spark.apache.org/docs/latest/sql-performance-tuning.html | Spark configuration https://spark.apache.org/docs/latest/configuration.html

---

## M4. JDBC pushdown and Postgres-side truth

**Objective:** know exactly what Postgres executes for a Spark query.

**Why it matters:** "pushed" in a Spark plan only means Spark *asked* Postgres to filter; it says nothing about whether Postgres used an index. Check three views of the same query: the Spark plan, the SQL text Postgres received, and Postgres's own plan for it.

**Do, in order:** for each case, capture the three views.

| Case | What to determine |
|---|---|
| Filter on an indexed column | pushed? index used? |
| The same filter on the unindexed timestamp | sequential scan; cost at M |
| Aggregate over the table | pushed or pulled across? |
| Limit | pushed? |
| Filter with a Spark-only function or UDF | why pushdown fails |
| Partitioned read (several connections) | does each slice use an index? |

Then toggle each pushdown option and record rows transferred and time. Write a **rule of thumb**: when should logic run in Postgres, and when must it run in Spark?

**Traps:** a filter shown as pushed but evaluated again in Spark; stale Postgres statistics giving poor plans.

**Done when:** the matrix is complete with evidence for each row.

**Read more:** Spark JDBC options https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html | PostgreSQL EXPLAIN https://www.postgresql.org/docs/current/using-explain.html | PostgreSQL indexes https://www.postgresql.org/docs/current/indexes.html | pg_stat_statements https://www.postgresql.org/docs/current/pgstatstatements.html

---

## M5. Optimizer blockers (pitfall catalog)

**Objective:** a list of things that silently defeat optimization, each with a measured fix.

**Do, in order:** for each pitfall, record the plan and time before and after.

| Pitfall | Why it hurts | Fix |
|---|---|---|
| Python UDF inside a filter (Q8) | the filter can't be pushed into the scan | rewrite with built-in functions |
| Function or cast on the filtered column (for example cast of the timestamp to text) | defeats pruning and statistics (not sargable) | rewrite as a range on the original column |
| OR across partition and non-partition columns | prevents pruning | split into two queries and union |
| Wide select into a join | reads and shuffles unused columns | select only needed columns early |
| Many small files | many requests, slow planning | OPTIMIZE |
| Filter on an unindexed Postgres column | sequential scan on the source | add the index, or move the work to the lake |

**Done when:** every pitfall has a measured before and after.

**Read more:** Spark SQL performance tuning https://spark.apache.org/docs/latest/sql-performance-tuning.html | Delta optimizations https://docs.delta.io/latest/optimizations-oss.html

---

## M6. Layout choices

**Objective:** choose a table layout for a workload, with evidence.

**Do, in order:**
1. **Partitioning vs Z-order vs neither:** compare V1, V2, V3 for Q1, Q2 and Q3.
2. **File size:** write the same data at about 32 MB, 128 MB and 512 MB per file (OPTIMIZE target). Record request counts at RustFS, planning time and scan time.
3. **Compression:** compare two Parquet codecs for size and speed.
4. **Format comparison:** Delta versus the plain Parquet copy in landing for one query.
5. **Optional:** read about **liquid clustering** (clustering keys you can change without rewriting) and note when you'd prefer it over partitioning plus Z-order.

**Done when:** you can recommend a layout for three access patterns (point lookup, range scan, full scan) with numbers.

**Read more:** Delta liquid clustering https://docs.delta.io/latest/delta-clustering.html | Spark Parquet options https://spark.apache.org/docs/latest/sql-data-sources-parquet.html

---

## Depth (optional)

- **AQE re-planning:** build a query where one side looks large at plan time but is small after filtering; compare the initial and final adaptive plans.
- **Runtime filters:** check whether your versions insert runtime filters (partition or bloom-filter based) for a selective dimension filter; compare rows scanned.
- **Whole-stage codegen:** find the codegen markers in a plan; disable and compare a CPU-bound query; note the fallback for very wide tables.

**Read more:** Spark SQL performance tuning https://spark.apache.org/docs/latest/sql-performance-tuning.html

---

## M7. Synthesis: the planner cheat sheet

Write a one-page cheat sheet: how to read a Delta scan and a JDBC scan; what to check first when a query is slow; your top blockers; your layout rules. Link each rule to an experiment id.

## Config and property experiments (summary)

| Area | Knobs |
|---|---|
| Spark SQL | cost-based optimizer, join reordering, AQE, codegen |
| JDBC | pushdown options |
| Delta layout | partition columns, Z-order columns, number of statistics columns, target file size |

## Exit criteria for Project 3
1. You can read a plan and predict runtime behavior.
2. Every pruning and pushdown claim has evidence.
3. You have a ranked list of layout choices for your data.
