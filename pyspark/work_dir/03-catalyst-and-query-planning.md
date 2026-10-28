# Project 3: Catalyst and Query Planning (Instacart)

## Goal

Read plans fluently, predict what the optimizer will do, and prove each prediction with evidence from four places: the Spark plan and UI, Iceberg metadata, RustFS request activity, and Postgres. Cover three kinds of scans: Iceberg tables, JDBC tables and raw files in RustFS.

## Work plan (what happens when)

| Phase | Milestone | Starts when | Produces |
|---|---|---|---|
| A | M0 Layout variants and query suite | silver and gold exist | test tables, 12 reference queries |
| B | M1 Plan literacy | M0 done | annotated plans |
| C | M2 to M3 Iceberg pruning and runtime filters | M1 done | pruning evidence |
| D | M4 Statistics and cost-based planning | M2 done | estimate-vs-actual analysis |
| E | M5 JDBC pushdown | Postgres source live | pushdown matrix |
| F | M6 to M7 AQE and codegen | M1 done | adaptive and codegen notes |
| G | M8 Blockers | all above | pitfall catalog |
| H | M9 Layout optimization | M2 done | layout recommendations |
| I | M10 Synthesis | all above | planner cheat sheet |

---

## M0. Layout variants and the reference query suite

**Objective:** a controlled set of tables and queries that differ in only one respect at a time.

**Do, in order:**
1. **Build layout variants** of the lines table (about 34M rows) and the events table:
   - unsorted, partitioned by month;
   - sorted by product id within partitions;
   - z-ordered on department and order hour;
   - unpartitioned but sorted;
   - a raw Parquet copy in landing for comparison with Iceberg scans.
2. **Compact every variant** to the same target file size so file counts don't distort results.
3. **Define the reference queries** and keep them fixed:

| Id | Query | Shows |
|---|---|---|
| Q1 | lines for one product, one month | partition and file pruning |
| Q2 | lines for one department (low-selectivity filter) | limits of pruning |
| Q3 | orders in a date range with an hour filter | partition pruning plus pushed filters |
| Q4 | five-way join (lines, orders, products, aisles, departments) with a selective filter on one dimension | join order, runtime filters |
| Q5 | same as Q4 written in a bad join order | cost-based reordering |
| Q6 | count, min and max over the whole lines table | metadata-only answers |
| Q7 | per-user window over order history | exchange and sort |
| Q8 | product pair counting on a small sample | explosion and plan size |
| Q9 | price lookup (range join) | non-equi join planning |
| Q10 | filter and aggregate on a live Postgres table | JDBC pushdown |
| Q11 | the same as Q4 but with a Python UDF in a filter | blockers |
| Q12 | a wide select (many columns) vs a narrow select | column pruning and bytes read |

4. **Record the baseline** (time, bytes read, files scanned, plan fingerprint) for each query on each relevant variant.

**Done when:** all variants exist and are compacted; the suite runs and is logged.

---

## M1. Plan literacy

**Objective:** read any plan top-down and explain it.

**Do, in order:**
1. For each of Q1 to Q4, **write a prediction first**: scan type, pushed filters, join types, exchanges. Then read the parsed, analyzed, optimized and physical plans and compare.
2. Find these rules in action: constant folding, column pruning, predicate pushdown through a join, limit pushdown.
3. Learn every field on a scan node: what was pushed, what was read, what remains for Spark to filter.
4. **Turn off one optimizer rule** on a tiny sample and watch the plan change; turn it back on.
5. Do all of this for the three scan types and write a paragraph on how each looks different in the plan.
6. Learn plan formats: formatted, cost, codegen, and the SQL tab graph.

**Record:** prediction accuracy per query; annotated plans.

**Traps:** confusing the initial adaptive plan with the final one; assuming a filter shown in the plan was actually applied at the source.

**Done when:** your predictions match for at least 80% of plan features.

---

## M2. Iceberg pruning: the metadata is the index

**Objective:** prove how many files and bytes each query skipped and why.

**Do, in order:**
1. Run Q1 and Q3 on the partitioned variant. Confirm that a filter on the **timestamp column** prunes partitions without referencing any partition column (hidden partitioning).
2. After partition pruning, test **file-level skipping** from per-file min and max values: compare the unsorted, sorted and z-ordered variants for an equality filter and for a range filter.
3. **Prove** each claim with the metadata tables: number of files scanned versus total, bytes scanned versus total.
4. Compare **planning time** and execution time in the SQL tab. See what dominates when the table has many manifests (create a table with many small commits to test).
5. Run Q6 and check whether count, min and max can be answered from metadata alone in your versions.
6. Run Q2 (low-selectivity filter) and explain why pruning helps little.

**Options to explore:**

| Variable | Values | Watch |
|---|---|---|
| Partition transform | month, day, bucket, none | files scanned |
| Sort within files | none, by product, z-order | files skipped by min/max |
| File size | small, target, large | planning time, request count on RustFS |
| Filter shape | equality, range, IN list, OR | what gets pruned |

**Done when:** for each query you can state files skipped, which metadata made that possible, and how to improve it.

---

## M3. Runtime filtering

**Objective:** understand filters that appear only at runtime.

**Do, in order:**
1. Run Q4 where a small dimension has a **selective filter** (for example one department) joined to the big lines table.
2. Look for runtime partition pruning and, where supported, bloom-filter-based runtime filters (check defaults and support in your versions).
3. Toggle each setting; record rows scanned on the fact side and time.
4. Find when the filter is **not** inserted: non-selective filter, large build side, unsupported join type, non-partitioned fact table.
5. Compare with a manual rewrite (filtering the fact table explicitly with a subquery).

**Done when:** you can describe, for your tables, the conditions that trigger each runtime filter.

---

## M4. Statistics and cost-based planning

**Objective:** see how estimates drive join order, and where they fail.

**Do, in order:**
1. Run Q5 (bad order) with cost-based optimization **off**; record join order and time.
2. Collect table and column statistics; enable the cost-based optimizer and join reordering; re-run.
3. Read the cost plan: estimated sizes and row counts per operator.
4. **Compare estimates with actuals** in the SQL tab: find the worst misestimate and explain it (correlated columns, skewed distribution, derived expressions, stale stats).
5. Test histograms on a skewed column (department) and see whether selectivity estimates improve.
6. Find out which statistics Iceberg and Spark actually use in your versions, and what must be run to produce them.
7. Change the data (add a day) and see how stale stats degrade the plan.

**Options to explore:** cost-based on/off; join reordering on/off; with and without column stats; with histograms; stale vs refreshed stats.

**Done when:** you can say when cost-based planning helped, when it hurt, and what upkeep it needs.

---

## M5. JDBC pushdown and Postgres-side truth

**Objective:** know exactly what Postgres executes for a Spark query.

**Do, in order:** for each case below, capture three views of the same work: the Spark plan, the SQL text Postgres received, and Postgres's own execution plan.

| Case | What to determine |
|---|---|
| Equality or range filter on an indexed column | pushed? index used? |
| Same filter on the unindexed timestamp | sequential scan; cost at M |
| Aggregate over the table | pushed or pulled across? |
| Limit and offset | pushed? ordering issues |
| Sampling | pushed? |
| Filter with a Spark-only function or UDF | why pushdown fails |
| Cast on a filtered column | blocks index use? |
| Partitioned read with explicit predicates | does each partition hit an index? |
| Reading a view or arbitrary query | how aliasing changes planning |

Toggle each pushdown option and record rows transferred and time. Write a **rule of thumb**: when should logic run in Postgres, and when must it run in Spark?

**Traps:** a filter shown as pushed but evaluated again in Spark; Postgres plans changing with stale statistics.

**Done when:** the matrix is complete with evidence for each row.

---

## M6. AQE re-planning

**Objective:** see how runtime statistics change the plan.

**Do, in order:**
1. Build a query where one side looks large at plan time but is small after filtering.
2. Compare the initial and final adaptive plans in the SQL tab.
3. Record which AQE features fired: partition coalescing, local shuffle reader, skew handling, join switch.
4. Disable each feature in turn and measure the difference.
5. Find a case where AQE **doesn't** help.

**Done when:** you can predict which AQE features will fire for a given query.

---

## M7. Whole-stage code generation

**Objective:** know what codegen gives and when it's skipped.

**Do, in order:**
1. Find the codegen stage markers in a plain filter, project and aggregate.
2. Disable whole-stage codegen and compare a CPU-bound query.
3. Build a table with 200 or more columns and see whether codegen falls back; read what the plan shows.
4. Note operators that break codegen stages.

**Done when:** you can explain the speed difference and the fallback rule.

---

## M8. Optimizer blockers (pitfall catalog)

**Objective:** a list of things that silently defeat optimization, each with a fix.

**Do, in order:** for each pitfall, record the before and after plans and the speedup.

| Pitfall | Fix |
|---|---|
| Python UDF inside a filter (Q11) | rewrite with built-in functions |
| Expression applied to the filtered column (casts, formatting) | rewrite the predicate to be sargable |
| OR across partition and non-partition columns | split and union |
| Non-deterministic function in a filter | move after the join |
| Cached table hides the scan | cache only the narrowed data |
| Wide select into a wide join | project early |
| Many small files or manifests | compact; rewrite manifests |
| Self-join of lines for co-occurrence | prune to frequent products and cap baskets first |
| Filter on an unindexed Postgres column | add the index or move the work to the lake |
| Join on a normalized product name | normalize once in silver |
| Range join to the price book | expand to daily rows and equi-join |

**Done when:** every pitfall has a measured before and after.

---

## M9. Layout optimization

**Objective:** choose layout for a workload with evidence.

**Do, in order:**
1. **Sort order:** compare sorting lines by product versus by order date versus z-order on two columns, for Q1, Q2, Q3 and Q12.
2. **Parquet settings:** row-group size, page size, compression codec, dictionary encoding; test column-level bloom filters on a high-cardinality lookup.
3. **Format choice:** Iceberg/Parquet versus ORC versus the raw landing Parquet for one table.
4. **File size and count:** compare 32 MB, 128 MB, 512 MB targets on request counts at RustFS and on planning time.
5. **Compaction strategy:** which strategy gives the best query speed per rewrite cost.
6. Capture request count and throughput at RustFS for each layout.

**Done when:** you can recommend a layout for three access patterns (point lookup, range scan, full scan) with numbers.

---

## M10. Synthesis: the planner cheat sheet

Write a one-page cheat sheet: how to read each scan type; what to check first when a query is slow; the top ten blockers; the top five layout rules. Link each rule to an experiment id.

## Config and property experiments (summary)

| Area | Knobs |
|---|---|
| Spark SQL | cost-based optimizer, join reordering, histograms, dynamic pruning, runtime filters, codegen, AQE |
| JDBC | pushdown options per source |
| Iceberg read | split size, vectorization, planning properties |
| Iceberg write | sort order, row-group size, bloom filters, compression |

## Stretch goals
- Write a plan-report helper summarizing scans, pushed filters, join types and exchanges for any query.
- Produce a three-way comparison (Spark plan, SQL received, Postgres execution plan) for each JDBC case.
- Compare metadata-only query answering across two Iceberg versions.

## Exit criteria for Project 3
1. You can read a plan and predict runtime behavior.
2. Every pruning and pushdown claim has evidence.
3. You have a ranked list of layout choices for your data.
