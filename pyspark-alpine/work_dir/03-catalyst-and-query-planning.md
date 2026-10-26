# Project 3: Catalyst and Query Planning

## Goal

Become fluent at reading Spark plans and predicting what the optimizer will do, across **three kinds of scan**: Iceberg tables, JDBC tables, and raw files in RustFS. Prove every prediction with measurements from Spark, Iceberg metadata, RustFS and Postgres.

## Concepts covered

Logical vs physical plans, rule-based optimization, column pruning, predicate pushdown, Iceberg hidden partitioning and manifest-level pruning, runtime filters, statistics and the cost-based optimizer, JDBC pushdown, AQE re-planning, whole-stage codegen, planning cost from metadata, table layout (sort order, z-order, Parquet settings).

## Data

The Instacart silver and gold tables from Project 1 (orders, order lines, products, aisles, departments, users, stores, price book, payments), the clickstream events table, the live Postgres dimensions, and raw Parquet extracts in the landing bucket for comparison. Make sure at least one large Iceberg table (for example `order_items`, about 34M rows, or the events table) exists in three variants: unsorted, sorted by a filter column (such as product id or order date), and z-ordered on two columns (such as department and order hour).

## Milestones

### M1. Plan literacy

- For each query, **write down your prediction first**, then compare it with the parsed, analyzed, optimized and physical plans.
- Find these in plans: constant folding, column pruning, predicate pushdown through a join, limit pushdown.
- Learn every field on a scan node: what was pushed, what was read, what was left for Spark to filter.
- Turn off a single optimizer rule on a tiny sample and see the plan change. Don't try this at scale.
- Do this for three scan types and write one paragraph on how each looks different in the plan: Iceberg, JDBC, raw Parquet in RustFS.

### M2. Iceberg pruning (the metadata is the index)

- Filter on a timestamp column in a table partitioned by a day transform of that column. Confirm partitions are pruned **without referencing a partition column** (hidden partitioning).
- After partition pruning, test file-level skipping from per-file min/max bounds: compare the unsorted, sorted and z-ordered variants for an equality filter and a range filter.
- Prove your claims with the metadata tables: files scanned vs total, bytes scanned vs total, from partitions and files metadata.
- Look at planning time vs execution time in the SQL tab. What dominates for a table with thousands of manifests?
- Check whether simple aggregates (count, min, max) can be answered from metadata alone in your Iceberg/Spark version.

### M3. Runtime filtering

- Query a big Iceberg fact table joined to a small, selective dimension. Look for runtime partition pruning and, where supported, bloom-filter-based runtime filters (check defaults and support in your versions).
- Measure rows scanned and time with each setting toggled.
- Identify when the filter is **not** inserted (non-selective filter, large build side, unsupported join type).

### M4. Statistics and cost-based planning

- Write a five-way join in a deliberately poor order. Record the join order and runtime with cost-based optimization off.
- Collect statistics, enable the cost-based optimizer and join reordering, then compare plans. Look at estimated sizes and row counts in the plan output.
- Compare estimates with actuals per operator in the SQL tab. Where is the estimate worst, and why (correlated columns, skew, UDFs)?
- Find out which statistics your Iceberg version actually uses (table-level, column-level, NDV sketches) and what must be run to produce them.

### M5. JDBC pushdown and Postgres-side truth

For each Spark query on a Postgres table, capture three views of the same work: the Spark plan, the SQL text Postgres actually received, and Postgres's own execution plan for it.

| Case | What to determine |
|---|---|
| Simple equality or range filter | is it pushed? does Postgres use an index? |
| Aggregate over the whole table | pushed or pulled across? |
| Limit and offset | pushed? what does Postgres do about ordering? |
| Sampling | pushed? |
| Filter with a Spark-only function or UDF | why does pushdown fail? |
| Cast on the filtered column | does it block index use in Postgres? |
| Partitioned read with explicit predicates | does every partition predicate hit an index? |
| Reading a view or an arbitrary query | how do subquery aliases change planning? |

Toggle each pushdown option and record rows transferred and time. Write the rule of thumb: when should logic run in Postgres, and when must it run in Spark?

### M6. AQE re-planning

- Run a join where one side looks large at plan time but is small after filtering. Find the runtime switch to a broadcast join in the final plan.
- Compare the initial and final adaptive plans. Record which AQE features fired: partition coalescing, local shuffle reader, skew handling, join switch.

### M7. Whole-stage codegen

- Find the codegen stage markers in a plain filter-project-aggregate. Disable whole-stage codegen and compare a CPU-bound query.
- Build a table with 200+ columns and see whether codegen falls back. What does the plan show?

### M8. Optimizer blockers and layout

Pitfall gallery (before and after plans for each):

| Pitfall | Fix |
|---|---|
| Python UDF inside a filter | rewrite with built-ins |
| Non-sargable expressions on the partition source column | rewrite the predicate |
| OR across partition and non-partition columns | split and union |
| Non-deterministic function in a filter | move after the join |
| Cached table hides the scan | cache the narrowed data only |
| Wide select into a wide join | project early |
| Many small files or many manifests | compact and rewrite manifests |
| Self-join of order lines on order id for co-occurrence (pair explosion) | prune to frequent products and cap huge baskets before pairing |
| Filter on the order timestamp in Postgres without an index | add the index or push the work into the lake |
| Join on a normalized product name (expression on the join key) | normalize once in silver and join on the stored key |
| Range join of lines to the effective-dated price book | expand prices to daily rows and equi-join |

Layout experiments, each measured by bytes read and time:

- **Sort order** on the table vs z-order compaction on two columns.
- **Parquet settings:** row group size, page size, compression codec, and column-level bloom filters for high-cardinality lookups.
- **Format choice:** compare Iceberg/Parquet with ORC for one table, and the raw landing Parquet.

## Config and property experiments

| Area | Knobs |
|---|---|
| Spark SQL | cost-based optimizer, join reordering, histograms, dynamic pruning, runtime filters, codegen, AQE |
| JDBC | pushdown options per source |
| Iceberg read | split size, vectorization, planning properties |
| Iceberg write | sort order, row group size, bloom filters, compression |

## Stretch goals

- Write a plan report helper that summarizes scans, pushed filters, join types and exchanges for any query.
- Use `EXPLAIN ANALYZE` in Postgres to produce a three-way comparison table for the JDBC cases.
- Test aggregate pushdown and metadata-only queries across two Iceberg versions if you have them.

## Done when you can answer

1. Why does Iceberg need no partition column in the query to prune partitions?
2. Which metadata proves that a filter skipped files, and where do you read it?
3. What stops a filter from reaching Postgres, and what does that cost?
4. Why is cost-based optimization off by default, and what does it need?
5. How can the optimized plan differ from the final AQE plan?
