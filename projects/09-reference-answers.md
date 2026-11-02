# Reference Answers

Three things live here: **worked experiments** that show what good evidence looks like, **expected plan fingerprints** for the reference queries, and a **magnitude calibration** method so you can tell "weird number" from "learned something".

> All numbers in the worked examples are **illustrative**, not measurements. They show the shape of a good log entry. Your numbers will differ; what matters is the structure and the reasoning.

---

## 1. Worked experiment A: a skew fix (Project 2, M5)

### Experiment-log entry

| Field | Entry |
|---|---|
| Exp ID | E-031 |
| Project / milestone | P2 / M5 Skew-fix ladder |
| Prediction (before running) | The reorder-rate join on product id is skewed by the staple products. AQE skew handling will cut the max task time by about half because it splits the hot partition. Resource stressed: shuffle and memory of a single task. |
| Baseline | 3 runs (second and third recorded): 214 s and 209 s; max task 160 s, median 11 s (ratio about 14); spill on one task, 3.1 GB to disk |
| Setting changed | AQE skew join thresholds lowered (partition-size threshold to 32 MB, factor to 3); nothing else |
| Result | 118 s and 121 s; max task 38 s, median 12 s (ratio about 3); spill gone; shuffle read the same |
| Conclusion | AQE split the hot partition into several tasks. Ratio dropped from about 14 to about 3. It did not reduce total shuffle volume. |
| Prediction correct? | Partly: improvement was a little less than half, but the mechanism matched |
| Mistaken model | I assumed "half" without computing how many sub-tasks the hot partition would split into; the speed-up is bounded by the slowest remaining task |
| Follow-up | Repeat for the aggregation by product (expect no effect); then salting |

### Four-views evidence

| View | What was captured |
|---|---|
| Spark UI | Stages tab for the join stage before and after: task time min / median / max and the spill column; SQL tab plan showing the skew marker on the sort-merge join after the change |
| Iceberg metadata | files table: the lines table has the same number of files in both runs (confirms the input didn't change); snapshot id unchanged |
| RustFS | request counts and bytes read equal across runs (confirms no I/O difference) |
| Postgres | idle during both runs (no federation involved) |

### Why this is a good entry
- The prediction names the resource and a number **before** the run.
- One setting changed.
- The baseline has repeated runs and the tail metric (max/median), not only total time.
- Evidence comes from more than one view, including a control (same input snapshot).
- The conclusion states a mechanism and a limit (volume unchanged).
- The mistaken model is one sentence.

---

## 2. Worked experiment B: JDBC partition count (Project 1, M1)

| Field | Entry |
|---|---|
| Exp ID | E-009 |
| Prediction | Going from 4 to 16 partitions will give about 3x speed-up until Postgres becomes the limit, at about 8 connections. Resource: the source database. |
| Baseline | 4 partitions: 148 s, 4 Postgres connections, equal rows per task not guaranteed |
| Setting changed | partition count only (4, 8, 16, 32 in turn; id-range strategy fixed) |
| Result | 8: 85 s; 16: 83 s; 32: 96 s. Slowest task per run: 4 partitions 128 s, 8 partitions 61 s, 16 partitions 58 s |
| Conclusion | Speed-up stopped near 8 partitions; the slowest task stopped shrinking because large baskets make some id ranges hold far more rows. Beyond 16, extra queries add overhead. |
| Evidence | Spark Stages tab (task durations and input records per task); Postgres connection count and the queries Spark sent; RustFS write throughput flat (not the limit) |
| Mistaken model | I assumed equal id ranges meant equal work |
| Next | Try hash-based predicates for balance |

---

## 3. What counts as evidence (checklist)

An experiment-log entry is evidence only if it has:

- [ ] a prediction written before the run
- [ ] one setting changed from a recorded baseline
- [ ] at least two runs, with the second recorded
- [ ] units on every number (seconds, bytes, rows, tasks)
- [ ] at least two of the four views agreeing
- [ ] a control where relevant (same input snapshot, same session)
- [ ] a one-sentence conclusion that names a mechanism
- [ ] the mistaken model, if the prediction was wrong

---

## 4. Expected plan fingerprints

Use these as **reference expectations** to compare against your plans. They depend on your table sizes and settings; where the outcome depends on version or statistics, it is marked "check".

### Project 2 benchmark suite (J1 to J8)

| Query | Expected join types | Expected exchanges | Expected scan behavior | Watch for |
|---|---|---|---|---|
| J1 reorder rate by department and aisle | broadcast hash joins for products, aisles, departments; no sort-merge joins | one hash exchange for the final aggregation (after a partial aggregation) | lines scan reads only the product and flag columns | staple-product skew in the aggregation |
| J2 orders by store and day | broadcast hash joins for users and stores (small after projection) | one exchange for the aggregation | date filter becomes a partition filter if orders is partitioned | the popular store as a hot key |
| J3 revenue per store per day | sort-merge join for lines with orders (orders too large for the default broadcast limit); price book join depends on how you wrote it | exchanges on both sides of the big join (by order id), plus the aggregation | column pruning on lines | range-join row multiplication if price periods overlap |
| J4 top 10 products per user | sort-merge or broadcast per sizes for the order join | exchange for the user and product aggregation, then another by user for the window | | skew on heavy users (the bulk buyer) |
| J5 co-occurrence | sort-merge join of lines with itself on order id | one side shown as a reused exchange; another exchange for pair aggregation | | pair explosion; very large shuffle |
| J6 price lookup | equi-join on product with a range filter, or a nested-loop style join | exchange by product | | output rows exceeding input rows |
| J7 payments reconciliation | aggregate lines to order level first, then join to payments (hash or sort-merge by size) | exchange for the aggregation; exchange(s) for the join | | many-to-many mistakes if you join before aggregating |
| J8 lake lines with live Postgres | initial plan may show sort-merge because JDBC scans carry no size statistics; the final adaptive plan may switch to broadcast (check) | exchange(s) depend on the final join | JDBC scan lists pushed columns and filters | repeated Postgres load on every action |

### Project 3 reference queries (Q1 to Q12)

| Query | Expected scan and plan |
|---|---|
| Q1 point lookup (one product, one month) | partition filter selects one month; product filter pushed; the sorted variant skips most files while the unsorted variant scans most files in that month |
| Q2 low-selectivity filter (one department) | little pruning; a large share of files scanned; filter pushed but not very effective |
| Q3 date range plus hour | partition filters for the range; hour filter pushed; file skipping depends on sort order |
| Q4 five-way join with a selective dimension | broadcast hash joins for the small dimensions; a runtime filter or pruning may appear on the fact scan (check your versions) |
| Q5 same join written in a bad order | with cost-based planning off, the written order is kept; with statistics and reordering on, the order may change |
| Q6 count, min and max over the table | may be answered from metadata or by a cheap scan with partial aggregation (check your versions) |
| Q7 window by user | exchange by user, sort, window operator |
| Q8 sampled pairs | smaller version of J5 |
| Q9 price range join | non-equi condition; nested-loop or sort-merge with a range filter |
| Q10 JDBC filter and aggregate | JDBC scan with pushed filter; aggregate pushdown may appear (check) |
| Q11 UDF in a filter | Python evaluation node above the scan; the filter is not pushed into the scan |
| Q12 wide vs narrow select | read schema differs; bytes read differ proportionally to selected columns |

### Plan reporter (what it should output)
A one-screen summary per query: scan nodes with pushed and remaining filters, join types in order, exchange count, whether the final plan differs from the initial plan, and files scanned from Iceberg metrics. You write it in Project 3; build one that prints these six fields and use this table as your oracle.

---

## 5. Magnitude calibration (are my numbers sane?)

### Method (use this first)
1. From the hello world, record: JDBC rows per second per connection, Parquet bytes per row, Iceberg write seconds per million rows, fixed job overhead.
2. For any big run, **extrapolate**: expected time is roughly rows divided by (your per-connection rate times connections), or bytes divided by your measured scan rate.
3. If the actual is more than **3x** off, investigate; if more than **10x** off, stop and fix the environment before tuning.

### Rough bands to sanity-check your setup
These are order-of-magnitude assumptions for this style of platform on a single laptop-class host, **not benchmarks**; replace them with your hello-world numbers.

| Quantity | Rough band | Notes |
|---|---|---|
| Raw Instacart CSV total | about 0.6 to 0.7 GB | confirm in your download |
| Lines in Parquet (with a few extra columns, compressed) | about 0.3 to 1 GB for 34M rows | depends on added columns and codec |
| Lines in Postgres with primary and foreign-key indexes | about 3 to 6 GB | table plus indexes |
| JDBC read, narrow rows, per connection | about 50k to 300k rows per second | 34M rows over 8 connections: minutes, not seconds |
| Fixed Spark job overhead | about 0.5 to 3 seconds | for a trivial query |
| Co-occurrence unordered pair count at M | on the order of a few hundred million | estimated from an assumed mean basket of about 10 items and a wide spread; compute your own from the basket-size distribution |

### Listing-cost cliff for streaming (Project 5 M1)
- S3-style listing returns up to **1,000 keys per request** and pages are fetched one after another.
- 10k objects need about 10 requests; 100k need about 100; 1M need about 1,000.
- If a request takes tens of milliseconds, listing grows from well under a second to tens of seconds across that range. The cliff appears when listing time approaches your trigger interval.
- Measure the per-request time on RustFS in your hello world, then compute where your cliff is. Mitigate by archiving or deleting processed files, or by partitioning the input prefix.

---

## 6. How to use this file

- Before an experiment: read the matching fingerprint and write your prediction.
- After: compare, and log any difference with its explanation (new mental model or environment issue).
- When you're stuck on whether a number is reasonable: use section 5 first, then the playbook (`08-debugging-playbook.md`).
