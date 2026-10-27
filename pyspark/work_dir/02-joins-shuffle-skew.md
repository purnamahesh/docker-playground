# Project 2: Joins, Shuffles and Skew (Instacart)

## Goal

Run the same business questions through every join strategy over **Iceberg tables and live Postgres tables**, use the dataset's natural and injected skew, fix it four ways, and measure each fix.

## Concepts covered

Broadcast, sort-merge and shuffle-hash joins, join hints, AQE (coalescing, skew join, runtime broadcast), salting, hot-key isolation, pre-aggregation, pair-explosion self-joins, range joins, Iceberg write distribution modes, storage-partitioned joins, skew inside MERGE INTO, federated joins with JDBC, memory pressure and spill.

## Data

From Project 1 (M or L tier): silver `orders`, `order_items` (about 34M rows), `products`, `aisles`, `departments`, the synthetic `users`, `stores`, `price_book` and `payments`, the clickstream events table, and the small **campaign calendar**.

Skew keys to profile first (share of rows held by the top 1 / 10 / 100 keys):

| Key | Kind | Why it matters |
|---|---|---|
| product id | natural | staples such as bananas dominate lines |
| aisle and department | natural | produce and dairy hold a large share; very few distinct values |
| order hour and weekday | natural | low-cardinality keys with uneven volume |
| basket size per order | natural | a long tail of very large orders |
| bulk buyer user id | injected | about 5% of orders on one key |
| promotional product id | injected | a hot key in line and event joins |
| popular store id | injected | a hot dimension key |

Large-large pairs: `order_items` x `orders` (34M x 3.4M) and `order_items` x `order_items` (the co-occurrence self-join).

## Milestones

### M1. Baseline plans

1. Join lines -> orders -> products -> aisles -> departments over Iceberg. Read the formatted plan and record the join type per join.
2. Disable auto-broadcast and AQE to force sort-merge joins. Count Exchange and Sort operators.
3. Re-enable AQE and look for runtime plan changes in the final adaptive plan.
4. Note how Iceberg scans appear in the plan and which filters are pushed into them.

### M2. Join strategies by hint

| Hint | Test | Record |
|---|---|---|
| Broadcast | lines x products (about 50k rows) and x aisles/departments | time, broadcast size, driver memory |
| Merge | same | shuffle bytes, sort time |
| Shuffle-hash | same | memory per task, spill |
| Shuffle-replicate-nested-loop | tiny x tiny only | why it exists |

Find the largest table that broadcasts safely given driver memory and the broadcast timeout. Don't test failure modes at large scale.

### M3. Federated joins (Iceberg x Postgres)

- Join a large Iceberg fact table with a live Postgres dimension (`products`, `price_book`, `stores`). Record what Spark pushes down and what it pulls across.
- Show the cost of **re-reading Postgres every time** an action runs. Compare: broadcast the JDBC read, cache it, or snapshot it to Iceberg first.
- Observe the load on Postgres (connections, queries). Decide when federation is acceptable and when snapshotting is mandatory.
- Compare "join in Postgres through a pushed-down query" vs "join in Spark" for a selective and an unselective query.

### M4. Reproduce and diagnose skew

- Aggregate and join on product id, department, hour of day and the bulk buyer id. In the Stages tab, record task duration (min/median/max), shuffle read of the slowest task vs the median, and spill.
- Show skew in three places: a join, a window function over user history, and a **MERGE INTO** where the source has many rows for the hot key.
- Show a fourth kind: JDBC partitions with uneven row counts per id range (from Project 1).
- Contrast **high-cardinality skew** (a hot product) with **low-cardinality skew** (21 departments across 200 shuffle partitions): what happens to the idle partitions?

### M5. The skew-fix ladder

Implement each fix on the same query (for example, reorder rate per product joined back to the lines) and compare:

| Fix | Notes |
|---|---|
| A. AQE skew join | lower the size thresholds so it triggers on lab-sized data; find the skew markers in the plan; explain what was split |
| B. Salting | salt the big side, replicate the small side; sweep the salt factor; for aggregations use two-phase |
| C. Hot-key isolation | broadcast-join the hot products, shuffle-join the rest, union |
| D. Pre-aggregation | aggregate to one row per product and day before the join; measure shuffle volume |

Fill a table with time, max/median task ratio, shuffle bytes and implementation complexity. State which fix you would choose for a join, an aggregation and a MERGE, and why.

### M6. Pair explosion (the co-occurrence trap)

- Self-join `order_items` on `order_id` to count products bought together. Predict the output row count from the basket-size distribution, then measure it.
- Explain why a few very large baskets dominate the pair count, and what that does to shuffle size, spill and task times.
- Fix it several ways and compare: cap or sample large baskets, restrict to frequent products first (prune before pairing), use a canonical pair order to halve the work, and pre-aggregate per product pair.
- State which fix changes the **answer** (sampling) and which only changes the **plan**.

### M7. Range joins on prices and promotions

- Price each order line from the effective-dated `price_book` (`order_ts` between valid_from and valid_to). Read the plan and record the join type.
- Compare approaches: raw range join, expanded daily price table with an equi-join, and bucketing time into weeks then filtering.
- Join orders to the campaign calendar the same way.
- Which approach scales to the L tier, and why?

### M8. Partition sizing and write distribution

- Sweep shuffle partition counts with AQE off and plot time vs partitions.
- Turn AQE on and vary the advisory partition size. Find the coalesced-read nodes in the plan.
- On the Iceberg write path, compare the three **write distribution modes** (none, hash, range) for the same insert. Record shuffle cost, file counts per partition and file sizes.
- Explain how shuffle partitions and distribution mode together determine output file counts.

### M9. Storage-partitioned joins

- Create two Iceberg tables bucketed identically on the join key (orders and lines on `order_id`) with the same bucket count.
- Enable the Spark and Iceberg settings for **storage-partitioned joins** (check the Iceberg docs for your version) and confirm there is **no Exchange** on either side.
- Break it on purpose: different bucket counts, an extra partition column, a different join key. Which conditions still allow the optimization?
- Compare with a regular shuffle join, and with Hive-style bucketing from older Spark.
- State the trade-off: bucketing decisions are made at table design time.

### M10. Memory pressure and spill

- Run a large sort/aggregation/merge with small executors and few shuffle partitions. Record spill (memory and disk).
- Fix it three ways: more partitions, fewer cores per executor, a larger execution memory share. Which helps most?
- Repeat for a **MERGE INTO** on a large target table. Compare copy-on-write and merge-on-read memory behavior.

### M11. Anti-pattern gallery

On small samples, read the plans for:

- non-equi joins and joins on expressions (for example, normalized product names)
- distinct before join
- a Cartesian product created by a missing condition
- a window over a whole user history that could have been a bounded window

## Config experiments

| Knob | Values | Watch |
|---|---|---|
| auto-broadcast threshold | disabled, 10 MB, 100 MB | join type |
| AQE (overall, coalesce, skew, local reader) | on/off, thresholds | plan changes |
| shuffle partitions | 8 to 1000 | task overhead vs spill |
| advisory partition size | 16 MB to 256 MB | coalesced partitions |
| executor cores | 1, 2 | memory per task |
| Iceberg distribution mode | none, hash, range | shuffle, file counts |
| bucket count | 8, 16, 32 | storage-partitioned join eligibility |

## Stretch goals

- Compare skew handling in the RDD API (custom partitioner).
- Build a skew-report helper: top keys, share of rows, recommended technique.
- Measure how the bulk buyer affects the Iceberg write distribution.

## Done when you can answer

1. Why does one hot key slow the whole stage while seven cores sit idle?
2. What does AQE skew join split, and why doesn't it help a skewed aggregation?
3. When does salting beat hot-key isolation?
4. Why does the co-occurrence self-join explode, and which fixes change the answer?
5. What conditions are required for a storage-partitioned join?
