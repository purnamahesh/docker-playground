# Project 2: Joins, Shuffles and Skew

## Goal

Run the same business questions through every join strategy over **Iceberg tables and live Postgres tables**, create real skew from your hot customer and hot product, fix it four different ways, and measure each fix.

## Concepts covered

Broadcast, sort-merge and shuffle-hash joins, join hints, AQE (coalescing, skew join, runtime broadcast), salting, hot-key isolation, pre-aggregation, Iceberg write distribution modes, storage-partitioned joins, skew inside MERGE INTO, federated joins with JDBC, memory pressure and spill.

## Data

From Project 1: silver `orders`, `order_items`, `payments`, `customers`, `products`, and the clickstream events table. Add:

- A **large-large pair** with unique keys: `order_items` x `payments` (or a refunds table you derive).
- A **small, volatile dimension** that stays in Postgres for the federated joins.
- A **campaign calendar** table with start/end timestamps for the range-join work.

Always begin by profiling the key distributions: top customers by order count, top products by item count, share of rows held by the top 1 / 10 / 100 keys.

## Milestones

### M1. Baseline plans

1. Join items -> orders -> customers -> products over Iceberg. Read the formatted plan and record the join type per join.
2. Disable auto-broadcast and AQE to force sort-merge joins. Count Exchange and Sort operators.
3. Re-enable AQE and look for runtime plan changes in the final adaptive plan.
4. Note how Iceberg scans appear in the plan, and which filters are pushed into them.

### M2. Join strategies by hint

| Hint | Test | Record |
|---|---|---|
| Broadcast | items x products (100k rows) | time, broadcast size, driver memory |
| Merge | same | shuffle bytes, sort time |
| Shuffle-hash | same | memory per task, spill |
| Shuffle-replicate-nested-loop | tiny x tiny only | why it exists |

Find the largest table that broadcasts safely given driver memory and the broadcast timeout. Do not test failure modes at large scale.

### M3. Federated joins (Iceberg x Postgres)

- Join a large Iceberg fact table with a live Postgres dimension. Record what Spark pushes down to Postgres and what it pulls across.
- Show the cost of **re-reading Postgres every time** an action runs (no caching). Compare: broadcast the JDBC read, cache it, or snapshot it to Iceberg first.
- Observe the load on Postgres (connections and queries). Decide when federation is acceptable and when snapshotting is mandatory.
- Compare "join in Postgres through a pushed-down query" vs "join in Spark" for a selective query and an unselective one.

### M4. Reproduce and diagnose skew

- Aggregate and join on `customer_id`. In the Stages tab, record task duration (min/median/max), shuffle read of the slowest task vs the median, and spill.
- Show skew in three places: a join, a window function, and a **MERGE INTO** whose source contains many rows for the hot customer.
- Show a fourth kind: JDBC partitions with uneven ranges (from Project 1).

### M5. The skew-fix ladder

Implement each fix for the same query and compare:

| Fix | Notes |
|---|---|
| A. AQE skew join | lower the size thresholds so it triggers on lab-sized data; find the skew markers in the plan; explain what was split |
| B. Salting | salt the big side, replicate the small side; sweep the salt factor; for aggregations use two-phase |
| C. Hot-key isolation | broadcast-join the hot keys, shuffle-join the rest, then union |
| D. Pre-aggregation | reduce rows before the join; measure shuffle volume |

Fill a table with time, max/median task ratio, shuffle bytes and implementation complexity. State which fix you would choose for a join, an aggregation, and a MERGE, and why.

### M6. Partition sizing and write distribution

- Sweep shuffle partition counts with AQE off. Plot time vs partitions.
- Turn AQE on and vary the advisory partition size. Find the coalesced-read nodes in the plan.
- On the Iceberg write path, compare the three **write distribution modes** (none, hash, range) for the same insert. Record shuffle cost, file counts per partition, and file sizes.
- Explain how shuffle partitions and distribution mode together determine output file counts.

### M7. Storage-partitioned joins

- Create two Iceberg tables bucketed identically on the join key, with the same bucket count.
- Enable the Spark and Iceberg settings that allow **storage-partitioned joins** (check the Iceberg docs for your version) and confirm there is **no Exchange** on either side.
- Break it on purpose: different bucket counts, an extra partition column, a different join key. Which conditions still allow the optimization?
- Compare with a regular shuffle join, and with Hive-style bucketing from older Spark.
- State the trade-off: bucketing decisions are made at table design time.

### M8. Memory pressure and spill

- Run a large sort/aggregation/merge with small executors and few shuffle partitions. Record spill (memory and disk).
- Fix it three ways: more partitions, fewer cores per executor, a larger execution memory share. Which helps most?
- Repeat for a **MERGE INTO** on a large target table. Compare copy-on-write and merge-on-read memory behavior.

### M9. Anti-pattern gallery

On small samples, read the plans for:

- range joins (events between campaign start and end)
- non-equi joins and joins on expressions
- distinct before join
- a Cartesian product created by a missing condition

Rewrite the range join as an equi-join by bucketing time, then filter. Compare plans.

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

- Compare with RDD-level skew handling (custom partitioner).
- Build a skew report helper: top keys, share of rows, recommended technique.
- Measure how the **hot customer** affects the Iceberg write distribution.

## Done when you can answer

1. Why does one hot key slow the whole stage while seven cores sit idle?
2. What does AQE skew join split, and why doesn't it help a skewed aggregation?
3. When does salting beat hot-key isolation?
4. What conditions are required for a storage-partitioned join?
5. Why is federated joining risky for the source database?
