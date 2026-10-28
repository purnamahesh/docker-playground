# Project 2: Joins, Shuffles and Skew (Instacart)

## Goal

Understand how Spark executes joins and aggregations over your lakehouse and your live Postgres source: pick join strategies deliberately, diagnose skew from evidence, fix it several ways, and design tables that make joins cheap.

## Work plan (what happens when)

| Phase | Milestone | Starts when | Produces |
|---|---|---|---|
| A | M0 Prepare tables, queries and baselines | Project 1 silver exists (M tier) | join benchmark suite, profile of keys |
| B | M1 to M2 Plans and strategies | M0 done | baseline plans, strategy comparison |
| C | M3 Federated joins | Postgres source is live | federation guidance |
| D | M4 to M5 Skew diagnosis and fixes | strategies understood | skew playbook |
| E | M6 to M7 Pair explosion and range joins | skew playbook | patterns for the two hardest queries |
| F | M8 to M9 Partitioning and storage-partitioned joins | tables can be rebuilt | table-design rules |
| G | M10 to M11 Spill and anti-patterns | all above | failure catalog |
| H | M12 Synthesis | all above | decision tree document |

---

## M0. Prepare tables, the benchmark suite and the key profile

**Objective:** a stable test bed so every result is comparable.

**Do, in order:**
1. **Freeze the data version.** Record the Iceberg snapshot id of every table you'll use (lines, orders, products, aisles, departments, users, stores, price book, payments, events). All experiments must read those snapshots.
2. **Define the benchmark suite** (run each as the baseline):

| Id | Question | Tables | What it stresses |
|---|---|---|---|
| J1 | Reorder rate by department and aisle | lines x products x aisles x departments | small-dimension joins, aggregation |
| J2 | Order volume by store and day | orders x users x stores | star join |
| J3 | Revenue per store per day | lines x orders x price book | big join plus lookup |
| J4 | Each user's top 10 products | lines x orders | window and aggregation by user |
| J5 | Product co-occurrence (pairs) | lines x lines | explosion |
| J6 | Price each line from the effective-dated price book | lines x price book | range join |
| J7 | Reconcile payments with priced lines | payments x lines | large-large, many-to-many risk |
| J8 | Join lake lines to live Postgres products and prices | lines x JDBC tables | federation |

3. **Profile the keys** that matter (the share of rows held by the top 1, 10 and 100 keys): product id, aisle, department, store, user, order hour and weekday, basket size per order. Make a table; it predicts where skew will appear.
4. **Prepare the table variants** you'll need later: lines partitioned by month; lines bucketed on order id (several bucket counts); orders bucketed likewise; a compacted copy of everything, so small-file noise doesn't confuse results.
5. **Record baselines:** run J1 to J8 with defaults, twice each, and log time, shuffle bytes, spill, task counts and plan fingerprint.

**Options to explore:** none yet; this milestone is about control.

**Done when:** every query has a baseline and a recorded plan; the key-skew table exists; table variants are built and compacted.

---

## M1. Reading baseline plans

**Objective:** be able to explain every operator in the join plans.

**Do, in order:**
1. For J1 to J4, read the formatted plan top-down and annotate: scan (what was pushed), join type, exchange (what key, how many partitions), sort, aggregate (partial and final).
2. **Predict then check:** for each join, write down the join type you expect and why (sizes versus the broadcast limit), then compare.
3. **Force the alternative.** Disable auto-broadcast and AQE and re-run to get sort-merge plans. Count exchanges and sorts and compare time.
4. **Re-enable AQE** and find what changed at runtime in the final adaptive plan (join switch, coalesced partitions, local reader).
5. Note how Iceberg scans appear and which filters were pushed into them.

**Record:** per query: join types, number of exchanges, shuffle bytes, partitions before and after coalescing.

**Traps:** reading only the initial plan; AQE changes the plan at runtime, so the final plan is what ran.

**Done when:** you correctly predicted join types for at least 80% of joins and can explain the mismatches.

---

## M2. Join strategies by hint

**Objective:** know when each strategy wins and what it costs.

**Do, in order:**
1. For the product, aisle and department joins (small right sides), force each strategy by hint and compare.
2. Measure **broadcast limits:** find the largest table you can broadcast given driver memory and the broadcast timeout. Note the symptoms near the limit (slow collect, timeouts). Don't push into failure at L scale.
3. Compare **shuffle-hash vs sort-merge** where the build side fits in memory: time, memory per task, spill.
4. Explore the **nested-loop** hint only on tiny inputs, to see what it does.
5. Change the build side by swapping table order or hints and observe.

**Options to explore:**

| Variable | Values | Watch |
|---|---|---|
| Strategy | broadcast / sort-merge / shuffle-hash | time, shuffle bytes, spill, driver memory |
| Table size on the broadcast side | 50k rows, 1M rows, 5M rows | memory, time to collect |
| Auto-broadcast threshold | disabled, 10 MB, 100 MB | what gets chosen automatically |
| Build side choice | each side | memory per task |

**Traps:** broadcasting a table that's small on disk but large in memory; a hint silently ignored when the join type or conditions don't allow it (check the plan).

**Done when:** you have a table giving, for each query, the best strategy and the margin over the default.

---

## M3. Federated joins (lake x Postgres)

**Objective:** decide when joining against the live source is acceptable.

**Do, in order:**
1. Run J8 as a plain join of lake lines against JDBC products and price book.
2. Read the plan: which filters and columns reached Postgres, and what Spark pulled across.
3. Run it twice and see that Postgres is hit **again** each time (no caching).
4. Try the alternatives: cache the JDBC read; broadcast it; snapshot it into Iceberg first and join there; push the join into Postgres through a subquery.
5. Observe Postgres: connections, queries, load, locks.
6. Compare selective and unselective cases (a few products vs all).
7. Write a rule: when is federation fine (small, selective, tolerable staleness) and when must you snapshot?

**Options to explore:**

| Approach | Pro | Con |
|---|---|---|
| Direct join | always fresh | repeated load on Postgres |
| Cached read | one load | stale; memory |
| Snapshot to Iceberg | cheapest repeated use | extra pipeline and delay |
| Join inside Postgres | less data moved | Postgres CPU; limited functions |

**Done when:** you can state the break-even (queries per hour, data volume) for each approach.

---

## M4. Diagnosing skew

**Objective:** recognize skew from the UI in under a minute.

**Do, in order:**
1. Run aggregations and joins on each candidate key: product id, department, aisle, store, hour, and the injected bulk buyer.
2. For each, open the Stages tab and record task duration (min, median, max), shuffle read of the slowest task versus the median, and spill.
3. Show skew in **four places:** a join, a window by user, a MERGE where the source has many rows for one key, and JDBC partitions with uneven row counts.
4. Contrast **high-cardinality skew** (a hot product) with **low-cardinality skew** (21 departments across many partitions): what happens to idle partitions?
5. Build a skew checklist: signals in the UI, signals in the key profile.

**Record:** a table with key, query, max/median ratio, spill, and diagnosis.

**Traps:** skew in the **input** (uneven file splits) is different from skew in the **shuffle key**; salting the wrong one helps nothing.

**Done when:** you can identify the cause from the UI alone for each of the four cases.

---

## M5. The skew-fix ladder

**Objective:** choose the right fix per situation, with evidence.

**Do, in order**, using one skewed query (reorder rate per product joined back to lines) and then repeat for an aggregation and a MERGE:
1. **AQE skew handling:** lower the size thresholds so it triggers on lab data; find the skew markers in the plan; explain what was split. Note where it does **not** apply.
2. **Salting:** salt the large side and replicate the small side; sweep the salt factor; for aggregations use two phases.
3. **Hot-key isolation:** find the top keys; broadcast-join them; shuffle-join the rest; union the results.
4. **Pre-aggregation:** reduce rows before the join (for example to one row per product and day).
5. **Data-model fix:** push the join earlier or avoid it by denormalizing the dimension into the lines table.
6. Compare all fixes in one table.

**Options to explore:**

| Fix | Parameters | Works for | Doesn't help |
|---|---|---|---|
| AQE skew | factor, size threshold | sort-merge joins | skewed aggregations |
| Salting | factor 4, 8, 32 | joins, aggregations | adds complexity, replicates data |
| Hot-key isolation | top K keys | joins with a few hot keys | many moderately hot keys |
| Pre-aggregation | grain | aggregations feeding joins | when you need detail rows |
| Denormalize | which dimension | repeated joins | storage and update cost |

**Record:** time, max/median ratio, shuffle bytes, spill, and implementation complexity per fix.

**Done when:** you can state which fix you'd pick for a join, an aggregation and a MERGE, and give the evidence.

---

## M6. Pair explosion (co-occurrence)

**Objective:** make the most expensive query run, and know what each shortcut changes.

**Do, in order:**
1. **Predict** the pair count from the basket-size distribution (sum over baskets of size times size minus one, divided by two for unordered pairs).
2. **Run on a small sample** to check the prediction, then measure on M if feasible.
3. Explain why a few huge baskets dominate the pair count and what that does to shuffle size, spill and task durations.
4. **Try fixes** in turn:
   - cap or sample huge baskets;
   - restrict to frequent products before pairing (prune first);
   - use a canonical pair order to halve the work;
   - pre-aggregate per pair in partial steps;
   - split by department and combine.
5. Mark each fix as **changes the answer** (sampling, caps) or **changes only the plan**.

**Done when:** you have the same top pairs from at least two methods, with a documented difference in cost and accuracy.

---

## M7. Range joins on prices and promotions

**Objective:** replace an expensive non-equi join with something that scales.

**Do, in order:**
1. Price each line from the effective-dated price book (order time between valid-from and valid-to) and record the join type (likely nested-loop or an inefficient sort-merge variant).
2. Try the alternatives: expand the price book to one row per product per day and use an equi-join; bucket time into weeks and join on bucket then filter; as-of style lookup using sorting; broadcast the price book if it fits.
3. Repeat with the campaign calendar.
4. Test at M and see which approach survives at L.

**Options to explore:** raw range join; expanded daily table (storage cost); time-bucket then filter; sorted as-of; broadcast.

**Traps:** overlapping validity periods multiplying rows; open-ended intervals.

**Done when:** one approach runs in reasonable time at the L tier, with a verified identical result to the raw range join on a sample.

---

## M8. Partition sizing and write distribution

**Objective:** control task counts and output file counts.

**Do, in order:**
1. With AQE **off**, sweep `spark.sql.shuffle.partitions` (8 to 1000) on J1, J3 and J4 and plot time versus partitions.
2. With AQE **on**, vary the advisory partition size and find the coalesced-read nodes; compare final partition counts.
3. On the Iceberg **write path**, insert the same data with each distribution mode (none, hash, range), record shuffle cost, file counts per partition and file sizes.
4. Combine: shuffle partitions times distribution mode times table partitioning decides output file counts; write the relationship in your own words and test it.

**Done when:** you can predict the file count of a write before running it.

---

## M9. Storage-partitioned joins

**Objective:** remove shuffles by design.

**Do, in order:**
1. Build orders and lines **bucketed identically on order id** with the same bucket count.
2. Enable the Spark and Iceberg settings required (check the Iceberg docs for your version).
3. Confirm the plan has **no exchange** on either side; compare time with a regular join.
4. **Break it on purpose:** different bucket counts; an extra partition column; a different join key; one side unbucketed. Record which cases still avoid a shuffle.
5. Compare with the older Hive-style bucketing.
6. Consider the cost: bucketing is decided at design time and affects writes.

**Done when:** you have a list of conditions under which the optimization applies and a recommended bucket count for your data.

---

## M10. Memory pressure and spill

**Objective:** know the early signs and the fixes.

**Do, in order:**
1. Run a large sort, aggregation and merge with small executors and few shuffle partitions; record spill (memory and disk) and GC.
2. Apply fixes one at a time: more partitions; fewer cores per executor (more memory per task); a larger execution share of memory; reduce row width by projecting early.
3. Repeat for a merge on a large target; compare copy-on-write and merge-on-read memory behavior.
4. Rank the fixes by effect per effort.

**Done when:** you can explain each spill in a run and say which fix addressed it.

---

## M11. Anti-pattern gallery

**Objective:** a catalog of plans that look fine but aren't.

**Do, in order:** on small samples, read the plans for each and note the failure mode and the fix:
- non-equi join and joins on expressions (for example a normalized product name);
- `distinct` before a join;
- a Cartesian product from a missing condition;
- a window across a whole user history that could be bounded;
- an unnecessary sort or repartition before a write;
- a join key with mismatched types causing casts.

**Done when:** each pattern has a plan screenshot or description, the symptom and the fix.

---

## M12. Synthesis: the decision tree

Produce a one-page decision tree: given table sizes, key skew, join type and freshness needs, which strategy, which settings, which table design. Include the evidence (experiment ids) behind each branch.

## Config experiments (summary)

| Knob | Values | Watch |
|---|---|---|
| auto-broadcast threshold | disabled, 10 MB, 100 MB | join type |
| AQE (overall, coalesce, skew, local reader) | on/off, thresholds | plan changes |
| shuffle partitions | 8 to 1000 | overhead vs spill |
| advisory partition size | 16 MB to 256 MB | coalesced partitions |
| executor cores | 1, 2 | memory per task |
| Iceberg distribution mode | none, hash, range | shuffle, file counts |
| bucket count | 8, 16, 32 | eligibility of storage-partitioned joins |

## Stretch goals
- Compare skew handling with the RDD API and a custom partitioner.
- Build a skew-report helper: top keys, share of rows, recommended technique.
- Measure how the bulk buyer changes Iceberg write distribution.

## Exit criteria for Project 2
1. You predicted join types and skew locations before running.
2. Each fix has numbers behind it.
3. You can design a table (partitioning, buckets, sort) to make a given join cheap.
