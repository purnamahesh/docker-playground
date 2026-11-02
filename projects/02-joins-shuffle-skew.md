# Project 2: Joins, Shuffles and Skew (Instacart, Delta Lake)

> **Before you start:** read `00-start-here.md` once. Use `07-learning-companion.md` (P2 section) for prerequisites, the minimal path, time-boxes, prediction prompts, concept checks, rubric and teach-back. Use `08-debugging-playbook.md` when stuck and `09-reference-answers.md` for expected plans.

## Goal

Understand how Spark executes joins and aggregations over your Delta tables: choose join strategies deliberately, diagnose skew from evidence, fix it, tame the most expensive query (product pairs), and control how many files your jobs write.

**Simplified scope:** eight core milestones (M0 to M7) plus an optional Depth block. There is no CDC here, and no bucketing or storage-partitioned joins: Delta Lake does not support bucketed tables, so shuffle-free joins are not available. Instead you reduce join cost with broadcast joins, AQE and good file layout.

## Work plan

| Phase | Milestone | Core? | Produces |
|---|---|---|---|
| A | M0 Test bed and baselines | Core | frozen tables, 5 benchmark queries, key profile |
| B | M1 Reading join plans | Core | annotated plans |
| C | M2 Join strategies | Core | strategy comparison |
| D | M3 Diagnosing skew | Core | skew checklist |
| E | M4 Fixing skew | Core | fix comparison table |
| F | M5 The pair explosion | Core | co-occurrence method |
| G | M6 Partitions and output files | Core | file-count rules |
| H | M7 Memory pressure and spill | Core | spill playbook |
| I | Depth: federated joins, range joins, anti-patterns | Optional | extra patterns |
| J | M8 Synthesis | Core | decision tree |

## Terms used in this project

| Term | Meaning |
|---|---|
| **Stage / task** | Spark splits a query into stages; each stage runs many tasks (one per partition) in parallel |
| **Exchange** | the plan operator for a shuffle: data moves across the network so matching keys meet |
| **Broadcast join** | the small table is copied to every executor so the big table needs no shuffle |
| **Sort-merge join** | both tables are shuffled by the join key, sorted, then merged; the default for big-big joins |
| **Shuffle-hash join** | both shuffled, then one side is built into an in-memory hash table |
| **Skew** | one partition (or key) holds far more data than the others |
| **Hot key** | a key value that owns a large share of rows (for example bananas) |
| **Salting** | adding a random suffix to a hot key to spread it across several partitions |
| **Spill** | when a task runs out of memory and writes intermediate data to disk |
| **AQE** | adaptive query execution; re-plans at runtime using real sizes |

---

## M0. Test bed and baselines

**Objective:** a stable set of tables and queries so every result is comparable.

**Why it matters:** performance work only means something if the data and the question stay the same while you change one thing. Freezing the inputs turns "it felt faster" into evidence.

**Do, in order:**
1. **Freeze the data version.** Delta tables are versioned. Write down the current version number of each table you'll use (lines, orders, products, aisles, departments, users, stores, price book). Reading "as of version N" later guarantees identical input.
2. **Define five benchmark queries** and keep them fixed:

| Id | Question | Tables | What it stresses |
|---|---|---|---|
| J1 | Reorder rate by department and aisle | lines x products x aisles x departments | small-dimension joins, aggregation |
| J2 | Order count by store and day | orders x users x stores | star join |
| J3 | Each user's top 10 products | lines x orders | window and aggregation by user |
| J4 | Product pairs bought together | lines x lines | explosion |
| J5 | Price each line from the effective-dated price book | lines x price book | range join |

3. **Profile the keys** that matter. For each (product, aisle, department, store, user, hour, basket size) compute the share of rows held by the top 1, top 10 and top 100 values. *Good result:* a table like the one below; it predicts where skew will appear.

| Key | Distinct values | Top 1 share | Top 10 share | Expectation |
|---|---|---|---|---|
| product_id (illustrative) | ~50k | ~1.5% | ~8% | mild to moderate skew |
| department_id | 21 | ~30% (produce) | ~95% | strong, low-cardinality skew |
| user_id | ~206k | ~0.05% | small | none, except the injected bulk buyer (~5%) |

4. **Prepare two table variants:** (a) lines as a single Delta table partitioned by month (via a generated date column), (b) lines unpartitioned. Run OPTIMIZE on both so small-file noise doesn't distort results.
5. **Record baselines:** run J1 to J5 with default settings, twice each. Log time, shuffle bytes, spill, task count and the plan's join types.

**Done when:** every query has a baseline and a recorded plan; the key table exists; both variants exist and are compacted.

**Read more:** Spark web UI guide https://spark.apache.org/docs/latest/web-ui.html | Delta time travel and history https://docs.delta.io/latest/delta-batch.html | Delta OPTIMIZE https://docs.delta.io/latest/optimizations-oss.html

---

## M1. Reading join plans

**Objective:** explain every operator in a join plan.

**Why it matters:** you can't fix what you can't read. A plan is a tree of operators; the leaves are scans, the root is the result. For joins, the questions are: which join type, how many exchanges, and what was filtered before the join.

**Do, in order:**
1. For J1 to J3, print the formatted plan and annotate each node in your notes: scan (columns read, filters pushed), join type, exchange (which key, how many partitions), sort, aggregate (partial then final).
2. **Predict first:** for each join, write down the expected join type and why, using table sizes versus the broadcast limit (default 10 MB).
3. **Force the alternative:** disable auto-broadcast and AQE, rerun, and count exchanges and sorts. *Expected:* more exchanges and a slower run.
4. **Re-enable AQE** and look at the *final* adaptive plan: did a join switch? Were partitions coalesced?
5. Note how a Delta scan looks in the plan (partition filters, pushed filters, file count) and compare with the number of files in the table detail.

**Traps:** reading only the initial plan (AQE may change it at runtime); assuming a filter in the plan was applied at the source.

**Done when:** at least 80% of your join-type predictions were right and you can explain each miss.

**Read more:** Spark SQL performance tuning (joins, hints, AQE) https://spark.apache.org/docs/latest/sql-performance-tuning.html | EXPLAIN https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html

---

## M2. Join strategies

**Objective:** know when each strategy wins and what it costs.

**Why it matters:** the same join can take 20 seconds or 5 minutes depending on strategy. Strategy depends on table sizes, not on your intentions.

**Do, in order:**
1. On J1 (lines joined to small tables), run each of: broadcast, sort-merge, shuffle-hash (force each with a hint). Record time, shuffle bytes and spill.
2. **Find the broadcast limit.** Make the dimension side bigger step by step (50k rows, 500k, 5M, using a synthetic copy). Note where driver memory or the broadcast timeout starts to hurt. *Why:* a broadcast table is collected on the driver and sent to every executor.
3. Compare **shuffle-hash vs sort-merge** where the build side fits memory.
4. Swap the table order or hints and see whether the build side changes.

**Options to explore:**

| Variable | Values | Watch |
|---|---|---|
| Strategy | broadcast / sort-merge / shuffle-hash | time, shuffle bytes, spill, driver memory |
| Dimension size | 50k, 500k, 5M rows | where broadcast stops being safe |
| Auto-broadcast threshold | disabled / 10 MB / 100 MB | which strategy Spark chooses |

**Traps:** broadcasting a table that is small on disk but large in memory; a hint silently ignored if the join type doesn't allow it (check the plan).

**Done when:** you can state the best strategy for J1 with evidence and its margin over the default.

**Read more:** Join hints https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html | Spark SQL performance tuning https://spark.apache.org/docs/latest/sql-performance-tuning.html

---

## M3. Diagnosing skew

**Objective:** recognize skew from the Spark UI in under a minute.

**Why it matters:** a stage finishes only when its slowest task finishes. With one hot key, one task may do 40 times the work of the rest while seven cores sit idle.

**Do, in order:**
1. Run an aggregation and a join on product id, then on department, then (if present) on the bulk buyer's user id.
2. For each, open the Stages tab and record for the slow stage: task duration min / median / max, the shuffle read of the largest task versus the median, and spill.
3. Compute **max divided by median**. Rough guide: under 2 is fine; 3 to 10 is noticeable; above 10 is skew.
4. Show skew in three places: a join, an aggregation, a window by user.
5. Contrast **high-cardinality skew** (one hot product among 50k) with **low-cardinality skew** (21 departments spread across many partitions): in the second case many partitions are simply empty.
6. Write a **skew checklist** (UI signals plus key-profile signals).

**Worked example (illustrative):**

| Key | Query | Max task | Median task | Ratio | Spill | Diagnosis |
|---|---|---|---|---|---|---|
| product_id | join with lines | 160 s | 11 s | 14.5 | one task, 3 GB | hot product |
| department_id | aggregate | 45 s | 40 s | 1.1 | none | low-cardinality, not skewed per task |
| user_id | window | 12 s | 9 s | 1.3 | none | no skew |

**Traps:** confusing input skew (uneven files) with key skew; judging by total time instead of the task distribution.

**Done when:** from the UI alone you can say which of the three situations a stage is in.

**Read more:** Web UI (stages and tasks) https://spark.apache.org/docs/latest/web-ui.html | Tuning guide https://spark.apache.org/docs/latest/tuning.html

---

## M4. Fixing skew

**Objective:** pick the right fix for a join, an aggregation and a MERGE, with evidence.

**Do, in order**, using one skewed query (reorder rate per product joined back to lines):
1. **AQE skew handling.** Lower the size thresholds so it triggers on lab-sized data; find the skew marker in the plan; explain what Spark split. Note where it doesn't apply (aggregations).
2. **Salting.** Add a random number from 0 to N-1 to the big side's key, replicate the small side N times, and join on key plus salt. For aggregations: aggregate by key plus salt first, then by key. Try N = 4, 8, 32.
3. **Hot-key isolation.** Find the top K keys; broadcast-join those rows; shuffle-join the rest; combine the results.
4. Build a comparison table: time, max/median ratio, shuffle bytes, spill, complexity.
5. **Optional:** pre-aggregate before the join (for example to product and day).

**Salting example (illustrative):** key "banana" owns 10% of rows. With N = 8, it becomes "banana_0" to "banana_7", each holding about 1.25%, so eight tasks share the work. The small side must contain all eight versions.

**Options to explore:**

| Fix | Works for | Doesn't help |
|---|---|---|
| AQE skew handling | sort-merge joins | aggregations |
| Salting | joins and aggregations | adds complexity and copies the small side |
| Hot-key isolation | a few very hot keys | many moderately hot keys |

**Done when:** you chose a fix for a join, an aggregation and a MERGE and can show the numbers.

**Read more:** Spark SQL performance tuning (AQE skew join) https://spark.apache.org/docs/latest/sql-performance-tuning.html | Delta MERGE https://docs.delta.io/latest/delta-update.html

---

## M5. The pair explosion (products bought together)

**Objective:** run the most expensive query and know what each shortcut changes.

**Why it matters:** joining lines to itself on order id creates one row per pair of items in the same order. Pair count grows with the **square** of basket size, so a few huge baskets dominate.

**Worked example:** pairs in one basket = n x (n - 1) / 2.

| Basket size | Pairs |
|---|---|
| 5 | 10 |
| 10 | 45 |
| 50 | 1,225 |
| 100 | 4,950 |

One basket of 100 items creates as many pairs as 110 baskets of 10.

**Do, in order:**
1. **Predict the total pair count** from your basket-size distribution (sum over orders of n x (n - 1) / 2). Write the number down.
2. **Run on a small sample** to check your prediction; then measure on M if feasible.
3. Explain why a few huge baskets dominate shuffle size, spill and task times.
4. **Try fixes** and record each: (a) cap or sample huge baskets; (b) keep only frequent products before pairing (prune first); (c) pair in a canonical order (product A < product B) to halve the work.
5. Mark each fix: **changes the answer** (caps, sampling, pruning) or **changes only the plan** (canonical order).

**Done when:** two methods produce the same top pairs, with the cost and accuracy trade-off documented.

**Read more:** Market basket analysis background https://en.wikipedia.org/wiki/Association_rule_learning

---

## M6. Partitions and output files

**Objective:** control task counts and how many files your jobs write.

**Concepts:** the number of **shuffle partitions** decides how many tasks run after a shuffle. Too few = huge tasks that spill; too many = thousands of tiny tasks and tiny output files. By default Spark uses 200; on 8 cores you usually want far fewer, and AQE can merge small partitions automatically.

**Do, in order:**
1. With AQE **off**, sweep shuffle partitions (8, 16, 64, 200, 1000) on J1 and J3 and plot time against partitions.
2. With AQE **on**, vary the advisory partition size (16 MB, 64 MB, 256 MB) and find the "coalesced" partition counts in the plan.
3. **Output files:** write the same data with different settings (repartition to 8, to 64; AQE on and off). Count files per table partition and their sizes from the table detail and the RustFS console. If your Delta version supports optimized writes or auto-compaction, compare them (check availability).
4. Run OPTIMIZE and see how files change; remember it creates new files (old ones remain until VACUUM).
5. Write down the relationship: shuffle partitions x table partitions decides output file count.

**Done when:** you can predict the number of files a write will create before running it.

**Read more:** Spark SQL performance tuning (AQE coalescing) https://spark.apache.org/docs/latest/sql-performance-tuning.html | Delta optimizations https://docs.delta.io/latest/optimizations-oss.html

---

## M7. Memory pressure and spill

**Objective:** know the early signs of memory trouble and the fixes.

**Do, in order:**
1. Run a large sort or aggregation with small executors (for example 512 MB) and few shuffle partitions. Record spill (memory and disk) and GC time.
2. Apply one fix at a time and re-measure: (a) more shuffle partitions; (b) fewer cores per executor (more memory per task); (c) a larger execution share of memory; (d) select only needed columns early.
3. Rank the fixes by effect per effort.

**Rule of thumb:** memory available to one task is roughly executor memory times the usable fraction, divided by cores. Halving cores per executor roughly doubles memory per task.

**Done when:** you can explain each spill in your runs and name the fix that addressed it.

**Read more:** Tuning guide (memory management) https://spark.apache.org/docs/latest/tuning.html

---

## Depth (optional): three extra patterns

Do these only after the core path.

**D1. Federated joins (lake x Postgres).** Join a lake table to a live Postgres table. Look at what Spark pushes to Postgres, and rerun to see Postgres is hit again each time (no caching). Compare caching the read, broadcasting it, or copying it to Delta first. Rule: federation is fine for small, selective, fresh-data needs; otherwise copy.

**D2. Range joins.** Pricing each line from an effective-dated price book needs a "date between valid_from and valid_to" join, which is slow. Compare the raw range join with an expanded price table (one row per product per day) joined by equality.

**D3. Anti-patterns to read about:** joins on expressions (for example a lowercased name), `distinct` before a join, a missing join condition creating a Cartesian product, a whole-history window that could be bounded, a join key with mismatched types causing casts.

**Read more:** Spark JDBC https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html

---

## M8. Synthesis: the decision tree

Write a one-page decision tree: given table sizes, key skew, join type and freshness needs, which strategy, which settings, which layout. Attach the experiment ids behind each branch.

## Config experiments (summary)

| Knob | Values | Watch |
|---|---|---|
| auto-broadcast threshold | disabled, 10 MB, 100 MB | join type |
| AQE (overall, coalesce, skew) | on/off, thresholds | plan changes |
| shuffle partitions | 8 to 1000 | overhead vs spill |
| advisory partition size | 16 MB to 256 MB | coalesced partitions |
| executor cores and memory | 1 or 2 cores, 512 MB to 2 GB | spill |

## Exit criteria for Project 2
1. You predicted join types and skew locations before running.
2. Each fix has numbers behind it.
3. You can predict output file counts and explain spill.
