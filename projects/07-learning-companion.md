# Learning Companion

This file wraps each project with the scaffolding that turns activity into understanding: what you must have before you start, which parts are essential, how long things take, what to predict, concept checks, a self-graded rubric, a teach-back, and a retrospective.

---

## 0. How to use this file

### The prediction box (copy this before every milestone)

| Before you start (write it down) | After you finish |
|---|---|
| (a) Which resource does this stress: CPU, memory, storage, source database, network? | Did the resource you named turn out to be the bottleneck? |
| (b) What will the plan or behavior look like? | Did the plan match? Where did it differ? |
| (c) Which number will move, and by roughly how much? | Actual number versus your estimate |
| | **If wrong: what was the mistaken model?** (write one sentence) |

Record this in the Experiments tab (prediction column and "prediction correct?").

### Hard gates (do not skip)
- A baseline that varies more than **10%** between runs: stop and find out why before you experiment.
- A smoke test that fails **twice**: stop and use the FAQ and playbook.
- Three wrong predictions in a row: stop and re-read the relevant mental model section before continuing.
- A milestone overrunning its time-box by **2x**: do only the minimal path for it, park the rest.

### Time-boxes are estimates
They assume you have the prerequisites and work in focused blocks. Calibrate after your first two milestones: if you're running at 1.5x, multiply every later estimate by 1.5 and trim to the minimal path.

---

## Project 1: Lakehouse medallion ETL

### Artifacts you need before starting
Environment verified (all 10 bring-up steps and the hello world), Instacart files stored untouched, helpers usable, defect register and logs created.

### Minimal path versus depth

| Essential (do these) | Depth (stretch) |
|---|---|
| M0 steps 1 to 10 (profiling, union, constraints, timeline, synthetic tables) | M0 tier L, partitioned Postgres lines table |
| M1 with at least 2 partition strategies and the validation | M1 full format/compression grid |
| M3 bronze with 2 partition designs | M3 all five partition designs |
| M4: orders, lines, products, users SCD2, MERGE idempotency, quarantine | M4 payments reconciliation, clickstream |
| M5: three marts (reorder rate, user 360, a sample co-occurrence) | remaining marts |
| M6: time travel, rollback, schema evolution | partition evolution, branches |
| M7: compaction and snapshot expiry | manifest rewrite, orphans, delete files |
| M8: reruns, two drills, reconciliation | all five drills |
| M2 (needed before Project 5) | strategy comparison grid |

### Time-boxes (hours)
M0: 6 to 10. M1: 3 to 5. M2: 4 to 6. M3: 3 to 4. M4: 8 to 12. M5: 6 to 8. M6: 3 to 4. M7: 3 to 4. M8: 3 to 5.

### Prediction prompts to use
- M1: predict the row-count imbalance across id-range partitions before measuring.
- M3: predict file counts for each partition design before writing.
- M4: predict how many rows each rule will quarantine.
- M7: predict the speed-up of compaction on a filtered query.

### Mental-model sheet (fill in as you go)

| Component | Its job in this project | What it holds | Where I observe it | What I got wrong at first |
|---|---|---|---|---|
| Postgres | | | | |
| Spark JDBC reader | | | | |
| RustFS landing | | | | |
| Iceberg catalog | | | | |
| Iceberg metadata tree | | | | |

### Concept checks
1. Your JDBC read of the lines table is slow although 16 partitions run. One task takes 10 times longer than the rest. Most likely cause? (a) fetch size too small (b) equal id ranges with very unequal row counts (c) too few shuffle partitions (d) AQE disabled
2. Even with an overlap window, which change can a high-watermark extract on `updated_at` never see? (a) a late-committed update (b) a hard delete (c) a duplicate at the window boundary (d) a status change
3. A MERGE applied twice with the same batch changes rows on the second run. Most likely missing? (a) partition overwrite (b) a newer-wins comparison and collapsing the batch to one row per key (c) compaction (d) a larger target file size
4. You expired snapshots older than one day and now try to read yesterday's snapshot. What happens, and what could have prevented it?
5. After many small merge-on-read updates, reads slow down. Which maintenance step addresses it? (a) rewrite manifests only (b) compaction that also applies delete files (c) expire snapshots (d) remove orphans

### Integration checkpoint (before Project 2)
Run the Project 2 M0 baseline of query J1 once on silver. If it doesn't return the expected shape (departments and aisles with rates between 0 and 1), fix Project 1 first.

### Rubric (self-graded)

| Criterion | Meets | Exceeds | Evidence artifact |
|---|---|---|---|
| Source understanding | defect register covers every issue in README 3.3 with counts | found at least 3 extra issues, each with a proving test | Defects tab |
| Extraction | every table extracted with matching counts and checksums; two partition strategies compared | Postgres connection and scan impact quantified | manifests, experiment IDs |
| Incremental correctness | a replayed day has no missed updates; reruns are idempotent | overlap window derived from measured lateness; deletes detected within a stated lag | reconciliation output |
| Silver trust | row accounting balances; SCD2 as-of queries correct | quality metrics charted per run | quarantine counts, as-of results |
| Iceberg operations | rollback, schema evolution, compaction and expiry each shown with before and after metrics | a branch publish blocked a bad load | metadata screenshots |
| Reconciliation | zero unexplained differences | reconciliation automated | final report |

### Teach-back (5 minutes, out loud)
"Explain why a high-watermark extract can miss data and how your pipeline compensates, using your own reconciliation numbers."

### If data doubled
What breaks first in extraction, silver MERGE, and maintenance? What would you change?

### Parking lot (defer these questions)
REST catalog differences (Project 4), streaming CDC design (Project 5), z-order tuning (Project 3).

### Retrospective (write three sentences each)
1. What mental model changed?
2. What would I do differently?
3. What do I still not understand?

---

## Project 2: Joins, shuffles and skew

### Artifacts you need from Project 1
Silver `orders`, `order_items` (M tier), `products`, `aisles`, `departments`, users, stores, price book, payments, events; Iceberg snapshot ids frozen; Postgres products and price book live; clickstream events table optional.

### Minimal path versus depth

| Essential | Depth |
|---|---|
| M0 baselines for J1 to J4 | J5 to J8 baselines |
| M1 plan reading | |
| M2 broadcast vs sort-merge vs shuffle-hash on one query | broadcast-size limits |
| M4 skew diagnosis on product and department | skew in a MERGE and in JDBC |
| M5 AQE, salting, hot-key isolation | pre-aggregation, denormalization |
| M6 pair explosion with two fixes | all five fixes |
| M8 partition sweep and distribution mode | |
| M3 federated, M7 range joins, M9 storage-partitioned joins, M10 spill, M11 gallery | all of these are depth |

### Time-boxes (hours)
M0: 3 to 4. M1: 2. M2: 3. M3: 3. M4: 3. M5: 5 to 6. M6: 4 to 5. M7: 3 to 4. M8: 3. M9: 3 to 4. M10: 3. M11: 2. M12: 1 to 2.

### Prediction prompts to use
- M1: predict the join type of every join from table sizes.
- M4: predict the max-to-median task ratio for each candidate key from your key profile.
- M6: compute the expected pair count from basket sizes before running.
- M8: predict output file count from partitions and distribution mode.

### Mental-model sheet

| Component | Its job in this project | What it holds | Where I observe it | What I got wrong at first |
|---|---|---|---|---|
| Iceberg scan | | | | |
| Exchange (shuffle) | | | | |
| AQE | | | | |
| Executor memory | | | | |
| Postgres (federated) | | | | |

### Concept checks
1. The Stages tab shows one task 40 times the median and only that task spills. First move? (a) more shuffle partitions (b) more executor memory (c) a skew-specific fix (AQE skew handling or salting) (d) disable AQE
2. A stage's tasks all spill a little and the max is 1.3 times the median. First move? (a) salting (b) more shuffle partitions or less cores per executor (c) hot-key isolation (d) broadcast
3. You hint a broadcast on a 3M-row table that is 600 MB in memory with a 1 GB driver. What risk do you take?
4. AQE skew handling is on, yet your aggregation by product shows no skew handling in the plan. Why?
5. Two tables are bucketed on order id, one with 32 buckets and one with 16. What do you expect in the plan, and why?

### Integration checkpoint (before Project 3)
Both M-tier lines and orders must exist as compacted variants; J1 and J3 must have baselines. Project 3 needs the same table variants.

### Rubric

| Criterion | Meets | Exceeds | Evidence |
|---|---|---|---|
| Plan prediction | at least 80% of join-type predictions correct | explained every miss with the size or statistic that fooled you | M1 prediction log |
| Skew diagnosis | cause identified from the UI in four situations | built a reusable skew checklist that caught a new case | checklist, screenshots |
| Fix selection | compared four fixes on one query with numbers | chose different fixes for a join, an aggregation and a MERGE with evidence | comparison table |
| Explosion control | pair count predicted within 20% | two methods agree on top pairs with cost and accuracy documented | M6 notes |
| File-count control | predicted output file count correctly | explained the interaction of shuffle partitions, distribution mode and partitioning | M8 table |
| Table design | stated conditions for storage-partitioned joins | recommended bucket counts with evidence | M9 notes |

### Teach-back
"Show in the Stages tab why one hot key slows a stage even with idle cores, then justify your fix."

### If data doubled
Which join strategy changes, which fix still works, and which table layout would you revisit?

### Parking lot
Cost-based join reordering (Project 3), executor sizing (Project 4), commit conflicts on MERGE (Project 4).

### Retrospective
What mental model changed? What would I do differently? What do I still not understand?

---

## Project 3: Catalyst and query planning

### Artifacts you need from earlier projects
Compacted lines and events variants (unsorted, sorted, z-ordered); silver and gold tables; Postgres live with one unindexed column; frozen snapshot ids; a plan-reporter helper.

### Minimal path versus depth

| Essential | Depth |
|---|---|
| M0 with two variants | all variants |
| M1 plan literacy | |
| M2 pruning with sorted vs unsorted | many-manifest planning test |
| M4 one CBO before and after | histograms, stale stats |
| M5 indexed vs unindexed, aggregate pushdown, UDF failure | all nine cases |
| M8 five blockers | all eleven |
| M9 sort vs z-order | Parquet settings, ORC |
| M3 runtime filters, M6 AQE, M7 codegen | depth |

### Time-boxes (hours)
M0: 3 to 4. M1: 2. M2: 3 to 4. M3: 2. M4: 3 to 4. M5: 4 to 5. M6: 2. M7: 1 to 2. M8: 4. M9: 4 to 5. M10: 1.

### Prediction prompts to use
- M2: predict files skipped for each variant before running.
- M4: predict whether the join order will change after statistics.
- M5: predict whether each filter reaches Postgres and whether an index is used.

### Mental-model sheet

| Component | Its job | What it holds | Where I observe it | What I got wrong at first |
|---|---|---|---|---|
| Catalyst optimizer | | | | |
| Iceberg manifest statistics | | | | |
| JDBC source | | | | |
| AQE | | | | |

### Concept checks
1. A filter on the order timestamp prunes partitions even though the query doesn't mention the partition column. Why?
2. A filter shows as pushed in a JDBC scan, but Postgres does a sequential scan. Give two reasons.
3. The cost-based optimizer is on, but the join order didn't change. What is the most likely missing ingredient?
4. A predicate casts the timestamp to a string before comparing. What happens to pruning, and what is the fix?
5. The initial plan shows a sort-merge join; the final adaptive plan shows a broadcast join. What happened?

### Integration checkpoint (before Project 4)
For Q1 to Q4, you can state files skipped and bytes read from metadata and match them to the UI. If not, repeat M2.

### Rubric

| Criterion | Meets | Exceeds | Evidence |
|---|---|---|---|
| Plan reading | predicted 80% of plan features | predicted runtime changes from plan alone | prediction log |
| Pruning evidence | files and bytes skipped proven from metadata | explained a case where pruning failed and fixed it | metadata results |
| Statistics | showed one case where CBO helped and one where stale stats hurt | estimate-versus-actual analysis per operator | cost plan notes |
| JDBC pushdown | three-view evidence for each case | wrote and tested the rule of thumb | pushdown matrix |
| Blockers | five blockers measured before and after | all eleven | M8 table |
| Layout | recommended layout for three access patterns with numbers | included request counts and planning time | M9 notes |

### Teach-back
"Pick any query and explain, using the plan and Iceberg metadata only, why it read exactly the bytes it read."

### If data doubled
Which pruning still works, which metadata becomes a planning cost, and when would you re-sort or compact?

### Parking lot
Skew (Project 2), executor sizing (Project 4), streaming layouts (Project 5).

### Retrospective
What mental model changed? What would I do differently? What do I still not understand?

---

## Project 4: Executors, memory and I/O performance

### Artifacts you need from earlier projects
Silver and gold at M tier (L if available); compacted variants; Postgres source with the indexes you chose; history server working; four-view capture routine.

### Minimal path versus depth

| Essential | Depth |
|---|---|
| M0 with W1, W2, W3 | all six workloads |
| M1 two layouts and a scaling curve | all four layouts |
| M2 memory fraction sweep and one OOM | off-heap, GC |
| M4 rungs 1, 3, 4, 5 | all seven rungs and sweeps |
| M5 connection pool and file size | all S3A settings |
| M6 read-parallelism sweep | write-back study |
| M9 worker kill | other failures |
| M3 serialization, M7 concurrent writers, M8 scheduling, M10 shuffle internals | depth |

### Time-boxes (hours)
M0: 3. M1: 4. M2: 3. M3: 2. M4: 4 to 5. M5: 4. M6: 3 to 4. M7: 3. M8: 2 to 3. M9: 3. M10: 2. M11: 2.

### Prediction prompts to use
- M1: predict where each workload's scaling curve flattens and which resource saturates.
- M5: predict the file size at which throughput stops improving.
- M6: predict the number of Postgres connections for each parallelism setting.
- M9: predict what Spark will do when a worker dies.

### Mental-model sheet

| Component | Its job | What it holds | Where I observe it | What I got wrong at first |
|---|---|---|---|---|
| Executor JVM | | | | |
| Python worker | | | | |
| RustFS | | | | |
| Postgres connections | | | | |
| Iceberg commit | | | | |

### Concept checks
1. Strong scaling: 2x at 2 cores, 3.8x at 4, 4.1x at 8, and RustFS throughput is maxed at 8. What is the bottleneck?
2. Why does a pandas UDF beat a row-at-a-time Python UDF?
3. You raise JDBC partitions from 8 to 64 on an 8-core cluster. What happens to simultaneous connections and total time?
4. In standalone mode a container exceeds its memory overhead setting. Does Spark kill it? What can still kill it?
5. Two applications append to the same Iceberg table; two others merge into overlapping files. Which pair can conflict, and why?

### Integration checkpoint (before Project 5)
You have a documented safe maximum read parallelism for Postgres and a maintenance schedule that survives a concurrent writer. If not, repeat M6 and M7.

### Rubric

| Criterion | Meets | Exceeds | Evidence |
|---|---|---|---|
| Bottleneck naming | a named bottleneck with evidence for each of three workloads | all six | scaling and resource charts |
| Memory | recognized spill, GC and OOM signatures in the UI | tuned a workload to remove spill with an explanation | M2 notes |
| Python | chose the right UDF rung for two computations | quantified JVM, serialization and Python time | UDF table |
| Storage | recommended file size and S3A settings with evidence | compared S3A with Iceberg file I/O | M5 results |
| Source protection | stated a safe maximum Postgres parallelism | wrote a backpressure policy | M6 policy |
| Recovery | recorded a timeline for a worker loss | added storage and source failures to the runbook | failure runbook |

### Teach-back
"Explain why workload W2's scaling curve flattens at N cores using only the four views."

### If data doubled
Which workload hits its limit first, what would you change (layout, memory, storage settings, parallelism), and what can't you change on this hardware?

### Parking lot
Streaming-specific resource use (Project 5), REST catalog trade-offs.

### Retrospective
What mental model changed? What would I do differently? What do I still not understand?

---

## Project 5: Streaming and CDC into Iceberg

### Artifacts you need from earlier projects
Silver tables and gold marts; change simulator and trigger-filled change log working (Project 1 M2); clickstream generator; maintenance runbook (Project 1 M7 and Project 4 M7); a safe parallelism policy.

### Minimal path versus depth

| Essential | Depth |
|---|---|
| M0 producers at low and medium speed, service levels written | high speed |
| M1 clickstream to bronze with restarts | listing-cost study |
| M3 windows with two watermark values | sliding windows |
| M4 sessions verified against batch | session-gap sweep, arbitrary state |
| M5 CDC apply: idempotency and disorder tests | design comparison, incremental reads |
| M8 worker kill and driver kill | other drills |
| M2 maintenance, M6 dedup and joins, M7 state tuning, M9 latency | depth |

### Time-boxes (hours)
M0: 3 to 4. M1: 3. M2: 2 to 3. M3: 3. M4: 3. M5: 5 to 6. M6: 4. M7: 3. M8: 3. M9: 2. M10: 1 to 2.

### Prediction prompts to use
- M1: predict files and snapshots per hour from trigger and rate.
- M3: predict rows dropped by the watermark from the producer's late-event rate.
- M5: predict the final state if three batches arrive out of order.
- M8: predict what survives a driver failure.

### Mental-model sheet

| Component | Its job | What it holds | Where I observe it | What I got wrong at first |
|---|---|---|---|---|
| Checkpoint | | | | |
| State store | | | | |
| Watermark | | | | |
| Change log | | | | |
| Iceberg sink commits | | | | |

### Concept checks
1. Why can't you change the shuffle partition count on an existing stateful checkpoint?
2. You raise a watermark from 10 minutes to 1 hour. What changes for latency, state size and dropped rows?
3. A CDC batch is replayed after a failure. Name two things that make the replay safe.
4. Which output modes does Iceberg's streaming sink support, and what do you use when you need upserts?
5. Why does a file-source stream slow down as the landing prefix grows?

### Integration checkpoint (end of the lab)
Closed periods of stream results equal the batch rebuild; your service-level checks fire when you break something on purpose.

### Rubric

| Criterion | Meets | Exceeds | Evidence |
|---|---|---|---|
| Exactly-once | restarts never lose or duplicate | tested restarts mid-batch | count comparisons |
| Windows | stream equals batch except rows beyond the watermark | explained the difference exactly with dropped-row metrics | M3 reconciliation |
| CDC correctness | replay, reorder and drop tests all pass | compared two apply designs with cost and audit trade-offs | M5 notes |
| Maintenance | schedule runs while stream is live | conflict count minimized and explained | M2 results |
| Recovery | recorded outcomes of worker and driver failures | also storage, source and checkpoint-damage drills | failure runbook |
| Freshness | measured per stage | checks alert correctly at set thresholds | M9 report |

### Teach-back
"Explain what happens to data and state if the driver dies mid-batch, and why you lose nothing."

### If data doubled
What is the first streaming bottleneck (state, commits, listing, source), and which setting or design would you change?

### Parking lot
Log-based CDC with a broker, dashboards, multi-writer designs.

### Retrospective
What mental model changed? What would I do differently? What do I still not understand?

---

## Cross-cutting concept checks (any time)

X1. A teammate says "the job is slow, add more executors." What three pieces of evidence would you gather first?
X2. Your experiment result contradicts your prediction. What do you write down, and what do you do next?
X3. Two runs of the same job differ by 25%. What do you do before drawing any conclusion?

---

## Appendix: answer key (no peeking until you've answered)

### Project 1
1. (b). JDBC partitions are id ranges; large baskets make some ranges hold many more rows. Shuffle partitions and AQE don't affect JDBC reads; fetch size affects all tasks equally.
2. (b). Hard deletes leave no row to carry a new timestamp. An overlap window mitigates late commits; deduplication handles boundary duplicates.
3. (b). Without a newer-wins check and a one-row-per-key batch, reapplying changes rewrites rows.
4. The read fails or returns an error because the snapshot and possibly its files are gone. Prevent with longer retention, or a tag or branch that keeps the snapshot.
5. (b). Compaction that applies delete files (and delete-file rewrite) restores read speed; manifest rewrite and expiry address planning and storage, not row-level deletes.

### Project 2
1. (c). One hot partition means key skew; AQE skew handling or salting targets it. More partitions don't split a single hot key.
2. (b). Mild, uniform spill means partitions are too big for the memory per task; increase partitions or reduce cores per executor.
3. The driver collects and broadcasts the table, so memory pressure, long collection time or a timeout. A hint doesn't guarantee safety.
4. AQE skew handling applies to sort-merge join shuffle partitions, not aggregations; use salting with two-phase aggregation.
5. Expect an exchange: storage-partitioned joins need compatible partitioning (same transform and bucket count). Verify with your versions.

### Project 3
1. Hidden partitioning: the partition is a transform of the column (for example a day or month of the timestamp), so predicates on the column are translated to partition filters.
2. No usable index on the filtered column, or an expression or cast on the column prevents index use; also very low selectivity makes a sequential scan cheaper.
3. Column statistics (and possibly table statistics) weren't collected or are stale, or the engine doesn't use the statistics you collected.
4. The expression defeats pruning and file skipping; rewrite as a range predicate on the timestamp column.
5. Adaptive execution replaced the join at runtime after statistics showed one side was small enough to broadcast.

### Project 4
1. Storage-bound (RustFS), not Spark. More cores won't help; fewer, larger files or storage settings might.
2. Arrow batches amortize serialization and enable vectorized work, instead of per-row overhead.
3. Only as many tasks run at once as cores (8), so connections stay near 8; more partitions mean more, smaller queries, which may add overhead without speed-up.
4. Standalone doesn't enforce the setting. The container runtime can still kill it for exceeding its own memory limit (exit code 137).
5. Concurrent appends usually succeed after retries; two merges touching overlapping files can conflict because of validation at commit.

### Project 5
1. State is stored per shuffle partition; a different count breaks the mapping between keys and stored state.
2. Later emission, larger state, fewer dropped late rows.
3. A sequence comparison that ignores older changes, and recording the last applied sequence atomically with the merge (so a replay is a no-op).
4. Append and complete (verify in your Iceberg version); use a per-batch function with MERGE for upserts.
5. Each micro-batch lists the input prefix; listing returns a limited number of keys per request, pages sequentially, and grows with object count.

### Cross-cutting
X1. Bottleneck evidence: Stages tab (skew, spill, task count), resource usage (CPU versus storage versus source), and the plan (shuffle volume or pushdown).
X2. Write the mistaken model in one sentence in the experiment log; form a new prediction from the corrected model and re-test.
X3. Check for caching and warm-up effects, run more times, and find the source of variance before comparing anything.
