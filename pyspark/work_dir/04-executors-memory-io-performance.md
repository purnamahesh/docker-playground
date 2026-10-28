# Project 4: Executors, Memory and I/O Performance (Instacart)

## Goal

Build a capacity model for your platform by testing, in a deliberate order: executor layout, memory behavior, serialization, Python performance, object-store (S3A) behavior against RustFS, JDBC concurrency against Postgres, Iceberg commit conflicts, scheduling and failure recovery.

## Work plan (what happens when)

| Phase | Milestone | Starts when | Produces |
|---|---|---|---|
| A | M0 Workloads and baselines | gold and silver exist | six reference workloads |
| B | M1 Topology and scaling | M0 done | scaling curves, bottleneck diagnosis |
| C | M2 to M3 Memory and serialization | M1 done | memory settings guidance |
| D | M4 Python UDF ladder | M0 done | UDF decision table |
| E | M5 Object-store tuning | M1 done | S3A and file-size guidance |
| F | M6 JDBC concurrency | Postgres live | connection policy |
| G | M7 Concurrent writers | tables exist | conflict playbook |
| H | M8 Scheduling and dynamic allocation | M1 done | scheduling rules |
| I | M9 to M10 Failures and shuffle internals | all above | recovery notes |
| J | M11 Capacity model | all above | final model |

---

## M0. Reference workloads and baselines

**Objective:** six fixed workloads that stress different resources.

**Do, in order:**
1. **Define the workloads** (all on the M tier; use L when you need pressure):

| Id | Workload | Mainly stresses |
|---|---|---|
| W1 | scan-heavy: read lines, filter, count by product | storage throughput, request count |
| W2 | shuffle-heavy: aggregate lines by user and product, join to orders | network, disk, shuffle settings |
| W3 | CPU-heavy: per-line reorder-propensity score (math) | CPU, Python overhead |
| W4 | memory-heavy: cache lines, then run wide aggregations | memory, GC, spill |
| W5 | write-heavy: rewrite lines with a new layout | write path, commit, file counts |
| W6 | mixed: the full user-feature build (cadence, reorder ratio, basket stats, department vectors, next-basket candidates) | everything |

2. **Instrument each run** with the four views from the README (Spark UI, Iceberg metadata, RustFS, Postgres) plus host CPU, memory and disk I/O.
3. **Record baselines** with the default layout (2 cores and 1 GB per executor, all 8 cores), twice each.
4. **Classify each workload** by its apparent bottleneck, to be tested later.

**Done when:** each workload has a stable baseline (variation under 10% between runs) and a first guess at its bottleneck.

---

## M1. Executor topology and strong scaling

**Objective:** find the best layout and the point where adding cores stops helping.

**Do, in order:**
1. **Layout matrix** (new session each time; verify in the Executors tab):

| Layout | Cores x memory |
|---|---|
| 4 executors | 2 cores x 1 GB |
| 8 executors | 1 core x 1 GB |
| 4 executors | 2 cores x 2 GB |
| 8 executors | 1 core x 512 MB |

2. Run W1 to W4 on each layout and record time, spill and GC.
3. **Strong scaling:** for each workload, run with total cores of 1, 2, 4 and 8; compute speedup and efficiency; find where it flattens.
4. **Diagnose the flat part.** Compare executor CPU usage, RustFS throughput and Postgres load to decide whether the job is **CPU-bound, storage-bound, source-bound or shuffle-bound**.
5. Compare the best layout for a shuffle-heavy workload and a scan-heavy workload.
6. Note memory per task for each layout (executor memory divided by cores).

**Traps:** one-core executors duplicate JVM overhead and broadcast data; two-core executors share memory between tasks; a flat curve may be storage, not Spark.

**Done when:** a scaling curve exists for each workload and each has a named bottleneck.

---

## M2. Memory model

**Objective:** know how Spark uses its heap and what to change when it struggles.

**Do, in order:**
1. Read the Executors tab: storage memory, execution memory, GC time per task.
2. Run W4 (cache then aggregate). Record cached fraction, spill and GC share.
3. **Sweep one setting at a time:**

| Setting | Values | Watch |
|---|---|---|
| `spark.memory.fraction` | 0.4, 0.6, 0.8 | spill, GC |
| `spark.memory.storageFraction` | 0.2, 0.5, 0.8 | cached fraction, eviction |
| Off-heap memory | off, on with a fixed size | GC share |
| GC algorithm (via executor Java options) | default, parallel | pause time, throughput |
| Executor memory | 512m, 1g, 2g | spill, failures |

4. Cause an **out-of-memory** failure deliberately (small executors, big partitions) and read the error and the UI to learn the signature.
5. Note that the memory-overhead setting is **not enforced** in standalone mode; record what that implies on YARN or Kubernetes.

**Done when:** you can look at a task's metrics and say whether memory, GC or spill is the issue and what to change.

---

## M3. Serialization and the RDD API

**Objective:** understand where serialization settings matter.

**Do, in order:**
1. Run the same aggregation three ways and compare shuffle bytes and time: group-by-key, reduce-by-key (or aggregate-by-key), and the DataFrame aggregation.
2. Switch the serializer and repeat; explain why DataFrame jobs barely change while RDD jobs do.
3. Compare **persistence levels** on an RDD and a DataFrame cache (memory only, serialized, memory and disk, disk only).
4. Compare **shuffle compression codecs** (lz4, zstd, snappy) on W2.
5. Compare shuffle buffer settings.

**Done when:** you have rules for when RDD tricks help and when to stay with DataFrames.

---

## M4. The Python UDF performance ladder

**Objective:** pick the right level of Python integration per task.

**Do, in order:**
1. **Choose two per-row computations:** a math-heavy reorder-propensity score from recency and frequency features, and a product-name normalization plus tokenization step.
2. Implement each at every rung and benchmark on the M tier with the no-op sink:

| Rung | Implementation | Expectation to test |
|---|---|---|
| 1 | built-in functions | fastest |
| 2 | SQL expressions and higher-order functions | close to rung 1 |
| 3 | row-at-a-time Python UDF | slowest |
| 4 | Arrow-optimized Python UDF | better than 3 |
| 5 | pandas UDF | best Python option for math |
| 6 | map-in-Arrow or map-in-pandas | flexible, batch-based |
| 7 | RDD map | JVM-to-Python overhead |

3. **Sweep** the Arrow batch size, the Python worker memory limit and Python worker reuse.
4. **Decompose time** into JVM time, serialization and Python time where the UI allows.
5. **Hot-key trap:** run a grouped pandas computation per user. The injected **bulk buyer** concentrates data into one Python worker; watch memory and task time, then fix it with window functions, or with salting plus two-phase processing.
6. Test **result correctness** across rungs (identical outputs within tolerance).

**Done when:** you can say, for any new transformation, which rung to start from and why.

---

## M5. Object-store tuning (S3A and Iceberg file I/O against RustFS)

**Objective:** know how storage settings and file layout affect throughput.

**Do, in order:**
1. **Baseline W1 and W5** and measure request counts and throughput at RustFS for a full scan, a filtered scan and a write.
2. **Sweep S3A settings** one at a time:

| Setting | Values | Watch |
|---|---|---|
| connection pool maximum | small, default, large | throughput, errors |
| thread pool size | small, large | concurrent transfers |
| fast-upload buffering mode | disk, array, byte buffer | memory, speed |
| multipart size and threshold | small, large | request count |
| block size and read-ahead | small, large | scan speed |
| access-pattern hint | sequential, random, adaptive | columnar reads |

3. **Compare S3A with Iceberg's own S3 file I/O** for the same table: speed, error clarity, credential and endpoint settings.
4. **File size study:** create the same data at several file sizes and plot throughput versus file size; find where more size stops helping.
5. Find the setting that governs **Iceberg scan task size** (it differs from the general file-source setting) and sweep it.
6. Watch for retry storms and timeouts under load; look in RustFS logs.

**Done when:** you have recommended storage settings and a target file size, with evidence.

---

## M6. JDBC concurrency and writes (protecting Postgres)

**Objective:** extract and write without hurting the source.

**Do, in order:**
1. **Raise read parallelism step by step** (1, 2, 4, 8, 16, 32). At each step record Postgres active connections, CPU, query latency and Spark time.
2. Find the point where Postgres hurts or Spark stops improving.
3. Explain how executors and cores bound the number of simultaneous connections, and which Postgres connection limit applies.
4. Compare **fetch size** values for executor memory behavior (some drivers buffer the entire result set unless configured).
5. **Write a gold aggregate back to Postgres:** sweep batch size, the driver's batched-insert rewriting option and write parallelism; observe locks, WAL volume and index maintenance.
6. Compare writing into an unindexed staging table then swapping, versus writing into the live table.
7. Write a **backpressure policy:** maximum parallelism, quiet windows, a read replica, or extraction from the change log instead.

**Done when:** you can state the maximum safe parallelism for each Postgres workload and why.

---

## M7. Concurrent writers and commit conflicts

**Objective:** know what happens when two jobs touch the same Iceberg table.

**Do, in order:**
1. Run two Spark applications against the same table for each scenario:

| Scenario | Expected | Record |
|---|---|---|
| two appends at once | both succeed (with retries) | retry counts, commit time |
| two merges touching the same files | one fails or retries | error text |
| merge versus compaction | one loses | which and why |
| append during snapshot expiry | check safety | outcome |
| write during orphan removal | check safety | outcome |

2. Tune commit retries and the per-operation isolation levels; compare serializable and snapshot isolation behavior.
3. Verify a failed commit leaves **no visible partial data**; measure orphan files left behind.
4. Design **write isolation rules** (which jobs may run together, in what order).

**Done when:** you have a table of safe and unsafe job combinations and a rule for scheduling them.

---

## M8. Scheduling and dynamic allocation

**Objective:** share the cluster predictably.

**Do, in order:**
1. Enable dynamic allocation with shuffle tracking (needed in standalone) and watch executors scale up and down; sweep the idle timeout and bounds.
2. Run two notebooks (two applications) concurrently; see how standalone shares cores (`spark.cores.max`, spread-out placement).
3. Inside one session, run two jobs from separate threads under FIFO and FAIR scheduling with pools; measure each job's latency.
4. Sweep the data-locality wait; decide if it matters when all data comes from RustFS.

**Done when:** you can describe your fair-sharing configuration and the numbers behind it.

---

## M9. Resilience: speculation and failures

**Objective:** see how Spark recovers and what it leaves behind.

**Do, in order:**
1. **Straggler:** create a slow task, enable speculation, tune multiplier and quantile; check whether speculative attempts leave extra files in an Iceberg write.
2. **Executor loss:** kill a worker container during a long job. Record lost executor, task and stage retries, and the outcome; repeat with different retry limits.
3. **Storage failure:** stop RustFS mid-write. Record errors, retries, table state, and leftover files.
4. **Source failure:** stop Postgres mid-extract. Record errors and whether a half-published extract exists.
5. **Exclusion:** make one task fail deterministically and test failure-based exclusion.
6. Bring services back; confirm workers re-register and the next run succeeds.

**Done when:** each failure has a recorded timeline and a runbook entry.

---

## M10. Shuffle internals

**Objective:** connect UI shuffle numbers with files on disk.

**Do, in order:**
1. Compare shuffle buffer and in-flight fetch settings on a large shuffle.
2. Point the local directory somewhere specific and inspect shuffle files on a worker during a job.
3. Relate shuffle write, read, spill and disk bytes in the Stages tab to the files.
4. Check what happens when local disk fills.

**Done when:** you can explain each shuffle metric in terms of files and memory.

---

## M11. Capacity model

Produce a document that answers: for each workload class (scan, shuffle, CPU, memory, write), what layout, memory, storage settings and parallelism are best on this platform; where the bottleneck is; what limits Postgres and RustFS; and what you'd change when data doubles. Link each statement to experiment ids.

## Config experiments (summary)

| Area | Knobs |
|---|---|
| Topology | executor cores and memory, `spark.cores.max` |
| Memory | memory fraction, storage fraction, off-heap, GC flags |
| Python | Arrow settings, worker reuse, worker memory |
| Object store | S3A pool, threads, buffers, read-ahead, file I/O choice, Iceberg split size |
| JDBC | partitions, fetch size, batch size, rewrite option |
| Iceberg | commit retries, isolation, target file size |
| Scheduling | dynamic allocation, scheduler mode, locality wait |
| Resilience | speculation, task retries, exclusion |

## Exit criteria for Project 4
1. Every workload has a named bottleneck with evidence.
2. You have safe limits for Postgres and recommended storage settings for RustFS.
3. You have a written failure runbook.
