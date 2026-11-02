# Project 4: Executors, Memory and I/O Performance (Instacart, Delta Lake)

> **Before you start:** read `00-start-here.md` once. Use `07-learning-companion.md` (P4 section) for prerequisites, the minimal path, time-boxes, prediction prompts, concept checks, rubric and teach-back. Use `08-debugging-playbook.md` when stuck and `09-reference-answers.md` for magnitude calibration.

## Goal

Build a capacity model for your platform by testing, in a deliberate order: executor layout, memory behavior, Python performance, object-store (S3A) behavior against RustFS, JDBC concurrency against Postgres, failure recovery, and the rules for writing Delta tables safely.

**Simplified scope:** eight core milestones (M0 to M7) plus an optional Depth block. No CDC in this project.

## Work plan

| Phase | Milestone | Core? | Produces |
|---|---|---|---|
| A | M0 Workloads and baselines | Core | four reference workloads |
| B | M1 Executor layout and scaling | Core | scaling curves, bottleneck diagnosis |
| C | M2 Memory and spill | Core | memory guidance |
| D | M3 Python UDF ladder | Core | UDF decision table |
| E | M4 Object storage and file size | Core | S3A and file-size guidance |
| F | M5 JDBC concurrency | Core | Postgres connection policy |
| G | M6 Failures and recovery | Core | failure runbook |
| H | M7 Delta writer rules | Core | safe job combinations |
| I | Depth: serialization, scheduling, shuffle files, speculation | Optional | extra notes |
| J | M8 Capacity model | Core | final model |

## Terms used in this project

| Term | Meaning |
|---|---|
| **Executor** | a JVM process on a worker that runs tasks and holds cached data |
| **Core / task slot** | one task runs per core at a time |
| **Strong scaling** | same job, more cores: how much faster does it get? |
| **Speedup / efficiency** | speedup = time on 1 core / time on n cores; efficiency = speedup / n |
| **Bottleneck** | the resource that limits throughput: CPU, memory, storage, source database, or network |
| **Execution vs storage memory** | execution = shuffles, joins, sorts; storage = cached data. They share one pool |
| **Spill** | writing intermediate data to disk because memory ran out |
| **GC** | JVM garbage collection; high GC time means too many objects or too little heap |
| **OCC** | optimistic concurrency control: writers don't lock; conflicts are detected at commit |

---

## M0. Workloads and baselines

**Objective:** four fixed workloads that stress different resources, each with a stable baseline.

**Do, in order:**
1. **Define the workloads** (M tier; L when you want pressure):

| Id | Workload | Mainly stresses |
|---|---|---|
| W1 | scan-heavy: read lines, filter, count by product | storage throughput, request count |
| W2 | shuffle-heavy: aggregate lines by user and product, join to orders | network, disk, shuffle |
| W3 | CPU-heavy: a per-line score computed with a Python function | CPU, Python overhead |
| W4 | memory-heavy: cache lines, then run wide aggregations | memory, GC, spill |

2. **Instrument every run** with the four views plus host CPU, memory and disk use.
3. **Record baselines** with the default layout, twice each.
4. **Classify** each workload by its apparent bottleneck (to be tested).

**Done when:** each workload has a stable baseline (variation under 10%) and a first guess at its bottleneck.

**Read more:** Spark web UI guide https://spark.apache.org/docs/latest/web-ui.html | Monitoring https://spark.apache.org/docs/latest/monitoring.html

---

## M1. Executor layout and strong scaling

**Objective:** find the best layout and the point where more cores stop helping.

**Why it matters:** 8 cores can be arranged as 4 executors with 2 cores, or 8 executors with 1 core, and memory per task changes accordingly. More cores only help while CPU is the limit; after that something else (storage, the source database, the driver) is.

**Do, in order:**
1. **Try layouts** (new session each time; verify in the Executors tab): 4 x (2 cores, 1 GB); 8 x (1 core, 1 GB); 4 x (2 cores, 2 GB).
2. Run W1 to W4 on each; record time, spill and GC.
3. **Strong scaling:** for each workload, run with 1, 2, 4 and 8 total cores. Compute speedup and efficiency.
4. **Diagnose the flat part:** compare executor CPU, RustFS throughput and Postgres load to decide whether the job is **CPU-bound, storage-bound, source-bound or shuffle-bound**.

**Worked example (illustrative):**

| Cores | Time | Speedup | Efficiency |
|---|---|---|---|
| 1 | 400 s | 1.0 | 100% |
| 2 | 205 s | 1.95 | 98% |
| 4 | 108 s | 3.7 | 93% |
| 8 | 98 s | 4.1 | 51% (RustFS throughput is saturated: storage-bound) |

**Traps:** one-core executors duplicate JVM overhead and broadcast data; two-core executors share memory between tasks; a flat curve may be storage, not Spark.

**Done when:** a scaling curve exists for each workload and each has a named bottleneck.

**Read more:** Spark standalone https://spark.apache.org/docs/latest/spark-standalone.html | Amdahl's law (why curves flatten) https://en.wikipedia.org/wiki/Amdahl%27s_law

---

## M2. Memory and spill

**Objective:** recognize memory trouble in the UI and know what to change.

**Concepts:** each executor's heap is split: a small reserved part, and a unified pool (default 60%) shared by execution (shuffles, joins, sorts) and storage (cache). If a task needs more execution memory than it has, it **spills** to disk (slow but safe). If even that fails, you get an out-of-memory error.

**Do, in order:**
1. Read the Executors tab: storage memory, execution memory, GC time per task.
2. Run W4; record cached fraction, spill, GC share.
3. **Sweep one setting at a time:**

| Setting | Values | Watch |
|---|---|---|
| Memory fraction | 0.4, 0.6, 0.8 | spill, GC |
| Storage fraction | 0.2, 0.5, 0.8 | cached fraction, eviction |
| Executor memory | 512m, 1g, 2g | spill, failures |

4. **Cause an out-of-memory failure on purpose** (small executors, big partitions) and read the error and the UI to learn its signature.
5. Note that the memory-overhead setting is **not enforced** in standalone mode; the container limit (Docker) can still kill an executor (exit code 137).

**Done when:** from task metrics you can say whether memory, GC or spill is the issue and what to change.

**Read more:** Tuning guide (memory management, GC) https://spark.apache.org/docs/latest/tuning.html

---

## M3. The Python UDF ladder

**Objective:** choose the right level of Python integration for a task.

**Why it matters:** a Python function called once per row crosses the JVM-to-Python boundary for every row, which is slow. Batched approaches move many rows at once.

**Do, in order:**
1. **Choose one per-row computation:** a math-heavy reorder-propensity score from recency and frequency features.
2. Implement it four ways and benchmark each on the M tier with a no-op write:

| Rung | Implementation | Expectation to test |
|---|---|---|
| 1 | built-in functions only | fastest |
| 2 | plain row-at-a-time Python UDF | slowest |
| 3 | Arrow-optimized Python UDF (Spark 4.2 enables Arrow-optimized UDFs by default; confirm in your Environment tab) | better than rung 2 |
| 4 | pandas UDF (vectorized) | best Python option for math |

3. Check that all rungs give the same results within tolerance.
4. **Hot-key trap:** run a grouped pandas computation per user. The injected bulk buyer (about 5% of orders) overloads one Python worker. Watch memory and task time, then fix it with a window function or two-phase processing.

**Done when:** for any new transformation you can say which rung to start from and why.

**Read more:** PySpark with Arrow and pandas https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html | Spark ANSI mode https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html

---

## M4. Object storage (S3A) and file size

**Objective:** know how storage settings and file layout affect throughput.

**Concepts:** reading from RustFS means many HTTP requests. Each file costs at least one request, so thousands of tiny files are slow even when the data is small. S3A has settings for how many connections and threads it uses.

**Do, in order:**
1. Baseline W1 and a write job; record request counts and throughput at RustFS for a full scan, a filtered scan and a write.
2. **Sweep S3A settings** one at a time: connection pool size; thread pool size; upload buffering mode; read-ahead.
3. **File-size study:** rewrite the same data at several target file sizes (use OPTIMIZE). Plot scan throughput against file size; find where more size stops helping.
4. **Delta listing:** Delta reads its log by listing the `_delta_log` folder. Check how many log files you have and how checkpoints reduce replay (also read about the experimental fast-listing setting for S3A).
5. Watch for retry storms and timeouts under load in the RustFS logs.

**Done when:** you have recommended storage settings and a target file size, with evidence.

**Read more:** Hadoop S3A https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html | S3A performance https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/performance.html | Delta storage configuration https://docs.delta.io/latest/delta-storage.html

---

## M5. JDBC concurrency (protecting Postgres)

**Objective:** extract without hurting the source database.

**Why it matters:** each JDBC partition is a database connection and a query. Too many can overload Postgres, and more partitions than cores doesn't make Spark faster, because only as many tasks run at once as there are cores.

**Do, in order:**
1. **Raise read parallelism step by step** (1, 2, 4, 8, 16, 32). At each step record Postgres active connections, CPU, query latency and Spark time.
2. Find the point where Postgres hurts or Spark stops improving.
3. Explain how executors and cores bound simultaneous connections, and which Postgres connection limit applies.
4. Compare fetch size values for executor memory behavior (some drivers buffer the whole result unless configured).
5. Write a **policy**: maximum parallelism, quiet windows for extraction, or a read replica.

**Done when:** you can state the maximum safe parallelism and why.

**Read more:** Spark JDBC https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html | PostgreSQL monitoring statistics https://www.postgresql.org/docs/current/monitoring-stats.html

---

## M6. Failures and recovery

**Objective:** see how Spark and Delta recover and what they leave behind.

**Concepts:** a Delta commit has two phases: first the data files are written, and only then the log entry that makes them part of the table. If a job dies in between, the table is unchanged (readers never see the new files) but the unreferenced files remain as **orphans** until VACUUM removes them.

**Do, in order**, recording a timeline for each:
1. **Executor loss:** kill a worker container during a long job. Record lost executor, task retries, stage retries, outcome.
2. **Storage failure:** stop RustFS during a Delta write. Record the error and retries; then confirm the table still reads at its last good version; list leftover data files.
3. **Source failure:** stop Postgres during an extract. Record the error, and confirm no half-published extract exists (no manifest).
4. Bring services back; confirm workers re-register and the next run succeeds.

**Done when:** each failure has a recorded timeline and a runbook entry.

**Read more:** Delta concurrency control and ACID https://docs.delta.io/latest/concurrency-control.html | Spark standalone (worker recovery) https://spark.apache.org/docs/latest/spark-standalone.html

---

## M7. Delta writer rules (what may run at the same time)

**Objective:** know which jobs can write the same table together, and design around the limits.

**Concepts:**
- **OCC (optimistic concurrency):** Delta writers don't take locks. Each reads the latest version, does its work, then tries to commit the next version. If another writer committed something that conflicts, the commit fails with an exception and you retry.
- **Isolation level:** the default (write-serializable) allows more concurrency; the stricter serializable level detects more conflicts.
- **The single-writer rule (Gate F):** on S3-style storage, Delta's default commit logic only guarantees correctness when all concurrent writes come from **one Spark driver**. Several separate applications writing the same table can lose data.

**Do, in order:**
1. **Inside one application** (two threads in the same session), run these and record the outcome:

| Scenario | Expected | Record |
|---|---|---|
| two appends at the same time | both succeed | commit times |
| two MERGEs touching overlapping rows | one fails with a conflict exception | the error text |
| MERGE while OPTIMIZE runs | check which conflicts | outcome |

2. Compare the default and stricter isolation levels on one scenario.
3. Write **scheduling rules:** which jobs may run together, and a simple **writer lock** so only one application writes a table at a time.
4. **Optional, scratch table only:** with a throwaway table, try two *separate* applications writing at once to see the failure mode. Never do this on a real table.

**Done when:** you have a table of safe and unsafe job combinations and a scheduling rule.

**Read more:** Delta concurrency control https://docs.delta.io/latest/concurrency-control.html | Delta storage configuration (S3 single vs multi-cluster) https://docs.delta.io/latest/delta-storage.html | Optimistic concurrency control https://en.wikipedia.org/wiki/Optimistic_concurrency_control

---

## Depth (optional)

- **Serialization and the RDD API:** compare group-by-key, reduce-by-key and a DataFrame aggregation by shuffle bytes; switch the serializer and note why DataFrames barely change.
- **Scheduling and dynamic allocation:** enable dynamic allocation with shuffle tracking; run two applications at once and see how standalone shares cores; try FIFO and FAIR pools.
- **Shuffle internals:** inspect shuffle files on a worker during a job; relate shuffle write/read and spill metrics to files.
- **Speculation:** make one task slow and see whether a speculative copy finishes first.

**Read more:** Job scheduling https://spark.apache.org/docs/latest/job-scheduling.html | Tuning guide https://spark.apache.org/docs/latest/tuning.html

---

## M8. Capacity model

Write a document answering: for each workload class (scan, shuffle, CPU, memory), what layout, memory, storage settings and parallelism work best on this platform; where the bottleneck is; what limits Postgres and RustFS; and what you'd change if the data doubled. Link every statement to experiment ids.

## Config experiments (summary)

| Area | Knobs |
|---|---|
| Topology | executor cores and memory, `spark.cores.max` |
| Memory | memory fraction, storage fraction |
| Python | UDF type (Arrow settings) |
| Object store | S3A pool, threads, buffers, read-ahead, target file size |
| JDBC | partitions, fetch size |
| Delta writes | isolation level, retries |

## Exit criteria for Project 4
1. Every workload has a named bottleneck with evidence.
2. You have safe limits for Postgres and recommended storage settings for RustFS.
3. You have a written failure runbook and writer rules.
