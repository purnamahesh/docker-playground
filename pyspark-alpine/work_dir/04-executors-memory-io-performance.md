# Project 4: Executors, Memory and I/O Performance

## Goal

Turn a feature-engineering workload into a tuning study that covers the whole stack: executor layout, the memory model, Python UDF speed, **object-store (S3A) behavior against RustFS**, **JDBC concurrency against Postgres**, **Iceberg commit conflicts**, scheduling, and failure recovery.

## Concepts covered

Executor sizing and strong scaling, I/O-bound vs CPU-bound diagnosis, unified memory and GC, serialization, RDD vs DataFrame shuffles, UDF ladder with Arrow, S3A connection and buffer settings, Iceberg file I/O alternatives, JDBC fetch and batch settings, Postgres connection limits, optimistic concurrency, dynamic allocation, FAIR scheduling, speculation, and injected failures.

## Workload

User-level features computed from Iceberg silver tables over the 34M order lines: order cadence, reorder ratio, basket size statistics, department-mix vectors, next-basket candidate features from each user's last N orders, a per-line reorder-propensity score (the per-row computation for the UDF ladder), and per-user sessionization over the clickstream. Run at the M scale tier, and use L when you want memory or I/O pressure.

## Milestones

### M1. Executor topology and strong scaling

Run one fixed job under each layout (a new session each time):

| Layout | Cores x memory |
|---|---|
| 4 executors | 2 cores x 1 GB |
| 8 executors | 1 core x 1 GB |
| 4 executors | 2 cores x 2 GB |
| 8 executors | 1 core x 512 MB |

- Verify each layout in the Executors tab.
- Plot speedup against total cores (1, 2, 4, 8). Where does it flatten?
- **Diagnose the bottleneck** at the flat part: executor CPU usage versus RustFS throughput versus Postgres load. Is the job CPU-bound, storage-bound, or source-bound?
- Compare the best layout for a shuffle-heavy job and a scan-heavy job.

### M2. Memory model

- Read storage vs execution memory and GC time in the Executors tab.
- Cache a large DataFrame, then run a wide aggregation. Sweep the memory fraction, the storage fraction, and off-heap memory. Compare GC algorithms through executor Java options.
- Record cached fraction, spill and GC share of task time.
- Note that the memory-overhead setting is **not enforced** in standalone mode. What does that imply for production on a managed cluster manager?

### M3. Serialization and the RDD API

- Compare three RDD/DataFrame formulations of the same aggregation by shuffle bytes: group-by-key, reduce-by-key, DataFrame aggregation.
- Switch the serializer and explain why DataFrame jobs barely change while RDD jobs do.
- Compare persistence levels on RDD and DataFrame caches. Compare shuffle compression codecs on a shuffle-heavy job.

### M4. The Python UDF ladder

Implement the same per-row computation seven ways (a math-heavy reorder-propensity score from recency and frequency features is a good one, plus a product-name normalization and tokenization step) and benchmark each on the M tier:

1. Built-in functions.
2. SQL expressions and higher-order functions.
3. Row-at-a-time Python UDF.
4. Arrow-optimized Python UDF.
5. pandas UDF.
6. Map-in-Arrow / map-in-pandas.
7. RDD map.

Sweep the Arrow batch size, the Python worker memory limit, and Python worker reuse. Then run a grouped pandas computation per user: the injected **bulk buyer** (about 5% of orders) will overload one Python worker. Fix it two ways (window functions; salting plus two-phase).

### M5. Object-store tuning (S3A against RustFS)

- Measure request counts and throughput in RustFS for a full scan, a filtered scan and a write.
- Sweep S3A settings: connection pool size, thread pool size, fast-upload buffering mode, multipart size, block size, read-ahead, and the input access-pattern hint (sequential vs random; the setting name depends on the Hadoop version).
- Compare **S3A** vs **Iceberg's own S3 file I/O** for the same table. Which is faster? Which gives clearer errors? What credential and endpoint settings does each need?
- Relate file size to request overhead: many small files vs fewer large files (after compaction). Find the file size where throughput stops improving.
- Find which setting governs **Iceberg scan task size** (it is not the general file-source partition size) and sweep it.

### M6. JDBC concurrency and writes (protecting Postgres)

- Increase JDBC read parallelism step by step. Record Postgres active connections, CPU, and query latency. Find the point where Postgres hurts.
- Explain how executor count and cores bound read concurrency, and which Postgres connection limit applies.
- Compare fetch size values for executor memory behavior (some drivers buffer the whole result set unless configured otherwise; verify what you observe).
- Write a gold aggregate back to Postgres: sweep batch size, the driver's batched-insert rewriting option, and write parallelism. Observe locks and WAL volume.
- Design a **backpressure policy**: maximum parallelism, quiet windows, or a read replica. Document it as an operating rule.

### M7. Concurrent writers and commit conflicts

Run two Spark applications against the same Iceberg table:

| Scenario | Expected result |
|---|---|
| two appends at once | both succeed (retries) |
| two MERGEs touching the same files | one fails or retries; record the error |
| MERGE vs compaction | which one wins? |
| append during snapshot expiry | what is safe? |

- Tune commit retries and the per-operation isolation levels. Compare serializable and snapshot isolation behavior.
- Verify that a failed commit leaves no visible partial data, and measure the orphan files it leaves.

### M8. Scheduling and dynamic allocation

- Enable dynamic allocation with shuffle tracking (required in standalone). Watch executors scale up and down.
- Run two notebooks concurrently and observe how standalone shares cores; then inside one session run jobs from two threads under FIFO and FAIR scheduling with pools.
- Sweep the data-locality wait. With RustFS, does locality matter at all? Why or why not?

### M9. Resilience: speculation and failures

- **Straggler:** simulate a slow task and test speculation. Check whether speculative attempts leave extra files in an Iceberg write.
- **Executor loss:** kill a worker container during a long job. Record lost executor, task retries, stage retries and outcome. Compare with different retry limits.
- **Storage failure:** stop RustFS mid-write; **source failure:** stop Postgres mid-extract. Record each Spark error, the retry behavior, and the state of the tables afterward.
- Bring everything back and verify the workers re-register.

### M10. Shuffle internals

- Compare shuffle buffer and in-flight fetch settings on a large shuffle.
- Point the local directory somewhere specific and inspect the shuffle files on a worker during a job.
- Relate Stages-tab shuffle/spill metrics to the files you see on disk.

## Config experiments (summary)

| Area | Knobs |
|---|---|
| Topology | executor cores/memory, `spark.cores.max` |
| Memory | memory fraction, storage fraction, off-heap, GC flags |
| Python | Arrow settings, worker reuse, Python worker memory |
| Object store | S3A pool/threads/buffers/read-ahead, file I/O choice, Iceberg split size |
| JDBC | partitions, fetch size, batch size, rewrite option |
| Iceberg | commit retries, isolation level, target file size |
| Scheduling | dynamic allocation, scheduler mode, locality wait |
| Resilience | speculation, task retries, exclusion |

## Done when you can answer

1. Why can 8 one-core executors beat 4 two-core executors, and vice versa?
2. How do you tell a CPU-bound job from a RustFS-bound one using only UIs?
3. What exactly does Arrow change for Python UDFs?
4. How many Postgres connections can your Spark job open, and why did you pick that limit?
5. What happens when two writers modify the same Iceberg files, and how do you design around it?
