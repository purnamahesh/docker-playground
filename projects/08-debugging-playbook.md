# Debugging Playbook

Use this when something is slow, wrong or failing. It is organized as **hard gates**, **symptom trees**, **first five checks**, an **error table**, and a **known version issues** page.

The rule behind all of it: **name the bottleneck from evidence before changing any setting.**

---

## 1. Hard gates (stop and fix before continuing)

| Gate | Trigger | Do this |
|---|---|---|
| G1 | A baseline varies more than 10% between runs | find the source of variance (cache warm-up, background load, noisy neighbor containers, shared disk, Postgres autovacuum) before any experiment |
| G2 | A smoke test fails twice | stop; use the FAQ in `00-start-here.md`; do not move to the next bring-up step |
| G3 | Three predictions wrong in a row | re-read the mental model for that area and write down what you misunderstood |
| G4 | A milestone runs 2x over its time-box | switch to its minimal path and park the rest |
| G5 | Counts don't reconcile between two layers | stop building on top; explain every difference first |
| G6 | You can't say which resource is the bottleneck | gather the four views before touching any setting |

---

## 2. Symptom trees

### A. "The job is slow"
1. Open the **Stages tab** for the slow stage. Look at min / median / max task time.
   - **Max is many times the median** -> skew. Go to tree B.
   - **All tasks similar but each is long, with spill or high GC** -> memory pressure. Go to tree C.
   - **Thousands of tiny tasks, short durations, lots of scheduling gaps** -> too many partitions or small files. Reduce shuffle partitions, let AQE coalesce, compact the input.
   - **Few tasks, each large** -> too few partitions or unsplittable large inputs. Increase partitions or rewrite files.
   - **Tasks idle waiting on input** -> the source is the bottleneck. Go to tree D.
2. Check **scaling:** does time drop when you add cores? If not, the limit is storage, the source database, or the driver.
3. Check the **SQL tab:** is a join type or exchange surprising? Is a filter not pushed?

### B. Skew
1. Confirm with the key distribution (top keys' share of rows).
2. Is it a **join**? Try AQE skew handling (check thresholds are low enough for your data), then hot-key isolation or salting.
3. Is it an **aggregation**? AQE skew handling doesn't apply; use two-phase aggregation with salt.
4. Is it a **window**? Partition on something finer, or bound the window.
5. Is it **input skew** (uneven file splits or JDBC ranges)? Change the split strategy, not the shuffle key.
6. Is it **low-cardinality** (few distinct keys across many partitions)? Idle partitions are expected; change the grouping or accept it.

### C. Memory pressure
1. Is spill on all tasks or only a few? Few = skew (tree B). All = partitions too big for memory per task.
2. Increase shuffle partitions, or reduce cores per executor (more memory per task).
3. Reduce row width (select only needed columns early).
4. Check cache usage: is storage memory squeezing execution? Adjust storage fraction or unpersist.
5. High GC share? Reduce object churn (Python UDFs, wide rows) or tune GC.
6. Container killed (exit 137)? The container limit, not Spark, ended it.

### D. Source or storage bottleneck
1. Is Postgres busy? Check active queries, plans, and connection counts. Fix with indexes, fewer partitions, snapshots to Iceberg.
2. Is RustFS saturated (throughput flat while Spark cores idle)? Fewer, larger files; tune S3A or file I/O settings; check request counts.
3. Are many small files the cause? Check file counts in the Iceberg files table; compact.
4. Is listing slow? Large prefixes in file-source streams or non-Iceberg tables.

### E. "The plan looks wrong"
Go to the first five checks below.

### F. "Results are wrong"
1. Reconcile counts layer by layer; find the first layer where they diverge.
2. Check for duplicates (keys, retries, replays) and for rows quarantined by rules.
3. Check the data version: are you reading the snapshot you think you are?
4. Check joins for explosion (row count after join versus before).
5. Check time boundaries and time zones.
6. Check idempotency: run the same step twice and diff.

### G. Streaming is falling behind
1. Compare input rate with processing rate in the progress metrics.
2. Look at the duration breakdown: reading, state, writing, commit.
3. State growing without bound? Watermark missing or too long.
4. Many small files or snapshots slowing commits? Maintenance.
5. File listing time growing? Cleanup of processed files, or partition the input.
6. Cap input per trigger to control the backlog while you fix the cause.

### H. Commit conflicts and failed writes
1. Which jobs wrote to the table at the same time?
2. Is a compaction or expiry running alongside a writer?
3. Do two merges touch overlapping files? Narrow the merge condition or serialize the jobs.
4. Check the isolation level and retry settings.
5. After a failure, list leftover files; plan orphan cleanup with a safe age.

---

## 3. First five checks when a plan looks wrong

1. **Initial vs final adaptive plan.** Which one did you read? AQE may have changed joins, coalesced partitions or split skew.
2. **Pushdown actually applied.** Is the filter in the scan's pushed list, or only re-applied above the scan? For JDBC, did Postgres receive it?
3. **Join type forced by a hint** or by a size estimate? Remove the hint and compare.
4. **Partition and file pruning.** Compare files scanned with total files from the Iceberg metadata tables.
5. **Bytes read versus bytes scanned.** If you read far more than the filter implies, the layout (sort order, file size, statistics) is the cause.

---

## 4. Error table: "if you see this, read this first"

| Error or symptom (key phrase) | Read first | Usual cause |
|---|---|---|
| "Initial job has not accepted any resources" | master UI, resource request | requesting more than a worker has; workers not registered |
| Executor lost / "ExecutorLostFailure" | Executors tab, worker container logs | executor killed (memory, container limit), worker died |
| "java.lang.OutOfMemoryError: Java heap space" | task metrics, partition sizes | partitions too large; skew; broadcast too large |
| Container exit code 137 | container memory limit | container-level OOM kill |
| "FetchFailedException" | shuffle service / worker logs | executor holding shuffle files died; stage retries |
| Broadcast timeout ("Could not execute broadcast in 300 secs") | size of the broadcast table | table too big for broadcast, or slow source |
| "Task not serializable" | the closure referencing a non-serializable object | capturing a session or connection in a function |
| "Py4JJavaError" with a Python traceback | the Python worker's traceback | UDF exception; missing package on executors |
| Python worker "Connection reset" / "Python worker exited unexpectedly" | worker memory, the data volume per group | grouped pandas computation on a huge group |
| "too many clients already" (Postgres) | active connections, partition count | JDBC parallelism above the database limit |
| "canceling statement due to statement timeout" | Postgres settings, the query plan | slow range query, missing index |
| CAST_INVALID_INPUT | the failing value | ANSI mode; use `try_` functions and quarantine |
| Iceberg commit/validation conflict | the jobs that ran together | concurrent writers on overlapping files |
| "No such table" in Iceberg | catalog and namespace names | wrong default catalog |
| Iceberg metadata file not found | RustFS objects, catalog pointer | a cleanup job removed files, or a failed write left the catalog inconsistent |
| S3A 403 or SignatureDoesNotMatch | credentials, region, path-style | credentials set on one path only |
| S3A slow listing / throttling | object counts per prefix | too many small files or huge prefixes |
| Streaming: "Queries with streaming sources must be executed with writeStream.start()" | how the query is run | using a batch action on a streaming frame |
| Streaming: output mode not supported | operation and mode | wrong mode for aggregation or sink |
| Streaming restart fails after a query change | checkpoint compatibility rules | changed state schema, key or partition count |
| Streaming batch duration keeps rising | state size, input backlog, commit time | missing watermark, too-small trigger, small-file buildup |
| History server empty | event log location | logging off or different directories |

---

## 5. Known version issues (consolidated)

These are the facts I verified while writing the guide. Re-verify before relying on them.

### Spark and Iceberg
- Iceberg publishes a Spark runtime **per Spark minor version** (Scala 2.13 for Spark 4.x). The runtime must match your Spark minor.
- Support for Spark 4.2 was merged into Iceberg and appears in the 1.12.0 release notes, but a bug report said the 4.2 runtime artifact was missing from Maven. Check Maven Central yourself. Iceberg 1.11 publishes a Spark 4.1 runtime.
- Spark 4.x runs with ANSI mode on by default, which changes cast and overflow behavior.

### Iceberg defaults (from the Iceberg docs at the time of writing)
- `write.distribution-mode` has no table-level default; for Spark writes the default is **hash** since Iceberg 1.2 (it was none in 1.1 and earlier). Delete and update operations also default to hash; the merge-specific mode is not set by default. When a table has a sort order, range distribution may apply; check your version's Spark writes page.
- `write.parquet.compression-codec`: **zstd** in recent docs; versions 1.3 and earlier used **gzip**.
- Parquet row group size default is 128 MB and page size 1 MB.
- Snapshot expiry defaults: keep 5 days of snapshots, at least 1 snapshot.
- `write.merge.mode`, `write.update.mode`, `write.delete.mode` default to **copy-on-write**; their isolation levels default to **serializable**.
- Manifests merge automatically after 100 manifests (`commit.manifest.min-count-to-merge`).

### RustFS
- RustFS 1.0.0 was announced as generally available in September 2026; the S3 API is on port 9000 and the console on 9001. Pin a release tag; avoid `latest`.
- Its built-in S3 Tables (Iceberg REST catalog) is documented as a preview feature, and an issue reports Spark metadata-table lookups failing with a request-signing mismatch. Use a JDBC catalog for the lab.
- The RustFS Spark integration page was validated on an older Spark and Hadoop pair.

### Standalone Spark
- The memory-overhead setting is accepted but not enforced by any cluster manager in standalone mode.
- Dynamic allocation in standalone needs shuffle tracking.
- Standalone mode doesn't support cluster deploy mode for Python applications (confirm in your version's docs).

### Instacart
- Row counts quoted in the guide for orders, users, prior and train lines come from public descriptions of the dataset; the other facts (placeholder aisle, capped gaps, test-set orders without lines) are from memory. Confirm all of them in Project 1 M0 profiling.

### Not verified (confirm yourself)
- Defaults marked "check" in the configuration map (runtime bloom filter, Arrow settings, S3A pool size, format version).
- Whether a Spark 4.1 and Iceberg combination has known issues with MERGE; search the Iceberg issue tracker before you pin.
