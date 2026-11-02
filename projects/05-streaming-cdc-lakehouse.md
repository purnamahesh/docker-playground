# Project 5: Streaming and CDC into Delta Lake (Instacart)

> **Before you start:** read `00-start-here.md` once. Use `07-learning-companion.md` (P5 section) for prerequisites, the minimal path, time-boxes, prediction prompts, concept checks, rubric and teach-back. Use `08-debugging-playbook.md` when stuck and `09-reference-answers.md` for calibration.

## Goal

Build a near-real-time pipeline: clickstream files landing in RustFS and row changes captured from Postgres flow through Structured Streaming into Delta bronze, silver and gold, with windows, sessions, change-data-capture (CDC) apply, failure recovery and measured freshness.

**This is the only project that uses CDC and database triggers.** Earlier projects used watermarks and key comparisons instead.

**Simplified scope:** seven core milestones (M0 to M6) plus an optional Depth block and a short operating-guide step.

## Work plan

| Phase | Milestone | Core? | Produces |
|---|---|---|---|
| A | M0 Sources, speeds and service levels | Core | producers, change log, targets |
| B | M1 Clickstream into bronze | Core | working stream, checkpoint understanding |
| C | M2 Windows and watermarks | Core | windowed metrics |
| D | M3 Sessions | Core | session table |
| E | M4 CDC apply into silver | Core | idempotent change-apply job |
| F | M5 Delta change data feed | Core | change-feed propagation to gold |
| G | M6 Failure drills and recovery | Core | recovery runbook |
| H | M7 Latency and service levels | Core | freshness report |
| I | Depth: maintenance, dedup and joins, state tuning | Optional | extra notes |
| J | M8 Operating guide | Core | one document |

## Terms used in this project

| Term | Meaning |
|---|---|
| **Micro-batch** | Structured Streaming processes new data in small batches |
| **Trigger** | how often a micro-batch starts (fixed interval, or "available now" = process what exists then stop) |
| **Checkpoint** | files that record how far the stream has read and committed, so it can resume exactly |
| **Event time vs processing time** | when the event happened vs when Spark saw it |
| **Watermark (streaming)** | how long Spark waits for late events before closing windows and dropping later data |
| **State** | data Spark keeps between batches (open windows, sessions, dedup keys) |
| **Exactly-once** | each input is reflected in the output once, even across restarts |
| **Idempotent sink** | writing the same batch twice has the same effect as once |
| **CDC** | capturing row changes (insert, update, delete) as events |
| **Change log** | a table in Postgres where triggers record each change |
| **Sequence number** | a strictly increasing number on each change so order is unambiguous |
| **Change data feed (CDF)** | Delta's built-in record of row-level changes between table versions |

---

## M0. Sources, speeds and service levels

**Objective:** controllable inputs and clear targets to measure against.

**Do, in order:**
1. **Clickstream producer.** Generate JSON sessions around real baskets (cart adds in the real add-to-cart order, searches, views, non-converting sessions). Control event rate and file size. Include planned defects: duplicates and replays, malformed lines, schema drift partway, out-of-order events, about 2% very late events. Write each file to a temporary name and rename it, so readers never see a partial file.
2. **Create the change log in Postgres (CDC starts here).** A change log is a table that records every change to a source table. Install **triggers** on the orders (and a few other) tables that insert one row per change into the log. Each log row holds: a sequence number (strictly increasing), table name, operation (I, U or D), the primary key, the row's values before and after, and the time of change. Install triggers **after** any bulk loading so you don't flood the log.
3. **Run the change simulator** against Postgres at an adjustable speed (for example one simulated hour per real second). Check the log is ordered and complete: every update to an order produces exactly one log row.
4. **Choose three speeds:** low (below capacity), medium (near it), high (above it).
5. **Define service levels before building:** *freshness* (a change visible in gold within N minutes), *completeness* (no loss), *duplicates* (none in silver), *late-data tolerance* (how late an event may be and still count).
6. **Prepare batch reference truth:** a batch computation of the same period for windows, sessions and current state, to compare against.

**Example change-log rows (illustrative):**

| seq | table | op | key | after (summary) | changed_at |
|---|---|---|---|---|---|
| 1001 | orders | I | 7788 | status=placed | 10:00:01 |
| 1002 | orders | U | 7788 | status=accepted | 10:02:30 |
| 1003 | orders | U | 7788 | status=delivered | 10:41:12 |
| 1004 | orders | D | 5521 | (erasure) | 10:42:00 |

**Done when:** producers run at three speeds, the change log is verified, and service levels are written down.

**Read more:** PostgreSQL CREATE TRIGGER https://www.postgresql.org/docs/current/sql-createtrigger.html | Spark Structured Streaming guide https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

---

## M1. Clickstream into bronze

**Objective:** ingest files exactly once and understand the checkpoint.

**Do, in order:**
1. Read the landing bucket as a **file stream** with an explicit schema (decide how to treat corrupt lines and the drifted field) and cap the number of files per batch.
2. Write to a Delta bronze table with the **checkpoint stored in RustFS**. A Delta sink supports append (and complete) output modes.
3. **Inspect the checkpoint folder:** offsets (what was planned), commits (what finished), sources, metadata. Write down what each represents.
4. **Compare triggers:** fixed interval vs available-now (process everything then stop).
5. **Prove exactly-once:** stop and restart the query several times (also in the middle of a batch), then compare the bronze count with a batch read of the same objects.
6. Watch progress metrics: input rate, processing rate, batch id, duration breakdown.
7. **Listing cost:** a file stream lists the input folder on every batch. Listing returns about 1,000 keys per request, one page after another, so 10k, 100k and 1M objects need roughly 10, 100 and 1,000 requests. Measure how batch time grows as the prefix grows; try the source's cleanup options (archive or delete processed files).

**Options to explore:**

| Variable | Values | Watch |
|---|---|---|
| Trigger | 2 s, 10 s, 60 s, available-now | latency, files per commit |
| Files per batch | 1, 2, 10, unlimited | batch size, backlog |
| Cleanup of processed files | off, archive, delete | listing time |

**Traps:** unbounded listing slows every batch; deleting files you may want to replay.

**Done when:** restarts never lose or duplicate; batch metrics are understood; counts match batch reads.

**Read more:** Delta streaming https://docs.delta.io/latest/delta-streaming.html | Structured Streaming guide https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

---

## M2. Windows and watermarks

**Objective:** correct windowed metrics with explicit late-data behavior.

**Concepts:** a **window** groups events by event time (for example each minute). The **watermark** is "the latest event time seen minus the allowed lateness". A window is finalized when the watermark passes its end; events older than the watermark are dropped.

**Worked example (illustrative), watermark = 10 minutes:**

| Event time | Latest event time seen | Watermark | Result |
|---|---|---|---|
| 10:05 | 10:30 | 10:20 | **dropped** (older than watermark) |
| 10:22 | 10:30 | 10:20 | kept (within lateness) |
| 10:30 | 10:30 | 10:20 | kept |

**Do, in order:**
1. Compute **one-minute tumbling** counts by event type with a ten-minute watermark; write to a Delta table (append mode emits a window only once the watermark has passed it).
2. Find which output modes your sink supports and which are disallowed for this query. When you need upserts, route through a per-batch MERGE (see M4).
3. **Sweep the watermark** (1 minute, 10 minutes, 1 hour); record how long a window takes to appear, how many late rows are dropped, and state size.
4. Reconcile the dropped rows with the producer's known late events.
5. Compare the stream result with the batch truth.

**Done when:** the stream matches batch except for rows beyond the watermark, and you can explain the difference exactly.

**Read more:** Structured Streaming guide (windows and watermarks) https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

---

## M3. Sessions

**Objective:** sessions per user with controlled state.

**Concepts:** a **session** is a run of events by one user where no gap exceeds a limit (for example 30 minutes). Spark has a built-in session window for this.

**Example (illustrative), gap = 30 minutes:**

| User | Event times | Sessions |
|---|---|---|
| 42 | 10:00, 10:10, 10:25, 11:30, 11:40 | session 1: 10:00 to 10:25; session 2: 11:30 to 11:40 |

**Do, in order:**
1. Compute 30-minute-gap sessions per user into a gold Delta table: events, cart adds, conversions, revenue, start, end.
2. **Verify against a batch computation** over the same closed period.
3. Watch state size and memory per batch. The injected bulk buyer concentrates state in one partition; find it in the metrics.
4. Try gaps of 10, 30 and 60 minutes; see how session counts change.

**Done when:** streamed sessions equal batch sessions on closed periods; you understand how state grows.

**Read more:** Structured Streaming guide (session windows) https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

---

## M4. CDC apply into silver

**Objective:** keep silver current-state tables correct under repeated, out-of-order and deleted changes.

**Why it matters:** the change log tells you *what changed*; applying it correctly means updates must land in order, a replayed batch must not double-apply, and deletes must be honored.

**Out-of-order example (illustrative):** order 7788 changes: seq 1002 (accepted), seq 1003 (delivered). If a batch arrives with 1003 first, then 1002, a naive apply ends with "accepted". The rule: ignore any change whose sequence is **lower than the sequence already applied** for that key.

**Do, in order:**
1. **Poll the change log in micro-batches,** using the last applied sequence as your position.
2. In each batch: **collapse to the latest change per key** (highest sequence); separate inserts, updates and deletes.
3. **MERGE into silver** with the match key, a sequence comparison (don't overwrite newer with older), and delete handling.
4. **Make each batch idempotent.** If a batch is replayed after a failure, the result must be unchanged. Two Delta-native tools: record the last applied sequence **in the same transaction** as the merge; and use Delta's idempotent-write options for per-batch writes (an application id plus the batch id as a transaction version, so Delta ignores a repeated batch).
5. **Test disorder:** shuffle changes within a batch; replay an old batch; drop one batch and detect the gap.
6. **Test deletes:** erasure requests (hard delete plus anonymize) and their cascade.
7. Compare two designs: **apply with MERGE** vs **append-only changes plus a latest-per-key view** (cost, audit, storage).

**Options to explore:**

| Variable | Values | Watch |
|---|---|---|
| Apply design | MERGE vs append-only log + view | write cost, read cost, audit |
| Batch size | small / large | commit rate |
| Update mechanism | rewrite files vs deletion vectors | write and read time |

**Traps:** applying changes without a sequence check; a MERGE that isn't atomic with its position update; changes for rows not yet seen; running the CDC job and a maintenance job as two separate applications on the same table (single-writer rule).

**Done when:** replaying, reordering and dropping batches never corrupts silver, and the final state equals the batch rebuild.

**Read more:** Delta MERGE https://docs.delta.io/latest/delta-update.html | Delta streaming (foreachBatch, idempotent writes) https://docs.delta.io/latest/delta-streaming.html | Idempotence https://en.wikipedia.org/wiki/Idempotence

---

## M5. Delta change data feed (the Delta-native alternative)

**Objective:** propagate changes from silver to gold using Delta's own change record.

**Concepts:** if a Delta table has the change data feed enabled, Delta records, for each commit, which rows were inserted, updated (before and after images) and deleted. You read it with a version or timestamp range. Each change row carries a change type (insert, update before, update after, delete), the commit version and commit time.

**Do, in order:**
1. Enable the change data feed on a silver table (it records changes only from that point on).
2. Update and delete some rows; **read the change feed** for those versions and match each change to what you did.
3. Build a small streaming or batch job that reads the feed and maintains a gold aggregate incrementally.
4. Compare with M4: what does the feed give you that the change log doesn't (it works at the table level, needs no triggers), and what does it not (it only exists for Delta tables; retention is tied to table history).
5. Check how the feed interacts with deletion vectors and VACUUM in your version (feed data can disappear after VACUUM).

**Traps:** reading versions that VACUUM has already cleaned; assuming the feed contains history from before you enabled it.

**Done when:** gold matches a full recompute after a series of updates and deletes, using only the change feed.

**Read more:** Delta change data feed https://docs.delta.io/latest/delta-change-data-feed.html | Delta streaming (reading from a Delta table; skipping change commits) https://docs.delta.io/latest/delta-streaming.html

---

## M6. Failure drills and recovery

**Objective:** a recovery runbook.

**Do, in order:** inject each failure, predict the outcome first, then record behavior, recovery steps and any data effect.

| Failure | What to verify |
|---|---|
| Kill a worker | executor loss, retries, no data loss |
| Stop the notebook kernel (the driver dies) | restart from checkpoint; exact resume; no duplicates |
| Stop RustFS during a batch | error, retry; checkpoint and table state afterward |
| Stop Postgres during change-log polling | resumes without gaps or duplicates |
| Change the query (add a column, change the aggregation key) | which changes are allowed with the old checkpoint, which errors appear |

**Done when:** each outcome matches your prediction, or the surprise is documented.

**Read more:** Structured Streaming guide (recovery semantics) https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

---

## M7. Latency and service levels

**Objective:** measure freshness end to end.

**Do, in order:**
1. Mark a test change in Postgres and track it: appears in the change log, in the micro-batch, in silver, in gold. Record the time at each point.
2. Break the total into stages: polling delay, waiting for the next batch, processing, commit, visibility to readers.
3. Compare latency at low, medium and high rates.
4. Write **checks** for each service level (freshness lag, row-count differences versus batch truth, duplicate counts) and confirm they fire when you deliberately break something.
5. Decide alert thresholds.

**Done when:** freshness is measured per stage and your checks alert correctly.

---

## Depth (optional)

- **Maintenance while streaming:** run OPTIMIZE (stream-safe) and later VACUUM against a streaming sink. **Remember the single-writer rule:** run maintenance inside the same application as the stream (another thread or query in the same session), or while the stream is stopped, never as a second application. A stream that lags behind the VACUUM retention can lose the files it still needs.
- **Dedup and joins:** deduplicate events by id within the watermark; join the stream to a static dimension table; try a stream-to-stream join (views to purchases within a time range) and see why both sides need watermarks.
- **State tuning:** change shuffle partitions on an existing stateful checkpoint (it won't work, and the error explains why); compare the default state store with the RocksDB-backed one for large state; raise the producer rate until processing falls behind and use a files-per-batch cap.

**Read more:** Delta streaming https://docs.delta.io/latest/delta-streaming.html | Delta optimizations https://docs.delta.io/latest/optimizations-oss.html

---

## M8. The operating guide

Write one document: the topology, rates and limits you measured, the maintenance schedule (and where it runs), recovery steps, service levels with their checks, and what you would monitor in production. Link each rule to an experiment id.

## Config experiments (summary)

| Knob | Values | Watch |
|---|---|---|
| Trigger | 2 s, 10 s, 60 s, available-now | latency vs efficiency |
| Files per batch | 1, 2, 10 | backlog |
| Watermark | 1 min, 10 min, 1 h | state, drops, delay |
| Delta sink | target file size, optimized writes (check) | small files |
| Checkpoint location | RustFS | commit latency |

## Exit criteria for Project 5
1. Silver and gold from the stream equal the batch rebuild on closed periods.
2. Replays, reordering and failures never corrupt data.
3. Freshness is measured and your service levels have working checks.
