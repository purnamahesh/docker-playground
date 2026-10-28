# Project 5: Streaming and CDC into Iceberg (Instacart)

## Goal

Build a near-real-time pipeline in which clickstream files in RustFS and row changes from Postgres flow through Structured Streaming into Iceberg bronze, silver and gold, with windows, sessions, CDC apply, deduplication, failure recovery, ongoing maintenance and measured freshness.

## Work plan (what happens when)

| Phase | Milestone | Starts when | Produces |
|---|---|---|---|
| A | M0 Sources, speeds and SLOs | Project 1 silver exists | producers, replay settings, targets |
| B | M1 Clickstream ingest to bronze | M0 done | working stream, checkpoint understanding |
| C | M2 Small files and maintenance | M1 running | maintenance schedule compatible with streaming |
| D | M3 to M4 Windows and sessions | M1 done | gold streaming tables |
| E | M5 CDC apply into silver | change log live | idempotent CDC pipeline |
| F | M6 Dedup and joins | M3 done | join and dedup patterns |
| G | M7 State and throughput tuning | stateful queries exist | tuning guide |
| H | M8 Failure and evolution drills | all queries running | recovery runbook |
| I | M9 Latency and SLOs | all above | freshness report |
| J | M10 Synthesis | all above | operating guide |

---

## M0. Sources, replay speeds and service levels

**Objective:** controllable inputs and targets to measure against.

**Do, in order:**
1. **Clickstream producer:** generate JSON sessions around real baskets (cart adds in the real add-to-cart order, searches, views, non-converting sessions). Control event rate, file size, and files per minute. Include the planned defects: duplicates and replays, malformed lines, schema drift partway through, out-of-order events, about 2% very late events.
2. **Write files safely:** write to a temporary name and rename so readers never see a partial file; decide the object naming convention (time-ordered or random).
3. **CDC source:** run the **lifecycle replayer** against Postgres with an adjustable time-compression factor (for example one real hour in one second). Check that the change log fills with ordered, complete entries.
4. **Choose three speeds:** low (below capacity), medium (near capacity) and high (above capacity), so you can study backlog.
5. **Define service levels before building:** freshness (a change visible in gold within N minutes), completeness (no loss), duplicates (none in silver), maximum late-data tolerance.
6. **Prepare reference truth:** a batch computation of the same period for each stream (windows, sessions, current state) to compare against.

**Done when:** producers run at three speeds, the change log is verified, and the service levels are written down.

---

## M1. Clickstream into bronze

**Objective:** ingest files exactly once and understand the checkpoint.

**Do, in order:**
1. Read the landing bucket as a file stream with an **explicit schema** (decide how to treat corrupt lines and the drifted field) and cap files per trigger.
2. Write to an Iceberg bronze table with the checkpoint stored in RustFS.
3. **Inspect the checkpoint** folder: offsets, commits, sources and metadata. Write down what each represents.
4. **Compare triggers:** fixed interval, available-now (process everything, then stop) and once.
5. **Prove exactly-once:** stop and restart the query a few times (also mid-batch), then compare the bronze count with a batch read of the same input objects.
6. Watch the progress metrics: input rate, processing rate, batch id, duration breakdown.
7. **Measure listing cost** on the object store as the landing prefix grows; test the file source's cleanup options (archive or delete processed files) and see the effect on listing time.
8. Decide the **partitioning** of the bronze table (day of ingestion, hour, none) and predict the file counts.

**Options to explore:**

| Variable | Values | Watch |
|---|---|---|
| Trigger | 2 s, 10 s, 60 s, available-now | latency, files per commit, efficiency |
| Files per trigger | 1, 2, 10, unlimited | batch size, backlog |
| Cleanup of processed files | off, archive, delete | listing time, risk |
| Schema handling | strict, permissive, evolve | behavior on drift |

**Traps:** unbounded file listing slows every batch; late-arriving files with older names can be missed if the naming scheme isn't monotonic (decide how the source orders files); cleanup deleting files you may want to replay.

**Done when:** restarts never lose or duplicate; batch metrics are understood; counts match batch reads.

---

## M2. Small files, snapshots and maintenance

**Objective:** keep streaming tables healthy without disturbing the stream.

**Do, in order:**
1. After an hour at medium speed, count data files and snapshots in bronze and relate them to batch frequency and parallelism.
2. Run **compaction and snapshot expiry while the stream runs.** Record commit conflicts or retries.
3. Test sink options: fanout writing for partitioned sinks versus a sorted write; table target file size; distribution mode.
4. Find the **trigger interval** that meets both freshness and file-count targets.
5. Design the **maintenance schedule:** cadence, order (compact, rewrite manifests, expire, remove orphans), safe windows, safety margins relative to the checkpoint.

**Options to explore:**

| Approach | Pro | Con |
|---|---|---|
| Frequent small commits | low latency | many files and snapshots |
| Longer trigger | fewer files | higher latency |
| Compact often in the background | fast reads | conflicts with the writer |
| Write bigger files from the start | fewer maintenance jobs | needs buffering |

**Traps:** orphan removal deleting files of an in-flight commit; expiring snapshots the stream still needs.

**Done when:** a schedule runs while the stream is live, with a recorded conflict count, and bronze stays within file-count targets.

---

## M3. Windowed aggregates and watermarks

**Objective:** correct windowed metrics with explicit late-data behavior.

**Do, in order:**
1. Compute **one-minute tumbling** and **five-minute sliding** counts by event type with a ten-minute watermark.
2. Find which **output modes** Iceberg's streaming sink supports and which are disallowed for this query; where you need upserts, route through a batch-by-batch merge instead.
3. **Sweep the watermark** (1 minute, 10 minutes, 1 hour) and record emission delay, dropped late rows and state size.
4. Reconcile dropped rows with the producer's known late events.
5. Compare tumbling and sliding windows on state size.
6. Compare the stream result with the batch truth.

**Options to explore:**

| Variable | Values | Watch |
|---|---|---|
| Watermark | 1 min, 10 min, 1 h | delay, drops, state |
| Window | tumbling vs sliding | state, output volume |
| Output mode | append, update, complete (where allowed) | behavior and sink support |

**Done when:** the stream result matches batch truth except for late rows beyond the watermark, and you can explain the difference exactly.

---

## M4. Sessionization

**Objective:** sessions per user with controlled state.

**Do, in order:**
1. Compute **30-minute-gap sessions** per user into a gold Iceberg table: events, cart adds, conversions, revenue, start and end.
2. Verify against a batch computation over the same period.
3. Watch state size and memory per batch for different watermark lengths.
4. Find the **hot user's** state partition in the metrics (the bulk buyer).
5. Test session gap values (10, 30, 60 minutes) and see how session counts change.
6. Stretch: implement the same logic with the arbitrary-state API (check availability in PySpark for your Spark version) and compare flexibility, cost and correctness.

**Traps:** sessions that straddle watermark boundaries get emitted late or split; hot keys concentrate state.

**Done when:** streamed sessions equal batch sessions on closed periods; the state growth model is understood.

---

## M5. CDC apply into silver

**Objective:** keep silver current-state tables correct under out-of-order, repeated and deleted changes.

**Do, in order:**
1. **Poll the change log in micro-batches,** using the last applied sequence as the position.
2. In each batch: collapse to the **latest change per key** by sequence; separate inserts, updates, soft deletes and hard deletes.
3. **MERGE into silver** with the match key, a rule that ignores older changes than what the target already holds (sequence comparison), and delete handling.
4. **Make each batch idempotent:** if a batch is replayed after a failure the result must be unchanged. Decide where you record the last applied batch or sequence (a control table, or a table property updated in the same commit).
5. **Test disorder:** shuffle the order of changes in a batch, replay old batches, drop one batch and detect the gap.
6. **Test deletes:** erasure requests (hard deletes plus anonymization) and cascade effects.
7. Compare two designs: **apply with MERGE** versus **append-only changelog plus a latest-per-key view**. Compare write cost, read cost, auditability and storage.
8. Try **incremental reads from an Iceberg table** (as a streaming source and through a changelog view); document which snapshot types (append, overwrite, delete) are supported and how skip options behave.

**Options to explore:**

| Variable | Values | Watch |
|---|---|---|
| Apply design | merge vs append-only log | cost, correctness, audit |
| Batch size | small, large | commit rate, conflict risk |
| Merge mode | copy-on-write vs merge-on-read | write time, read time |
| Ordering key | sequence vs timestamp | tie handling |
| Position storage | control table vs table property | atomicity |

**Traps:** applying changes out of order without a sequence check; a merge that isn't atomic with its position update; changes that arrive for rows not yet seen.

**Done when:** replaying, reordering and dropping batches never corrupts silver, and the final state equals the batch rebuild.

---

## M6. Deduplication and joins

**Objective:** correct streaming dedup and joins with bounded state.

**Do, in order:**
1. **Dedup** clickstream by event id: classic dedup with a watermark versus dedup within the watermark. Test with the producer's replays and compare state size.
2. **Stream-static join** against a silver Iceberg dimension (such as users or products) that the CDC job keeps updating. Does the stream see dimension updates committed while it runs? Prove it experimentally.
3. **Stream-stream join:** views to cart adds to purchases within a window. Requirements: watermarks on both sides and a time-range condition. Try inner and outer joins and note when unmatched rows are emitted.
4. **Break it:** remove a watermark and observe the error or unbounded state.
5. Compare the result with the batch truth.

**Options to explore:** dedup flavor; static-side refresh behavior; join type; time-range width.

**Done when:** each pattern has a documented state size, latency and correctness result.

---

## M7. State store and throughput tuning

**Objective:** find limits and tune.

**Do, in order:**
1. Set the shuffle partition count, start a stateful query, stop it, change the setting and restart from the same checkpoint. Record what happens, and why.
2. Compare the default state store with the **RocksDB-backed** provider for large state: memory, batch time, checkpoint size.
3. Raise the producer rate until processing falls behind input; control the backlog with files-per-trigger and trigger interval.
4. Chart batch duration against trigger interval from the recent progress records.
5. Find the maximum sustainable rate on your cluster and what limits it (CPU, RustFS, commits, state).

**Done when:** you have a maximum sustainable rate per query and know the first bottleneck.

---

## M8. Failure and evolution drills

**Objective:** a recovery runbook.

**Do, in order:** inject each and record behavior, recovery steps, and any data effect.

| Failure or change | What to verify |
|---|---|
| Kill a worker | executor loss, retries, no data loss |
| Stop the notebook kernel (driver dies) | restart from checkpoint, exact resume |
| Stop RustFS during a batch | error, retry, checkpoint and table state |
| Stop Postgres during CDC polling | resumes without gaps or duplicates |
| Add a column to the sink table while running | does the stream continue? |
| Change the aggregation key or output mode | which errors appear? |
| Damage part of the checkpoint | what recovery is possible? |

**Done when:** each drill's outcome matches your prediction, or the surprise is documented.

---

## M9. Latency and service levels

**Objective:** measure freshness end to end.

**Do, in order:**
1. Mark a test change in Postgres and track when it appears in the change log, in the micro-batch, in silver, and in gold.
2. Break the total into stages (poll, batch wait, processing, commit, visibility) and chart each.
3. Compare latency at low, medium and high rates.
4. Write **checks** for each service level (freshness lag, row-count differences versus batch truth, duplicate checks) and test that they fire when you break something.
5. Decide alert thresholds.

**Done when:** freshness is measured per stage and your checks alert correctly.

---

## M10. Synthesis: the operating guide

Document: topology, rates and limits, maintenance schedule, recovery steps, service levels with their checks, and what you'd monitor in production. Link each rule to an experiment id.

## Config experiments (summary)

| Knob | Values | Watch |
|---|---|---|
| Trigger | 2 s, 10 s, 60 s, available-now | latency vs efficiency |
| Files per trigger | 1, 2, 10 | backlog, batch size |
| Watermark | 1 min, 10 min, 1 h | state, drops, delay |
| Shuffle partitions | 8, 16, 32 | batch time, state layout |
| State store provider | default vs RocksDB | memory, latency |
| Iceberg sink | fanout, target file size, distribution | small files |
| Checkpoint location | RustFS vs shared volume | commit latency |

## Stretch goals
- Add a message broker and log-based CDC; compare latency, ordering guarantees and failure behavior with the polled change log.
- Expose streaming metrics to a dashboard (batch duration, state size, backlog, freshness).
- Build a replay-and-compare harness that runs the same input through batch and streaming and asserts identical results.

## Exit criteria for Project 5
1. Silver and gold from the stream equal the batch rebuild on closed periods.
2. Replays, reordering and failures never corrupt data.
3. Freshness is measured and your service levels have working checks.
