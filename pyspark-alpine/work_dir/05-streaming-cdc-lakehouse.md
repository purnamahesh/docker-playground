# Project 5: Streaming and CDC into Iceberg

## Goal

Build a near-real-time pipeline: **clickstream files in RustFS** and **row changes from Postgres** flow through Structured Streaming into Iceberg bronze, silver and gold, with windows, sessions, CDC apply, deduplication, failure recovery, and ongoing table maintenance.

## Concepts covered

Triggers, checkpoints (stored in RustFS), event time vs processing time, watermarks, output modes, windowed and session aggregates, state stores, deduplication, stream-static and stream-stream joins, `foreachBatch` with MERGE for CDC, ordering and idempotency, Iceberg as streaming sink and source, small files and snapshot growth, concurrent maintenance, end-to-end latency.

## Sources

| Source | How it arrives |
|---|---|
| Clickstream | JSON objects written continuously to the RustFS `landing` bucket, generated around real Instacart baskets (cart-add events in the real `add_to_cart_order` sequence, searches and views, plus sessions that never convert), with out-of-order events, 2% very late events, duplicates and replays |
| Postgres changes | the **lifecycle replay** of Instacart orders on the rebuilt timeline (placed, accepted, picking, item substitutions and removals, delivered or cancelled, plus membership-tier changes and privacy erasures) captured by the trigger-filled `change_log` table (operation, key, before/after image, sequence, changed_at), polled in micro-batches |
| Stretch | real log-based CDC through a connector and a message broker |

## Milestones

### M1. Clickstream into bronze

- Read the landing bucket as a file stream with an explicit schema and a files-per-trigger cap. Write to an Iceberg bronze table with the checkpoint in RustFS.
- Compare triggers: fixed interval, available-now, once.
- Inspect the checkpoint directory (offsets, commits, sources, state) and relate each file to a concept.
- Observe **listing cost** on an object store as the landing bucket grows. Test the file source's cleanup options (archive vs delete) and the effect on listing time.
- Prove exactly-once: stop and restart the query, then compare bronze counts with a batch read of the same objects.
- Watch micro-batch metrics: input rate, processing rate, batch id, duration breakdown.

### M2. Small files and snapshot growth

- After an hour, count data files and snapshots in the bronze table. Relate them to micro-batch frequency and parallelism.
- Run compaction and snapshot expiry **while the stream is running**. Record commit conflicts or retries, and design a maintenance schedule that doesn't disturb the stream.
- Compare fanout writing vs a pre-sorted write for a partitioned sink.
- Define freshness and storage targets, then find the trigger interval that meets both.

### M3. Windowed aggregates and watermarks

- Compute one-minute tumbling and five-minute sliding windows by event type with a ten-minute watermark.
- Find which output modes Iceberg's streaming sink supports, and which are not allowed for this query. Where you need upserts, route through a batch-by-batch MERGE instead.
- Compare watermark lengths (1 minute, 10 minutes, 1 hour): emission delay, dropped late rows, state size.
- Track dropped-by-watermark rows in the progress metrics and reconcile them against the producer's late events.

### M4. Sessionization

- Compute 30-minute-gap sessions per user into a gold Iceberg table: event count, revenue, start and end.
- Verify against a batch computation of the same period.
- Watch state size and memory per micro-batch. The **hot user** concentrates state in one partition; find it in the metrics.
- Stretch: build the same logic with Spark's arbitrary-state API (check availability in PySpark for your Spark version) and compare flexibility, cost and correctness.

### M5. CDC apply into silver

- Poll the change log in micro-batches; take the latest change per key within each batch (by sequence), then MERGE into the silver current-state table.
- Handle inserts, updates, soft deletes and hard deletes. Handle **late and out-of-order changes** with sequence comparison so older changes never overwrite newer ones.
- Make each batch **idempotent**: if a batch is retried after a failure, the result must be unchanged. Record how you track the last applied batch or sequence.
- Compare two designs: apply with MERGE vs append-only changelog plus a "latest per key" read view. When does each win on write cost, read cost and auditability?
- Try incremental reads from an Iceberg table (as a streaming source and through the changelog view). Document which snapshot types (append, overwrite, delete) are supported, and how skip options behave.

### M6. Deduplication and joins

- Deduplicate clickstream by event id: classic dedup with a watermark vs dedup within the watermark. Compare state size using the producer's replays.
- **Stream-static join** against a silver Iceberg dimension that the CDC job keeps updating. Does the stream see updates committed while it runs? Prove it experimentally.
- **Stream-stream join:** views joined to purchases within 30 minutes. Needs watermarks on both sides plus a time-range condition. Test an outer join and note when unmatched rows are emitted. Remove a watermark to see the error and the unbounded state.

### M7. State store and throughput tuning

- Set shuffle partitions, start a stateful query, stop it, change the setting, and restart from the same checkpoint. What happens, and why?
- Compare the default state store with the RocksDB-backed provider for large state: memory, batch time, checkpoint size.
- Raise producer volume until processing falls behind input. Control the backlog with the files-per-trigger cap and the trigger interval.
- Chart batch duration against trigger interval from the recent progress records.

### M8. Failure and evolution

Do each deliberately and record the behavior:

| Failure or change | What to verify |
|---|---|
| Kill a worker | executor loss, retries, no data loss |
| Stop the notebook kernel (driver dies) | restart from checkpoint, exact resume |
| Stop RustFS during a batch | error, retry, state of checkpoint and table |
| Stop Postgres during CDC polling | does the job resume without gaps or duplicates? |
| Add a column to the sink table while the stream runs | does the stream continue? |
| Change the aggregation key or output mode | which errors appear? |

### M9. Latency, SLOs and operations

- Measure **end-to-end freshness**: from a Postgres commit to visibility in a gold table. Break the total into stages (poll, micro-batch, commit, query).
- Define SLOs (freshness, completeness, duplicates) and write the checks that would alert on a breach.
- Combine your maintenance jobs into an operating runbook: compaction cadence, snapshot retention vs checkpoint retention vs time-travel needs, orphan cleanup guardrails.

## Config experiments

| Knob | Values | Watch |
|---|---|---|
| Trigger | 2 s, 10 s, 60 s, available-now | latency vs efficiency |
| Files per trigger | 1, 2, 10 | backlog and batch size |
| Watermark | 1 min, 10 min, 1 h | state, drops, delay |
| Shuffle partitions | 8, 16, 32 | batch time, state layout |
| State store provider | default vs RocksDB | memory, latency |
| Iceberg sink | fanout, target file size, distribution | small files |
| Checkpoint location | RustFS vs shared volume | commit latency |

## Stretch goals

- Add a message broker and log-based CDC. Compare latency, ordering guarantees, and failure behavior with the polled change log.
- Expose metrics to a dashboard (batch duration, state size, backlog, freshness).
- Build a replay-and-compare harness: run the same input through batch and streaming and assert identical results.

## Done when you can answer

1. What does the checkpoint store, and why does that give exactly-once on an Iceberg sink?
2. How does the watermark decide when to emit and drop, and what does a longer one cost?
3. How did you make CDC apply idempotent and order-safe?
4. Why can't the shuffle-partition count change on an existing stateful checkpoint?
5. What did maintenance jobs do to your running stream, and how did you avoid it?
