# Configuration Map

A single reference for the settings that matter most in this lab, grouped by what they control. Defaults are given as I recall them for recent Spark 3.x/4.x and Iceberg releases. **Verify each one** in the Spark UI Environment tab, the Spark docs for your exact version, and the Iceberg docs for your runtime. Rows marked "check" are ones whose defaults or names I'm unsure of or that vary by version.

Legend: ★ means learn first. The last column points to the project that exercises it.

---

## 1. How to explore settings yourself

- The Spark SQL `SET -v` command lists every SQL configuration with its default and description.
- Read a single value with the session's configuration getter, or list everything the application started with through the context's configuration.
- The **Environment tab** in the Spark UI is the source of truth for what took effect.
- For Iceberg, table properties are visible in the table's properties listing, and effective file, manifest and snapshot facts are in the metadata tables.
- Docs to read end to end: Spark Configuration, SQL Performance Tuning, Tuning Guide, Standalone mode, Structured Streaming guide, the JDBC data source page, Hadoop's S3A guide, and Iceberg's Spark configuration, Spark writes, Spark queries, Spark procedures, and table configuration pages.

### Where settings come from (highest priority first)

1. Code (session builder or runtime set)
2. Submit flags
3. The defaults file
4. Built-in defaults

Hadoop and S3A settings passed through Spark need the `spark.hadoop.` prefix. Worker-level settings in standalone (cores, memory, work directory) are **environment variables on the worker**, not Spark configs.

### Three kinds of settings

| Kind | Examples | How to change |
|---|---|---|
| Runtime SQL | shuffle partitions, AQE, broadcast threshold | change on a live session |
| Static (session start) | executor memory/cores, `spark.cores.max`, serializer, catalog definitions, extensions | new session |
| Driver JVM | driver memory, driver Java options | restart the kernel |

---

## 2. Resources and topology

| Config | Default | Meaning | Where |
|---|---|---|---|
| ★ `spark.executor.cores` | all worker cores (standalone) | cores per executor | P4 |
| ★ `spark.executor.memory` | 1g | executor heap | P4 |
| ★ `spark.cores.max` | unlimited | total cores an app may use | P4 |
| ★ `spark.driver.memory` | 1g | driver heap (collect, broadcast build) | P2 |
| `spark.driver.maxResultSize` | 1g | cap on results returned to the driver | P1 |
| `spark.executor.memoryOverhead` | max(10%, 384 MB) | native overhead; **not enforced in standalone** | P4 |
| `spark.default.parallelism` | total cores | RDD shuffle partitions | P4 |

## 3. Memory

| Config | Default | Meaning | Where |
|---|---|---|---|
| ★ `spark.memory.fraction` | 0.6 | heap share for execution and storage | P4 |
| ★ `spark.memory.storageFraction` | 0.5 | part of that protected for cache | P4 |
| `spark.memory.offHeap.enabled` / `.size` | false / 0 | off-heap memory | P4 |
| `spark.executor.extraJavaOptions` | none | GC and JVM flags | P4 |
| `spark.executor.pyspark.memory` | unset | Python worker memory limit | P4 |

## 4. Shuffle

| Config | Default | Meaning | Where |
|---|---|---|---|
| ★ `spark.sql.shuffle.partitions` | 200 | partitions after a shuffle | P1, P2, P5 |
| `spark.io.compression.codec` | lz4 | shuffle and spill compression | P4 |
| `spark.shuffle.file.buffer` | 32k | shuffle write buffer | P4 |
| `spark.reducer.maxSizeInFlight` | 48m | shuffle fetch buffer | P4 |
| `spark.local.dir` | /tmp | shuffle and spill location | P4 |

## 5. Adaptive Query Execution

| Config | Default | Meaning | Where |
|---|---|---|---|
| ★ `spark.sql.adaptive.enabled` | true | master switch | P2, P3 |
| `spark.sql.adaptive.coalescePartitions.enabled` | true | merge small shuffle partitions | P2 |
| ★ `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 64MB | target partition size | P2 |
| `spark.sql.adaptive.coalescePartitions.initialPartitionNum` | unset | starting partition count | P2 |
| ★ `spark.sql.adaptive.skewJoin.enabled` | true | split skewed partitions in sort-merge joins | P2 |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | 5 | skew ratio vs median | P2 |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | 256MB | minimum size to count as skewed | P2 |
| `spark.sql.adaptive.localShuffleReader.enabled` | true | skip network after join switch | P2 |

## 6. Joins

| Config | Default | Meaning | Where |
|---|---|---|---|
| ★ `spark.sql.autoBroadcastJoinThreshold` | 10MB | auto-broadcast limit (-1 disables) | P2 |
| `spark.sql.broadcastTimeout` | 300s | broadcast wait | P2 |
| `spark.sql.join.preferSortMergeJoin` | true | prefer sort-merge over shuffle-hash | P2 |
| Join hints | n/a | broadcast, merge, shuffle-hash, shuffle-replicate-NL | P2 |
| ★ Storage-partitioned join settings (`spark.sql.sources.v2.bucketing.*`, Iceberg's data-grouping preservation, co-partition rules) | check | enable shuffle-free joins on identically bucketed tables | P2 |

## 7. File sources and general writes

| Config | Default | Meaning | Where |
|---|---|---|---|
| `spark.sql.files.maxPartitionBytes` | 128MB | scan partition size for file sources (not Iceberg tables) | P3, P4 |
| `spark.sql.files.openCostInBytes` | 4MB | packing cost per file | P3 |
| `spark.sql.parquet.compression.codec` | snappy | Parquet output compression for non-Iceberg writes | P1 |
| `spark.sql.parquet.filterPushdown` | true | push filters into Parquet | P3 |
| `spark.sql.sources.partitionOverwriteMode` | static | `dynamic` overwrites only touched partitions | P1 |
| `spark.sql.files.ignoreCorruptFiles` | false | skip unreadable files | P1 |

## 8. Optimizer and planning

| Config | Default | Meaning | Where |
|---|---|---|---|
| `spark.sql.cbo.enabled` | false | use statistics | P3 |
| `spark.sql.cbo.joinReorder.enabled` | false | cost-based join order | P3 |
| `spark.sql.statistics.histogram.enabled` | false | histograms | P3 |
| `spark.sql.optimizer.dynamicPartitionPruning.enabled` | true | prune via joins | P3 |
| `spark.sql.optimizer.runtime.bloomFilter.enabled` | check | bloom-filter runtime filter | P3 |
| `spark.sql.codegen.wholeStage` | true | whole-stage codegen | P3 |
| `spark.sql.codegen.maxFields` | 100 | wide schemas skip codegen | P3 |
| `spark.sql.optimizer.excludedRules` | none | disable specific rules (experiments only) | P3 |
| `spark.sql.debug.maxToStringFields` | 25 | stops plans being truncated | P3 |

## 9. Python and Arrow

| Config | Default | Meaning | Where |
|---|---|---|---|
| ★ `spark.sql.execution.arrow.pyspark.enabled` | check | Arrow for pandas conversion | P4 |
| `spark.sql.execution.arrow.maxRecordsPerBatch` | 10000 | rows per Arrow batch | P4 |
| `spark.sql.execution.pythonUDF.arrow.enabled` | check | Arrow-optimized Python UDFs | P4 |
| `spark.python.worker.reuse` | true | reuse Python workers | P4 |

## 10. Scheduling and dynamic allocation

| Config | Default | Meaning | Where |
|---|---|---|---|
| `spark.scheduler.mode` | FIFO | FIFO or FAIR within an app | P4 |
| ★ `spark.dynamicAllocation.enabled` | false | scale executors | P4 |
| `spark.dynamicAllocation.shuffleTracking.enabled` | false | required without an external shuffle service | P4 |
| `spark.dynamicAllocation.minExecutors` / `maxExecutors` | 0 / unlimited | bounds | P4 |
| `spark.dynamicAllocation.executorIdleTimeout` | 60s | release idle executors | P4 |
| `spark.locality.wait` | 3s | wait for a data-local slot | P4 |
| `spark.deploy.spreadOut` | true | master spreads executors across workers | P4 |

## 11. Fault tolerance and networking

| Config | Default | Meaning | Where |
|---|---|---|---|
| ★ `spark.speculation` (+ multiplier, quantile) | false | re-run slow tasks | P4 |
| `spark.task.maxFailures` | 4 | task retries | P4 |
| `spark.stage.maxConsecutiveAttempts` | 4 | stage retries | P4 |
| `spark.excludeOnFailure.enabled` | false | stop using failing executors/nodes | P4 |
| `spark.network.timeout` | 120s | general RPC timeout | P4 |
| `spark.executor.heartbeatInterval` | 10s | must be well below the network timeout | P4 |

## 12. SQL behavior

| Config | Default | Meaning | Where |
|---|---|---|---|
| ★ `spark.sql.ansi.enabled` | true in 4.x | strict casts and overflow errors; use `try_*` | P1 |
| `spark.sql.session.timeZone` | JVM zone | timestamp interpretation | P1 |
| `spark.sql.legacy.*` | varies | old-behavior switches | P1 |

---

## 13. JDBC source and sink (Postgres)

| Option | Default | Meaning | Where |
|---|---|---|---|
| ★ `numPartitions` | unset | read/write parallelism and max concurrent connections | P1, P4 |
| ★ `partitionColumn`, `lowerBound`, `upperBound` | unset | range split for parallel reads | P1 |
| ★ `fetchsize` | driver default | rows per round trip; affects executor memory | P1, P4 |
| `batchsize` | 1000 | rows per insert batch when writing | P4 |
| `dbtable` vs `query` | n/a | table or pushed-down subquery | P1, P3 |
| Predicate list per partition | n/a | custom partitioning by where-clauses | P1 |
| `pushDownPredicate`, `pushDownAggregate`, `pushDownLimit`, `pushDownOffset`, `pushDownTableSample` | check | which operators run inside Postgres | P3 |
| `isolationLevel`, `queryTimeout`, `sessionInitStatement` | varies | transaction and session behavior | P4 |
| `truncate`, `createTableOptions` | varies | overwrite semantics | P4 |
| Driver URL options (for example batched-insert rewriting) | varies | driver-side performance | P4 |

**Postgres-side settings to know:** maximum connections, shared buffers, work memory, slow-query logging, query statistics extension, and the WAL level (must be raised for any log-based CDC).

## 14. Object storage (S3A and MinIO)

All set via the `spark.hadoop.` prefix.

| Setting | Meaning | Where |
|---|---|---|
| ★ endpoint, path-style access, SSL flag, access key, secret key | required for MinIO | setup |
| ★ connection pool maximum (`fs.s3a.connection.maximum`) | concurrent HTTP connections | P4 |
| `fs.s3a.threads.max` | worker threads for transfers | P4 |
| `fs.s3a.fast.upload` and buffer mode (disk, array, bytebuffer) | upload buffering | P4 |
| `fs.s3a.multipart.size` and thresholds | multipart upload sizing | P4 |
| `fs.s3a.block.size` | split size for reads | P4 |
| `fs.s3a.readahead.range` | read-ahead window | P4 |
| Input fadvise policy (name varies by Hadoop version) | sequential vs random reads for columnar data | P4 |
| Iceberg S3 file I/O settings (endpoint, path-style, credentials) | alternative to S3A | P4 |

## 15. Iceberg

### Catalog and session

| Config | Meaning | Where |
|---|---|---|
| ★ `spark.sql.extensions` (Iceberg SQL extensions) | enables MERGE, procedures, branches, DDL extensions | setup |
| ★ `spark.sql.catalog.<name>` and its type/implementation, warehouse, uri, file I/O implementation | defines the catalog | setup |
| `spark.sql.defaultCatalog` | default catalog for unqualified names | setup |
| JDBC catalog connection settings | where catalog metadata lives | setup |

### Table properties (set at creation or by ALTER)

| Property | Default | Meaning | Where |
|---|---|---|---|
| `format-version` | check | table format features (row-level deletes need v2+) | P1 |
| ★ `write.target-file-size-bytes` | 512MB | target data file size | P1, P4 |
| `write.format.default` | parquet | file format | P1 |
| `write.parquet.compression-codec` | check | compression | P1, P3 |
| `write.parquet.row-group-size-bytes`, `write.parquet.page-size-bytes` | check | Parquet layout | P3 |
| `write.parquet.bloom-filter-enabled.column.<col>` | off | column bloom filters | P3 |
| ★ `write.distribution-mode` | check | shuffle before write: none, hash, range | P2 |
| ★ `write.merge.mode`, `write.update.mode`, `write.delete.mode` | copy-on-write | copy-on-write vs merge-on-read | P1, P2 |
| `write.merge.isolation-level` (and update/delete equivalents) | serializable | conflict detection strictness | P4 |
| `commit.retry.num-retries` and related wait settings | 4 | optimistic commit retries | P4 |
| `write.spark.fanout.enabled` | false | fanout writers for streaming to partitioned tables | P5 |
| `read.split.target-size` | 128MB | scan task size | P4 |
| `read.parquet.vectorization.enabled` | check | vectorized reads | P3 |
| `history.expire.max-snapshot-age-ms`, `history.expire.min-snapshots-to-keep` | check | default snapshot retention | P1 |
| `write.metadata.delete-after-commit.enabled`, `write.metadata.previous-versions-max` | check | metadata file cleanup | P1 |
| `commit.manifest.min-count-to-merge`, manifest merge enabled | check | manifest compaction on commit | P3 |

### Procedures and metadata tables (names to learn)

- **Procedures:** rewrite data files (bin-pack, sort, z-order), rewrite manifests, rewrite position-delete files, expire snapshots, remove orphan files, rollback to snapshot or timestamp, create changelog view, add files, migrate/snapshot.
- **Metadata tables:** snapshots, history, files, data files, delete files, manifests, partitions, refs, entries.

---

## 16. Streaming

| Config / option | Default | Meaning | Where |
|---|---|---|---|
| ★ Checkpoint location | none | offsets, commits, state | P5 |
| ★ Trigger | as fast as possible | fixed interval, available-now | P5 |
| Files-per-trigger (file source option) | unlimited | backpressure | P5 |
| File source cleanup options | off | archive or delete processed files | P5 |
| `spark.sql.streaming.stateStore.providerClass` | HDFS-backed | RocksDB provider for large state | P5 |
| `spark.sql.shuffle.partitions` | 200 | **fixed per stateful checkpoint** | P5 |
| `spark.sql.streaming.stateStore.maintenanceInterval` | 60s | state cleanup cadence | P5 |
| Iceberg streaming read options (start timestamp, skip overwrite/delete snapshots, rate limits) | varies | incremental source behavior | P5 |

## 17. Observability

| Config | Default | Meaning |
|---|---|---|
| ★ `spark.eventLog.enabled` / `spark.eventLog.dir` | false | write logs for the history server (can point at a MinIO bucket) |
| `spark.history.fs.logDirectory` | n/a | history server input |
| `spark.ui.port` | 4040 | live UI port |

---

## 18. Study method: one card per setting

For each ★ setting, write a card after one experiment, with these lines:

- **Setting and default**
- **What it controls**
- **Too low / too high**
- **How I observed it** (which UI, which metadata table, which MinIO or Postgres metric)
- **Value I chose and why**
- **Interacts with** (the settings that change its effect)

### Suggested learning order (about 20 settings cover most real tuning)

1. Shuffle partitions, the AQE switch, advisory partition size
2. Auto-broadcast threshold and the skew join settings
3. Executor cores, executor memory, `spark.cores.max`
4. Memory fraction and storage fraction
5. JDBC partitioning and fetch size
6. Iceberg target file size, distribution mode, merge mode
7. S3A connection pool and Iceberg split size
8. Arrow settings and the UDF type
9. Dynamic allocation with shuffle tracking, and speculation
10. Streaming: checkpoint, trigger, watermark, state provider
11. Event logs so every experiment is comparable in the history server

### The rule behind most tuning

Before touching a setting, name the bottleneck from the UIs: **skew** (uneven tasks), **spill** (memory), **too many or too small tasks** (partitions), **too much shuffle** (plan), **GC**, **Python overhead**, **object-store throughput** (MinIO), **source database load** (Postgres), or **commit conflicts** (Iceberg). Then change the setting that targets that bottleneck. Changing settings without a diagnosis is how clusters collect cargo-cult configuration.
