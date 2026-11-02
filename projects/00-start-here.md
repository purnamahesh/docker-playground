# Start Here

Read this before the README. It gets you from zero to a verified, working platform with **feedback at every step**, so that when you reach Instacart you already know what "healthy" looks like.

Contents: how the lab teaches, prerequisite self-check, the 30-minute hello world, expected output for every bring-up step, and a troubleshooting FAQ.

---

## 1. How this lab teaches (the loop you repeat in every milestone)

1. **Predict.** Before running, write down: (a) which resource this stresses (CPU, memory, storage, source database, network), (b) what you expect the plan or behavior to be, (c) which number you expect to move, and by roughly how much.
2. **Run once, change one thing.**
3. **Observe four views:** Spark UI, Iceberg metadata, RustFS console, PostgreSQL.
4. **Name the bottleneck** from evidence before touching any setting.
5. **Record** the experiment (Experiments tab in the tracker).
6. **Reflect:** did the prediction match? If not, what was wrong with your mental model? Write that down; it is the actual learning.

Companion files:

| File | Use |
|---|---|
| `README.md` | architecture, dataset, gates, conventions |
| `01` to `05` | the five projects |
| `06-spark-configuration-map.md` | configs by category |
| `07-learning-companion.md` | per project: prerequisites from earlier projects, minimal path, time-boxes, prediction prompts, concept checks, rubrics, teach-back, retrospective |
| `08-debugging-playbook.md` | symptom trees, first-five-checks, error table, hard gates, known version issues |
| `09-reference-answers.md` | worked experiments, expected plan fingerprints, magnitude calibration |
| Tracker workbook | progress, experiments, defects, decisions, rubrics, concept checks, retrospectives |

---

## 2. Prerequisite self-check

Answer these **without looking anything up**. Write short answers. Check against the key at the end of this file.

### Spark
1. What does a shuffle do, and name two operations that trigger one.
2. What is the difference between a transformation and an action?
3. What is a partition, and what decides how many tasks a stage has?
4. What runs on the driver and what runs on executors?
5. Why can a single slow task delay a whole stage?

### SQL and data modeling
6. When would you use a window function instead of GROUP BY?
7. What is the difference between an inner join and a left anti join?
8. What does a primary key guarantee that a unique index might not?

### Python and tooling
9. What is a UDF and why can it be slow in PySpark?
10. What does a virtual environment isolate, and where does a notebook kernel get its packages?

### Containers
11. Two containers on the same compose network: how does one reach the other, and why does "localhost" fail?
12. What does a bind-mounted volume do, and what happens to files written outside it when a container is removed?

### Lakehouse and storage
13. What is an Iceberg snapshot, and what is a manifest?
14. How does an object store differ from a file system (rename, listing, directories)?
15. Why can an index make one query fast and another query on the same table slow?

**Scoring:** 13 to 15 correct: go ahead. 9 to 12: read short primers for the topics you missed, then continue. Below 9: spend a few days on a Spark and SQL fundamentals primer first; this lab assumes you can read a query plan's vocabulary.

---

## 3. The 30-minute hello world

**Purpose:** prove the whole path on a tiny dataset and capture **reference numbers** that you will use later to judge whether your Instacart numbers are sane.

**The path:** one small table in Postgres -> one Parquet extract in RustFS landing -> one Iceberg table in the warehouse bucket -> a count query. Then do the four-views check, and repeat once.

### Steps

| # | Do | Expected result | Check in |
|---|---|---|---|
| 1 | In the source database create a table with about 1M rows (an id, a number, a category with few distinct values, a timestamp) and an index on the id | row count is exactly 1,000,000 | Postgres |
| 2 | Start a Spark session with the platform's settings | the application appears under Running Applications; the Executors tab lists the driver plus your executors | Spark master UI, app UI |
| 3 | Read the table over JDBC with **one** partition and count it | count 1,000,000; one task in the stage; one connection to Postgres while it runs | Spark UI Stages, Postgres active connections |
| 4 | Read again with **four** partitions on the id column | same count; four tasks; up to four connections; roughly equal rows per task | Spark UI Stages, Postgres |
| 5 | Write the four-partition read to landing as Parquet | a folder of Parquet files plus a success marker in the landing bucket; file count equals partitions with data | RustFS console |
| 6 | Create an Iceberg table in a bronze namespace and insert the landing data | one new snapshot of type append | Iceberg metadata |
| 7 | Run a count over the Iceberg table | 1,000,000 | Spark UI SQL tab |
| 8 | **Repeat steps 6 and 7 as an overwrite** of the same data | count still 1,000,000; two snapshots now exist | Iceberg metadata |
| 9 | Query the first snapshot by id (time travel) | the old state is returned | Spark |
| 10 | Stop the session | the application moves to Completed and appears in the history server | history server |

### Four-views checklist (what "healthy" looks like)

| View | You should see |
|---|---|
| **Spark UI** | each action is one job; the SQL tab shows a JDBC scan for the read and an Iceberg scan for the count; task counts equal the partition counts you chose; no failed tasks; no spill |
| **Iceberg metadata** | snapshots table with one row per write (append, then overwrite); files table listing a few Parquet files with row counts that sum to the table size; a manifests table with at least one entry |
| **RustFS console** | under the warehouse bucket, the table folder has a data folder with Parquet files and a metadata folder with a metadata JSON file, a manifest list and manifest files; the landing bucket has your extract |
| **Postgres** | during step 4 several active connections from Spark, each running a range query on the id column; none left idle afterward |

### Capture your reference numbers

Record these in the tracker's Environment tab or Experiments tab. They are your personal baselines:

- JDBC read throughput per connection (rows per second) for 1 and 4 partitions.
- Time to write the 1M rows as Parquet and as Iceberg.
- Parquet size on disk for the 1M rows (bytes per row).
- Number of files created by the Iceberg write, and their sizes.
- Spark job overhead for a trivial query (seconds of fixed cost).

**Why they matter:** later, if W1's scan takes 100 times longer than your hello-world extrapolation, something is wrong in the environment, not in your tuning.

### Hello-world "break it" drills (10 minutes, optional but recommended)

1. Read with 1 partition then 8 partitions on a machine with 8 cores; note where more partitions stop helping.
2. Remove the index on the id column and repeat step 4; watch the Postgres plan change and the extract slow down.
3. Stop RustFS briefly during a write and see what the error looks like before it ever happens during a real run.

---

## 4. Expected output for each environment bring-up step

Use this to tell "healthy" from "broken". Numbers assume the layout in the README (4 workers, 2 cores, 2 GB each).

| # | Step | What you should see |
|---|---|---|
| 1 | RustFS | console login page loads on the console port; after login you can create buckets; the five buckets appear in the list; an S3 client listing returns the same five names |
| 2 | PostgreSQL | you can connect from your laptop and from inside the Jupyter container; listing databases shows both the source and the catalog database; connection limit and memory settings match what you chose |
| 3 | Spark master | "Alive Workers: 4"; "Cores in use: 8 Total, 0 Used"; "Memory in use: 8.0 GiB Total, 0.0 B Used" (or your values); each worker row shows state ALIVE, 2 cores, 2.0 GiB |
| 4 | Jars and session | the session starts without class-loading errors; the app appears under Running Applications with executors; the Environment tab lists the Iceberg extension and catalog settings you configured; the Executors tab shows the number of executors your layout implies |
| 5 | Catalog | creating a namespace succeeds; listing namespaces shows it; dropping it succeeds; with a JDBC catalog, new rows appear in the catalog database's tables |
| 6 | Iceberg smoke test | after create and insert: the snapshots metadata table has one row of operation append; the files table lists the data file(s); RustFS shows the data and metadata objects |
| 7 | S3A smoke | a Parquet file written to an s3a path is readable back with the same row count; the object is visible in the console |
| 8 | JDBC smoke | the 1M-row read with 4 partitions returns the right count; Postgres shows up to four connections during the read |
| 9 | History server | after stopping the session, the application appears in the history server with all its jobs and the SQL tab; event log object visible in the spark-events bucket |
| 10 | Hello pipeline | the second run leaves the table unchanged (same count, same content) |

**If any step fails twice, stop and use the FAQ below before moving on.** (See also the hard gates in `08-debugging-playbook.md`.)

---

## 5. Troubleshooting FAQ (environment failures)

Messages vary slightly between versions; match on the **key phrase**.

| # | Symptom / key phrase | Likely cause | Fix |
|---|---|---|---|
| 1 | "Cannot find catalog plugin class for catalog ... SparkCatalog" | Iceberg runtime jar not on the driver classpath, or catalog config missing | Make sure the runtime jar is present **before** the session starts (jars are read when the JVM launches); restart the kernel and start a fresh session |
| 2 | NoSuchMethodError or IncompatibleClassChangeError inside Iceberg or Spark classes | Iceberg runtime built for a different Spark minor version | Use the runtime artifact that matches your Spark minor version and Scala version (Gate A) |
| 3 | "ClassNotFoundException: Class org.apache.hadoop.fs.s3a.S3AFileSystem not found" | hadoop-aws jar missing | Add the hadoop-aws jar matching your Hadoop version to driver and executors |
| 4 | NoClassDefFoundError for AWS SDK classes | AWS SDK bundle missing or the wrong generation for your hadoop-aws version | Use the SDK bundle the hadoop-aws release expects |
| 5 | "No suitable driver" or ClassNotFoundException for org.postgresql.Driver | JDBC driver jar not available on the executors, or malformed URL | Add the driver to the driver and executor classpath; verify the URL starts with the PostgreSQL scheme |
| 6 | UnknownHostException with a host name that looks like "bucketname.endpoint" | virtual-host style addressing against a single-host endpoint | Enable path-style access (for S3A and for Iceberg file I/O) |
| 7 | 403 Forbidden, "SignatureDoesNotMatch", or "The AWS Access Key Id you provided does not exist" | wrong credentials, credentials set for one access path only, or region/signing mismatch | Set credentials for **both** S3A and Iceberg's file I/O; confirm the same keys work in the console; check the region value |
| 8 | "Connection refused" from executors, works from the driver | endpoint set to localhost | Use the container service name on the compose network |
| 9 | "Initial job has not accepted any resources" | asking for more memory or cores than a worker offers (for example executor memory above 2 GB), or workers not registered | Lower the request or fix worker registration; check the master UI |
| 10 | Executors start then fail with "Failed to connect to" the driver | driver host not resolvable or port blocked from workers | Set the driver's advertised host and bind address; ensure the Jupyter container name resolves from workers |
| 11 | "Table or view not found" for an Iceberg table | wrong default catalog or unqualified name | Use catalog.namespace.table, or set the default catalog |
| 12 | FileNotFound on a path that exists in the console | scheme confusion (s3 vs s3a), wrong bucket in path | Use s3a paths for Spark file reads and the catalog warehouse path for Iceberg |
| 13 | "Cannot modify the value of a Spark config: spark.executor.memory" | resource settings changed on a running session | Stop the session and start a new one with the setting |
| 14 | Driver memory setting has no effect | driver JVM already running in the kernel | Restart the kernel before the first session |
| 15 | PSQLException "sorry, too many clients already" | more concurrent JDBC connections than Postgres allows | Reduce partitions, or raise the limit deliberately (and understand the memory cost) |
| 16 | "Java gateway process exited before sending its port number" | Java missing or wrong version | Check the Java version and the java executable on the path |
| 17 | UnsupportedClassVersionError | class compiled for a newer Java than you run | Use the Java version your Spark and jars require |
| 18 | Container exit code 137 | killed for exceeding the container memory limit | Raise the container limit or lower executor memory |
| 19 | CAST_INVALID_INPUT with a bad string value | Spark 4 ANSI mode is on | Use `try_cast` and quarantine failures (a Project 1 lesson, not a bug) |
| 20 | Iceberg commit fails with a validation or conflict message | concurrent writers touching the same files | Retry; see isolation levels and scheduling rules in Project 4 M7 |
| 21 | History server shows nothing | event logging off, wrong directory, or log not finished | Enable event logs, point both sides at the same bucket location, stop the application cleanly |

---

## 6. Answer key for the self-check

1. A shuffle redistributes data across partitions by key over the network and disk; triggered by joins (non-broadcast), group-by aggregations, repartition, distinct, window functions with partitioning.
2. Transformations build a plan lazily; actions trigger execution.
3. A partition is a slice of data processed by one task; stage task count equals the number of partitions of its input (shuffle partitions after an exchange, file splits for a scan).
4. The driver plans, schedules, and collects; executors run tasks and hold cached data and shuffle files.
5. A stage ends only when all tasks finish; the slowest task determines its duration.
6. When you need per-row results that also depend on other rows in a group (ranks, running totals, previous row) rather than collapsing rows.
7. Inner keeps rows with a match; left anti keeps left rows with no match.
8. Uniqueness plus not-null and a single identity for the row (and foreign key targets); a unique index may allow nulls.
9. A user-defined function; row-at-a-time Python execution crosses the JVM-Python boundary with serialization per row.
10. Python packages per environment; the kernel uses the interpreter and packages of the environment it was started from.
11. By service name on the shared network; localhost inside a container is the container itself.
12. Maps a host directory into the container; files outside it are lost when the container is removed.
13. A snapshot is a version of the table (a set of files at a point in time); a manifest lists data files with statistics and partition values.
14. Objects, not files: no cheap rename, flat key space with prefixes, listing is paginated and has latency.
15. If the filter or join can use the index it is fast; if the predicate can't (function on column, low selectivity) the database scans the table.
