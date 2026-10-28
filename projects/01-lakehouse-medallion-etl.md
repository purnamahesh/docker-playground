# Project 1: Lakehouse Medallion ETL (Instacart)

## Goal

Turn the raw Instacart files into a governed lakehouse: profile and enrich them into an operational model in Postgres, extract them with Spark into RustFS landing, and build Iceberg bronze, silver and gold layers with data quality handling, incremental loads, time travel, evolution, maintenance and reconciliation.

## Work plan (what happens when)

| Phase | Milestone | Starts when | Produces |
|---|---|---|---|
| A | M0 Load, profile, enrich | raw files stored in landing | typed source in Postgres, defect register, rebuilt timeline |
| B | M1 Full extraction | S tier exists in Postgres | landing extracts, manifests |
| C | M3 Bronze | extraction works | bronze tables |
| D | M4 Silver | bronze exists | silver, quarantine, SCD2 |
| E | M5 Gold | silver exists | marts |
| F | M2 Incremental extraction | change simulator exists | incremental pipeline |
| G | M6 Evolution and WAP | gold exists | evolved schemas, branches, tags |
| H | M7 Maintenance | tables have history | runbook |
| I | M8 Orchestration, failure drills, reconciliation | everything exists | final report |

Work on the **S tier** until each phase works end to end, then re-run on M. Only touch L in Projects 2 and 4.

---

## M0. Load, profile, enrich and constrain the source

**Objective:** a trustworthy operational database whose defects you know and have recorded.

**Do, in order:**
1. **Preserve the raw files.** Keep the untouched copy in `landing/raw_dump`. Everything later must be reproducible from it.
2. **Stage as text.** Create a staging schema where every column is text so loading never fails. Decide how to load, and measure rows per second for each way you try.
3. **Fix parsing early.** Product names contain commas and quotes. Load once with a naive reader, count the broken rows, then with a proper quoted-CSV reader. The difference is your first defect entry.
4. **Type the data.** Cast to proper types in a second step and capture every row that fails to cast (there should be few or none). Choose the smallest sensible types (weekday and hour fit small integers).
5. **Profile every table.** For each: row count, null rate per column, distinct counts, min/max, length distribution of text, top values, duplicate rate on candidate keys. Add **relationship profiling** (orphan counts for every candidate foreign key, cardinality of each relationship), **distribution profiling** (basket size, orders per user, product frequency, hour and weekday), and **temporal profiling** (distribution of `days_since_prior_order`, size of the spike at the cap, share of nulls by `order_number`).
6. **Union the line files.** Combine prior and train lines into one lines table with a source-file column. Decide how test-set orders (no lines) are represented: keep them with no lines, flag them, or move them aside. Write the decision down.
7. **Stage constraints.** Add them in this order and record the result of each: primary keys on the small tables, on orders, then the composite key on lines; foreign keys from lines to orders and to products; products to aisles and departments. Use the "add without validating, then validate" approach where available so you can count violations instead of failing.
8. **Verify `reordered`.** Define the rule (the user bought this product in any earlier order). Check it against the data. Interpret mismatches: is the rule wrong, or is the flag?
9. **Build the rebuilt timeline.** Follow the design decisions in the README (start date, first order, capped gaps, weekday mismatch). Output a timestamp and a quality flag per order, plus a report of how often the weekday doesn't fit.
10. **Generate the synthetic tables** (users, stores, price book, totals and payments, lifecycle, erasures, campaign calendar) in the order they depend on each other: users and stores first, then the price book, then totals, then payments, then lifecycle. Test each with its validity rules.
11. **Add OLTP columns and indexes.** Add `updated_at` and `is_deleted`. Create keys and foreign-key indexes. **Deliberately skip one useful index** (for example on the order timestamp) so Projects 1 to 3 can show its effect.
12. **Install the change-log triggers last**, after bulk loading, so loading doesn't flood the change log.
13. **Build tiers.** S first (sample by user), then M, later L. Re-profile each tier and compare distributions with M.

**Options to explore:**

| Decision | Options | What to compare |
|---|---|---|
| Loading method | server-side bulk copy / client-side copy / Spark JDBC write | rows per second, ease, error visibility |
| Index and constraint timing | before load / after load | total load time |
| Source table design in Postgres | single large lines table / partitioned by order date | extract speed, partition pruning, maintenance |
| Test-set orders | keep / flag / separate table | complexity downstream |
| Surrogate keys | natural keys only / add a surrogate to lines | JDBC partitioning ease (see M1) |
| Statistics | analyze after load / skip | quality of Postgres plans in M1 |

**Record / observe:** load times, database size with and without indexes, per-table profile, defect register v1, timeline quality report, constraint status table (constraint, result, violations).

**Traps:**
- Loading with constraints on hides defects behind errors.
- Sampling S by order instead of by user breaks reorder logic.
- Triggers installed before the bulk load make the change log huge and meaningless.
- Timeline rebuilt without keeping the original gap and hour loses your ability to audit it.

**Done when:** every table loaded and typed; defect register has an entry for every issue in README 3.3 (or a note that you didn't find it); constraint status table complete; timeline report written; S, M tiers exist and match the file row counts.

---

## M1. Full-snapshot extraction over JDBC

**Objective:** land consistent, validated raw extracts of every table in RustFS, quickly and without hurting Postgres.

**Do, in order:**
1. **Choose a consistent snapshot approach** for the extraction, since data changes while you read: a single repeatable-read snapshot shared by all partitions, extract at a quiet moment, or accept inconsistency and fix it via reconciliation later. Decide per table.
2. **Pick a partitioning strategy per table** (below), and determine bounds from a min/max query run just before the extract.
3. **Measure balance first.** Before running at scale, compute the row count per partition. Large baskets mean equal order-id ranges can carry very different line counts.
4. **Run the extract** with a modest partition count, then scale up.
5. **Write to landing** using a path convention (source, table, extract date, extract id) in a temporary location, then publish by writing a **manifest** (table, extract id, row count, byte count, partition count, timestamps, source snapshot id) as the last step.
6. **Validate** row counts against Postgres and a column checksum (such as a sum of ids and of a numeric column).
7. **Repeat** with other formats and settings (grid below).

**Partitioning options by table:**

| Table | Options | Notes |
|---|---|---|
| orders (3.4M) | numeric id ranges / hash-based predicates / per-user-bucket predicates | ids may have gaps; check evenness |
| order_items (34M) | ranges on order id / hash of order id / month of order timestamp (needs index) / surrogate numeric key | ranges are unequal in row count |
| small dimensions | single read | not worth splitting |

**Options to explore:**

| Knob | Values | Watch |
|---|---|---|
| Partition count | 1, 4, 8, 16, 32 | time, Postgres connections, slowest task |
| `fetchsize` | small, default, large | executor memory, round trips |
| Table option vs query with subquery | both | what Postgres executes |
| Landing format | Parquet, CSV, JSON | size, write time, later read speed |
| Compression | snappy, zstd, none | size, time |
| Output file size | many small / a few large | later scan cost on RustFS |

**Record / observe:** per-partition row counts and durations; Postgres active connections, sequential vs index scans, and queries actually received; RustFS object counts; effect of the missing index when partitioning by timestamp.

**Traps:**
- A hex or text key can't be range-partitioned numerically.
- Predicates that don't hit an index turn every partition into a full table scan.
- Landing without a manifest leaves you unable to tell complete extracts from partial ones.
- Too many partitions can exceed Postgres's connection limit.

**Done when:** every table extracted with matching counts and checksums; a manifest per extract; you can explain, with evidence, why you chose each partition strategy; extract of order_items is repeatable in a stable time.

---

## M2. Incremental extraction

**Objective:** keep landing and bronze current with only the changes, correctly, including late updates and deletes.

**Do, in order:**
1. **Build or start the lifecycle replayer** so rows in Postgres change continuously (new orders, status moves, substitutions, tier changes, erasures).
2. **Choose a strategy per table** from the options below; justify each.
3. **Implement a control table** recording, per table and run: run id, lower and upper bound, status, row counts, started and finished times. Decide where it lives (Postgres or Iceberg).
4. **Design the overlap window** by measuring how late updates actually arrive (maximum lag between the change and its `updated_at` becoming visible). Set the window from data, not guesswork.
5. **Extract** the rows in the window, land them with their run id, and deduplicate on key and latest change.
6. **Handle deletes** using the chosen approach (below).
7. **Prove idempotency:** re-run the same window and show no change downstream. Then fail a run midway and show the next run recovers.
8. **Reconcile** at intervals: counts and key sets between source and landing.

**Strategy options by table:**

| Strategy | Good for | Weakness |
|---|---|---|
| High watermark on `updated_at` | orders, payments, users | misses hard deletes; needs overlap for late rows; ties |
| Monotonic key (append-only) | order lines that never change | substitutions are updates, so not safe if lines change |
| Change-log polling | anything with triggers | extra write cost on the source |
| Full snapshot diff | small dimensions | expensive on big tables |
| Log-based CDC | production-grade | extra infrastructure (stretch goal) |

**Delete detection options:** soft-delete flag with `updated_at`; tombstone rows in the change log; periodic full key reconciliation; both flag and reconciliation.

**Traps:**
- Clock skew and transaction commit order can make a row with an earlier timestamp appear after you've passed it.
- Equal timestamps at the window boundary: use inclusive lower bounds plus deduplication.
- Reviewing only counts hides updates that changed values without changing counts.

**Done when:** a day of replayed changes arrives complete in landing; no missed late update in a reconciliation; hard deletes detected within your documented lag; every table's strategy has a justification and a known weakness.

---

## M3. Bronze tables in Iceberg

**Objective:** a faithful, append-only, queryable copy of every source with full lineage.

**Do, in order:**
1. **Decide the bronze contract:** raw values as received, no business cleaning, one extra set of lineage columns (run id, extract id, source system, ingested time, source file, operation type).
2. **Create one bronze table per source table** (snapshot loads and incremental loads in the same table, distinguished by operation and run).
3. **Choose types:** keep source types, or keep everything as strings in bronze and cast in silver (decide and record).
4. **Choose properties:** format version, file format, compression, target file size.
5. **Choose partitioning** (below), predict the number of files each choice will make, then verify with the metadata tables.
6. **Load** from landing (batch); then load the clickstream the same way. Use an explicit schema, capture corrupt records separately, and handle the planned schema drift.
7. **Inspect** after every write: snapshots, files, manifests, partitions; compare with the objects in RustFS.
8. **Query sanity checks:** row counts match landing manifests; sample rows traced back to source.

**Partitioning options for the lines table:**

| Option | Pros | Cons |
|---|---|---|
| Month of order date (day transform on timestamp) | prunes date filters, hidden partitioning | needs the timeline joined early |
| Day of load time | simple, matches incremental loads | doesn't prune business-date filters |
| Bucket on order id | enables shuffle-free joins later | no pruning |
| Department | prunes department queries | skewed (produce) |
| Unpartitioned with sort order | fewer small files | no partition pruning |

**Options to explore:** two compression codecs; two target file sizes; typed vs all-string bronze; one table for snapshots and increments vs two.

**Traps:** too many tiny partitions create many small files; changing partitioning later is possible but leaves two specs; unlabelled lineage makes later debugging impossible.

**Done when:** all bronze tables loaded and reconciled with landing; each partition choice evaluated with file counts and sizes; lineage columns complete.

---

## M4. Silver: conform, deduplicate, quarantine, version

**Objective:** trusted current-state and historical tables, with every rejected row explained.

**Do, in order, per table:**

| Table | Rules and decisions |
|---|---|
| **Order lines** | deduplicate on order and product; validate `add_to_cart_order` as a gap-free sequence per order; flag orders with no lines; join the price valid at order time |
| **Orders** | validate the user history (order numbers contiguous and unique); keep raw `eval_set`; apply the rebuilt timeline; flag cancelled; quarantine impossible lifecycles |
| **Products and taxonomy** | conform the three-level taxonomy; replace the placeholder aisle and department with an explicit unknown; normalize names (trim, case, whitespace) |
| **Users (SCD2)** | derive from synthetic users; track membership tier history with valid_from, valid_to, is_current; prove with as-of queries |
| **Prices** | effective-dated prices; detect overlaps and gaps; resolve promotions by precedence |
| **Payments** | reconcile against priced lines plus fees and tip; classify mismatches (rounding, refunds, failed tenders, missing rows) |
| **Reorder flag** | rebuild from history and compare with the source flag; quarantine or flag disagreements |
| **Clickstream** | deduplicate on event id; handle late events; map sessions to orders; quarantine malformed events |

**MERGE design (do this for every current-state table):**
1. Define the match key and the "newer wins" rule (updated_at, then sequence).
2. Collapse the incoming batch to one row per key first.
3. Decide how deletes arrive and how each kind (soft, hard) is applied.
4. Prove idempotency (apply the same batch twice, nothing changes).

**Quarantine design:** one quarantine table per entity, with the original row, a reason code from a **catalog of codes** (for example: cast failure, lifecycle violation, missing parent, duplicate key, out-of-range), rule version, and run id. Decide for each rule whether it **rejects**, **flags**, or only **counts**.

**Data-quality rule catalog (build and run these):** completeness (required fields), validity (ranges and formats), uniqueness (keys), referential integrity (parent exists), consistency (cross-table totals), timeliness (late arrivals). Store metrics per run so you can chart them.

**Options to explore:**

| Experiment | Options | Measure |
|---|---|---|
| Write mode for merges | copy-on-write vs merge-on-read | write time, read time, delete files |
| Update size | 0.1%, 1%, 10% of rows | break-even point between modes |
| SCD2 construction | MERGE in steps vs build the changed rows then insert | complexity and speed |
| Price join | range join vs daily-expanded price table | time and plan |
| Dedup | arbitrary row vs deterministic latest | plan and result stability |
| Casting | strict (fail) vs `try_*` (quarantine) | rows rejected, behavior |

**Traps:** double-counting test orders; silently dropping rows instead of quarantining; non-deterministic dedup (different runs choose different rows); merging without collapsing the batch first.

**Done when:** each silver table has a documented rule list, a quarantine reason catalog, and row accounting that balances (bronze rows = silver rows + quarantined + deduplicated); SCD2 as-of queries return correct history; merge idempotency is proven.

---

## M5. Gold marts

**Objective:** analytics-ready tables with verified numbers and a refresh strategy.

**Do, in order, for each mart:** write its grain and purpose; list inputs; define the logic and pitfalls; choose a refresh strategy; test with invariants; record plan and runtime.

| Mart | Grain | Logic and pitfalls | Invariant to test |
|---|---|---|---|
| Reorder rate | product, aisle, department | exclude orders with no lines; define denominators carefully | rates between 0 and 1; totals match lines |
| Basket analytics | order | items per order, department mix, position vs reorder | sum of items equals line count |
| Order cadence | user, hour by weekday | gap distribution; treat capped gaps as censored | counts match orders |
| User 360 | user | recency, frequency, monetary on priced orders; favorites; department vector | user counts match |
| Cohorts | first-order month by order number | careful with incomplete months | cohort sizes sum to users |
| Top-N | department by month | ties and deterministic ordering | N rows per group |
| Co-occurrence | product pair | the expensive one (see Project 2); decide pruning and basket caps | symmetry and counts reproduce on a sample |
| Next-basket features | user, as-of date | window over last N orders; leakage control (nothing after the as-of date) | no future data used |

**Refresh strategy options:** full rebuild; partition overwrite by date; MERGE of changed groups; delete and insert. **Test each for idempotency by running twice and diffing.**

**Options to explore:** partition by date vs by department; bucket by user; pre-aggregate before joining; cache an input used by several marts; sort order for the dominant filter.

**Done when:** every mart has a documented grain and passing invariants; each of the three refresh strategies tried on one mart with evidence; reruns produce identical outputs.

---

## M6. Time travel, evolution and write-audit-publish

**Objective:** use Iceberg's versioning safely.

**Do, in order:**
1. **Time travel:** read past snapshots by id and by timestamp; compare a gold number before and after a load.
2. **Bad-load recovery:** deliberately load wrong data (for example a mis-versioned price book), detect it with a check, and roll back. Record what was rolled back and what remained.
3. **Schema evolution matrix:** add a nullable column, add a column with a default meaning, rename, drop, widen a type, reorder. For each, check old and new readers and previously written files.
4. **Partition evolution:** change the lines table from monthly to daily. Confirm old files stay in the old layout; read plans that touch both specs; decide whether to rewrite old data.
5. **Branches and tags:** stage a load on a branch; run the data-quality suite against the branch; publish by moving main only if checks pass; tag month-end snapshots for audit.
6. **Retention policy:** define how long you keep snapshots and tags and why.

**Traps:** forgetting that expiring snapshots removes time travel; renames that downstream readers depend on; publishing a branch without re-running checks after later changes.

**Done when:** a rollback restored a clean state; every evolution type has a recorded outcome; a failing check blocked a publish.

---

## M7. Table maintenance

**Objective:** a runbook that keeps tables fast and storage bounded.

**Do, in order:**
1. **Baseline** the tables: file counts and sizes, manifest counts, snapshot counts, delete files, scan time of three reference queries.
2. **Create the mess:** many tiny appends, many merges producing delete files, many snapshots.
3. **Re-measure** the same baselines to see the damage.
4. **Apply each action separately** and measure what it fixes:

| Action | Parameters to explore | Question to answer |
|---|---|---|
| Compaction | bin-pack vs sort vs z-order; target size; minimum input files | how much faster are filtered queries? |
| Manifest rewrite | when planning time is high | does planning time drop? |
| Delete-file compaction | after many merges in merge-on-read | does read time recover? |
| Snapshot expiry | retention age, minimum snapshots kept | what time travel did you lose; how much storage returned? |
| Orphan removal | age threshold | why dangerous with in-flight writes; what is a safe cutoff? |

5. **Order the actions** in a schedule and test the schedule while a writer is running.
6. **Write the runbook:** cadence per table type, triggers (file count or small-file ratio), safety rules, and alerts.

**Traps:** orphan removal with too short an age can delete files of an in-progress commit; compaction competing with writers causes conflicts; expiry shrinking history below your rollback needs.

**Done when:** each action has a measured benefit; the schedule survives a concurrent writer; the runbook exists.

---

## M8. Orchestration, failure drills and reconciliation

**Objective:** prove the pipeline is repeatable, recoverable and accountable.

**Do, in order:**
1. **Draw the dependency graph** (extract, bronze, silver, gold, maintenance) and choose run ordering and retry rules.
2. **Rerun tests:** rerun one day end to end; rerun a failed middle step; backfill a past day. Show identical results each time.
3. **Failure drill matrix:**

| Inject | Expect | Verify |
|---|---|---|
| Kill a worker mid-write | task retry, success | no duplicate or partial data |
| Stop RustFS mid-commit | failed or retried commit | table unchanged; list leftover files |
| Stop Postgres mid-extract | failed extract | no half-published extract; manifest absent |
| Kill the driver mid-merge | failed run | rerun applies exactly once |
| Corrupt a landing file | quarantine path | pipeline continues, defect recorded |

4. **Reconciliation report** across the raw files, Postgres, landing, bronze, silver and gold: row counts, key-set differences, and totals (lines, items, priced amounts). Explain **every** difference, linking each to a defect-register entry (filtered, quarantined, deleted, deduplicated).
5. **Lessons:** list what you would automate, monitor and alert on.

**Done when:** every drill's outcome matches your prediction (or the surprise is explained); the reconciliation has zero unexplained differences.

---

## Config and property experiments (summary)

| Area | Knobs |
|---|---|
| JDBC | partitions, fetch size, partition strategy, pushdown options |
| Spark | shuffle partitions, AQE, partition overwrite mode |
| Iceberg write | target file size, distribution mode, compression, merge/update/delete mode |
| Iceberg layout | partition transforms, sort order, format version |
| Storage | S3A or Iceberg file I/O settings against RustFS (Project 4) |

## Stretch goals
- Move the catalog to a standalone REST catalog and note what changes.
- Package the pipeline as a parameterized application with `spark-submit` (client mode, then cluster mode on standalone).
- Add a reverse-ETL step writing a gold aggregate to a Postgres reporting table with controlled parallelism.

## Exit criteria for Project 1
1. You can state each Instacart defect, how you found it, and which rule handles it.
2. Every layer reconciles to the one before it.
3. Rerunning any step is safe.
4. Your maintenance runbook has measured effects.
