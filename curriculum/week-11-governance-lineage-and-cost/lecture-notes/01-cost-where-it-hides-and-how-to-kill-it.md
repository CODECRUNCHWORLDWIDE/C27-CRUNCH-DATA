# Lecture 11.1 — Cost: Where It Hides and How to Kill It

> "The senior engineer reasons about the bill even when no bill arrives."

On your laptop there is no invoice. Every query runs against local Parquet on MinIO, every Spark job spins up in a container, and nothing costs a cent. That is exactly why cost is the discipline this course can least afford to skip and most easily does. The 40 GB full scan that returns one integer runs in eight seconds on your SSD and would cost a real team a real dollar amount every single time it ran on a metered engine. You do not learn this from a bill — by the time the bill arrives, the bad query has been in a dashboard refresh loop for three months. You learn it by building the instinct to ask, before you run anything: *how many bytes does this touch, where do they live, and could the engine have touched fewer?*

This lecture builds that instinct on three numbers — scan, shuffle, storage — and then spends most of its length on the single most common, most fixable cost trap in a lakehouse: the small-files problem, and its two cures, compaction and partition pruning.

---

## 1. The cost model in three numbers

Every dollar a data platform spends collapses into three categories. Memorize them; everything else is detail.

### 1.1 Scan cost — bytes read

A query costs, first and foremost, the number of bytes it reads off storage. Not rows returned — bytes *read*. A `SELECT count(*) FROM events WHERE day = '2026-06-01'` that has to open every file in the table reads the whole table even though it returns one number. The same query against a table partitioned by `day` reads one day. The output is identical; the cost differs by three orders of magnitude.

This is the model that metered warehouses bill on directly:

- **BigQuery** on-demand pricing is *bytes billed* — the bytes scanned by the query, rounded up, at a per-TiB rate. A `SELECT *` on a wide table is the canonical way to set money on fire.
- **Snowflake** bills compute *credits* by warehouse-second, but the bytes a query scans drive how long the warehouse runs and how big it must be — so scan cost is still the lever, one indirection removed.
- **Athena / Trino-as-a-service** bill bytes scanned almost exactly like BigQuery.

On the laptop the bill is hidden but the *bytes* are not. Columnar formats (Parquet) and ACID table formats (Iceberg, Delta) exist largely to drive bytes-read down: column projection reads only the columns you select, predicate pushdown skips row groups whose statistics cannot match, and partition pruning skips whole files. Every technique in this lecture is a bytes-read reduction.

### 1.2 Shuffle cost — the network

The second cost is the *shuffle*: when a distributed engine must move data across the network to bring related rows together. You met it in Week 7 — "the shuffle is the enemy." A wide transformation (a `GROUP BY`, a non-broadcast join, a `DISTINCT`, a window with a non-trivial partition) forces every executor to send rows to whichever executor owns the matching key. That network transfer, plus the disk spill when the data does not fit in memory, is pure overhead that produces no output bytes.

Shuffle cost does not appear on a storage bill, but it appears on the *compute* bill (Snowflake credits, Spark cluster-hours) and in wall-clock latency. The fixes are the Week 7 fixes — broadcast the small side of a join, salt a skewed key, pre-aggregate before the shuffle — plus one this week adds: lay the data out so the join key is already co-located (bucketing), so the shuffle is smaller or unnecessary.

### 1.3 Storage cost — capacity × tier × time

The third cost is the quietest: storage you pay for whether anyone queries it or not. It is `bytes stored × $/byte/month × how-many-months-you-keep-it`. It hides in:

- **Old snapshots.** Time travel (Iceberg snapshots, Delta versions) keeps every prior version of every file. A table you overwrite daily can hold 90× its logical size if you never expire snapshots. This is also why GDPR deletion is hard — covered in lecture 3.
- **Uncompacted small files.** Many tiny files cost more than a few big ones even at the same logical size, because object stores meter per-request and per-object overhead.
- **Cold data on a hot tier.** Cloud object stores have tiers — S3 Standard / Infrequent-Access / Glacier; the cheaper the tier, the slower and the more per-retrieval. Data older than 90 days that nobody queries should not live on the hot tier.

The cloud analogue is **S3 storage classes** and lifecycle policies; on the laptop, MinIO has no tiers, but the *habit* — expire what you do not need, lifecycle what is cold — is what transfers.

> **The senior move.** Before shipping any pipeline, answer three questions: *How many bytes does the typical query scan? Does any step shuffle, and how much? How fast does this table grow and what is its retention?* If you cannot answer all three, you have shipped a cost you cannot see.

---

## 2. Where cost hides

Cost is never in the obvious place. Five hiding spots account for nearly every "why did the bill triple" incident.

### 2.1 Full scans where a pruned scan would do

The query filters on `customer_id` but the table is partitioned by `event_date`, so the predicate cannot prune and the engine reads everything. Or there is no partitioning at all. The fix is layout (§5), not the query.

### 2.2 Oversized and undersized files

Parquet has a sweet spot — roughly **128 MB to 1 GB** per file, with row groups around 128 MB. Too small (the small-files problem, §3) and metadata overhead dominates. Too large and the engine cannot parallelize the read across executors and loses row-group-level skipping granularity. Both are layout problems compaction fixes.

### 2.3 Unpruned partitions

The table *is* partitioned, but the query's predicate does not align with the partition column, or the partition column has the wrong granularity (partitioned by `hour` when queries filter by `month`, producing millions of tiny partitions — the "over-partitioning" trap). Pruning silently does not fire and you scan everything.

### 2.4 Runaway shuffles

A join that should broadcast does not because the planner mis-estimated the small side's size; a `GROUP BY` on a high-cardinality key spills to disk; a skewed key sends 90% of rows to one executor. The Spark UI's *Shuffle Read / Shuffle Write* columns are where you catch this (§6).

### 2.5 Missing predicate pushdown

The filter is there, the partitioning is fine, but the engine cannot push the predicate down to the file reader — usually because the predicate is wrapped in a function the reader does not understand (`WHERE date_trunc('day', ts) = ...` instead of `WHERE ts >= ... AND ts < ...`), so it reads every row group and filters after. Always write predicates the reader can push down.

---

## 3. The small-files problem

This is the cost trap you will fix with your own hands this week, so it gets the most detail.

### 3.1 The symptom

You have a table that is, logically, 2 GB. You query it and it is slow out of proportion to its size. You list the files and find **18,000 of them**, averaging 110 KB each. That is the small-files problem.

It is the natural exhaust of streaming and frequent micro-batches. Your Week 9 Spark Structured Streaming job writes a file (or a few) per micro-batch trigger; trigger every 30 seconds and you have ~2,880 files a day from one job, most of them tiny. Append-heavy ingestion does the same.

### 3.2 Why it costs

The cost is **not the bytes** — it is the metadata and per-file overhead:

- **Per-file open/seek/close.** Every file is an object-store `GET` (or a filesystem `open`). Each has latency. Reading 18,000 files of 110 KB takes far longer than reading 16 files of 128 MB even though the bytes are identical, because you pay 18,000 round-trips instead of 16.
- **Manifest and metadata bloat.** Iceberg tracks every data file in a manifest; Delta tracks every `add`/`remove` in the transaction log. 18,000 files means 18,000 manifest entries the planner must read and reason about *before the query even starts*. Planning time itself becomes the bottleneck.
- **Lost parallelism efficiency.** A task per file means 18,000 tiny tasks, each with scheduling overhead that dwarfs its 110 KB of work.
- **Object-store request cost.** On S3, `GET` requests are billed per thousand. 18,000 GETs per query × thousands of queries is a line item.

### 3.3 The cure: compaction

Compaction rewrites many small files into a few right-sized ones, leaving the logical table contents identical. Both table formats provide it as a maintenance operation.

**Apache Iceberg — `rewrite_data_files`** (a Spark stored procedure on the catalog):

```sql
-- Bin-pack small files into ~512 MB targets across the whole table.
CALL local.system.rewrite_data_files(
  table       => 'db.events',
  strategy    => 'binpack',
  options      => map(
    'target-file-size-bytes', '536870912',   -- 512 MB
    'min-input-files',         '5',            -- only rewrite groups of 5+ small files
    'min-file-size-bytes',     '134217728'     -- treat anything < 128 MB as "small"
  )
);
```

The procedure returns `rewritten_data_files_count` and `added_data_files_count` — your before/after file count, directly. To sort within files for better row-group skipping, use `strategy => 'sort'` with a `sort_order`.

Compaction creates a *new snapshot*; the old small files still exist until you expire snapshots (§7 of lecture 3 — and the reason deletion is hard). Iceberg also separates *data* compaction from *metadata* compaction: `rewrite_manifests` merges manifest files when the manifest list itself has grown unwieldy.

**Delta Lake — `OPTIMIZE`** bin-packing:

```sql
-- Bin-pack the whole table to the configured target file size.
OPTIMIZE db.events;

-- Bin-pack only recent partitions (cheaper, the usual operational pattern).
OPTIMIZE db.events WHERE event_date >= '2026-06-01';

-- Z-ORDER: co-locate rows by columns you filter on, so data skipping is far more effective.
OPTIMIZE db.events
  WHERE event_date >= '2026-06-01'
  ZORDER BY (customer_id, product_id);
```

`OPTIMIZE` returns metrics: `numFilesAdded`, `numFilesRemoved`, and the min/max/avg file sizes before and after. **Z-ORDER** is the multi-dimensional clustering trick: it interleaves the bits of several columns so that rows with similar values in *any* of them land in the same files, dramatically improving data-skipping for queries that filter on those columns — at the cost of a more expensive rewrite. Use it for the columns you filter on most that are *not* the partition column.

`OPTIMIZE` also leaves the old files behind for time travel until you `VACUUM` (lecture 3, §7).

### 3.4 Right-sizing at write time (prevention)

Compaction is the cure; prevention is cheaper. Configure your writers so they emit fewer, larger files:

- Spark: `spark.sql.files.maxRecordsPerFile`, and `df.repartition()` / `coalesce()` before the write so each task writes one well-sized file.
- Iceberg: the table property `write.target-file-size-bytes` (default 512 MB).
- Streaming: trigger less often (a 5-minute `Trigger.ProcessingTime` instead of 30 seconds produces ~10× fewer files), or run a scheduled compaction job downstream of the stream.

---

## 4. Partitioning for pruning

Compaction fixes file *size*; partitioning fixes file *relevance* — letting the engine skip files it knows cannot match.

### 4.1 The idea

If the table is physically laid out so all rows for `2026-06-01` live under one directory, a query for that day reads one directory and *prunes* (skips) the rest. The engine knows it can prune from the partition metadata, before opening a single data file. Pruning is the single largest bytes-read reduction available, often 100×+.

### 4.2 Iceberg hidden partitioning and partition transforms

Hive-style partitioning has a famous footgun: the user must know the table is partitioned by `event_day` and write `WHERE event_day = '2026-06-01'` *in addition to* `WHERE event_ts >= ...`, or pruning silently does not fire. Iceberg fixes this with **hidden partitioning**: you declare a partition *transform* on a real column, and Iceberg prunes automatically when you filter that real column — the partition column is not something the user has to know about.

```sql
-- Partition by a transform of the real timestamp column. No synthetic partition column.
CREATE TABLE local.db.events (
  event_id     BIGINT,
  customer_id  BIGINT,
  event_ts     TIMESTAMP,
  amount       DECIMAL(12,2)
)
USING iceberg
PARTITIONED BY (days(event_ts), bucket(16, customer_id));
```

Now `WHERE event_ts >= '2026-06-01' AND event_ts < '2026-06-02'` prunes to one day automatically — the user filters the natural column and Iceberg does the rest. The common transforms:

- **`days(ts)` / `hours(ts)` / `months(ts)` / `years(ts)`** — time bucketing at the granularity your queries use. Match the granularity to the query: `days` for daily dashboards, not `hours` (which over-partitions).
- **`bucket(N, col)`** — hash `col` into `N` buckets. Use for high-cardinality keys you join or filter on (`customer_id`), so a point lookup scans 1/N of the data and joins can co-locate.
- **`truncate(W, col)`** — truncate strings to `W` chars or numbers to a width. Useful for prefix-based access patterns.

Iceberg also supports **partition evolution**: you can change the partition spec on an existing table without rewriting old data — new data lands under the new spec, old data keeps its old spec, and queries prune both correctly.

### 4.3 Delta partitioning and liquid clustering

Delta's classic approach is Hive-style `PARTITIONED BY`:

```sql
CREATE TABLE db.events (
  event_id BIGINT, customer_id BIGINT, event_ts TIMESTAMP, amount DECIMAL(12,2),
  event_date DATE GENERATED ALWAYS AS (CAST(event_ts AS DATE))
)
USING delta
PARTITIONED BY (event_date);
```

The generated column lets Delta prune on `event_date` automatically when you filter `event_ts`. But classic partitioning is rigid — you commit to a layout up front and over-partitioning hurts. Delta's modern answer is **liquid clustering**: instead of fixed directory partitions you declare `CLUSTER BY (customer_id, event_date)` and Delta adaptively clusters data into well-sized files by those keys, re-clustering incrementally as data arrives. It avoids both over-partitioning and the rigidity of choosing one partition column, and it replaces both `PARTITIONED BY` and `ZORDER` for new tables.

```sql
CREATE TABLE db.events ( ... )
USING delta
CLUSTER BY (customer_id, event_date);
```

### 4.4 Choosing the partition column — the rule

Partition by **the column your queries filter on most**, at **the coarsest granularity that still prunes well**, producing partitions that are **at least a few hundred MB each**. A partition smaller than one target file size is over-partitioning and reintroduces the small-files problem. Daily partitions on a table that ingests gigabytes a day: good. Hourly partitions on a table that ingests megabytes a day: a small-files factory.

---

## 5. Measuring bytes scanned

You cannot manage what you cannot measure. Here are the three ways to read bytes-scanned on the laptop, each mapping to a cloud-billing number.

### 5.1 Iceberg metadata and manifest statistics

Iceberg exposes its own bookkeeping as queryable metadata tables. Before and after a query — or before and after compaction — read them:

```sql
-- File count, total size, and record count for the current snapshot.
SELECT count(*) AS file_count,
       sum(file_size_in_bytes) AS total_bytes,
       sum(record_count) AS total_rows
FROM local.db.events.files;

-- Per-snapshot history (added/deleted files and rows) — see what compaction did.
SELECT snapshot_id, operation,
       summary['added-data-files']   AS added_files,
       summary['deleted-data-files'] AS deleted_files,
       summary['total-data-files']   AS total_files
FROM local.db.events.snapshots
ORDER BY committed_at;
```

For a *query's* scan, Spark's Iceberg reader reports `numFiles` and `numSplits` in the physical plan, and the manifest statistics (per-column min/max) are what drive pruning — `SELECT * FROM local.db.events.manifests` shows how many data files each manifest covers.

### 5.2 The Spark UI

For any Spark query, the SQL tab of the Spark UI (`http://localhost:4040`) shows, per scan node:

- **`number of files read`** and **`size of files read`** — your bytes-scanned for that scan.
- **`number of partitions pruned`** (Iceberg/Delta scan node) — proof pruning fired. If this is 0 when you expected pruning, your predicate is not pushing down.
- **`Shuffle Read` / `Shuffle Write`** on exchange nodes — your shuffle cost in bytes.

The number to screenshot for the lab is *size of files read*, before and after compaction/repartitioning, on the same query.

### 5.3 DuckDB `EXPLAIN ANALYZE`

DuckDB is your single-node measurement tool. `EXPLAIN ANALYZE` reports actual rows and timing per operator; for Parquet/Iceberg scans it reports how many row groups and files were read versus skipped:

```sql
-- See whether the filter pruned row groups (Filters pushed down, files/row-groups skipped).
EXPLAIN ANALYZE
SELECT count(*) FROM iceberg_scan('s3://lake/db/events')
WHERE event_ts >= TIMESTAMP '2026-06-01'
  AND event_ts <  TIMESTAMP '2026-06-02';
```

Look in the plan for the `PARQUET_SCAN` / `ICEBERG_SCAN` node: it lists `Filters` (the pushed-down predicate) and the count of files and row groups scanned. A correctly pruned query scans a tiny fraction of the table; if it scans everything, the predicate did not push down — usually a function-wrapped column or a type mismatch.

### 5.4 The cloud analogue, named honestly

| Laptop measurement | Cloud billing number |
| --- | --- |
| Iceberg `files.file_size_in_bytes` summed for a scan | BigQuery *bytes billed* |
| Spark UI *size of files read* | Athena / Trino *bytes scanned* |
| DuckDB row-groups scanned vs skipped | Snowflake *partitions scanned / total* (the pruning ratio) |
| MinIO object count and size | S3 storage cost × storage class |
| Spark UI *Shuffle Read* | Snowflake credits / Spark cluster-hours burned on the exchange |

The point of the table is that *the instinct transfers*. You will never run the cohort's queries against BigQuery, but the engineer who has measured bytes-scanned a hundred times on DuckDB walks up to a BigQuery console and knows exactly which number to read and which query to fix.

---

## 6. A worked optimization

Put it together. Suppose the streaming sink has produced a 2 GB `events` table in 18,000 files, unpartitioned, and the representative query is "daily revenue for one day":

```sql
SELECT sum(amount) FROM local.db.events
WHERE event_ts >= TIMESTAMP '2026-06-01' AND event_ts < TIMESTAMP '2026-06-02';
```

**Baseline.** Unpartitioned, the engine reads all 18,000 files (≈2 GB, ≈2 GB scanned) and pays 18,000 file opens. The Spark UI shows *size of files read ≈ 2 GB*, *files read = 18,000*, *partitions pruned = 0*.

**Step 1 — compact.** `CALL local.system.rewrite_data_files(table => 'db.events', strategy => 'binpack', options => map('target-file-size-bytes','536870912'))`. File count drops from 18,000 to ~4. The scan still reads ~2 GB (no pruning yet) but file-open overhead and planning time collapse. Latency drops sharply; bytes-scanned is unchanged.

**Step 2 — re-partition.** Rewrite the table partitioned by `days(event_ts)`:

```sql
CREATE TABLE local.db.events_pruned
USING iceberg
PARTITIONED BY (days(event_ts))
AS SELECT * FROM local.db.events;
```

Now the same daily query prunes to one day's partition. The Spark UI shows *size of files read ≈ 30 MB* (one day out of ~65 days), *partitions pruned = 64*. **Bytes scanned dropped from ~2 GB to ~30 MB — a ~65× reduction**, and that number is exactly what would have been billed on a metered engine.

Compaction bought latency; partitioning bought bytes-scanned. Real optimization needs both: compaction so each partition is a few big files, partitioning so the query reads only the partitions it needs.

---

## 7. What to carry into the lab and the capstone

- Cost is **scan (bytes) + shuffle (network) + storage (capacity × tier × time)**. Name which one you are paying before you optimize.
- The small-files problem is a **metadata/overhead** cost, not a bytes cost; **compaction** (`rewrite_data_files`, `OPTIMIZE`) is the cure and **right-sizing at write time** is the prevention.
- **Partition pruning** is the largest bytes-read reduction available; partition on the column you filter, at the coarsest granularity that still prunes, with partitions ≥ a target file size. Use Iceberg hidden partitioning or Delta liquid clustering so pruning fires without the user knowing the layout.
- Measure bytes scanned three ways — **Iceberg metadata, Spark UI, DuckDB `EXPLAIN ANALYZE`** — and map each to its cloud-billing number so the instinct transfers off the laptop.
- In the lab you will produce a before/after bytes-scanned screenshot. That screenshot, with a ≥10× reduction, is the artifact.

---

## References

- Joe Reis & Matt Housley, *Fundamentals of Data Engineering*, O'Reilly, 2022. ISBN 978-1-098-10830-4 — Ch. 6 (storage) and Ch. 8 (queries, modeling, and the cost of compute) for the scan/shuffle/storage cost model. <https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/>
- Apache Iceberg documentation — maintenance and compaction (`rewrite_data_files`, `rewrite_manifests`, `expire_snapshots`), partitioning and hidden partitioning, partition transforms, metadata tables. <https://iceberg.apache.org/docs/latest/>
- Apache Iceberg — Spark procedures reference. <https://iceberg.apache.org/docs/latest/spark-procedures/>
- Delta Lake documentation — `OPTIMIZE`, bin-packing, Z-ORDER, liquid clustering, and data skipping. <https://docs.delta.io/latest/index.html>
- Delta Lake — Optimizations (file compaction, Z-Ordering). <https://docs.delta.io/latest/optimizations-oss.html>
- DuckDB documentation — `EXPLAIN ANALYZE` and the Parquet/Iceberg readers' pushdown reporting. <https://duckdb.org/docs/sql/statements/explain.html>
