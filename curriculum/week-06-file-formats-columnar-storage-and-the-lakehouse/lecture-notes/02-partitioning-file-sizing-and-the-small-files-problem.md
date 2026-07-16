# Lecture 02 — Partitioning, File Sizing & the Small-Files Problem

> **Time:** ~2 hours · **Prerequisites:** Lecture 01 (Parquet internals,
> statistics, pruning), Week 3 idempotent ETL · **Citations:** DuckDB Parquet
> <https://duckdb.org/docs/data/parquet/overview>, DuckDB httpfs/S3
> <https://duckdb.org/docs/extensions/httpfs/overview>, PyArrow Parquet
> <https://arrow.apache.org/docs/python/parquet.html>, Iceberg partitioning
> <https://iceberg.apache.org/docs/latest/partitioning/>, MinIO docs
> <https://min.io/docs/minio/linux/index.html>

Lecture 01 gave you *intra-file* skipping: statistics let a reader skip row groups
inside one file. This lecture gives you *inter-file* skipping: partitioning lets a
reader skip entire files (or whole directories) before it even opens them. And it
gives you the most common operational pathology in any lakehouse — the small-files
problem — and how to fix it. Partitioning and file sizing are two sides of the same
coin: partition too aggressively and you create the small-files problem;
under-partition and you lose pruning. The job is to find the middle.

## 1. Hive-style partitioning: directories the engine can skip

Partitioning physically splits a table into directories keyed by the value of one
or more low-cardinality columns. The convention everyone follows — DuckDB, Spark,
Hive, Iceberg, Delta — is **Hive-style**: `column=value` directory names.

```
s3://crunch-lake/trips/
├── year=2024/
│   ├── month=01/
│   │   ├── part-0001.parquet
│   │   └── part-0002.parquet
│   ├── month=02/
│   │   └── part-0001.parquet
│   └── month=03/
│       └── part-0001.parquet
└── year=2025/
    └── month=01/
        └── part-0001.parquet
```

The partition columns (`year`, `month`) are encoded **in the path**, not
necessarily in the file data. When a reader sees:

```sql
SELECT SUM(fare_amount)
FROM read_parquet('s3://crunch-lake/trips/**/*.parquet', hive_partitioning = true)
WHERE year = 2024 AND month = 3;
```

it parses the directory names, recognizes `year`/`month` as partition columns,
and **only lists and opens files under `year=2024/month=03/`**. The other
directories are never touched — not opened, not listed beyond the prune, not
parsed. This is **partition pruning**, and it is coarser and cheaper than row-group
pruning: you skip files before reading any footer at all.

```python
# Writing Hive-style partitions with PyArrow.
import pyarrow.dataset as ds
import pyarrow as pa

ds.write_dataset(
    table,                       # a pyarrow.Table that includes year, month columns
    base_dir="s3://crunch-lake/trips",
    format="parquet",
    partitioning=ds.partitioning(
        pa.schema([("year", pa.int32()), ("month", pa.int32())]),
        flavor="hive",           # writes year=YYYY/month=MM/ directories
    ),
    existing_data_behavior="overwrite_or_ignore",
    filesystem=...,              # an s3fs filesystem pointed at MinIO (Exercise 02)
)
```

DuckDB reads it back with `hive_partitioning = true`; reference:
<https://duckdb.org/docs/data/parquet/overview>. Partition pruning + row-group
pruning + projection pushdown compose: prune to one directory, prune to one row
group inside the surviving files, read two columns out of that row group.

## 2. Choosing a partition column: low cardinality and you filter on it

The single most common lakehouse mistake is partitioning on the wrong column.
The rule:

- **Partition on a low-cardinality column you filter on constantly.** Date is the
  canonical choice: queries almost always have a date range, and the number of
  distinct days/months is small and bounded.
- **Never partition on a high-cardinality column.** Partitioning by `user_id`
  (millions of distinct values) creates millions of directories, each with one
  tiny file. That is the small-files problem by construction (§4).
- **Aim for partitions that hold a "right-sized" amount of data** — roughly enough
  to fill at least one or a few healthy files (see §3). If partitioning by day
  yields 50 KB per day, partition by month or year instead. If a single day is
  20 GB, partition by day (or day + hour).

A useful heuristic: pick the coarsest partitioning that still lets your *typical*
query prune away most of the data, such that each surviving partition still
contains files of a healthy size. Over-partitioning to "make pruning perfect" is
the road to ruin; the row-group statistics inside a file already give you
fine-grained skipping for free.

> Connecting to Week 3: writing a partition idempotently means **overwrite the
> partition, do not append into it**. If yesterday's run wrote
> `year=2024/month=03/part-0001.parquet` and you re-run, you must replace that
> partition's files, not add `part-0002.parquet` alongside, or you double-count.
> PyArrow's `existing_data_behavior="delete_matching"` does partition-level
> overwrite; this is the idempotency discipline from Week 3 applied to the lake.

## 3. File sizing: the 128 MB – 1 GB target

Inside each partition, the data is split into files. How big should each file be?
The industry consensus target for analytical Parquet is roughly **128 MB to 1 GB
per file**. The reasoning is mechanical:

**Too small (the small-files problem, §4):** every file you read costs a fixed
overhead — list the object, open it, read the footer, parse the Thrift metadata,
plan the scan. For a 1 MB file, that overhead can dwarf the time spent reading
actual data. A query over a thousand 1 MB files pays the open-and-parse tax a
thousand times.

**Too large (multi-GB files):** you lose parallelism granularity (one giant file
is harder to split across workers than several medium files), a single corrupt
file loses more data, and incremental writes/compaction become coarse. Also, a
giant file with one giant row group defeats row-group pruning — you want enough
row groups per file (each ~128 MB) that statistics can skip parts of the file.

The sweet spot — files in the hundreds of MB, each containing several ~128 MB row
groups — balances open overhead against parallelism and pruning granularity. On a
laptop with smaller datasets you will scale these numbers down (tens of MB files,
~hundreds of K rows per row group) but the *shape* of the trade is identical.

```python
# Control file size at write time. PyArrow datasets can cap rows per file.
ds.write_dataset(
    table,
    base_dir="s3://crunch-lake/trips",
    format="parquet",
    partitioning=ds.partitioning(pa.schema([("year", pa.int32())]), flavor="hive"),
    max_rows_per_file=2_000_000,     # cap file size by row count
    max_rows_per_group=200_000,      # row-group size inside each file
)
```

## 4. The small-files problem, with numbers

The small-files problem is what happens when a table accumulates many tiny files
instead of fewer right-sized ones. It is *the* default failure mode of streaming
or micro-batch ingestion: every 5-minute batch writes one small file, and after a
month you have ~8,600 tiny files in the table.

Why it hurts, concretely. Say a query needs to scan 10 GB of data:

| Layout | Files | Per-file overhead | Total overhead | Effect |
| --- | --- | --- | --- | --- |
| 10 × 1 GB files | 10 | ~5 ms each | ~50 ms | Negligible |
| 10,000 × 1 MB files | 10,000 | ~5 ms each | ~50 s | **Dominates the query** |

Same 10 GB of data, same answer. The 10,000-file version spends ~50 seconds *just
opening and parsing footers* before it reads a meaningful byte. On object storage
(S3, MinIO) it is worse: each file is a separate HTTP request with latency, and
listing a directory with 10,000 objects is itself slow. The metadata overhead and
request overhead scale with file *count*, not data *size*.

You will reproduce this in Challenge 02: write the same data as one file and as a
thousand tiny files, then time the same query against both and watch the tiny-file
version crawl. Measuring it yourself is the point — "small files are slow" is a
slogan until you see the wall-clock gap.

```python
# Reproduce the pathology: write 1000 tiny files for the same data.
import pyarrow.parquet as pq
n = len(table)
chunk = n // 1000
for i in range(1000):
    part = table.slice(i * chunk, chunk)
    pq.write_table(part, f"s3://crunch-lake/tiny/part-{i:04d}.parquet", filesystem=fs)
# Then: time SELECT COUNT(*) over s3://crunch-lake/tiny/*.parquet
# vs the same data in one file. Compare wall-clock and request counts.
```

## 5. Compaction: rewriting small files into big ones

The fix is **compaction** (also called "bin-packing" or "OPTIMIZE"): read the many
small files and rewrite them as a few right-sized files, ideally sorted by your
dominant filter column so the new files also prune well.

The plain-Parquet version is just a read-and-rewrite:

```python
# Compact a directory of tiny files into one (or a few) right-sized file(s).
import pyarrow.dataset as ds
import pyarrow.parquet as pq

dataset = ds.dataset("s3://crunch-lake/tiny", format="parquet", filesystem=fs)
combined = dataset.to_table()                       # read all rows
combined = combined.sort_by("pickup_date")          # sort for better pruning
pq.write_table(
    combined,
    "s3://crunch-lake/compacted/part-0001.parquet",
    compression="zstd",
    row_group_size=200_000,
    filesystem=fs,
)
# Then delete the tiny files (atomically, ideally — which is why table formats win).
```

The danger in plain Parquet: between "wrote the big file" and "deleted the small
files," a reader can see *both* and double-count, or see *neither* and read an
empty table. There is no atomic swap. This is precisely the gap that table formats
(next lecture) close: Iceberg's `rewrite_data_files` and Delta's `OPTIMIZE` compact
*as a transaction*, so readers always see exactly one consistent set of files. You
will do the plain-Parquet compaction in Challenge 02 and contrast it with the
transactional version once you have Iceberg in Lecture 03.

## 6. Iceberg's hidden partitioning — a preview

Hive-style partitioning has two sharp edges that Apache Iceberg removes, and it is
worth previewing here because it reframes everything above
(<https://iceberg.apache.org/docs/latest/partitioning/>):

1. **The query must reference the partition column.** With Hive partitioning, to
   prune you must write `WHERE year = 2024 AND month = 3`. If an analyst writes the
   natural `WHERE pickup_ts >= '2024-03-01' AND pickup_ts < '2024-04-01'`, the
   engine may *not* prune, because `pickup_ts` is not the partition column `month`.
   You end up maintaining a derived `month` column and teaching everyone to filter
   on it.
2. **You cannot change the partition scheme without rewriting all the data**,
   because the layout is baked into directory paths.

Iceberg fixes both with **hidden partitioning**: you declare a *partition
transform* such as `day(pickup_ts)` or `bucket(16, user_id)` in the table metadata.
Iceberg stores the partition value per file in its manifests and *derives the
partition from the source column automatically*. Queries filter on the raw column
(`WHERE pickup_ts >= ...`) and Iceberg still prunes, because it knows the transform.
And because the partition spec lives in metadata, you can **evolve the partition
scheme** (e.g. from `month` to `day`) for new data without rewriting old data —
old files keep their old spec, new files use the new one, and reads work across
both. We unpack the manifest mechanism in Lecture 03.

## 7. Object storage with MinIO — the laptop S3

Everything above lands on **object storage**, not a local disk and not a database.
Object storage (S3 and its compatibles) is the substrate of the lakehouse: cheap,
infinitely scalable, HTTP-accessed, and — critically — it stores *immutable
objects with no rename and only eventual list consistency in some
implementations*. There is no "append to a file" and no cheap atomic rename. That
property is exactly *why* you need a table format to get transactions: you cannot
lean on the filesystem to give you atomicity, because object storage does not have
filesystem semantics.

For the labs you run **MinIO**, an open-source S3-compatible server, in Docker
(<https://min.io/docs/minio/linux/index.html>). It speaks the S3 API, so PyArrow
(via `s3fs`), DuckDB (via the `httpfs` extension), `pyiceberg`, and `delta-rs` all
talk to it unchanged — the same code runs against AWS S3 in production.

```yaml
# docker-compose.yml — single-node MinIO for the week (full version in the lab).
services:
  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"   # S3 API
      - "9001:9001"   # web console
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - ./minio-data:/data
```

Pointing DuckDB at it:

```sql
INSTALL httpfs; LOAD httpfs;
SET s3_endpoint='localhost:9000';
SET s3_url_style='path';        -- MinIO uses path-style URLs
SET s3_use_ssl=false;           -- local, no TLS
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';

SELECT count(*) FROM read_parquet('s3://crunch-lake/trips/**/*.parquet',
                                  hive_partitioning = true);
```

DuckDB httpfs/S3 reference: <https://duckdb.org/docs/extensions/httpfs/overview>.
Pointing PyArrow at it (Exercise 02):

```python
import s3fs
fs = s3fs.S3FileSystem(
    key="minioadmin", secret="minioadmin",
    client_kwargs={"endpoint_url": "http://localhost:9000"},
)
fs.ls("crunch-lake/trips")    # verify the Hive directory layout you wrote
```

## Summary

- **Hive-style partitioning** (`col=value/` directories) lets the engine skip whole
  files via **partition pruning** — coarser and cheaper than row-group pruning, and
  it composes with it and with projection pushdown.
- **Partition on a low-cardinality column you filter on** (usually date); **never**
  partition on a high-cardinality column — that manufactures the small-files
  problem.
- **File-size target ~128 MB–1 GB.** Too small ⇒ open/parse overhead dominates; too
  large ⇒ lost parallelism and coarse row-group pruning.
- **The small-files problem** is real and scales with file *count*: thousands of
  tiny files spend most of a query's time opening and parsing footers, far worse on
  object storage where each open is an HTTP request.
- **Compaction** rewrites tiny files into right-sized, sorted files; in plain
  Parquet this has no atomicity, which is why table formats provide transactional
  `rewrite_data_files` / `OPTIMIZE`.
- **Iceberg hidden partitioning** records a partition *transform* in metadata, so
  queries filter on the raw column and the partition scheme can evolve without
  rewriting old data.
- **MinIO** is a local, open-source, S3-compatible object store; the same lakehouse
  code runs against it and against AWS S3. Object storage's lack of atomic rename is
  *why* you need a table format for transactions.

Cited pages: DuckDB Parquet <https://duckdb.org/docs/data/parquet/overview>;
DuckDB httpfs/S3 <https://duckdb.org/docs/extensions/httpfs/overview>; PyArrow
Parquet <https://arrow.apache.org/docs/python/parquet.html>; Iceberg partitioning
<https://iceberg.apache.org/docs/latest/partitioning/>; MinIO docs
<https://min.io/docs/minio/linux/index.html>.
