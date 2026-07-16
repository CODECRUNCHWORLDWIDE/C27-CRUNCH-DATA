# Challenge 01 — Prove Predicate Pushdown by Measuring Bytes Scanned

> **Time:** ~2 hours · **Prerequisites:** Lecture 01, Exercise 01 ·
> **Citations:** Parquet file format
> <https://parquet.apache.org/docs/file-format/>, PyArrow Parquet
> <https://arrow.apache.org/docs/python/parquet.html>, DuckDB Parquet
> <https://duckdb.org/docs/data/parquet/overview>, DuckDB httpfs
> <https://duckdb.org/docs/extensions/httpfs/overview>

## Premise

"Predicate pushdown reads fewer bytes" is a claim. This challenge makes you *prove*
it with numbers, two ways: (a) by computing, from the Parquet footer statistics,
exactly which row groups a filter prunes, and (b) by measuring the difference
between a layout that prunes well (sorted by the filter column) and one that prunes
nothing (shuffled), for the *same query on the same number of bytes*. You will
finish able to say "this filter scanned N of M row groups, ~X MB of ~Y MB" and back
it with footer evidence.

## Setup

You can run this entirely on local files, or land the data on MinIO to make the
byte-savings visible as request sizes. The MinIO path is optional but recommended.

```yaml
# docker-compose.yml (optional, for the MinIO variant)
services:
  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    ports: ["9000:9000", "9001:9001"]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes: ["./minio-data:/data"]
```

```bash
pip install pyarrow duckdb numpy s3fs
docker compose up -d   # only if doing the MinIO variant
```

Build two Parquet files with **identical data and identical row-group size**, one
**sorted by `pickup_date`** and one **shuffled**:

```python
import numpy as np, datetime as dt
import pyarrow as pa, pyarrow.parquet as pq

rng = np.random.default_rng(1)
N = 5_000_000
base = dt.date(2024, 1, 1)
days = rng.integers(0, 90, size=N)
date = np.array([base + dt.timedelta(days=int(d)) for d in days])
fare = np.round(rng.gamma(2.0, 8.0, size=N), 2)

tbl = pa.table({"pickup_date": pa.array(date, pa.date32()),
                "fare_amount": pa.array(fare, pa.float64())})

order = np.argsort(days)                       # sorted by date
sorted_tbl = tbl.take(pa.array(order))
pq.write_table(sorted_tbl, "sorted.parquet",  row_group_size=250_000,
               compression="zstd", write_statistics=True)
pq.write_table(tbl,        "shuffled.parquet", row_group_size=250_000,
               compression="zstd", write_statistics=True)
```

## Steps

1. **Count the row groups and read their `pickup_date` min/max** for both files
   using `pq.ParquetFile(...).metadata` (PyArrow) or `parquet_metadata(...)`
   (DuckDB). Confirm both files have the same number of row groups and the same
   total compressed size (same bytes, different order).
2. **For the filter `pickup_date = DATE '2024-03-15'`, compute the prune set by
   hand** for each file: a row group is pruned when `target < min OR target > max`.
   Report `READ_groups / total_groups` for sorted and for shuffled.
3. **Sum the `total_compressed_size` of only the READ row groups** for each file
   (you can read per-row-group, per-column compressed sizes from the footer). This
   is your "bytes that must be scanned" estimate. Report sorted vs shuffled.
4. **Confirm with DuckDB `EXPLAIN ANALYZE`** that the filtered query over
   `sorted.parquet` emits far fewer rows from the `PARQUET_SCAN` operator than over
   `shuffled.parquet`, even though both contain the same matching rows.
5. **(MinIO variant)** Put both files in MinIO, point DuckDB at them via httpfs,
   and observe in MinIO's console/metrics (or by timing) that the sorted query
   transfers materially less data.

## Acceptance criteria

- A table reporting, for each file: `total_row_groups`, `READ_row_groups`,
  `bytes_in_read_groups`, and the ratio `bytes_in_read_groups / total_bytes`.
- The sorted file reads roughly 1–2 of ~20 row groups (~5–10% of bytes); the
  shuffled file reads all ~20 (≈100% of bytes) — same filter, same data.
- DuckDB `EXPLAIN ANALYZE` evidence pasted for both, with the scan's emitted-row
  counts circled/annotated.
- A two-sentence written explanation of *why* sort order — not file size — is what
  decided how many bytes the filter scanned.

## Stretch goals

- Add a **second predicate column** (`vendor`) and sort by `(pickup_date, vendor)`;
  show a compound filter prunes even more, and explain why the sort *order* of the
  keys matters (the leading key prunes best).
- Repeat with **`row_group_size=25_000`** (more, smaller groups) and
  `row_group_size=2_500_000` (fewer, larger groups). Plot READ-groups vs row-group
  size and discuss the pruning-granularity vs metadata-overhead trade.
- Use `parquet_metadata()` in DuckDB to compute the bytes-scanned estimate purely
  in SQL, no Python.
- Disable statistics (`write_statistics=False`) on a third file and show the
  sorted layout no longer prunes — proving stats, not order alone, enable pushdown.

## Cited references

- Parquet file format (footer, statistics): <https://parquet.apache.org/docs/file-format/>
- PyArrow metadata API (`row_group`, `column`, `statistics`): <https://arrow.apache.org/docs/python/parquet.html>
- DuckDB Parquet (`parquet_metadata`, `EXPLAIN ANALYZE`): <https://duckdb.org/docs/data/parquet/overview>
- DuckDB httpfs/S3 (MinIO variant): <https://duckdb.org/docs/extensions/httpfs/overview>
