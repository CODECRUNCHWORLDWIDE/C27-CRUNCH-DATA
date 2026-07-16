# Challenge 02 — Reproduce the Small-Files Problem, Then Compact It

> **Time:** ~2 hours · **Prerequisites:** Lecture 02, Exercise 02 ·
> **Citations:** DuckDB Parquet
> <https://duckdb.org/docs/data/parquet/overview>, DuckDB httpfs
> <https://duckdb.org/docs/extensions/httpfs/overview>, PyArrow Parquet
> <https://arrow.apache.org/docs/python/parquet.html>, MinIO docs
> <https://min.io/docs/minio/linux/index.html>, Iceberg maintenance/partitioning
> <https://iceberg.apache.org/docs/latest/partitioning/>

## Premise

Streaming and micro-batch ingestion creates the same pathology over and over: many
tiny files. The slogan is "small files are slow." This challenge makes you *feel*
the wall-clock cost on object storage, then fix it with compaction and measure the
improvement — the exact maintenance task you would schedule in Airflow (Week 4).
You will write the same data three ways (one big file, a thousand tiny files, then
compacted), and you will leave with a number: "compaction cut this query from X s to
Y s and Z files to W files."

## Setup

```yaml
# docker-compose.yml — MinIO (object storage makes the small-files cost real,
# because each file open is a separate HTTP request).
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
pip install pyarrow duckdb s3fs numpy
docker compose up -d
```

Build a dataset and a MinIO filesystem handle:

```python
import numpy as np, datetime as dt
import pyarrow as pa, pyarrow.parquet as pq, s3fs

fs = s3fs.S3FileSystem(key="minioadmin", secret="minioadmin",
                       client_kwargs={"endpoint_url": "http://localhost:9000"})
if not fs.exists("crunch-lake"):
    fs.mkdir("crunch-lake")

rng = np.random.default_rng(3)
N = 4_000_000
base = dt.date(2024, 1, 1)
days = np.sort(rng.integers(0, 90, size=N))
date = np.array([base + dt.timedelta(days=int(d)) for d in days])
fare = np.round(rng.gamma(2.0, 8.0, size=N), 2)
tbl = pa.table({"pickup_date": pa.array(date, pa.date32()),
                "fare_amount": pa.array(fare, pa.float64())})
```

## Steps

1. **Write one big file** to `crunch-lake/big/part-0000.parquet` (one healthy file,
   ~tens of MB) with `pq.write_table(tbl, ..., filesystem=fs)`.
2. **Write 1,000 tiny files** to `crunch-lake/tiny/` by slicing `tbl` into 1,000
   chunks and writing each as its own Parquet object. Same total bytes, 1,000×
   the file count.
3. **Measure both.** Configure DuckDB for MinIO (`SET s3_endpoint`, `s3_url_style
   ='path'`, `s3_use_ssl=false`, keys). Run the **same** query against each layout
   several times and record wall-clock:
   ```sql
   SELECT count(*), SUM(fare_amount), MIN(pickup_date), MAX(pickup_date)
   FROM read_parquet('s3://crunch-lake/<big|tiny>/*.parquet');
   ```
   Also record the file count (`len(fs.find(...))`) and total bytes for each.
4. **Compact** the tiny layout: read all of `crunch-lake/tiny/` with a PyArrow
   dataset, sort by `pickup_date`, and rewrite as a small number of right-sized
   files into `crunch-lake/compacted/`. Delete the tiny files only *after* the
   compacted write succeeds (think about why ordering matters with no atomic swap).
5. **Re-measure** the same query against `crunch-lake/compacted/` and report the
   before/after: query time, file count, and (optionally) request count from MinIO
   metrics.

```python
# Step 4 reference shape — read, sort, rewrite right-sized.
import pyarrow.dataset as ds
dataset = ds.dataset("crunch-lake/tiny", format="parquet", filesystem=fs)
combined = dataset.to_table().sort_by("pickup_date")
ds.write_dataset(combined, "crunch-lake/compacted", format="parquet",
                 filesystem=fs, max_rows_per_file=1_000_000,
                 file_options=ds.ParquetFileFormat().make_write_options(compression="zstd"))
fs.rm("crunch-lake/tiny", recursive=True)   # only after the write above succeeded
```

## Acceptance criteria

- A before/after table: `layout | file_count | total_bytes | query_time_ms`
  covering big, tiny, and compacted.
- The tiny layout is **dramatically slower** than big/compacted for the same query
  and the same total bytes — your measurement, not an assertion.
- Compaction reduces the file count from ~1,000 to a handful and brings query time
  back to roughly the big-file level.
- A short written note: why the slowdown scales with *file count* not *data size*
  (open + list + footer-parse + per-object HTTP request overhead), and why the
  delete-after-write ordering in step 4 is unsafe without a table format.

## Stretch goals

- **Do it transactionally with Iceberg.** Create an Iceberg table over the tiny
  files, then call `rewrite_data_files` (PyIceberg/Spark) and observe that
  compaction is a single atomic commit — readers never see a torn state, unlike the
  plain-Parquet delete-after-write. Reference:
  <https://iceberg.apache.org/docs/latest/partitioning/> and the maintenance docs.
- **Find the knee.** Write the data as 10, 100, 1,000, and 10,000 files; plot query
  time vs file count and identify where overhead starts to dominate. Estimate the
  per-file fixed cost from the slope.
- **Right-sizing experiment.** Compact to target file sizes of ~8 MB, ~64 MB, and
  ~256 MB; measure query time and discuss the 128 MB–1 GB rule of thumb in light of
  your laptop's numbers.
- **Schedule it.** Sketch (or build) an Airflow DAG (Week 4) that runs the
  compaction nightly and only when the file count in a partition exceeds a
  threshold.

## Cited references

- DuckDB Parquet (globbing, scanning many files): <https://duckdb.org/docs/data/parquet/overview>
- DuckDB httpfs/S3 (reading from MinIO): <https://duckdb.org/docs/extensions/httpfs/overview>
- PyArrow datasets (`write_dataset`, `max_rows_per_file`): <https://arrow.apache.org/docs/python/parquet.html>
- MinIO (object metrics, console): <https://min.io/docs/minio/linux/index.html>
- Iceberg partitioning/maintenance (transactional compaction context): <https://iceberg.apache.org/docs/latest/partitioning/>
