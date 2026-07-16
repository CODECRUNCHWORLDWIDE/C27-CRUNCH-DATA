# Week 6 — Resources

Canonical, primary sources for everything in Week 6. Prefer these over blog posts;
when you cite in homework, cite from here. All links verified against the official
projects.

---

## Apache Parquet (the file format)

- **Parquet documentation (home):** <https://parquet.apache.org/docs/>
- **File-format overview** (row groups, column chunks, pages, footer):
  <https://parquet.apache.org/docs/file-format/>
- **Data-page encodings** (PLAIN, RLE/bit-packing, dictionary):
  <https://parquet.apache.org/docs/file-format/data-pages/encodings/>
- **parquet-format spec repository** (the Thrift definitions, the authoritative
  structure): <https://github.com/apache/parquet-format>

Read for: §3–§5 of Lecture 01 — the hierarchy, encodings, and the statistics that
drive pruning.

---

## Apache Arrow / PyArrow (reading & writing Parquet in Python)

- **Reading and writing Parquet with PyArrow:**
  <https://arrow.apache.org/docs/python/parquet.html>
- **Apache Arrow project home:** <https://arrow.apache.org/>

Read for: the `pq.ParquetFile`, `metadata`, `row_group`, `column`, `statistics`,
and `write_table` APIs used in Exercise 01 and the challenges.

---

## Apache Iceberg (table format)

- **Iceberg documentation (latest):** <https://iceberg.apache.org/docs/latest/>
- **Iceberg table spec** (snapshots, manifests, manifest lists, statistics):
  <https://iceberg.apache.org/spec/>
- **PyIceberg** (the Python implementation used in Exercise 03):
  <https://py.iceberg.apache.org/>
- **Partitioning incl. hidden partitioning & transforms:**
  <https://iceberg.apache.org/docs/latest/partitioning/>
- **Schema & partition evolution:** <https://iceberg.apache.org/docs/latest/evolution/>
- **Iceberg source repository:** <https://github.com/apache/iceberg>

Read for: Lecture 03 §3, §6–§8 — the manifest tree, catalog, hidden partitioning,
and id-based schema evolution; Exercise 03 and the mini-project.

---

## Delta Lake (table format)

- **Delta Lake documentation:** <https://docs.delta.io/latest/index.html>
- **Delta transaction-log protocol** (the `_delta_log` actions, checkpoints):
  <https://github.com/delta-io/delta/blob/master/PROTOCOL.md>
- **delta-rs (Python `deltalake` package, used on the laptop):**
  <https://delta-io.github.io/delta-rs/>
- **Delta Lake source repository:** <https://github.com/delta-io/delta>

Read for: Lecture 03 §4 and the Iceberg-vs-Delta contrast; the F6 comparison in the
mini-project and HW5.

---

## DuckDB (the laptop query engine)

- **DuckDB documentation (home):** <https://duckdb.org/docs/>
- **Parquet support** (`read_parquet`, `parquet_metadata`, Hive partitioning):
  <https://duckdb.org/docs/data/parquet/overview>
- **httpfs / S3 extension** (reading from MinIO/S3):
  <https://duckdb.org/docs/extensions/httpfs/overview>
- **Iceberg extension** (`iceberg_scan`, `iceberg_snapshots`):
  <https://duckdb.org/docs/extensions/iceberg>
- **Delta extension** (`delta_scan`): <https://duckdb.org/docs/extensions/delta>

Read for: every exercise and challenge — DuckDB is the engine you query the
lakehouse with this week.

---

## MinIO (S3-compatible object storage)

- **MinIO documentation:** <https://min.io/docs/minio/linux/index.html>
- **MinIO source repository:** <https://github.com/minio/minio>

Read for: standing up the local object store in Docker (Exercise 02 onward) and the
S3 endpoint/credentials configuration.

---

## How these fit together this week

```
        MinIO  (S3-compatible object storage — the substrate)
          │
          ├── Parquet files            ← Apache Parquet + PyArrow
          │
          ├── Iceberg metadata          ← Apache Iceberg + PyIceberg + SQLite catalog
          └── _delta_log/               ← Delta Lake + delta-rs
                          ▲
                          │  read by
                  DuckDB (httpfs + iceberg/delta extensions)
                          │
                  [Week 7: Apache Spark joins as a second engine over the same tables]
```

The contract is the format on object storage; the engine (DuckDB this week, Spark
next) is interchangeable.

---

Licensed **GPL-3.0**. Fork, teach, remix; PR improvements (including better or
updated source links) back to <https://github.com/CODE-CRUNCH-CLUB>.
