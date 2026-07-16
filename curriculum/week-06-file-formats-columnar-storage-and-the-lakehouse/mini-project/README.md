# Mini-Project — Crunch Lakehouse on MinIO

> **Time:** ~6.5 hours (Fri–Sat) · **Prerequisites:** all three lectures, all three
> exercises, both challenges · **Citations:** Iceberg
> <https://iceberg.apache.org/docs/latest/>, PyIceberg
> <https://py.iceberg.apache.org/>, Delta protocol
> <https://github.com/delta-io/delta/blob/master/PROTOCOL.md>, delta-rs
> <https://delta-io.github.io/delta-rs/>, DuckDB iceberg
> <https://duckdb.org/docs/extensions/iceberg>, DuckDB delta
> <https://duckdb.org/docs/extensions/delta>, MinIO
> <https://min.io/docs/minio/linux/index.html>

This is the capstone of Week 6: take the dimensional mart you built in Weeks 1–5
and stand it up as a **real lakehouse on object storage** — partitioned Parquet on
MinIO, an Iceberg table over it, predicate-pushdown queries you can *prove* read few
bytes, schema evolution, time travel, and a read of the same data as Delta for
comparison. The artifact becomes the substrate for Week 7 (Spark over this
lakehouse) and beyond. Build it like you will operate it.

## Runtime topology

```
                          your laptop (Docker)
  ┌───────────────────────────────────────────────────────────────────────┐
  │                                                                         │
  │   ┌──────────────┐     S3 API (path-style, :9000)   ┌────────────────┐  │
  │   │  Python ETL  │ ───────────────────────────────► │     MinIO      │  │
  │   │  (pyarrow +  │   writes partitioned Parquet      │  S3-compatible │  │
  │   │   pyiceberg) │   + Iceberg metadata              │ object storage │  │
  │   └──────┬───────┘                                   │                │  │
  │          │ commits via                               │  bucket:       │  │
  │          ▼                                           │  crunch-lake/  │  │
  │   ┌──────────────┐                                   │   warehouse/   │  │
  │   │ Iceberg      │  pointer: table -> metadata.json  │    crunch.db/  │  │
  │   │ SQL catalog  │ ◄──────────────────────────────── │      trips/    │  │
  │   │ (SQLite)     │                                   │        data/   │  │
  │   └──────────────┘                                   │        metadata│  │
  │                                                      │   trips_delta/ │  │
  │   ┌──────────────┐   reads via httpfs + iceberg/     │    _delta_log/ │  │
  │   │   DuckDB     │   delta extensions                └────────────────┘  │
  │   │  (query +    │ ◄─────────────────────────────────────────┘           │
  │   │   pushdown   │                                                        │
  │   │   proof)     │   [Week 7: Spark replaces/joins DuckDB here]           │
  │   └──────────────┘                                                        │
  └───────────────────────────────────────────────────────────────────────┘
```

Everything is local and open source. MinIO is the only long-running service; the
SQLite catalog is a file; DuckDB, PyArrow, PyIceberg, and delta-rs are libraries.

## Functional requirements

- **F1 — Land partitioned Parquet on MinIO.** Take the Week-1/Week-5 dimensional
  mart (fact + dimensions; if you lack one, use the provided synthetic trips
  generator) and write the fact table to MinIO as Hive-partitioned Parquet
  (partition by month or day), ZSTD-compressed, with row-group statistics on. The
  write must be **idempotent** (re-running overwrites partitions, does not double).
- **F2 — Iceberg table over the data.** Create an Iceberg table (SQLite catalog,
  MinIO warehouse) for the fact table and load at least **two appends** so you have
  **≥ 2 snapshots**. Use a sensible partition transform (e.g. `day(pickup_ts)` or
  `month`).
- **F3 — Predicate-pushdown query with a bytes-scanned proof.** Run a selective
  analytical query (e.g. revenue for one day/vendor) against the table from DuckDB,
  and produce **evidence** of how few bytes/row groups were scanned: Parquet footer
  stats and/or `parquet_metadata()` / `EXPLAIN ANALYZE` showing the READ vs PRUNE
  set. A claim without numbers does not count.
- **F4 — Schema evolution.** Add a new column (e.g. `surcharge`) to the Iceberg
  table and show that pre-existing rows read back NULL and **no old data file was
  rewritten** (compare file counts before/after).
- **F5 — Time travel.** Query the table as of an earlier snapshot and the current
  snapshot, and show the row counts differ as expected. Include the snapshot
  history.
- **F6 — Read the same data as Delta and compare.** Write the same fact data as a
  Delta table (delta-rs) on MinIO, read it back, and write a short comparison: the
  on-disk metadata structure you observe (`_delta_log/*.json` vs Iceberg
  `metadata/` + manifests), how each does time travel, and how each handles the
  add-column.
- **F7 — Compaction note.** Demonstrate (or, at minimum, document with measured
  numbers from Challenge 02) the small-files cost and a compaction that fixes it,
  framed as a maintenance task for this table.

## Non-functional requirements

- **Reproducible from scratch.** A `make setup && make run` (or a documented script
  sequence) brings up MinIO, creates the bucket, and builds the lakehouse with no
  manual console clicking.
- **Idempotent.** Re-running the build does not duplicate data or leave torn state.
- **No cloud, no secrets in code beyond the local MinIO dev creds.** Everything runs
  offline on the laptop.
- **Measured, not asserted.** Every performance/byte claim is backed by output you
  captured (footer stats, `EXPLAIN ANALYZE`, file counts, timings).
- **Engine-agnostic mindset.** Your query code should not hardcode assumptions that
  break when Week 7 swaps DuckDB for Spark over the same table.

## Suggested project layout

```
crunch-lakehouse/
├── README.md                  # what it is, how to run, the comparison write-up
├── Makefile                   # setup / run / clean targets
├── docker-compose.yml         # MinIO
├── requirements.txt           # pyarrow, duckdb, pyiceberg[...], deltalake, s3fs, numpy
├── src/
│   ├── config.py              # endpoint, keys, bucket, paths
│   ├── generate.py            # build/load the dimensional mart (or synthetic trips)
│   ├── land_parquet.py        # F1: partitioned Parquet to MinIO (idempotent)
│   ├── build_iceberg.py       # F2: Iceberg table + two appends
│   ├── query_pushdown.py      # F3: selective query + bytes-scanned proof
│   ├── evolve.py              # F4: add column, verify NULLs + no rewrite
│   ├── time_travel.py         # F5: snapshot history + as-of reads
│   ├── build_delta.py         # F6: same data as Delta + read-back
│   └── compact.py             # F7: small-files demo + compaction
├── proof/
│   ├── pushdown_explain.txt   # captured EXPLAIN ANALYZE / parquet_metadata output
│   ├── snapshots.txt          # snapshot history listing
│   └── file_counts.txt        # before/after add-column + before/after compaction
└── COMPARISON.md              # Iceberg vs Delta observations (F6)
```

## Validation & measurement plan

| What | How you measure it | Evidence to capture |
| --- | --- | --- |
| Partition layout (F1) | `fs.find()` the table path | directory tree with `month=`/`day=` |
| Pushdown (F3) | `parquet_metadata()` READ vs PRUNE; `EXPLAIN ANALYZE` rows out of scan | `proof/pushdown_explain.txt` + a bytes-scanned ratio |
| Snapshots (F2/F5) | PyIceberg `tbl.metadata.snapshots`; DuckDB `iceberg_snapshots(...)` | `proof/snapshots.txt` (≥ 2 snapshots) |
| Schema evolution (F4) | column present + all-NULL on old rows; file count unchanged | `proof/file_counts.txt` before/after |
| Time travel (F5) | row count at old snapshot vs current | numbers in `time_travel.py` output |
| Delta comparison (F6) | read-back row count matches; inspect `_delta_log/` | `COMPARISON.md` |
| Small files (F7) | query time + file count: tiny vs compacted | `proof/file_counts.txt` + timings |

## Grading rubric (100 points)

| Criterion | Points |
| --- | --- |
| F1 — Idempotent partitioned Parquet on MinIO, statistics on | 12 |
| F2 — Iceberg table with ≥ 2 snapshots, sensible partition transform | 14 |
| F3 — Predicate-pushdown query **with captured bytes/row-group proof** | 18 |
| F4 — Schema evolution: NULL on old rows, no rewrite, file-count evidence | 12 |
| F5 — Time travel: snapshot history + correct as-of row counts | 12 |
| F6 — Delta read of the same data + substantive Iceberg-vs-Delta write-up | 12 |
| F7 — Small-files demonstration + compaction with measured improvement | 10 |
| Reproducibility (one-command setup + run, clean re-runs) | 6 |
| Clarity of README/COMPARISON + captured proof artifacts | 4 |
| **Total** | **100** |

Pass mark 70. The 18 points on F3 are deliberately the largest single block: the
whole week is "do not read data you do not need," and proving it from the bytes is
the skill.

## Stretch goals

- **Query the Iceberg table from DuckDB's iceberg extension *and* from PyIceberg's
  DuckDB integration**, and confirm identical results — "the engine is
  interchangeable."
- **Expire old snapshots** (Iceberg `expire_snapshots`) and show time travel to the
  expired snapshot now fails, illustrating the storage-vs-time-travel trade.
- **Hidden-partition evolution:** change the Iceberg partition spec for new data
  (e.g. `month` → `day`) and show old data keeps its old spec while reads span both.
- **Rename a column** via Iceberg schema evolution and prove old files still read
  through the id mapping with no rewrite.
- **Wire one step into Airflow/Dagster** (Week 4): schedule the compaction or the
  daily append as a real DAG task.

## Submission

Push `crunch-lakehouse/` to your course repo and open a PR. The PR description must
include: (1) the `EXPLAIN ANALYZE`/footer evidence for F3 with your bytes-scanned
ratio, (2) the snapshot history, (3) the before/after file counts for evolution and
compaction, and (4) the `COMPARISON.md` Iceberg-vs-Delta write-up. Add a logged
"pipeline-run-and-inspect" entry: a screenshot of the MinIO console showing the
`warehouse/` and `_delta_log/` layout you produced.

Licensed **GPL-3.0**. Fork, teach, remix; PR improvements back to
<https://github.com/CODE-CRUNCH-CLUB>.
