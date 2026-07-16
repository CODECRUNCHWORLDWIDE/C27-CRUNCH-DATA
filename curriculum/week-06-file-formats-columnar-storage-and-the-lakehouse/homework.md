# Week 6 — Homework

Six problems, ~45 minutes each. Each has a concrete deliverable filename and real
citations. Do them after the matching lecture; they reinforce, they do not
introduce. Submit all deliverables in a `week06-homework/` directory.

---

## HW1 — Read a Parquet footer and write it up (≈45 min)

Take any Parquet file you produced this week (or convert a CSV you have). Using
PyArrow's metadata API, produce a short report covering: number of rows, number of
row groups, schema, and for **three columns of different cardinality** (e.g. a
low-cardinality category, an integer, and a high-cardinality string) their
**encodings**, compression codec, and compressed size. Explain which column got
dictionary encoding, which fell back to PLAIN, and why.

- **Deliverable:** `hw1-parquet-footer-report.md` (with the script that produced it
  inline or alongside as `hw1_inspect.py`).
- **Cite:** PyArrow Parquet <https://arrow.apache.org/docs/python/parquet.html>;
  Parquet encodings <https://parquet.apache.org/docs/file-format/data-pages/encodings/>.

---

## HW2 — CSV vs Parquet, measured (≈45 min)

Generate or use a ≥ 1M-row dataset. Write it as CSV (uncompressed), CSV (gzip), and
Parquet (ZSTD). Record on-disk size for each and the time DuckDB takes to run one
selective analytical query (a filtered `SUM ... GROUP BY`) against each. Present a
table and a one-paragraph explanation of *why* Parquet wins on both size and scan
time (typing, columnar projection, per-column compression, statistics).

- **Deliverable:** `hw2-csv-vs-parquet.md` with the results table and `hw2_bench.py`.
- **Cite:** DuckDB Parquet <https://duckdb.org/docs/data/parquet/overview>; Arrow
  project <https://arrow.apache.org/>.

---

## HW3 — Design a partition scheme (≈45 min)

For a clickstream table (`event_ts`, `user_id`, `event_type`, `country`,
`payload`), at ~50 GB/year and ~2M distinct users, write a one-page design that
answers: which column(s) you partition on and why; what file size you target and
why; one column you would explicitly *not* partition on and the small-files failure
it would cause; and how Iceberg hidden partitioning would let analysts filter on
`event_ts` without referencing a derived partition column. Include the Hive
directory layout your scheme produces.

- **Deliverable:** `hw3-partition-design.md`.
- **Cite:** Iceberg partitioning <https://iceberg.apache.org/docs/latest/partitioning/>;
  DuckDB Parquet (Hive partitioning) <https://duckdb.org/docs/data/parquet/overview>.

---

## HW4 — Compaction maintenance note (≈45 min)

Using your Challenge 02 numbers (or fresh measurements), write a maintenance
runbook entry: the symptom (query slow, file count high), the diagnosis (per-file
open/HTTP overhead scales with count, not bytes), the fix (compact to ~128 MB–1 GB
files sorted by the filter column), the safety concern in plain Parquet
(delete-after-write has no atomic swap), and the table-format alternative
(`rewrite_data_files` / `OPTIMIZE` as one transaction). Include a trigger condition
("compact when a partition exceeds N files").

- **Deliverable:** `hw4-compaction-runbook.md`.
- **Cite:** DuckDB httpfs <https://duckdb.org/docs/extensions/httpfs/overview>;
  Iceberg partitioning/maintenance <https://iceberg.apache.org/docs/latest/partitioning/>.

---

## HW5 — Iceberg vs Delta, structural compare (≈45 min)

Create the *same* small dataset as both an Iceberg table and a Delta table on
MinIO. Then, by listing the objects on disk, document the concrete metadata layout
each produced: for Iceberg the `metadata/*.json`, manifest list, and manifest
files; for Delta the `_delta_log/*.json` (and any checkpoint). Explain how each
format answers "what files are the current table?" and how each performs time
travel. End with one sentence on when you would reach for each.

- **Deliverable:** `hw5-iceberg-vs-delta.md` with the object listings pasted in.
- **Cite:** Iceberg spec <https://iceberg.apache.org/spec/>; Delta protocol
  <https://github.com/delta-io/delta/blob/master/PROTOCOL.md>; delta-rs
  <https://delta-io.github.io/delta-rs/>.

---

## HW6 — Time travel + schema evolution narrative (≈45 min)

On your Iceberg table, perform: (a) two appends, (b) an add-column, (c) a rename of
an existing column, and (d) a time-travel read to before the add-column. Write up,
for each operation, **what changed in the metadata and what did NOT change on
disk** (i.e. that no data files were rewritten for the schema changes, and how the
stable column id makes rename safe). Include the snapshot history and the schema
before/after.

- **Deliverable:** `hw6-time-travel-evolution.md` (+ the `hw6_evolve.py` script).
- **Cite:** Iceberg evolution <https://iceberg.apache.org/docs/latest/evolution/>;
  PyIceberg <https://py.iceberg.apache.org/>.

---

### Submission

Commit `week06-homework/` with all six deliverables and any supporting scripts to
your course repo and open a PR. Each `.md` should be self-contained — a reader who
did not run your code should still understand what you measured and concluded.
Where a problem asks for numbers, the numbers must come from output you actually
captured, not estimates.

Licensed **GPL-3.0**. PR improvements back to <https://github.com/CODE-CRUNCH-CLUB>.
