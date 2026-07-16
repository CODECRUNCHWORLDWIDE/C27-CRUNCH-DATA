# Week 6 — File Formats, Columnar Storage & the Lakehouse

> Code Crunch Club · C27 · Crunch Data · sub-brand **Data** (`#0EA5E9`)
> Phase II — The Lakehouse & Distributed Compute · Week 6 of 12 · ~33 hours

You have spent five weeks treating storage as a place where rows live and queries
go to read them. This week you open the file and look at the bytes. By Friday you
will be able to explain — from the byte layout up — why a columnar scan with
predicate pushdown reads a fraction of the data a row store would, what an ACID
table format adds on top of plain Parquet files, and how time travel and schema
evolution actually work when you peel back the metadata. The lecture spine for the
week is one sentence you should be able to defend in an interview: **the table
format is the contract, the engine is interchangeable.** Parquet, Iceberg, Delta,
DuckDB, and Spark are not a stack you memorize; they are layers with clean seams,
and once you can name the seam you can swap either side of it.

This is the first week of Phase II, and Phase II has a deliberate order: **storage
before compute.** Week 7 puts Apache Spark on top of the exact lakehouse you build
this week, and Week 8 starts streaming into it. None of that distributed compute
makes sense until you understand what it is computing *over* — a pile of Parquet
files on object storage, given transactional semantics by a metadata layer. So we
spend a full week on the storage layer alone, on a laptop, with MinIO standing in
for S3, DuckDB and PyArrow standing in for a cluster, and `pyiceberg` and
`delta-rs` standing in for a managed catalog. Everything you touch is open source
and runs in Docker; nothing here needs a cloud account.

Here are the **ten things to internalize this week**, each connected to where you
have already been:

1. **OLAP scans few columns over many rows; that single access pattern justifies
   columnar storage.** In Week 1 you built a star schema and in Week 2 you tuned
   analytical SQL. Almost every query you wrote touched a handful of columns
   (`SUM(amount) GROUP BY day`) but billions of rows. A row store
   (Postgres heap, a CSV) interleaves every column of every row, so reading one
   column forces you to drag the other forty past the disk head. Columnar storage
   co-locates a column's values, so a scan reads only the columns the query names.
   That is not a micro-optimization; it is a different shape of file.

2. **CSV and JSON are wire formats, not analytics formats.** They are untyped,
   uncompressed, row-oriented, and re-parsed on every read. A CSV of the taxi data
   has no schema, no statistics, and no way to skip rows you do not need. Parquet
   is typed, compressed per column, self-describing, and carries statistics that
   let a reader skip whole blocks. The same data is routinely 5–10× smaller and an
   order of magnitude faster to scan once it is Parquet. You will measure this
   yourself in Exercise 01.

3. **Parquet has a precise internal structure, and predicate pushdown lives in
   it.** A Parquet file is *file → row groups → column chunks → pages*, with a
   footer of metadata that includes per-column-chunk min/max/null-count
   statistics. When you filter `WHERE pickup_date = '2024-03-01'`, the reader looks
   at each row group's statistics, sees that a row group's max date is before your
   target, and skips the whole row group without decoding a single value. That is
   predicate pushdown / row-group pruning, and it is why partitioning and sort
   order matter so much.

4. **Encodings make columns small, and dictionary + RLE are the workhorses.** A
   column of `vendor_id` with three distinct values does not store the string three
   billion times; it builds a dictionary (`0→CMT, 1→VTS, 2→DDS`) and stores tiny
   integer codes, then run-length-encodes runs of the same code. You will read the
   actual encodings out of a file's metadata with PyArrow and see dictionary
   encoding kick in and fall back when cardinality is too high.

5. **Partitioning is physical layout you choose so the engine can prune.**
   Hive-style partitioning writes `year=2024/month=03/` directories so a query with
   `WHERE year = 2024` never opens the other directories. It is the coarse-grained
   sibling of row-group statistics. Partition on a low-cardinality column you filter
   on constantly (date), never on a high-cardinality one (user id) — that is the
   road to the small-files problem.

6. **The small-files problem is real and it has a number.** Every file a query
   opens costs a metadata read, a connection, and a footer parse. A thousand 1 MB
   Parquet files take dramatically longer to scan than one 1 GB file with the same
   bytes, because you pay the open-and-parse tax a thousand times. The target file
   size for an analytics table is roughly **128 MB to 1 GB**. You will reproduce the
   pathology in Challenge 02 and fix it with compaction.

7. **Plain Parquet on object storage has no transactions, and that is the gap a
   table format fills.** A directory of Parquet files cannot tell you which files
   belong to "the current version of the table." If a writer adds three files and a
   reader scans mid-write, the reader sees a torn, inconsistent table. There is no
   atomic commit, no isolation, no way to ask "what did this table look like
   yesterday." Everything you learned about transactions in a database (Week 1/2)
   is simply *absent* from a bag of files.

8. **A table format adds a metadata layer that provides ACID, snapshots, and time
   travel.** Apache Iceberg tracks the set of files that make up each *snapshot* in
   a tree of manifest files plus a catalog pointer; Delta Lake tracks it in a
   `_delta_log` of ordered JSON commits with periodic checkpoints. Both make a
   commit atomic (swap one pointer / append one log entry), give readers snapshot
   isolation (you read the snapshot that was current when you started), and let you
   *time travel* by reading an older snapshot by id or timestamp. This is the single
   most important conceptual jump of the week.

9. **Schema evolution and hidden partitioning are metadata tricks, not data
   rewrites.** Iceberg assigns every column a stable *id*, so renaming a column or
   adding one is a metadata change — old files are read through the id mapping, no
   data is rewritten. Hidden partitioning records the *partition transform*
   (e.g. `day(ts)`) in metadata, so queries filter on the raw column (`WHERE ts >=
   ...`) and the engine derives the partition itself — you never write
   `WHERE day = ...` and you can change the partition scheme without rewriting old
   data. Delta does add-column evolution too; the mechanics differ and you will see
   both.

10. **The engine is interchangeable because the format is the contract.** You will
    write the table once and read it from DuckDB *and* from Delta tooling, and next
    week from Spark, with no data migration — because the contract is the on-disk
    format and its metadata, not any one engine. Internalize this and the rest of
    the modern data stack stops looking like a vendor maze and starts looking like a
    set of swappable parts.

By the end of the week you will have built **a real lakehouse on your laptop**:
the dimensional mart from Weeks 1–5, landed as partitioned Parquet on MinIO,
wrapped in an Iceberg table, queried by DuckDB with provable predicate pushdown,
evolved with a new column, time-traveled to a prior snapshot, and read back as
Delta for comparison. That artifact is the substrate for the rest of Phase II.

---

## Learning objectives

By the end of Week 6 you will be able to:

- **Explain** the difference between row and columnar storage from the access
  pattern up, and quantify why a columnar scan reads fewer bytes for an analytical
  query — Apache Parquet docs, file-format overview: <https://parquet.apache.org/docs/file-format/>
- **Read** a Parquet file's internal structure with PyArrow — row groups, column
  chunks, pages, encodings, and per-column statistics — and predict which row
  groups a filter will prune — PyArrow Parquet guide: <https://arrow.apache.org/docs/python/parquet.html>
- **Describe** Parquet's column encodings (dictionary, RLE/bit-packing, plain) and
  identify which one a given column used — Parquet encodings spec: <https://parquet.apache.org/docs/file-format/data-pages/encodings/>
- **Write** Hive-style partitioned Parquet to S3-compatible object storage (MinIO)
  and verify the directory layout supports partition pruning — DuckDB Parquet docs:
  <https://duckdb.org/docs/data/parquet/overview>
- **Diagnose and fix** the small-files problem by measuring open/metadata overhead
  and compacting tiny files into right-sized ones — DuckDB httpfs/S3:
  <https://duckdb.org/docs/extensions/httpfs/overview>
- **Create and query** an Apache Iceberg table over object storage with `pyiceberg`
  and the DuckDB Iceberg extension — Iceberg docs: <https://iceberg.apache.org/docs/latest/>
  and PyIceberg: <https://py.iceberg.apache.org/>
- **Perform** schema evolution (add column) and a time-travel query to a prior
  snapshot, explaining the metadata mechanism behind each — Iceberg evolution:
  <https://iceberg.apache.org/docs/latest/evolution/>
- **Contrast** Iceberg (manifest tree + catalog) with Delta Lake (`_delta_log` +
  checkpoints), reading the same data through both — Delta protocol:
  <https://github.com/delta-io/delta/blob/master/PROTOCOL.md>
- **Prove** predicate pushdown empirically by measuring bytes / row groups scanned
  with and without a filter — Iceberg spec (statistics & manifests):
  <https://iceberg.apache.org/spec/>
- **Stand up** MinIO as a local S3-compatible object store in Docker and point
  PyArrow, DuckDB, and the table-format tooling at it — MinIO docs:
  <https://min.io/docs/minio/linux/index.html>

---

## Prerequisites

You should arrive at Week 6 having completed Weeks 1–5:

- **Week 1 (dimensional modeling).** You have a star schema with a fact table and
  conformed dimensions. This week you land *that* mart on the lakehouse, so have
  it queryable in DuckDB.
- **Week 2 (advanced SQL + query plans).** You can read a query plan and reason
  about why a scan is slow. Predicate pushdown is the storage-layer version of the
  index-vs-scan reasoning you did there; `EXPLAIN ANALYZE` returns this week.
- **Week 3 (idempotent Python ETL).** Writing partitioned Parquet idempotently
  (overwrite a partition, do not double-append) reuses the idempotency discipline
  from Week 3.
- **Week 4 (Airflow/Dagster).** Not directly required this week, but the compaction
  job in Challenge 02 is exactly the kind of maintenance task you would schedule.
- **Week 5 (dbt against DuckDB).** You know DuckDB well: extensions, `httpfs`, and
  reading Parquet. This week you point DuckDB at MinIO and at Iceberg/Delta tables.

Environment: a laptop with **Docker** and **Docker Compose**, **Python 3.10+**,
and a virtualenv with `pyarrow`, `duckdb`, `pyiceberg[s3fs,duckdb]`,
`deltalake` (delta-rs), `s3fs`, and `boto3`. A single MinIO container is all the
infrastructure you need; exact `docker-compose.yml` and `pip` lines are in the
challenges and the mini-project.

---

## Topics covered

- Row vs columnar storage and the OLAP access pattern
- CSV / JSON vs Parquet — typing, compression, self-description, statistics
- Parquet internals: file → row groups → column chunks → pages → footer metadata
- Column encodings: dictionary, RLE / bit-packing, plain; when each is chosen
- Per-column-chunk statistics (min / max / null_count) and row-group pruning
- Predicate pushdown and projection pushdown, proven from the bytes scanned
- Hive-style partitioning, partition pruning, and choosing a partition column
- File sizing and the small-files problem; compaction
- Object storage with MinIO (S3-compatible) on a laptop
- Apache Iceberg: manifests, snapshots, catalog, hidden partitioning
- Delta Lake: `_delta_log`, JSON commits, checkpoints
- ACID commits, snapshot isolation, time travel, schema evolution by column id

---

## Weekly schedule

Target ~33 hours. Hours are guidance, not a contract; spend them where you learn.

| Day | Focus | Lectures | Exercises | Challenges | Quiz/Read | Homework | Mini-Project | Self-Study | Daily Total |
| --- | --- | --: | --: | --: | --: | --: | --: | --: | --: |
| Mon | Row vs columnar, Parquet internals | 2.0 | 1.5 | 0.0 | 0.5 | 0.0 | 0.0 | 1.0 | **5.0** |
| Tue | Partitioning, file sizing, small files | 2.0 | 1.5 | 1.0 | 0.0 | 0.5 | 0.0 | 0.0 | **5.0** |
| Wed | Iceberg & Delta: ACID, time travel, evolution | 2.0 | 1.5 | 0.0 | 0.5 | 0.5 | 0.0 | 0.5 | **5.0** |
| Thu | Challenges: pushdown bytes + compaction | 0.0 | 0.0 | 2.5 | 0.0 | 1.0 | 1.0 | 0.5 | **5.0** |
| Fri | Mini-project: Crunch Lakehouse on MinIO | 0.0 | 0.0 | 0.0 | 0.0 | 1.0 | 3.5 | 0.5 | **5.0** |
| Sat | Mini-project finish + homework | 0.0 | 0.0 | 0.0 | 0.0 | 1.5 | 2.0 | 1.0 | **4.5** |
| Sun | Quiz, review, resources | 0.0 | 0.0 | 0.0 | 1.5 | 0.5 | 0.0 | 1.5 | **3.5** |
| **Total** | | **6.0** | **4.5** | **4.5** | **3.0** | **5.0** | **6.5** | **5.5** | **33.0** |

---

## How to navigate this week

| File | What it is | When to use it |
| --- | --- | --- |
| [`README.md`](./README.md) | This overview | Start here |
| [`lecture-notes/01-row-vs-columnar-and-parquet-internals.md`](./lecture-notes/01-row-vs-columnar-and-parquet-internals.md) | Row vs columnar, Parquet byte layout, encodings, statistics, pushdown | Monday lecture |
| [`lecture-notes/02-partitioning-file-sizing-and-the-small-files-problem.md`](./lecture-notes/02-partitioning-file-sizing-and-the-small-files-problem.md) | Partitioning, pruning, file sizing, small files, compaction | Tuesday lecture |
| [`lecture-notes/03-iceberg-delta-acid-time-travel-and-schema-evolution.md`](./lecture-notes/03-iceberg-delta-acid-time-travel-and-schema-evolution.md) | Table formats: ACID, snapshots, time travel, evolution, Iceberg vs Delta | Wednesday lecture |
| [`exercises/exercise-01-parquet-internals.py`](./exercises/exercise-01-parquet-internals.py) | Inspect a Parquet file's metadata; CSV vs Parquet size/scan | Mon/Tue, ~1.5h |
| [`exercises/exercise-02-partitioned-parquet-on-minio.py`](./exercises/exercise-02-partitioned-parquet-on-minio.py) | Write partitioned Parquet to MinIO; verify layout | Tue, ~1.5h |
| [`exercises/exercise-03-iceberg-time-travel-and-evolution.py`](./exercises/exercise-03-iceberg-time-travel-and-evolution.py) | Create/query Iceberg; add-column; time-travel read | Wed, ~1.5h |
| [`exercises/SOLUTIONS.md`](./exercises/SOLUTIONS.md) | Reference solutions, expected output, pitfalls | After attempting |
| [`challenges/challenge-01-predicate-pushdown-bytes-scanned.md`](./challenges/challenge-01-predicate-pushdown-bytes-scanned.md) | Prove pushdown by measuring bytes / row groups scanned | Thu, ~2h |
| [`challenges/challenge-02-compaction-and-the-small-files-problem.md`](./challenges/challenge-02-compaction-and-the-small-files-problem.md) | Reproduce small files, measure, compact, re-measure | Thu, ~2h |
| [`mini-project/README.md`](./mini-project/README.md) | Crunch Lakehouse on MinIO — graded build | Fri–Sat, ~6.5h |
| [`homework.md`](./homework.md) | Six ~45-min problems with deliverables | Across the week |
| [`quiz.md`](./quiz.md) | 10 MC questions + answer key + self-assessment | Sunday |
| [`resources.md`](./resources.md) | Canonical references | Throughout |

---

Closing note: the cheapest byte to scan is the one you never read. Everything this
week — columnar layout, statistics, partitioning, pruning, compaction — is a
different mechanism for *not reading data*. And the table format is what makes a
pile of those files behave like a table you can trust, version, and evolve. Next
week Spark arrives and computes over exactly this lakehouse; the week after, the
stream starts writing into it. Build the storage layer well now.

Licensed **GPL-3.0**. Fork, teach, remix; PR improvements back to
<https://github.com/CODE-CRUNCH-CLUB>.
