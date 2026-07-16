# Week 6 — Quiz

Ten multiple-choice questions on columnar storage, Parquet internals, predicate
pushdown, partitioning, the small-files problem, and Iceberg/Delta ACID, time
travel, and schema evolution. Pick one answer per question, then check the answer
key (with reasoning and citations) below.

---

**Q1.** Why is columnar storage faster than a row store for the query
`SELECT SUM(fare_amount) FROM trips` on a 40-column table?

- A. Columnar files are always smaller, so there is less disk total.
- B. The scan reads only the `fare_amount` column's contiguous values and ignores
  the other 39 columns (projection pushdown).
- C. Columnar engines keep the whole table in memory.
- D. Row stores cannot compute aggregates.

---

**Q2.** A Parquet file's internal hierarchy, from largest to smallest unit, is:

- A. page → column chunk → row group → file
- B. file → page → row group → column chunk
- C. file → row group → column chunk → page
- D. file → column chunk → row group → page

---

**Q3.** Where does a Parquet reader find the per-column-chunk `min`/`max`
statistics it uses to skip data?

- A. In a separate sidecar `.stats` file.
- B. At the start of each data page.
- C. In the file **footer** metadata, per row group, per column chunk.
- D. In the Hive partition directory name.

---

**Q4.** You filter `WHERE pickup_date = '2024-03-15'` on a Parquet file. Row group
12 has `pickup_date` statistics `min = 2024-05-01, max = 2024-05-31`. What does the
reader do?

- A. Reads and decodes row group 12, then discards non-matching rows.
- B. **Prunes** row group 12 entirely — the target date is outside its min/max, so
  no row can match.
- C. Reads only the `pickup_date` column of row group 12.
- D. Re-sorts row group 12 by date first.

---

**Q5.** A column `vendor` has three distinct string values across millions of rows.
Which Parquet encoding is most likely applied, and why?

- A. PLAIN, because strings cannot be dictionary-encoded.
- B. Dictionary encoding (e.g. `RLE_DICTIONARY`), because low cardinality lets it
  store tiny integer codes instead of repeating the strings.
- C. Delta encoding, because the values are sorted.
- D. No encoding; the strings are stored raw and uncompressed.

---

**Q6.** Which is the **best** partition column for a trips table that is almost
always queried with a date range?

- A. `trip_id` (unique per row).
- B. `fare_amount` (continuous float).
- C. `pickup_date` truncated to month (low cardinality, frequently filtered).
- D. `passenger_count` (1–4, but never filtered on).

---

**Q7.** A table is stored as 10,000 Parquet files of ~1 MB each on object storage.
Why is a full scan slow compared to the same data in ten ~1 GB files?

- A. Small files compress worse, so there are more total bytes.
- B. Each file incurs fixed open/list/footer-parse and a separate HTTP request, so
  overhead scales with **file count**, not data size.
- C. Object storage cannot read files smaller than 64 MB.
- D. Parquet statistics do not work below 128 MB.

---

**Q8.** What does an ACID table format (Iceberg/Delta) add that plain Parquet on
object storage does **not** provide?

- A. Columnar storage and compression.
- B. Predicate pushdown via min/max statistics.
- C. Atomic multi-file commits, snapshot isolation for readers, and time travel.
- D. The ability to store data on S3.

---

**Q9.** How does Iceberg make `add column` an instant, metadata-only operation that
does not rewrite existing data files?

- A. It rewrites every file in the background asynchronously.
- B. It matches columns by position, so a new column is appended to each row.
- C. It assigns the new column a stable **id**; old files lack that id and read back
  NULL for it, with no rewrite.
- D. It forbids adding columns to existing tables.

---

**Q10.** Which statement correctly contrasts Iceberg and Delta metadata?

- A. Iceberg uses an ordered JSON commit log with checkpoints; Delta uses a manifest
  tree and a catalog.
- B. Iceberg uses a manifest tree (manifest list → manifests → data files) resolved
  via a catalog pointer; Delta uses an ordered `_delta_log` of JSON commits with
  periodic Parquet checkpoints.
- C. Both store all metadata only in the data files themselves.
- D. Neither tracks which files belong to the current version of the table.

---

## Answer key

**Q1 — B.** Columnar layout co-locates each column's values, so an aggregate over
one column reads only that column and skips the rest (projection pushdown). A is
wrong because the win here is about *what* is read, not total size; C and D are
false. Lecture 01 §1; Parquet file format <https://parquet.apache.org/docs/file-format/>.

**Q2 — C.** The hierarchy is **file → row group → column chunk → page**. The row
group is a horizontal slice of rows; within it each column's data is a column
chunk; each chunk is a sequence of pages. Lecture 01 §3; parquet-format
<https://github.com/apache/parquet-format>.

**Q3 — C.** Statistics live in the **footer** metadata (Thrift-serialized at the end
of the file), recorded per row group per column chunk. This is why a reader reads
the footer first and can prune before fetching data pages. Lecture 01 §3, §5;
PyArrow Parquet <https://arrow.apache.org/docs/python/parquet.html>.

**Q4 — B.** `2024-03-15` is less than the group's `min` (`2024-05-01`), so no row in
group 12 can satisfy the filter; the reader **prunes** the whole group without
decoding it. That is row-group pruning / predicate pushdown. Lecture 01 §5;
<https://parquet.apache.org/docs/file-format/>.

**Q5 — B.** Low-cardinality columns get **dictionary encoding**: a dictionary maps
the few distinct values to small integer ids, and the column stores the ids
(often further RLE/bit-packed). Strings absolutely can be dictionary-encoded. The
writer falls back to PLAIN only when cardinality is too high. Lecture 01 §4;
encodings spec <https://parquet.apache.org/docs/file-format/data-pages/encodings/>.

**Q6 — C.** Partition on a **low-cardinality column you filter on constantly** —
date (here truncated to month) fits perfectly. `trip_id` is high cardinality
(manufactures the small-files problem), `fare_amount` is continuous, and
`passenger_count` is never filtered so partitioning by it buys nothing. Lecture 02
§2; Iceberg partitioning <https://iceberg.apache.org/docs/latest/partitioning/>.

**Q7 — B.** The cost of many tiny files scales with **file count**: each file means
an open, a directory listing entry, a footer parse, and on object storage a
separate HTTP request with latency. Same bytes, far more fixed overhead. A is
backwards-ish but not the dominant effect; C and D are false. Lecture 02 §4; DuckDB
httpfs <https://duckdb.org/docs/extensions/httpfs/overview>.

**Q8 — C.** Table formats add a metadata layer giving **atomic commits, snapshot
isolation, and time travel** (plus schema evolution). Columnar storage, predicate
pushdown, and "store on S3" are properties of Parquet/object storage that plain
files already have. Lecture 03 §1, §5; Iceberg spec
<https://iceberg.apache.org/spec/>.

**Q9 — C.** Iceberg tracks columns by **stable integer id**. `add column` assigns a
new id; existing data files simply have no value for that id and read back NULL, so
nothing is rewritten. Position-based matching (B) is exactly what id-based tracking
avoids. Lecture 03 §7; Iceberg evolution
<https://iceberg.apache.org/docs/latest/evolution/>.

**Q10 — B.** Iceberg: catalog pointer → `metadata.json` → snapshot → manifest list →
manifests → data files. Delta: an ordered `_delta_log` of JSON add/remove commits
with periodic Parquet **checkpoints**, where the log itself is the source of truth
(no external catalog). A swaps the two; C and D are false. Lecture 03 §3, §4; Delta
protocol <https://github.com/delta-io/delta/blob/master/PROTOCOL.md>.

---

## Self-assessment

- **9–10 correct — Lakehouse-ready.** You can reason from the bytes up and explain
  what a table format adds. Move into Week 7 (Spark over this lakehouse)
  confidently.
- **7–8 correct — Solid, patch the gaps.** Re-read the lecture sections cited next
  to any question you missed, especially anything on statistics/pruning (Q3–Q5) or
  Iceberg vs Delta (Q8–Q10).
- **5–6 correct — Re-run the exercises.** Do Exercises 01 and 03 again with the
  SOLUTIONS open, watching the metadata and snapshot output. The concepts land once
  you see them in the actual bytes.
- **0–4 correct — Re-read the three lectures before the mini-project.** Focus on the
  Parquet hierarchy (Lecture 01 §3) and the "metadata layer over immutable files"
  idea (Lecture 03 §2); everything else hangs off those two.
