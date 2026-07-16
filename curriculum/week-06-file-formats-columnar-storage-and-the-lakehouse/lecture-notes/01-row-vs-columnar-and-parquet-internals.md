# Lecture 01 — Row vs Columnar Storage & Parquet Internals

> **Time:** ~2 hours · **Prerequisites:** Week 1 dimensional model, Week 2 query
> plans, Week 5 DuckDB fluency · **Citations:** Parquet file-format spec
> <https://parquet.apache.org/docs/file-format/>, Parquet encodings
> <https://parquet.apache.org/docs/file-format/data-pages/encodings/>, PyArrow
> Parquet <https://arrow.apache.org/docs/python/parquet.html>, parquet-format repo
> <https://github.com/apache/parquet-format>, DuckDB Parquet
> <https://duckdb.org/docs/data/parquet/overview>

You have written hundreds of queries against tables and never once asked how the
bytes were arranged on disk. For an OLTP database that is fine — the engine hides
it. For analytics it is the whole game. This lecture takes you from "a CSV is a
file with commas" to "I can read a Parquet footer and predict which row groups my
filter will skip." We open the file and look at the bytes.

## 1. The access pattern decides the layout

Two workloads, two shapes of access:

- **OLTP (transactional).** "Fetch the order with id 91823, all of its columns."
  You touch *one row* and *all its columns*. You do this thousands of times a
  second.
- **OLAP (analytical).** "Sum `fare_amount` grouped by `pickup_date` for the last
  year." You touch *a few columns* across *hundreds of millions of rows*. You do
  this dozens of times an hour.

A row store keeps each row's columns physically next to each other:

```
Row store (heap / CSV), one row per line, all columns interleaved:
  [id=1, vendor=CMT, date=2024-03-01, fare=12.50, tip=2.00, ... 35 more cols]
  [id=2, vendor=VTS, date=2024-03-01, fare= 8.75, tip=1.50, ... 35 more cols]
  [id=3, vendor=CMT, date=2024-03-02, fare=23.10, tip=4.00, ... 35 more cols]
```

To `SUM(fare)` the engine must walk every row and, for each, skip past 39 other
columns it does not need. The bytes for `fare` are scattered across the whole
file, one small island per row. You read (and decompress) effectively the entire
table to sum one column. This is exactly right for OLTP — when you want the whole
row, having it contiguous is a win — and exactly wrong for OLAP.

A columnar store transposes that. All values of one column are contiguous:

```
Columnar store, one column at a time:
  id   : [1, 2, 3, ...]
  vendor: [CMT, VTS, CMT, ...]
  date : [2024-03-01, 2024-03-01, 2024-03-02, ...]
  fare : [12.50, 8.75, 23.10, ...]      <-- SUM(fare) reads only this run
  tip  : [2.00, 1.50, 4.00, ...]
```

Now `SUM(fare)` reads one contiguous run of doubles and ignores the other 39
columns entirely. This is **projection pushdown**: the reader fetches only the
columns the query names. On a 40-column table where your query uses 2 columns, you
read ~5% of the column data instead of 100%. That is not a tuning knob; it is the
reason columnar formats exist.

Columnar layout has a second, compounding benefit. A run of same-typed,
often-similar values compresses far better than a row of mixed types. A column of
`vendor` (3 distinct strings) or `payment_type` (5 distinct ints) compresses to
almost nothing. We exploit this in §4.

> The trade is symmetric and honest: columnar is bad at "give me this one whole
> row," because that row's values are now scattered across many column runs. That
> is why your operational Postgres stays row-oriented and your analytics warehouse
> goes columnar. C27 is an analytics course; from here on, columnar.

## 2. Why CSV and JSON are not analytics formats

CSV and JSON are *interchange* formats. They are excellent at being human-readable
and universally parseable, and terrible at being scanned at volume:

- **Untyped.** `"2024-03-01"` and `"12.50"` are strings until something parses
  them. Every read re-infers or re-parses types. There is no `DATE` or `DOUBLE` on
  disk, just text.
- **Row-oriented.** Even though it is text, a CSV interleaves columns per line, so
  it inherits the row-store scan penalty.
- **Uncompressed by default**, and when you gzip it you compress the *whole file*,
  so you cannot decompress just one column or skip a block.
- **No statistics, no schema, no index.** A reader cannot know that rows
  1,000,000–2,000,000 are all March without reading them.

Parquet fixes all four: it is **typed**, **columnar**, **compressed per column
chunk**, and **self-describing with statistics**. The same data is routinely
5–10× smaller as Parquet and an order of magnitude faster to scan for a selective
analytical query. You measure this in Exercise 01; do not take the multiplier on
faith.

```python
# Quick size comparison you will formalize in Exercise 01.
import duckdb
con = duckdb.connect()
con.sql("CREATE TABLE t AS SELECT * FROM read_csv_auto('trips.csv')")
con.sql("COPY t TO 'trips.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)")
# Then compare os.path.getsize('trips.csv') vs 'trips.parquet'
# and time SELECT SUM(fare_amount) ... on each.
```

## 3. Parquet's internal structure, top to bottom

Apache Parquet (<https://parquet.apache.org/docs/file-format/>) has a precise,
hierarchical layout. Memorize this hierarchy; every other concept hangs off it.

```
Parquet file
├── (4 bytes) "PAR1"  magic header
├── Row Group 0                  <- a horizontal slice of N rows (e.g. ~1M rows)
│   ├── Column Chunk: id         <- all of column `id` for those N rows
│   │   ├── Page 0  (data page: encoded + compressed values)
│   │   ├── Page 1
│   │   └── ...
│   ├── Column Chunk: vendor
│   │   └── (dictionary page + data pages)
│   ├── Column Chunk: fare
│   └── ... one column chunk per column
├── Row Group 1
│   └── ...
├── ... more row groups
├── File Footer (Thrift-serialized metadata):
│   ├── schema
│   ├── for each row group: for each column chunk:
│   │   ├── encodings used
│   │   ├── compression codec
│   │   ├── total compressed/uncompressed size
│   │   ├── byte offset of the chunk
│   │   └── STATISTICS: min, max, null_count, distinct_count
│   └── ...
├── (4 bytes) length of footer
└── (4 bytes) "PAR1"  magic trailer
```

Four levels, from coarse to fine:

1. **File.** The unit you put on object storage. The footer at the *end* holds all
   metadata — which is why a reader does two reads: a small read of the tail to get
   the footer, then targeted reads of only the column chunks it needs.
2. **Row group.** A horizontal band of rows (commonly ~128 MB or ~1M rows worth).
   This is the unit of *parallelism* (one task per row group) and the unit of
   *pruning* (skip a whole row group via its statistics). Row-group size is the
   single most important physical tuning parameter for scan-skipping.
3. **Column chunk.** All the data for one column within one row group, stored
   contiguously. This is what projection pushdown reads.
4. **Page.** The smallest encode/compress/decode unit, typically ~1 MB. A column
   chunk is a sequence of pages, optionally preceded by a dictionary page.

The footer is the key to everything that follows. It is read first, it is small,
and it tells the reader exactly which byte ranges to fetch and which to skip.

### Reading the structure with PyArrow

```python
import pyarrow.parquet as pq

pf = pq.ParquetFile("trips.parquet")

# File-level metadata
md = pf.metadata
print("num_rows      :", md.num_rows)
print("num_row_groups:", md.num_row_groups)
print("created_by    :", md.created_by)
print("schema        :", pf.schema_arrow)

# Drill into row group 0, the column-chunk for column index 4 (e.g. fare_amount)
rg0 = md.row_group(0)
col = rg0.column(4)
print("path_in_schema :", col.path_in_schema)
print("encodings      :", col.encodings)        # e.g. ('PLAIN_DICTIONARY','RLE','BIT_PACKED')
print("compression    :", col.compression)      # e.g. 'ZSTD'
print("total_compressed_size  :", col.total_compressed_size)
print("total_uncompressed_size:", col.total_uncompressed_size)

# THE PART THAT MATTERS FOR PUSHDOWN: per-column-chunk statistics
st = col.statistics
print("has_min_max:", st.has_min_max)
print("min        :", st.min)
print("max        :", st.max)
print("null_count :", st.null_count)
print("distinct   :", st.distinct_count)   # may be None if not written
```

Reference for these objects: <https://arrow.apache.org/docs/python/parquet.html>.
You will run exactly this in Exercise 01 against your taxi data.

## 4. Encodings: why a column chunk is small

A page does not store raw values; it *encodes* them, then optionally compresses
the result. The encodings spec is at
<https://parquet.apache.org/docs/file-format/data-pages/encodings/>. The two you
will see constantly:

**Dictionary encoding (`PLAIN_DICTIONARY` / `RLE_DICTIONARY`).** When a column has
relatively few distinct values, Parquet builds a *dictionary page* mapping each
distinct value to a small integer id, then stores the column as a sequence of those
ids:

```
Raw column:   [CMT, VTS, CMT, CMT, DDS, VTS, CMT]
Dictionary:   {0: CMT, 1: VTS, 2: DDS}          <- stored once, in the dict page
Encoded data: [0,   1,   0,   0,   2,   1,   0]  <- tiny integers, in data pages
```

Storing `0,1,0,0,2,1,0` instead of seven strings is a huge win, and it stacks with
the next encoding. Parquet writers (PyArrow, Spark) turn dictionary encoding on by
default and **fall back to plain encoding when the dictionary would grow too large**
(controlled by a dictionary page size limit). So a low-cardinality column like
`vendor` gets `RLE_DICTIONARY`; a high-cardinality column like a free-text
`comment` falls back to `PLAIN`. You will *see* this fallback in the encodings list
when you inspect different columns in Exercise 01.

**Run-length encoding + bit-packing (`RLE`, `BIT_PACKED`).** Once values are small
integers (dictionary ids, or naturally small ints, or the repetition/definition
levels used for nullability and nesting), Parquet packs them tightly:

```
Run-length: 1000 consecutive 0s  ->  store the pair (value=0, run=1000)
Bit-packing: ids in range 0..3   ->  store each in 2 bits, not 32
```

`RLE` is spectacular on *sorted* low-cardinality columns: if you sort by `vendor`,
the column becomes long runs of identical ids that collapse to a handful of
(value, run-length) pairs. This is one reason sort order inside a file matters.

**Plain encoding (`PLAIN`).** Just the values, fixed-width or
length-prefixed. Used for high-cardinality columns where a dictionary would not
help, or floats that do not repeat.

After encoding, a page is compressed with a codec — typically **Snappy** (fast,
moderate ratio) or **ZSTD** (slower, better ratio). The codec compresses the
*already-encoded* bytes. So the real size of a column chunk is
`compress(encode(values))`, and both layers matter.

> Practical rule of thumb you will verify: a low-cardinality column you query and
> sort on will be a tiny fraction of file size; a high-cardinality free-text column
> will dominate. Layout (sort order, which columns you even keep) is a cost lever.

## 5. Statistics and row-group pruning — the heart of predicate pushdown

Here is the payoff. Every column chunk carries **statistics**: `min`, `max`,
`null_count`, and sometimes `distinct_count`. A reader uses these to *prune* —
to skip whole row groups without decoding them.

Suppose `trips.parquet` has 100 row groups of ~1M rows each, and the file is
roughly sorted by `pickup_date`. Your query is:

```sql
SELECT SUM(fare_amount)
FROM 'trips.parquet'
WHERE pickup_date = DATE '2024-03-15';
```

The reader does **not** start decoding. It:

1. Reads the footer (a few KB).
2. For each of the 100 row groups, reads the `pickup_date` column chunk's
   `min`/`max` from the footer statistics.
3. For row group 7, sees `min = 2024-01-01, max = 2024-01-31`. Your predicate
   wants `2024-03-15`. `2024-03-15 > 2024-01-31`, so **no row in this group can
   match** — skip it entirely. No I/O for its data pages, no decode, no
   decompress.
4. For row group 41, sees `min = 2024-03-10, max = 2024-03-20`. Your target falls
   in that range, so this group *might* contain matches — read it.

If the data is sorted by `pickup_date`, exactly one or two row groups survive
pruning and the engine reads ~1–2% of the file. If the data is *not* sorted by
`pickup_date` — say it is shuffled — then every row group's `min`/`max` straddles
the whole date range, no group can be pruned, and you read the entire file. **Same
filter, same file size, 50× difference in bytes scanned, purely from sort order.**
That is the lesson Challenge 01 makes you prove with measurements.

This skipping is **predicate pushdown** (sometimes "filter pushdown" or "row-group
pruning"): the filter is *pushed down* into the scan so the scan does less work,
rather than reading everything and filtering in the engine afterward. Combined with
**projection pushdown** (read only needed columns), a columnar scan of a selective
analytical query touches a tiny fraction of the file.

### Watching pushdown happen in DuckDB

DuckDB reads Parquet statistics and prunes row groups automatically. You can see
the effect in the query plan:

```sql
-- DuckDB. The Parquet scan reports how many row groups / how much it read.
EXPLAIN ANALYZE
SELECT SUM(fare_amount)
FROM 'trips.parquet'
WHERE pickup_date = DATE '2024-03-15';
```

The `EXPLAIN ANALYZE` output shows the `PARQUET_SCAN` operator and the rows it
actually produced; with a selective, sorted predicate you will see far fewer rows
flow out of the scan than the table's total, because pruned row groups never
produced rows. DuckDB also exposes file/row-group metadata directly:

```sql
-- Inspect every row group's column statistics straight from SQL.
SELECT row_group_id, column_id, path_in_schema, stats_min, stats_max, total_compressed_size
FROM parquet_metadata('trips.parquet')
WHERE path_in_schema = 'pickup_date'
ORDER BY row_group_id;

-- File-level summary.
SELECT * FROM parquet_file_metadata('trips.parquet');
```

Docs: <https://duckdb.org/docs/data/parquet/overview>. Use `parquet_metadata()` in
Exercise 01 and Challenge 01 to *show* which row groups could be pruned.

## 6. Putting numbers on it

Make the win concrete with a back-of-envelope on a realistic table:

- 200,000,000 rows, 40 columns, ~120 bytes/row uncompressed ≈ **24 GB** as CSV.
- As ZSTD Parquet, say **~4 GB** on disk (6× compression is typical for this mix).
- Query: `SUM(fare_amount) WHERE pickup_date = '2024-03-15'`, data sorted by date,
  ~550k rows match (one day).

| Path | Bytes read |
| --- | --- |
| CSV, full scan (no choice) | ~24 GB |
| Parquet, projection only (`fare`,`date`) — 2 of 40 cols | ~0.2 GB |
| Parquet, projection + row-group pruning (1–2 of ~32 groups) | ~0.01 GB |

From 24 GB to ~10 MB for the same answer. Two mechanisms — read fewer *columns*,
read fewer *row groups* — stacked. Every one of them is "do not read data you do
not need," and every one of them depends on physical layout you choose: which
columns you keep, what you sort by, how big your row groups are, and (next lecture)
how you partition.

## 7. Writing Parquet well from PyArrow

You control the knobs that decide whether pushdown can help:

```python
import pyarrow as pa
import pyarrow.parquet as pq

table = ...  # a pyarrow.Table, e.g. from your Week-1 mart

pq.write_table(
    table,
    "trips.parquet",
    compression="zstd",          # better ratio than snappy; snappy is faster
    use_dictionary=True,         # dictionary-encode low-cardinality columns
    row_group_size=1_000_000,    # rows per row group -> controls pruning granularity
    data_page_size=1024 * 1024,  # ~1 MB pages
    write_statistics=True,       # MUST be on for min/max pruning (default True)
)
```

Two things to never forget:
- **`write_statistics=True`** (the default) is what makes pruning possible. If a
  writer disables statistics, every reader is blind and must scan everything.
- **Sort before you write** if you have a dominant filter column. Sorting by
  `pickup_date` makes each row group cover a narrow date range, which makes
  `min`/`max` tight, which makes pruning aggressive. Unsorted data writes the same
  bytes but defeats pruning.

Reference: <https://arrow.apache.org/docs/python/parquet.html>.

## Summary

- The **access pattern** decides the layout: OLTP wants whole rows (row store);
  OLAP wants a few columns over many rows (columnar store).
- **Columnar** co-locates a column's values, enabling **projection pushdown** (read
  only needed columns) and far better compression of same-typed runs.
- **CSV/JSON** are untyped, row-oriented, statistics-free interchange formats;
  **Parquet** is typed, columnar, per-column compressed, self-describing.
- Parquet structure: **file → row group → column chunk → page**, with a **footer**
  carrying schema, encodings, codecs, byte offsets, and per-chunk **statistics**.
- **Encodings** (dictionary, RLE/bit-packing, plain) plus a **compression codec**
  (Snappy/ZSTD) make column chunks small; dictionary encoding falls back to plain
  at high cardinality.
- **Statistics (min/max/null_count)** drive **row-group pruning** — the reader
  skips a whole row group when its `min`/`max` cannot satisfy the filter. That is
  **predicate pushdown**, and it depends on sort order and row-group size.
- You control pushdown effectiveness at write time: keep `write_statistics=True`,
  pick a row-group size, and **sort by your dominant filter column**.

Cited pages: Parquet file format <https://parquet.apache.org/docs/file-format/>;
encodings <https://parquet.apache.org/docs/file-format/data-pages/encodings/>;
parquet-format spec repo <https://github.com/apache/parquet-format>; PyArrow
Parquet <https://arrow.apache.org/docs/python/parquet.html>; DuckDB Parquet
<https://duckdb.org/docs/data/parquet/overview>.
