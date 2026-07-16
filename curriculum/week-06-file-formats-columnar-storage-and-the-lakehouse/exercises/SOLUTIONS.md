# Week 6 Exercises — Reference Solutions

> Read these **after** you have attempted each exercise. The learning is in the
> struggle with the metadata APIs and the MinIO setup; the solution is here to
> confirm and to explain the pitfalls, not to skip the work.

References used throughout: PyArrow Parquet
<https://arrow.apache.org/docs/python/parquet.html>, DuckDB Parquet
<https://duckdb.org/docs/data/parquet/overview>, DuckDB httpfs
<https://duckdb.org/docs/extensions/httpfs/overview>, PyIceberg
<https://py.iceberg.apache.org/>, Iceberg evolution
<https://iceberg.apache.org/docs/latest/evolution/>.

---

## Exercise 01 — Parquet internals

### What the exercise asks
Write the same data as CSV and Parquet, compare size, walk the Parquet structure
(row groups, encodings, statistics), predict row-group pruning by hand from
min/max, confirm with DuckDB, and time a CSV vs Parquet scan.

### Reference solution (the five COMPLETE-ME spots)

```python
# COMPLETE-ME (1): write the Parquet file with statistics + ZSTD + dictionary.
def write_inputs(table: pa.Table) -> None:
    pacsv.write_csv(table, OUT_CSV)
    pq.write_table(
        table,
        OUT_PARQUET,
        compression="zstd",
        use_dictionary=True,
        write_statistics=True,   # this is what enables min/max pruning
        row_group_size=ROW_GROUP_SIZE,
    )

# COMPLETE-ME (2): per-column encodings for row group 0.
def inspect_structure() -> None:
    pf = pq.ParquetFile(OUT_PARQUET)
    md = pf.metadata
    print(f"num_rows={md.num_rows:,} num_row_groups={md.num_row_groups} "
          f"created_by={md.created_by}")
    rg = md.row_group(0)
    for i in range(rg.num_columns):
        col = rg.column(i)
        print(f"  {col.path_in_schema:<16} enc={col.encodings} "
              f"codec={col.compression} bytes={col.total_compressed_size:,}")
    # vendor -> a dictionary encoding (RLE_DICTIONARY / PLAIN_DICTIONARY)
    # trip_id -> PLAIN (dictionary fell back at high cardinality)

# COMPLETE-ME (3): PRUNE/READ verdict from lo/hi.
        if TARGET_DAY < lo or TARGET_DAY > hi:
            verdict = "PRUNE"
        else:
            verdict = "READ"
            read_groups += 1
        print(f"  row group {g:>2}: min={lo} max={hi} -> {verdict}")

# COMPLETE-ME (4): EXPLAIN ANALYZE the filtered query.
    plan = con.sql(
        f"""EXPLAIN ANALYZE
            SELECT SUM(fare_amount) FROM '{OUT_PARQUET}'
            WHERE pickup_date = DATE '{TARGET_DAY.isoformat()}'"""
    ).fetchall()
    for row in plan:
        print(row[-1])

# COMPLETE-ME (5): time CSV vs Parquet.
    t_pq, r_pq = timeit(query_pq)
    t_csv, r_csv = timeit(query_csv)
    print(f"  Parquet : {t_pq*1000:8.1f} ms  sum={r_pq:,.2f}")
    print(f"  CSV     : {t_csv*1000:8.1f} ms  sum={r_csv:,.2f}")
    assert t_pq < t_csv, "Parquet should beat CSV on a selective analytical scan"
```

### Expected output (numbers vary by machine)
```
=== SIZE ===
CSV     :  ~150,000,000 bytes
Parquet :   ~25,000,000 bytes
Ratio   :   ~6.0x  (Parquet is smaller)

=== PARQUET STRUCTURE ===
num_row_groups : 10
  vendor   enc=('PLAIN_DICTIONARY', 'RLE', 'BIT_PACKED' ...) codec=ZSTD ...
  trip_id  enc=('PLAIN', ...) codec=ZSTD ...      # dictionary fell back

=== ROW-GROUP PRUNING PREDICTION (filter: pickup_date == 2024-03-15) ===
  row group  0: min=2024-01-01 max=2024-01-10 -> PRUNE
  ...
  row group  7: min=2024-03-11 max=2024-03-20 -> READ
  -> 1 of 10 row groups would be READ
```

### Common pitfalls
- **Forgetting `write_statistics=True`.** It defaults on, but if you ever turn it
  off, every reader becomes blind and prunes nothing. The asymmetry: stats are
  cheap to write, priceless to read.
- **Unsorted data defeats pruning.** If you skip the sort in `build_table`, every
  row group's date range spans all 90 days, every group is READ, and the prediction
  shows `10 of 10`. That is the lesson — sort by your filter column.
- **Comparing dates as strings.** `st.min`/`st.max` for a `date32` column come back
  as `datetime.date`; compare against `TARGET_DAY` (a `date`), not a string.
- **Expecting `trip_id` to be dictionary-encoded.** High-cardinality columns fall
  back to PLAIN; that is correct behavior, not a bug.

---

## Exercise 02 — Partitioned Parquet on MinIO

### What the exercise asks
Write Hive-partitioned (`year`/`month`) Parquet to MinIO via s3fs, verify the
directory layout, and read it back with DuckDB proving partition pruning.

### Reference solution (the four COMPLETE-ME spots)

```python
# COMPLETE-ME (1): write partitioned dataset to MinIO.
def write_partitioned(table, fs):
    ds.write_dataset(
        table,
        base_dir=TABLE_PATH,
        format="parquet",
        partitioning=ds.partitioning(
            pa.schema([("year", pa.int32()), ("month", pa.int32())]),
            flavor="hive",
        ),
        filesystem=fs,
        existing_data_behavior="overwrite_or_ignore",
        file_options=ds.ParquetFileFormat().make_write_options(compression="zstd"),
    )

# COMPLETE-ME (2): assert Hive paths exist.
    objs = fs.find(TABLE_PATH)
    assert any("year=2024/month=1/" in o or "year=2024/month=01/" in o for o in objs)
    assert any("year=2024/month=3/" in o or "year=2024/month=03/" in o for o in objs)
    # NOTE: PyArrow writes month=1 (no zero-pad) for an int32 partition column.

# COMPLETE-ME (3): count March rows via partition filter.
    march = con.sql(
        f"SELECT count(*) FROM read_parquet('{glob}', hive_partitioning=true) "
        f"WHERE month = 3"
    ).fetchone()[0]
    print(f"March rows: {march:,}")

# COMPLETE-ME (4): partition-pruned SUM.
    total_fare = con.sql(
        f"SELECT SUM(fare_amount) FROM read_parquet('{glob}', hive_partitioning=true) "
        f"WHERE year = 2024 AND month = 3"
    ).fetchone()[0]
    print(f"March fare total: {total_fare:,.2f}")
```

### Expected output
```
=== OBJECT LAYOUT ON MINIO ===
   crunch-lake/trips/year=2024/month=1/part-0.parquet
   crunch-lake/trips/year=2024/month=2/part-0.parquet
   crunch-lake/trips/year=2024/month=3/part-0.parquet
=== DUCKDB READ-BACK ===
total rows: 1,000,000
March rows: ~330,000
March fare total: ~5,3xx,xxx.xx
```

### Common pitfalls
- **Zero-padding assumption.** PyArrow writes `month=1`, not `month=01`, for an
  integer partition column. Match either form in your assertion (the solution
  does).
- **`s3_url_style='path'` is mandatory for MinIO.** Without it DuckDB tries
  virtual-host-style (`bucket.localhost:9000`) and fails to resolve. Same idea as
  s3fs needing `endpoint_url`.
- **Forgetting `hive_partitioning=true`** on read means `year`/`month` are not
  recognized as columns, so the filter cannot prune and may even error on the
  missing column.
- **Stale data from a prior run.** `ensure_bucket` removes the table path first so
  the write is idempotent — re-running does not double the row count (Week 3
  discipline applied to the lake).
- **Connection refused** almost always means MinIO is not up or is on a different
  port; check `docker ps` and the `-p 9000:9000` mapping.

---

## Exercise 03 — Iceberg: create, query, evolve, time travel

### What the exercise asks
Create an Iceberg table (SQLite catalog + MinIO data) via PyIceberg, append two
batches (two snapshots), add a column via schema evolution, and time-travel to the
first snapshot.

### Reference solution (the four COMPLETE-ME spots)

```python
# COMPLETE-ME (1): create + first append.
    tbl = catalog.create_table(TABLE, schema=batch1.schema)
    tbl.append(batch1)

# COMPLETE-ME (2): second append + history.
    tbl.append(batch2)
    for snap in tbl.metadata.snapshots:
        print(f"  snapshot {snap.snapshot_id} ts={snap.timestamp_ms} {snap.summary}")
    assert len(tbl.metadata.snapshots) >= 2

# COMPLETE-ME (3): add column + verify NULLs.
    with tbl.update_schema() as us:
        us.add_column("surcharge", DoubleType())
    evolved = tbl.scan().to_arrow()
    assert "surcharge" in evolved.column_names
    assert evolved.column("surcharge").null_count == evolved.num_rows
    print(f"After add_column: surcharge present, all {evolved.num_rows:,} rows NULL")

# COMPLETE-ME (4): time travel.
    at_snap1 = tbl.scan(snapshot_id=first_snapshot_id).to_arrow().num_rows
    at_current = tbl.scan().to_arrow().num_rows
    print(f"time-travel @snapshot1 rows={at_snap1:,}  current rows={at_current:,}")
    assert at_snap1 == first_count
    assert at_current == combined_count
```

### Expected output
```
After batch1: rows=50,000  snapshot_id=...
  snapshot ... ts=... {'operation': 'append', 'added-records': '50000', ...}
  snapshot ... ts=... {'operation': 'append', 'added-records': '30000', ...}
After batch2: rows=80,000
After add_column: surcharge present, all 80,000 rows NULL
time-travel @snapshot1 rows=50,000  current rows=80,000
```

### Common pitfalls
- **Over-building the column type.** The simple `add_column("surcharge",
  DoubleType())` API is enough; you do not need to hand-build a `NestedField` with
  an explicit field id — Iceberg assigns the id for you.
- **Re-running without dropping the table.** PyIceberg raises if the table exists;
  the `try/except drop_table` block makes the script repeatable.
- **Wrong PyIceberg extras.** You need the SQL catalog and s3fs:
  `pip install "pyiceberg[s3fs,sql-sqlite,pyarrow]"`. Missing `sql-sqlite` gives an
  obscure catalog-driver error.
- **Expecting old files to be rewritten on `add_column`.** They are not. The new
  column gets a new id; old data files have no value for that id, so they read back
  NULL. That is the whole point of id-based evolution
  (<https://iceberg.apache.org/docs/latest/evolution/>) — verify file count did not
  jump after the evolution.
- **`current_snapshot_id` vs snapshot list order.** Capture `first_snapshot_id`
  *before* the second append; do not assume `snapshots[0]` is the first one across
  PyIceberg versions.
