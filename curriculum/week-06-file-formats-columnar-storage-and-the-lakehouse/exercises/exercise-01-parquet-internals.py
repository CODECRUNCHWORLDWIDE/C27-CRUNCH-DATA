"""
Exercise 01 — Parquet internals: read the bytes, prove the savings
====================================================================

TASK
----
Generate a realistic trips dataset, write it as CSV and as Parquet, then:
  1. Compare on-disk size (CSV vs Parquet).
  2. Open the Parquet file with PyArrow and walk its internal structure:
     row groups -> column chunks -> encodings -> statistics.
  3. Use the per-column-chunk min/max statistics to predict, BY HAND, which
     row groups a date filter would prune.
  4. Confirm with DuckDB that the same filter scans far fewer rows, and read
     the row-group statistics straight out of SQL via parquet_metadata().
  5. Compare scan time CSV vs Parquet for a selective analytical query.

This is the byte-level companion to Lecture 01.

ACCEPTANCE CRITERIA
-------------------
- Parquet file is materially smaller than the CSV (expect >= 3x; ZSTD often more).
- You print, for the sorted-by-date Parquet file, each row group's pickup_date
  min/max and a computed verdict "PRUNE" / "READ" for a single-day filter, and
  the number of READ groups is small (1-2) relative to total groups.
- DuckDB EXPLAIN ANALYZE on the filtered query reports fewer scanned rows than
  the table total, and the Parquet query is faster than the CSV query.
- All five COMPLETE-ME spots are implemented and the script runs end to end:
      python exercise-01-parquet-internals.py

RUN INSTRUCTIONS
----------------
    python -m venv .venv && source .venv/bin/activate
    pip install pyarrow duckdb numpy
    python exercise-01-parquet-internals.py

No MinIO needed for this exercise; everything is local files.
Read exercises/SOLUTIONS.md only AFTER you have attempted it.
"""

from __future__ import annotations

import datetime as dt
import os
import time

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

OUT_CSV = "trips.csv"
OUT_PARQUET = "trips_sorted.parquet"
N_ROWS = 2_000_000
ROW_GROUP_SIZE = 200_000  # -> ~10 row groups, good for seeing pruning
TARGET_DAY = dt.date(2024, 3, 15)


def build_table(n_rows: int) -> pa.Table:
    """Build a synthetic trips table SORTED by pickup_date.

    Sorting by the dominant filter column (pickup_date) is what makes each row
    group cover a narrow date range, which makes min/max statistics tight, which
    makes row-group pruning effective. See Lecture 01 section 5.
    """
    rng = np.random.default_rng(42)
    start = dt.date(2024, 1, 1)
    # 90 days of data, sorted ascending by day so row groups are date-contiguous.
    day_offsets = np.sort(rng.integers(0, 90, size=n_rows))
    pickup_date = [start + dt.timedelta(days=int(d)) for d in day_offsets]

    vendors = np.array(["CMT", "VTS", "DDS"])  # low cardinality -> dictionary encoded
    vendor = vendors[rng.integers(0, 3, size=n_rows)]
    payment_type = rng.integers(1, 6, size=n_rows)  # 5 distinct -> dictionary/RLE
    passenger_count = rng.integers(1, 5, size=n_rows)
    fare_amount = np.round(rng.gamma(shape=2.0, scale=8.0, size=n_rows), 2)
    tip_amount = np.round(fare_amount * rng.uniform(0.0, 0.3, size=n_rows), 2)
    # High-cardinality free-text-ish column -> expect PLAIN encoding (dict fallback).
    trip_id = [f"trip-{i:09d}-{rng.integers(0, 1_000_000)}" for i in range(n_rows)]

    return pa.table(
        {
            "trip_id": pa.array(trip_id, pa.string()),
            "pickup_date": pa.array(pickup_date, pa.date32()),
            "vendor": pa.array(vendor, pa.string()),
            "payment_type": pa.array(payment_type, pa.int32()),
            "passenger_count": pa.array(passenger_count, pa.int32()),
            "fare_amount": pa.array(fare_amount, pa.float64()),
            "tip_amount": pa.array(tip_amount, pa.float64()),
        }
    )


def write_inputs(table: pa.Table) -> None:
    """Write the same data as CSV and as Parquet."""
    pacsv.write_csv(table, OUT_CSV)

    # COMPLETE-ME (1): write the Parquet file with statistics ON, ZSTD compression,
    # dictionary encoding enabled, and ROW_GROUP_SIZE rows per row group.
    # Hint: pq.write_table(..., compression=?, use_dictionary=?,
    #                       write_statistics=?, row_group_size=?)
    raise NotImplementedError("COMPLETE-ME (1): write OUT_PARQUET with pq.write_table")


def compare_size() -> None:
    csv_bytes = os.path.getsize(OUT_CSV)
    pq_bytes = os.path.getsize(OUT_PARQUET)
    ratio = csv_bytes / pq_bytes
    print("\n=== SIZE ===")
    print(f"CSV     : {csv_bytes:>12,} bytes")
    print(f"Parquet : {pq_bytes:>12,} bytes")
    print(f"Ratio   : {ratio:6.2f}x  (Parquet is smaller)")
    assert ratio >= 3.0, "Expected Parquet to be at least 3x smaller; check COMPLETE-ME (1)."


def inspect_structure() -> None:
    """Walk file -> row groups -> column chunks -> encodings -> statistics."""
    pf = pq.ParquetFile(OUT_PARQUET)
    md = pf.metadata
    print("\n=== PARQUET STRUCTURE ===")
    print(f"num_rows       : {md.num_rows:,}")
    print(f"num_row_groups : {md.num_row_groups}")
    print(f"created_by     : {md.created_by}")

    # COMPLETE-ME (2): for the FIRST row group, print for each column its name,
    # encodings, compression, and total_compressed_size. Then separately print the
    # encodings for 'vendor' (expect a dictionary encoding) and for 'trip_id'
    # (expect PLAIN, i.e. dictionary fallback at high cardinality).
    # Hint: rg = md.row_group(0); for i in range(rg.num_columns): col = rg.column(i)
    #       col.path_in_schema, col.encodings, col.compression, col.total_compressed_size
    raise NotImplementedError("COMPLETE-ME (2): print per-column encodings for row group 0")


def predict_pruning() -> int:
    """Use min/max statistics to predict which row groups a single-day filter prunes.

    Returns the number of row groups that would be READ (not pruned).
    """
    pf = pq.ParquetFile(OUT_PARQUET)
    md = pf.metadata

    # Find the column index of pickup_date.
    schema = pf.schema_arrow
    date_col_idx = schema.get_field_index("pickup_date")

    print("\n=== ROW-GROUP PRUNING PREDICTION (filter: pickup_date == "
          f"{TARGET_DAY}) ===")
    read_groups = 0
    for g in range(md.num_row_groups):
        col = md.row_group(g).column(date_col_idx)
        st = col.statistics
        # PyArrow returns date stats as datetime.date for date32 columns.
        lo, hi = st.min, st.max

        # COMPLETE-ME (3): decide whether this row group can be PRUNED.
        # A group is PRUNED when TARGET_DAY is entirely outside [lo, hi],
        # i.e. TARGET_DAY < lo OR TARGET_DAY > hi. Otherwise it must be READ.
        # Set `verdict` to the string "PRUNE" or "READ" and increment read_groups
        # only when the group is READ.
        verdict = "READ"  # <-- replace with your computed verdict
        raise NotImplementedError("COMPLETE-ME (3): compute PRUNE/READ from lo/hi")

        # (unreachable until you implement COMPLETE-ME (3); leave as a template)
        print(f"  row group {g:>2}: min={lo} max={hi} -> {verdict}")

    print(f"  -> {read_groups} of {md.num_row_groups} row groups would be READ")
    return read_groups


def duckdb_confirm() -> None:
    """Confirm pruning with DuckDB and read row-group stats from SQL."""
    con = duckdb.connect()

    print("\n=== DUCKDB: row-group statistics via parquet_metadata() ===")
    stats = con.sql(
        f"""
        SELECT row_group_id, stats_min, stats_max, row_group_num_rows
        FROM parquet_metadata('{OUT_PARQUET}')
        WHERE path_in_schema = 'pickup_date'
        ORDER BY row_group_id
        """
    ).fetchall()
    for rg_id, mn, mx, nrows in stats:
        print(f"  rg {rg_id:>2}: min={mn} max={mx} rows={nrows:,}")

    print("\n=== DUCKDB: EXPLAIN ANALYZE on the filtered query ===")
    # COMPLETE-ME (4): write a query that SUMs fare_amount WHERE pickup_date equals
    # TARGET_DAY, wrapped in EXPLAIN ANALYZE, and print the plan. Observe that the
    # PARQUET_SCAN produces far fewer rows than the 2,000,000 total because pruned
    # row groups never emit rows.
    raise NotImplementedError("COMPLETE-ME (4): EXPLAIN ANALYZE the filtered SUM query")


def time_csv_vs_parquet() -> None:
    """Compare scan time for the same selective analytical query."""
    con = duckdb.connect()
    query_pq = (
        f"SELECT SUM(fare_amount) FROM '{OUT_PARQUET}' "
        f"WHERE pickup_date = DATE '{TARGET_DAY.isoformat()}'"
    )
    query_csv = (
        f"SELECT SUM(fare_amount) FROM read_csv_auto('{OUT_CSV}') "
        f"WHERE pickup_date = DATE '{TARGET_DAY.isoformat()}'"
    )

    def timeit(sql: str) -> tuple[float, float]:
        t0 = time.perf_counter()
        result = con.sql(sql).fetchone()[0]
        return time.perf_counter() - t0, result

    print("\n=== SCAN TIME: CSV vs PARQUET (same answer) ===")
    # COMPLETE-ME (5): time both queries, print the elapsed seconds and the result
    # for each, and assert the Parquet query is faster than the CSV query.
    raise NotImplementedError("COMPLETE-ME (5): time CSV vs Parquet and assert Parquet wins")


def main() -> None:
    print("Building synthetic trips table (sorted by pickup_date)...")
    table = build_table(N_ROWS)
    write_inputs(table)
    compare_size()
    inspect_structure()
    predict_pruning()
    duckdb_confirm()
    time_csv_vs_parquet()
    print("\nAll checks passed. You read Parquet from the bytes up.")


if __name__ == "__main__":
    main()
