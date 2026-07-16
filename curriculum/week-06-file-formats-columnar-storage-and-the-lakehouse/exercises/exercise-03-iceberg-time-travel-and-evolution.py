"""
Exercise 03 — Iceberg: create, query, evolve, and time travel
==============================================================

TASK
----
Create an Apache Iceberg table on MinIO with a local SQLite catalog (via
PyIceberg), then exercise the three things a table format gives you that plain
Parquet cannot:

  1. CREATE the table and APPEND an initial batch -> snapshot 1.
  2. APPEND a second batch -> snapshot 2. Inspect the snapshot history.
  3. SCHEMA EVOLUTION: add a column. Show old rows read back NULL for it and that
     no old data file was rewritten (it is a metadata-only change).
  4. TIME TRAVEL: read the table as of snapshot 1 and confirm the row count is the
     pre-second-append count, while the current snapshot has both batches.

This is the hands-on companion to Lecture 03.

ACCEPTANCE CRITERIA
-------------------
- table.metadata.snapshots has >= 2 entries after the two appends.
- After add_column, scanning the current table shows the new column present and
  NULL for rows written before the column existed.
- A scan at snapshot_id == <first snapshot> returns the first-batch row count;
  a scan at the current snapshot returns the combined row count.
- All four COMPLETE-ME spots are implemented and the script runs end to end.

RUN INSTRUCTIONS
----------------
1. MinIO running (same as Exercise 02):
     docker run -d --name minio -p 9000:9000 -p 9001:9001 \\
       -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \\
       minio/minio server /data --console-address ":9001"

2. Install deps and run:
     pip install "pyiceberg[s3fs,sql-sqlite,pyarrow]" pyarrow numpy
     python exercise-03-iceberg-time-travel-and-evolution.py

The script creates the bucket and catalog DB itself.
Read exercises/SOLUTIONS.md only AFTER you have attempted it.
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import pyarrow as pa
import s3fs
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.types import DoubleType  # used by the COMPLETE-ME (3) add_column step

ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET = "crunch-lake"
NAMESPACE = "crunch"
TABLE = "crunch.trips_iceberg"
CATALOG_DB = "iceberg_catalog.db"


def ensure_bucket() -> None:
    fs = s3fs.S3FileSystem(
        key=ACCESS_KEY, secret=SECRET_KEY,
        client_kwargs={"endpoint_url": ENDPOINT},
    )
    if not fs.exists(BUCKET):
        fs.mkdir(BUCKET)


def get_catalog() -> SqlCatalog:
    """A local SQLite-backed Iceberg catalog, warehouse on MinIO."""
    return SqlCatalog(
        "local",
        **{
            "uri": f"sqlite:///{CATALOG_DB}",
            "warehouse": f"s3://{BUCKET}/warehouse",
            "s3.endpoint": ENDPOINT,
            "s3.access-key-id": ACCESS_KEY,
            "s3.secret-access-key": SECRET_KEY,
            "s3.path-style-access": "true",
        },
    )


def make_batch(n: int, start_day: int) -> pa.Table:
    """A batch of trips. No 'surcharge' column yet (added later via evolution)."""
    rng = np.random.default_rng(start_day)
    base = dt.date(2024, 1, 1)
    days = rng.integers(start_day, start_day + 10, size=n)
    pickup_date = [base + dt.timedelta(days=int(d)) for d in days]
    return pa.table(
        {
            "trip_id": pa.array([f"t-{start_day}-{i}" for i in range(n)], pa.string()),
            "pickup_date": pa.array(pickup_date, pa.date32()),
            "fare_amount": pa.array(np.round(rng.gamma(2.0, 8.0, n), 2), pa.float64()),
        }
    )


def main() -> None:
    ensure_bucket()
    catalog = get_catalog()
    catalog.create_namespace_if_not_exists(NAMESPACE)

    # Drop the table if a prior run left it, so this is repeatable.
    try:
        catalog.drop_table(TABLE)
    except Exception:
        pass

    batch1 = make_batch(50_000, start_day=0)
    batch2 = make_batch(30_000, start_day=20)

    # --- Step 1: create + first append (snapshot 1) ---
    # COMPLETE-ME (1): create the Iceberg table from batch1.schema, then append
    # batch1. Capture the table object.
    # Hint: tbl = catalog.create_table(TABLE, schema=batch1.schema)
    #       tbl.append(batch1)
    raise NotImplementedError("COMPLETE-ME (1): create_table + append batch1")

    first_count = tbl.scan().to_arrow().num_rows
    first_snapshot_id = tbl.metadata.current_snapshot_id
    print(f"After batch1: rows={first_count:,}  snapshot_id={first_snapshot_id}")

    # --- Step 2: second append (snapshot 2) + history ---
    # COMPLETE-ME (2): append batch2 to the table, then print every snapshot's id,
    # timestamp_ms, and summary from tbl.metadata.snapshots. Assert there are >= 2.
    raise NotImplementedError("COMPLETE-ME (2): append batch2 and print snapshot history")

    combined_count = tbl.scan().to_arrow().num_rows
    print(f"After batch2: rows={combined_count:,}")
    assert combined_count == first_count + batch2.num_rows

    # --- Step 3: schema evolution (add column) ---
    # COMPLETE-ME (3): add a nullable DoubleType column named 'surcharge' using the
    # update_schema() context manager. Then scan the current table to Arrow and
    # confirm 'surcharge' is in the schema and is all-NULL for the existing rows
    # (no old data was rewritten -> they have no value for the new column id).
    # Hint:
    #   with tbl.update_schema() as us:
    #       us.add_column("surcharge", DoubleType())
    #   evolved = tbl.scan().to_arrow()
    #   assert "surcharge" in evolved.column_names
    #   assert evolved.column("surcharge").null_count == evolved.num_rows
    raise NotImplementedError("COMPLETE-ME (3): add_column 'surcharge' and verify NULLs")

    # --- Step 4: time travel ---
    # COMPLETE-ME (4): scan the table AS OF first_snapshot_id and confirm it returns
    # `first_count` rows, while the current scan returns `combined_count`. This
    # proves the old snapshot's file set is still readable.
    # Hint: tbl.scan(snapshot_id=first_snapshot_id).to_arrow().num_rows
    raise NotImplementedError("COMPLETE-ME (4): time-travel scan at first_snapshot_id")

    print("\nIceberg table created, evolved, and time-traveled. The format is the contract.")


if __name__ == "__main__":
    main()
