"""
Exercise 02 — Write partitioned Parquet to MinIO and verify the layout
=======================================================================

TASK
----
Land the trips data on MinIO (an S3-compatible object store running in Docker) as
Hive-style partitioned Parquet, then verify the directory layout and prove that a
partition filter reads only the matching partition.

  1. Stand up MinIO (see RUN INSTRUCTIONS) and create a bucket.
  2. Write the trips table partitioned by year/month using PyArrow datasets,
     pointed at MinIO through an s3fs filesystem.
  3. List the object layout via s3fs and confirm the year=YYYY/month=MM/ tree.
  4. Read it back with DuckDB's httpfs/S3 support using hive_partitioning, and
     show that filtering on month reads only that partition's files.

This is the hands-on companion to Lecture 02.

ACCEPTANCE CRITERIA
-------------------
- After writing, fs.find() lists objects under paths containing
  'year=2024/month=01/' ... 'year=2024/month=03/' (Hive layout).
- A DuckDB query with WHERE month = 3 returns only March rows, and the file list
  it touches (via the parquet glob + filter) is the March partition.
- All four COMPLETE-ME spots are implemented and the script runs end to end.

RUN INSTRUCTIONS
----------------
1. Start MinIO with Docker (single node):
     docker run -d --name minio -p 9000:9000 -p 9001:9001 \\
       -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \\
       minio/minio server /data --console-address ":9001"
   (Console at http://localhost:9001, login minioadmin/minioadmin.)

2. Install deps and run:
     pip install pyarrow duckdb s3fs numpy
     python exercise-02-partitioned-parquet-on-minio.py

The script creates the bucket itself if it does not exist.
Read exercises/SOLUTIONS.md only AFTER you have attempted it.
"""

from __future__ import annotations

import datetime as dt

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import s3fs

ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET = "crunch-lake"
TABLE_PATH = f"{BUCKET}/trips"  # s3fs paths omit the s3:// scheme
N_ROWS = 1_000_000


def make_fs() -> s3fs.S3FileSystem:
    """Return an s3fs filesystem pointed at local MinIO (path-style, no TLS)."""
    return s3fs.S3FileSystem(
        key=ACCESS_KEY,
        secret=SECRET_KEY,
        client_kwargs={"endpoint_url": ENDPOINT},
        # MinIO uses path-style addressing; s3fs handles this via endpoint_url.
    )


def build_table(n_rows: int) -> pa.Table:
    """Trips spanning Jan-Mar 2024 with explicit year/month partition columns."""
    rng = np.random.default_rng(7)
    start = dt.date(2024, 1, 1)
    day_offsets = np.sort(rng.integers(0, 90, size=n_rows))
    pickup_date = [start + dt.timedelta(days=int(d)) for d in day_offsets]
    year = np.array([d.year for d in pickup_date], dtype=np.int32)
    month = np.array([d.month for d in pickup_date], dtype=np.int32)
    fare_amount = np.round(rng.gamma(2.0, 8.0, size=n_rows), 2)
    vendor = np.array(["CMT", "VTS", "DDS"])[rng.integers(0, 3, size=n_rows)]
    return pa.table(
        {
            "pickup_date": pa.array(pickup_date, pa.date32()),
            "year": pa.array(year, pa.int32()),
            "month": pa.array(month, pa.int32()),
            "vendor": pa.array(vendor, pa.string()),
            "fare_amount": pa.array(fare_amount, pa.float64()),
        }
    )


def ensure_bucket(fs: s3fs.S3FileSystem) -> None:
    if not fs.exists(BUCKET):
        fs.mkdir(BUCKET)
    # Clean any prior run so the write is idempotent (Week 3 discipline).
    if fs.exists(TABLE_PATH):
        fs.rm(TABLE_PATH, recursive=True)


def write_partitioned(table: pa.Table, fs: s3fs.S3FileSystem) -> None:
    """Write Hive-style year=YYYY/month=MM Parquet to MinIO."""
    # COMPLETE-ME (1): use ds.write_dataset to write `table` to TABLE_PATH as
    # Parquet, partitioned by ("year","month") with flavor="hive", through `fs`.
    # Hint: partitioning=ds.partitioning(pa.schema([("year", pa.int32()),
    #       ("month", pa.int32())]), flavor="hive"); filesystem=fs;
    #       existing_data_behavior="overwrite_or_ignore"; compression handled by
    #       file_options=ds.ParquetFileFormat().make_write_options(compression="zstd")
    raise NotImplementedError("COMPLETE-ME (1): ds.write_dataset partitioned to MinIO")


def verify_layout(fs: s3fs.S3FileSystem) -> None:
    """List objects and confirm the Hive directory layout."""
    print("\n=== OBJECT LAYOUT ON MINIO ===")
    objects = fs.find(TABLE_PATH)
    for o in objects:
        print("  ", o)

    # COMPLETE-ME (2): assert that at least one object path contains
    # 'year=2024/month=01/' and one contains 'year=2024/month=03/'. This proves the
    # Hive partition directories were created.
    raise NotImplementedError("COMPLETE-ME (2): assert Hive partition paths exist")


def configure_duckdb_s3(con: duckdb.DuckDBPyConnection) -> None:
    """Point a DuckDB connection at MinIO via httpfs."""
    con.sql("INSTALL httpfs; LOAD httpfs;")
    con.sql("SET s3_endpoint='localhost:9000'")
    con.sql("SET s3_url_style='path'")
    con.sql("SET s3_use_ssl=false")
    con.sql(f"SET s3_access_key_id='{ACCESS_KEY}'")
    con.sql(f"SET s3_secret_access_key='{SECRET_KEY}'")


def read_back_and_prune() -> None:
    """Read the partitioned table with DuckDB and prove partition pruning."""
    con = duckdb.connect()
    configure_duckdb_s3(con)
    glob = f"s3://{TABLE_PATH}/**/*.parquet"

    print("\n=== DUCKDB READ-BACK ===")
    total = con.sql(
        f"SELECT count(*) FROM read_parquet('{glob}', hive_partitioning=true)"
    ).fetchone()[0]
    print(f"total rows: {total:,}")

    # COMPLETE-ME (3): run a query that counts rows WHERE month = 3 using
    # hive_partitioning=true, and print the count. Because month is a partition
    # column, DuckDB lists/opens only the month=03 directory.
    raise NotImplementedError("COMPLETE-ME (3): count March rows via partition filter")

    # COMPLETE-ME (4): run SELECT SUM(fare_amount) ... WHERE year = 2024 AND
    # month = 3 and print the result, confirming the partition-pruned aggregate
    # works end to end.
    raise NotImplementedError("COMPLETE-ME (4): partition-pruned SUM(fare_amount)")


def main() -> None:
    fs = make_fs()
    ensure_bucket(fs)
    print("Building table and writing partitioned Parquet to MinIO...")
    write_partitioned(build_table(N_ROWS), fs)
    verify_layout(fs)
    read_back_and_prune()
    print("\nLakehouse landing zone is live on MinIO with Hive partitions.")


if __name__ == "__main__":
    main()
