"""
Exercise 01 — Bulk-load a CSV into a staging table with COPY.

C27 · Crunch Data — Week 3 (Batch Ingestion & Python ETL).
Runs on: Python 3.12, psycopg 3 (`pip install "psycopg[binary]"`), PostgreSQL 16.

────────────────────────────────────────────────────────────────────────────
TASK
────────────────────────────────────────────────────────────────────────────
Implement `copy_csv_into_staging` so it bulk-loads a CSV of raw orders into the
`stg_orders` staging table using psycopg 3's `cursor.copy()`, then emit a single
structured JSON log line reporting the row count and the duration of the load.

This is the STAGE step of the extract -> stage -> transform -> load -> publish
pipeline from Lecture 1. You are only building STAGE here; Exercises 2 and 3
add the watermark, the upsert, and late-record handling on top.

────────────────────────────────────────────────────────────────────────────
ACCEPTANCE CRITERIA
────────────────────────────────────────────────────────────────────────────
1. Running this file twice in a row leaves `stg_orders` with exactly the row
   count of the CSV (NOT double), because the run TRUNCATEs staging first.
2. The load uses `cursor.copy()` — NOT a loop of single-row INSERTs.
3. Exactly one JSON log line is emitted for the staged step, containing at least:
   "stage": "stage", "rows": <int>, "duration_ms": <float>, and a "run_id".
4. `python exercise-01-bulk-load-with-copy.py` exits 0 and prints the row count
   from a verification `SELECT count(*) FROM stg_orders`.

Reference:
  - psycopg 3 COPY ........ https://www.psycopg.org/psycopg3/docs/basic/copy.html
  - PostgreSQL COPY ....... https://www.postgresql.org/docs/16/sql-copy.html
  - PostgreSQL TRUNCATE ... https://www.postgresql.org/docs/16/sql-truncate.html
  - Python logging HOWTO .. https://docs.python.org/3/howto/logging.html
"""

from __future__ import annotations

import csv
import json
import logging
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg

# ── Config ──────────────────────────────────────────────────────────────────
# A postgres:16 container started with:
#   docker run --name pg-week3 -e POSTGRES_PASSWORD=crunch -p 5432:5432 -d postgres:16
CONNINFO = "host=localhost port=5432 dbname=postgres user=postgres password=crunch"
CSV_PATH = Path(__file__).parent / "data" / "orders_day1.csv"


# ── Structured logging (provided — Lecture 1 §9) ─────────────────────────────
class JsonFormatter(logging.Formatter):
    """Emit each log record as a single JSON object — one line per event."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "msg": record.getMessage(),
        }
        for key in ("run_id", "stage", "rows", "duration_ms"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        return json.dumps(payload)


def build_logger() -> logging.LoggerAdapter:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    base = logging.getLogger("loader.ex01")
    base.setLevel(logging.INFO)
    base.handlers = [handler]
    return logging.LoggerAdapter(base, {"run_id": uuid.uuid4().hex})


log = build_logger()


# ── Schema + sample data bootstrap (provided) ────────────────────────────────
DDL = """
CREATE TABLE IF NOT EXISTS stg_orders (
    order_id      bigint,
    customer_id   bigint,
    product_id    bigint,
    order_ts      timestamptz,
    quantity      integer,
    unit_price    numeric(12,2),
    updated_at    timestamptz,
    ingested_at   timestamptz
);
"""

SAMPLE_CSV = """order_id,customer_id,product_id,order_ts,quantity,unit_price,updated_at,ingested_at
1001,42,7,2026-06-18T09:14:00+00,2,19.99,2026-06-18T09:14:00+00,2026-06-19T03:00:00+00
1002,42,9,2026-06-18T10:01:00+00,1,49.50,2026-06-18T10:01:00+00,2026-06-19T03:00:00+00
1003,17,7,2026-06-18T11:22:00+00,5,19.99,2026-06-18T11:22:00+00,2026-06-19T03:00:00+00
1004,88,3,2026-06-18T12:40:00+00,3,8.00,2026-06-18T12:40:00+00,2026-06-19T03:00:00+00
1005,17,3,2026-06-18T13:05:00+00,1,8.00,2026-06-18T13:05:00+00,2026-06-19T03:00:00+00
"""


def bootstrap(conn: psycopg.Connection) -> None:
    """Create the staging table and write the sample CSV if missing."""
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not CSV_PATH.exists():
        CSV_PATH.write_text(SAMPLE_CSV, encoding="utf-8")


# ── The function you implement ───────────────────────────────────────────────
def copy_csv_into_staging(conn: psycopg.Connection, csv_path: Path) -> int:
    """Bulk-load `csv_path` into stg_orders with COPY. Return the row count.

    Steps:
      1. TRUNCATE stg_orders (so a re-run does not double-load).
      2. Open `cur.copy("COPY stg_orders (...) FROM STDIN")` as a context manager.
      3. Read the CSV with csv.reader, skip the header, and `copy.write_row(record)`
         for each data row, counting rows.
      4. Return the count. (Do NOT commit here — let the caller's `with conn`
         block commit, matching the Lecture-1 transaction discipline.)
    """
    columns = (
        "order_id, customer_id, product_id, order_ts, "
        "quantity, unit_price, updated_at, ingested_at"
    )
    rows = 0

    # YOUR ANSWER: truncate staging, then COPY the CSV rows in. Use cur.copy()
    # and copy.write_row(). Increment `rows` per data row. Return `rows`.
    #
    # with conn.cursor() as cur:
    #     cur.execute("TRUNCATE stg_orders")
    #     with cur.copy(f"COPY stg_orders ({columns}) FROM STDIN") as copy:
    #         ...

    return rows


# ── Verification (provided) ──────────────────────────────────────────────────
def count_staging(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM stg_orders")
        return cur.fetchone()[0]


def main() -> int:
    with psycopg.connect(CONNINFO) as conn:
        bootstrap(conn)
        t0 = time.perf_counter()
        rows = copy_csv_into_staging(conn, CSV_PATH)
        conn.commit()
        log.info(
            "staged",
            extra={
                "stage": "stage",
                "rows": rows,
                "duration_ms": round((time.perf_counter() - t0) * 1000, 1),
            },
        )
        in_table = count_staging(conn)
        print(f"stg_orders now holds {in_table} rows")
        # Acceptance: loaded count equals table count equals CSV data rows.
        assert rows == in_table, f"loaded {rows} but table has {in_table}"
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
