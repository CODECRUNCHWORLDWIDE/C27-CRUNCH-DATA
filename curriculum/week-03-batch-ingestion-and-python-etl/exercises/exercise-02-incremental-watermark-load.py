"""
Exercise 02 — Incremental load with a watermark and an idempotent upsert.

C27 · Crunch Data — Week 3 (Batch Ingestion & Python ETL).
Runs on: Python 3.12, psycopg 3 (`pip install "psycopg[binary]"`), PostgreSQL 16.

────────────────────────────────────────────────────────────────────────────
TASK
────────────────────────────────────────────────────────────────────────────
Build the incremental core of the loader (Lecture 2):

  1. read_watermark(conn)          -> read the stored high-water mark.
  2. extract_since(conn, wm)       -> read ONLY src_orders rows with
                                      updated_at > wm (read-forward incremental).
  3. upsert_orders(conn, rows)     -> INSERT ... ON CONFLICT (order_id) DO UPDATE
                                      into orders_target (idempotent merge).
  4. advance_watermark(conn, new)  -> move the watermark to the batch max,
                                      IN THE SAME TRANSACTION as the upsert.

The point: running this twice loads N rows the first time and 0 the second time,
and the row count + SUM(amount) in the target are identical after 1 run or 5 runs.

────────────────────────────────────────────────────────────────────────────
ACCEPTANCE CRITERIA
────────────────────────────────────────────────────────────────────────────
1. First run loads every src_orders row; second run (no new source rows) loads 0.
2. The upsert uses ON CONFLICT (order_id) DO UPDATE — a re-seen order_id
   overwrites, never duplicates.
3. The watermark UPDATE and the upsert commit in ONE transaction
   (`with conn.transaction():`). If you split them, the exercise is wrong even
   if the numbers happen to look right.
4. Running main() five times in a row yields the same count(*) and SUM(amount)
   as running it once. The script asserts this for you.

Reference:
  - PostgreSQL ON CONFLICT . https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT
  - psycopg 3 transactions . https://www.psycopg.org/psycopg3/docs/basic/transactions.html
  - psycopg 3 docs ......... https://www.psycopg.org/psycopg3/docs/
"""

from __future__ import annotations

import json
import logging
import sys
import time
import uuid
from datetime import datetime, timezone

import psycopg

CONNINFO = "host=localhost port=5432 dbname=postgres user=postgres password=crunch"
SOURCE_NAME = "orders"


# ── Structured logging (provided) ────────────────────────────────────────────
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "msg": record.getMessage(),
        }
        for key in ("run_id", "stage", "rows", "duration_ms",
                    "watermark_from", "watermark_to"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        return json.dumps(payload, default=str)


def build_logger() -> logging.LoggerAdapter:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    base = logging.getLogger("loader.ex02")
    base.setLevel(logging.INFO)
    base.handlers = [handler]
    return logging.LoggerAdapter(base, {"run_id": uuid.uuid4().hex})


log = build_logger()


# ── Schema + seed data bootstrap (provided) ──────────────────────────────────
DDL = """
CREATE TABLE IF NOT EXISTS src_orders (
    order_id    bigint PRIMARY KEY,
    customer_id bigint,
    product_id  bigint,
    order_ts    timestamptz,
    quantity    integer,
    unit_price  numeric(12,2),
    updated_at  timestamptz NOT NULL
);

-- A simplified fact target. (In the mini-project you load the real Week-1
-- fact_sales; here orders_target stands in so the exercise is self-contained.)
CREATE TABLE IF NOT EXISTS orders_target (
    order_id    bigint PRIMARY KEY,
    customer_id bigint,
    product_id  bigint,
    order_ts    timestamptz,
    quantity    integer,
    unit_price  numeric(12,2),
    amount      numeric(14,2),
    updated_at  timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS etl_watermark (
    source_name text PRIMARY KEY,
    watermark   timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);
"""

SEED_ORDERS = [
    # order_id, customer_id, product_id, order_ts,                quantity, unit_price, updated_at
    (1001, 42, 7, "2026-06-18T09:14:00+00", 2, "19.99", "2026-06-18T09:14:00+00"),
    (1002, 42, 9, "2026-06-18T10:01:00+00", 1, "49.50", "2026-06-18T10:01:00+00"),
    (1003, 17, 7, "2026-06-18T11:22:00+00", 5, "19.99", "2026-06-18T11:22:00+00"),
    (1004, 88, 3, "2026-06-18T12:40:00+00", 3, "8.00",  "2026-06-18T12:40:00+00"),
    (1005, 17, 3, "2026-06-18T13:05:00+00", 1, "8.00",  "2026-06-18T13:05:00+00"),
]


def bootstrap(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)
        # Seed the source (idempotent — ON CONFLICT DO NOTHING).
        cur.executemany(
            "INSERT INTO src_orders (order_id, customer_id, product_id, order_ts, "
            "quantity, unit_price, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (order_id) DO NOTHING",
            SEED_ORDERS,
        )
        # Seed the watermark far in the past so the first run reads everything.
        cur.execute(
            "INSERT INTO etl_watermark (source_name, watermark) "
            "VALUES (%s, 'epoch') ON CONFLICT (source_name) DO NOTHING",
            (SOURCE_NAME,),
        )
    conn.commit()


# ── Functions you implement ──────────────────────────────────────────────────
def read_watermark(conn: psycopg.Connection, source: str) -> datetime:
    """Return the stored high-water mark for `source`."""
    # YOUR ANSWER: SELECT watermark FROM etl_watermark WHERE source_name = %s
    raise NotImplementedError


def extract_since(conn: psycopg.Connection, watermark: datetime) -> list[tuple]:
    """Return src_orders rows with updated_at > watermark, ordered by updated_at.

    Columns, in order: order_id, customer_id, product_id, order_ts,
                       quantity, unit_price, updated_at
    """
    # YOUR ANSWER: SELECT ... FROM src_orders WHERE updated_at > %s ORDER BY updated_at
    raise NotImplementedError


def upsert_orders(conn: psycopg.Connection, rows: list[tuple]) -> int:
    """Idempotently merge `rows` into orders_target. Return rows processed.

    Use INSERT ... ON CONFLICT (order_id) DO UPDATE SET ... = EXCLUDED.... .
    Compute amount = quantity * unit_price. Prefer cur.executemany over a
    Python loop of single-row execute calls.
    """
    # YOUR ANSWER:
    #   sql = ("INSERT INTO orders_target (order_id, customer_id, product_id, "
    #          "order_ts, quantity, unit_price, amount, updated_at) "
    #          "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
    #          "ON CONFLICT (order_id) DO UPDATE SET "
    #          "customer_id=EXCLUDED.customer_id, product_id=EXCLUDED.product_id, "
    #          "order_ts=EXCLUDED.order_ts, quantity=EXCLUDED.quantity, "
    #          "unit_price=EXCLUDED.unit_price, amount=EXCLUDED.amount, "
    #          "updated_at=EXCLUDED.updated_at")
    #   build params with amount computed, then cur.executemany(sql, params)
    raise NotImplementedError


def advance_watermark(conn: psycopg.Connection, new_watermark: datetime) -> None:
    """Advance the watermark to new_watermark (never backwards)."""
    # YOUR ANSWER: UPDATE etl_watermark SET watermark = %s, updated_at = now()
    #              WHERE source_name = %s AND watermark < %s
    raise NotImplementedError


def run_load(conn: psycopg.Connection) -> int:
    """One incremental run: read wm, extract, upsert + advance wm atomically."""
    t0 = time.perf_counter()
    wm = read_watermark(conn, SOURCE_NAME)
    rows = extract_since(conn, wm)
    if not rows:
        log.info("no-new-rows", extra={"stage": "extract", "rows": 0,
                                       "watermark_from": wm, "watermark_to": wm})
        return 0

    new_wm = max(r[6] for r in rows)  # max(updated_at) over the batch

    # YOUR ANSWER: the load and the watermark advance MUST share one transaction.
    # with conn.transaction():
    #     upsert_orders(conn, rows)
    #     advance_watermark(conn, new_wm)
    raise NotImplementedError  # replace with the transaction block above + the log below

    log.info("loaded", extra={
        "stage": "load", "rows": len(rows),
        "duration_ms": round((time.perf_counter() - t0) * 1000, 1),
        "watermark_from": wm, "watermark_to": new_wm,
    })
    return len(rows)


# ── Verification (provided) ──────────────────────────────────────────────────
def target_checksum(conn: psycopg.Connection) -> tuple[int, str]:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*), coalesce(sum(amount),0) FROM orders_target")
        n, total = cur.fetchone()
        return n, str(total)


def main() -> int:
    with psycopg.connect(CONNINFO) as conn:
        bootstrap(conn)

        run_load(conn)               # first run: loads everything
        once = target_checksum(conn)
        print(f"after 1 run:  count={once[0]} sum={once[1]}")

        for _ in range(4):           # four more runs: should be no-ops
            run_load(conn)
        five = target_checksum(conn)
        print(f"after 5 runs: count={five[0]} sum={five[1]}")

        assert once == five, f"NOT IDEMPOTENT: 1 run {once} != 5 runs {five}"
        print("IDEMPOTENT: 5 runs == 1 run")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
