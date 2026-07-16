"""
Exercise 03 — Handle a deliberately-injected late / out-of-order record.

C27 · Crunch Data — Week 3 (Batch Ingestion & Python ETL).
Runs on: Python 3.12, psycopg 3 (`pip install "psycopg[binary]"`), PostgreSQL 16.

────────────────────────────────────────────────────────────────────────────
TASK
────────────────────────────────────────────────────────────────────────────
Extend the Lecture-2 loader so it correctly absorbs:

  (a) a LATE record — an order whose order_ts (event time) is 3 days in the past
      but whose updated_at (ingestion/version time) is now; and
  (b) an OUT-OF-ORDER correction — a second, NEWER version of an order that was
      already loaded, which must overwrite, plus a STALE re-send of an OLD
      version that must NOT overwrite the newer one.

You make two changes versus Exercise 2:

  1. extract_with_lookback(conn, wm, lookback) — read updated_at > (wm - lookback)
     instead of strictly > wm, so re-reading the recent past is routine.
  2. upsert_newer_wins(conn, rows) — INSERT ... ON CONFLICT (order_id) DO UPDATE
     ... WHERE orders_target.updated_at < EXCLUDED.updated_at, so an out-of-order
     OLDER copy cannot clobber a NEWER stored value (newer-wins by version time).

────────────────────────────────────────────────────────────────────────────
ACCEPTANCE CRITERIA
────────────────────────────────────────────────────────────────────────────
1. The late record (order 1001, event time 3 days ago, arriving now) IS loaded —
   an event-time watermark would have dropped it; the lookback + updated_at
   watermark catches it.
2. The out-of-order correction (order 1002 v2, qty 1 -> 5, newer updated_at)
   overwrites; the stale re-send (order 1002 v0, older updated_at) does NOT.
3. Running the loader once and five times yields identical count(*) and
   SUM(amount) on orders_target.
4. The corrected aggregate (SUM(amount)) matches the expected value printed by
   the script.

Reference:
  - PostgreSQL ON CONFLICT . https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT
  - Kleppmann DDIA ch.11 ... https://dataintensive.net/
  - Kimball late-arriving .. https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/late-arriving-dimensions/
"""

from __future__ import annotations

import json
import logging
import sys
import uuid
from datetime import datetime, timedelta, timezone

import psycopg

CONNINFO = "host=localhost port=5432 dbname=postgres user=postgres password=crunch"
SOURCE_NAME = "orders_late"
LOOKBACK = timedelta(days=3)   # cover lateness up to 3 days


# ── Structured logging (provided) ────────────────────────────────────────────
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {"ts": datetime.now(timezone.utc).isoformat(),
                   "level": record.levelname, "msg": record.getMessage()}
        for key in ("run_id", "stage", "rows", "watermark_from", "watermark_to"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        return json.dumps(payload, default=str)


def build_logger() -> logging.LoggerAdapter:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    base = logging.getLogger("loader.ex03")
    base.setLevel(logging.INFO)
    base.handlers = [handler]
    return logging.LoggerAdapter(base, {"run_id": uuid.uuid4().hex})


log = build_logger()


# ── Schema bootstrap (provided) ──────────────────────────────────────────────
DDL = """
CREATE TABLE IF NOT EXISTS src_orders_late (
    order_id    bigint,
    customer_id bigint,
    product_id  bigint,
    order_ts    timestamptz,          -- EVENT time (can be in the past)
    quantity    integer,
    unit_price  numeric(12,2),
    updated_at  timestamptz NOT NULL  -- INGESTION/VERSION time (monotonic)
);
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


def bootstrap(conn: psycopg.Connection) -> None:
    """Reset and seed a deterministic late/out-of-order scenario.

    Timeline (NOW = 2026-06-19T03:00):
      - Day 1 (06-16): order 1002 v0 placed, qty 1  (updated_at 06-16 ingestion)
      - Day 2 (06-17): order 1003 placed, qty 2
      - Day 3 (06-18): watermark advanced past 06-18 by a prior run
      - NOW   (06-19): three new arrivals in the source:
          * 1001  LATE: event time 06-16 (3 days ago), updated_at = NOW
          * 1002 v2 CORRECTION: qty 1 -> 5, updated_at = NOW (NEWER than v0)
          * 1002 v0 STALE re-send: qty 1, updated_at = 06-16 (OLDER) -> must lose
    """
    with conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute("TRUNCATE src_orders_late, orders_target")
        # The prior run already loaded 1002 v0 (qty 1) and 1003 (qty 2),
        # and advanced the watermark to just past 06-18.
        cur.executemany(
            "INSERT INTO orders_target (order_id, customer_id, product_id, order_ts, "
            "quantity, unit_price, amount, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            [
                (1002, 42, 9, "2026-06-16T10:00:00+00", 1, "49.50", "49.50",  "2026-06-16T10:00:00+00"),
                (1003, 17, 7, "2026-06-17T11:00:00+00", 2, "19.99", "39.98",  "2026-06-17T11:00:00+00"),
            ],
        )
        cur.execute(
            "INSERT INTO etl_watermark (source_name, watermark) VALUES (%s, %s) "
            "ON CONFLICT (source_name) DO UPDATE SET watermark = EXCLUDED.watermark",
            (SOURCE_NAME, "2026-06-18T23:59:00+00"),
        )
        # The three arrivals now sitting in the source, all with updated_at >= 06-16.
        cur.execute("DELETE FROM src_orders_late")
        cur.executemany(
            "INSERT INTO src_orders_late (order_id, customer_id, product_id, order_ts, "
            "quantity, unit_price, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            [
                # LATE: event time 06-16, but ingested NOW (06-19)
                (1001, 88, 3, "2026-06-16T08:00:00+00", 4, "8.00",  "2026-06-19T03:00:00+00"),
                # CORRECTION: 1002 qty 1 -> 5, ingested NOW (NEWER than stored 06-16)
                (1002, 42, 9, "2026-06-16T10:00:00+00", 5, "49.50", "2026-06-19T03:00:00+00"),
                # STALE re-send of 1002 v0: OLDER updated_at -> must NOT win
                (1002, 42, 9, "2026-06-16T10:00:00+00", 1, "49.50", "2026-06-16T10:00:00+00"),
            ],
        )
    conn.commit()


# ── Functions you implement ──────────────────────────────────────────────────
def read_watermark(conn: psycopg.Connection, source: str) -> datetime:
    with conn.cursor() as cur:
        cur.execute("SELECT watermark FROM etl_watermark WHERE source_name = %s", (source,))
        return cur.fetchone()[0]


def extract_with_lookback(conn: psycopg.Connection, watermark: datetime,
                          lookback: timedelta = LOOKBACK) -> list[tuple]:
    """Read src_orders_late rows with updated_at > (watermark - lookback).

    Stepping the cutoff BACK by `lookback` is what lets a late arrival
    (updated_at = NOW, but for an old event) be re-read. Order by updated_at so
    that within one batch a newer version is applied after an older one.
    Columns: order_id, customer_id, product_id, order_ts, quantity, unit_price, updated_at
    """
    # YOUR ANSWER: cutoff = watermark - lookback; SELECT ... WHERE updated_at > cutoff
    #              ORDER BY updated_at
    raise NotImplementedError


def upsert_newer_wins(conn: psycopg.Connection, rows: list[tuple]) -> int:
    """Idempotent upsert with a newer-wins guard.

    INSERT ... ON CONFLICT (order_id) DO UPDATE SET ... = EXCLUDED.*
    WHERE orders_target.updated_at < EXCLUDED.updated_at
    so a stale (older updated_at) re-send cannot overwrite a newer stored row.
    amount = quantity * unit_price.
    """
    # YOUR ANSWER: build the INSERT ... ON CONFLICT ... DO UPDATE ... WHERE (newer-wins)
    #              and cur.executemany over the rows (with amount computed).
    raise NotImplementedError


def advance_watermark(conn: psycopg.Connection, new_watermark: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE etl_watermark SET watermark = %s, updated_at = now() "
            "WHERE source_name = %s AND watermark < %s",
            (new_watermark, SOURCE_NAME, new_watermark),
        )


def run_load(conn: psycopg.Connection) -> int:
    wm = read_watermark(conn, SOURCE_NAME)
    rows = extract_with_lookback(conn, wm)
    if not rows:
        return 0
    new_wm = max(r[6] for r in rows)
    with conn.transaction():
        n = upsert_newer_wins(conn, rows)
        advance_watermark(conn, new_wm)
    log.info("loaded", extra={"stage": "load", "rows": n,
                              "watermark_from": wm, "watermark_to": new_wm})
    return n


# ── Verification (provided) ──────────────────────────────────────────────────
def checksum(conn: psycopg.Connection) -> tuple[int, str]:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*), coalesce(sum(amount),0) FROM orders_target")
        n, total = cur.fetchone()
        return n, str(total)


def order_qty(conn: psycopg.Connection, order_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT quantity FROM orders_target WHERE order_id = %s", (order_id,))
        return cur.fetchone()[0]


def main() -> int:
    with psycopg.connect(CONNINFO) as conn:
        bootstrap(conn)

        run_load(conn)
        once = checksum(conn)
        for _ in range(4):
            run_load(conn)
        five = checksum(conn)

        # Expected final state after correction:
        #   1001 LATE  : 4 * 8.00  = 32.00   (caught by lookback)
        #   1002 CORR  : 5 * 49.50 = 247.50  (newer version wins; stale loses)
        #   1003       : 2 * 19.99 = 39.98   (unchanged)
        # total = 319.48 over 3 rows
        print(f"after 1 run:  count={once[0]} sum={once[1]}")
        print(f"after 5 runs: count={five[0]} sum={five[1]}")
        assert once == five, f"NOT IDEMPOTENT: {once} != {five}"
        assert five[0] == 3, f"expected 3 orders, got {five[0]}"
        assert five[1] == "319.48", f"expected sum 319.48, got {five[1]}"
        assert order_qty(conn, 1002) == 5, "newer-wins guard failed: stale copy won"
        print("PASS: late record caught, correction applied, stale rejected, idempotent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
