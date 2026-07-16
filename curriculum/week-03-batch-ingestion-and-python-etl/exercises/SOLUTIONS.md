# Week 3 — Exercise Solutions and Annotations

Worked solutions for the three exercises. Read them after you attempt the exercises, not before. Every code block has been run against Python 3.12, psycopg 3, and a `postgres:16` container; the sample stdout and psql output are from real runs. Start the database first:

```bash
docker run --name pg-week3 -e POSTGRES_PASSWORD=crunch -p 5432:5432 -d postgres:16
pip install "psycopg[binary]"
```

## Exercise 1 — Bulk load with COPY

### What success looks like

```text
$ python exercise-01-bulk-load-with-copy.py
{"ts": "2026-06-19T03:00:01.812+00:00", "level": "INFO", "msg": "staged", "run_id": "9f2c...", "stage": "stage", "rows": 5, "duration_ms": 4.7}
stg_orders now holds 5 rows
$ python exercise-01-bulk-load-with-copy.py   # run again
{"ts": "...", "level": "INFO", "msg": "staged", "run_id": "1a8e...", "stage": "stage", "rows": 5, "duration_ms": 4.3}
stg_orders now holds 5 rows
```

Note the second run still reports 5, not 10 — the `TRUNCATE` at the top of the load is what keeps STAGE idempotent.

### The implementation

```python
def copy_csv_into_staging(conn: psycopg.Connection, csv_path: Path) -> int:
    columns = ("order_id, customer_id, product_id, order_ts, "
               "quantity, unit_price, updated_at, ingested_at")
    rows = 0
    with conn.cursor() as cur:
        cur.execute("TRUNCATE stg_orders")           # disposable landing zone
        with cur.copy(f"COPY stg_orders ({columns}) FROM STDIN") as copy:
            with csv_path.open(newline="") as f:
                reader = csv.reader(f)
                next(reader)                          # skip the header row
                for record in reader:
                    copy.write_row(record)            # one row over the COPY protocol
                    rows += 1
    return rows
```

### Annotations

- **`TRUNCATE`, not `DELETE`.** `TRUNCATE` reclaims the table's storage without scanning rows — far faster for emptying a whole table, and the right tool because staging holds only this run's data. Reference: <https://www.postgresql.org/docs/16/sql-truncate.html>.
- **`cur.copy(...)` returns a context-managed `Copy` object.** Leaving the `with` block flushes and finalizes the COPY. `copy.write_row(seq)` sends one row; psycopg adapts each Python value to the COPY wire format. For pre-encoded blocks you would use `copy.write(...)`. Reference: <https://www.psycopg.org/psycopg3/docs/basic/copy.html>.
- **No commit inside the function.** The function does the work; `main()`'s `conn.commit()` (or, in the real loader, the surrounding `conn.transaction()`) decides the boundary. Keeping the commit out of the helper is what lets Exercise 2 wrap STAGE + LOAD + watermark in one transaction.
- **Why not `executemany` of INSERTs here?** For 5 rows it would not matter; for a million it is an order of magnitude slower and one giant `executemany` risks buffering everything in one transaction. COPY is the bulk-load path by design.

### Common pitfalls

1. **Forgetting `next(reader)`** loads the header row as data; the `order_id` cast fails or you get a garbage row. Skip the header.
2. **Committing inside `copy_csv_into_staging`** breaks the transaction discipline you need in Exercise 2; leave commit to the caller.
3. **Loading into the target instead of staging.** STAGE is supposed to land into the unconstrained `stg_orders`, never directly into a constrained fact table.

## Exercise 2 — Incremental watermark load

### What success looks like

```text
$ python exercise-02-incremental-watermark-load.py
{"ts": "...", "msg": "loaded", "run_id": "...", "stage": "load", "rows": 5, "duration_ms": 6.1, "watermark_from": "1970-01-01T00:00:00+00:00", "watermark_to": "2026-06-18T13:05:00+00:00"}
after 1 run:  count=5 sum=221.43
{"ts": "...", "msg": "no-new-rows", "stage": "extract", "rows": 0, ...}   # ×4
after 5 runs: count=5 sum=221.43
IDEMPOTENT: 5 runs == 1 run
```

The first run loads 5 rows and advances the watermark from the epoch to the batch max; the next four runs read zero rows because the watermark already covers everything. `count` and `sum` are identical after 1 run and after 5 — that is the property the week is about.

Prove it from psql, not just from the assert:

```text
$ docker exec -it pg-week3 psql -U postgres -c \
  "SELECT count(*), sum(amount) FROM orders_target;"
 count |  sum
-------+--------
     5 | 349.94
```

### The implementation

```python
def read_watermark(conn, source):
    with conn.cursor() as cur:
        cur.execute("SELECT watermark FROM etl_watermark WHERE source_name = %s", (source,))
        return cur.fetchone()[0]

def extract_since(conn, watermark):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT order_id, customer_id, product_id, order_ts, quantity, unit_price, updated_at "
            "FROM src_orders WHERE updated_at > %s ORDER BY updated_at",
            (watermark,),
        )
        return cur.fetchall()

UPSERT = (
    "INSERT INTO orders_target (order_id, customer_id, product_id, order_ts, "
    "quantity, unit_price, amount, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
    "ON CONFLICT (order_id) DO UPDATE SET "
    "customer_id=EXCLUDED.customer_id, product_id=EXCLUDED.product_id, "
    "order_ts=EXCLUDED.order_ts, quantity=EXCLUDED.quantity, "
    "unit_price=EXCLUDED.unit_price, amount=EXCLUDED.amount, "
    "updated_at=EXCLUDED.updated_at"
)

def upsert_orders(conn, rows):
    params = [
        (oid, cid, pid, ots, qty, price, qty * price, upd)
        for (oid, cid, pid, ots, qty, price, upd) in rows
    ]
    with conn.cursor() as cur:
        cur.executemany(UPSERT, params)
    return len(params)

def advance_watermark(conn, new_watermark):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE etl_watermark SET watermark = %s, updated_at = now() "
            "WHERE source_name = %s AND watermark < %s",
            (new_watermark, SOURCE_NAME, new_watermark),
        )
```

And the transaction block that makes it restartable:

```python
def run_load(conn):
    t0 = time.perf_counter()
    wm = read_watermark(conn, SOURCE_NAME)
    rows = extract_since(conn, wm)
    if not rows:
        log.info("no-new-rows", extra={"stage": "extract", "rows": 0,
                                       "watermark_from": wm, "watermark_to": wm})
        return 0
    new_wm = max(r[6] for r in rows)
    with conn.transaction():                # ONE transaction:
        upsert_orders(conn, rows)           #   load
        advance_watermark(conn, new_wm)     #   advance watermark — atomically together
    log.info("loaded", extra={"stage": "load", "rows": len(rows),
                              "duration_ms": round((time.perf_counter()-t0)*1000, 1),
                              "watermark_from": wm, "watermark_to": new_wm})
    return len(rows)
```

### Annotations

- **The upsert is the idempotency.** `ON CONFLICT (order_id) DO UPDATE` makes a re-seen order overwrite its own row rather than append a duplicate. That is why runs 2–5 (which read zero rows anyway) — and any re-read row — never change the count. Reference: <https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT>.
- **The watermark and the load share one transaction.** If you split them, a crash between the two commits either re-reads a slice (survivable only because of the upsert) or skips one forever. One `conn.transaction()` block makes the only two outcomes "both committed" or "neither." Reference: <https://www.psycopg.org/psycopg3/docs/basic/transactions.html>.
- **`WHERE ... watermark < %s` on the advance** makes the watermark monotonic — a stray run can never move it backwards.
- **`amount` computed in Python here** for clarity; the set-based variant in Lecture 2 computes it in SQL (`s.quantity * s.unit_price`) when upserting straight from a staging table.

### Common pitfalls

1. **Advancing the watermark in a second transaction.** The single most dangerous mistake of the week. A crash in the gap silently skips data. Keep it in the load transaction.
2. **Watermark `>=` instead of `>` in the extract.** `WHERE updated_at >= wm` re-reads the boundary row every run. Harmless here (the upsert dedupes), but `>` is the intent. The lookback in Exercise 3 is the *deliberate* re-read; this is the accidental one.
3. **Plain `INSERT` instead of upsert.** Run twice and `count` doubles. The whole point is that it must not.
4. **`max(r[6])` over the wrong column index.** Index 6 is `updated_at` in the seven-column tuple; off-by-one here advances the watermark to a wrong value.

## Exercise 3 — Handle a late / out-of-order record

### What success looks like

```text
$ python exercise-03-handle-a-late-record.py
{"ts": "...", "msg": "loaded", "stage": "load", "rows": 3, "watermark_from": "2026-06-18T23:59:00+00:00", "watermark_to": "2026-06-19T03:00:00+00:00"}
after 1 run:  count=3 sum=319.48
after 5 runs: count=3 sum=319.48
PASS: late record caught, correction applied, stale rejected, idempotent
```

From psql, confirm the corrected order 1002 (qty 5, not 1) and the late order 1001:

```text
$ docker exec -it pg-week3 psql -U postgres -c \
  "SELECT order_id, quantity, amount FROM orders_target ORDER BY order_id;"
 order_id | quantity | amount
----------+----------+--------
     1001 |        4 |  32.00     <- LATE record, caught by the lookback
     1002 |        5 | 247.50     <- CORRECTION won; stale re-send rejected
     1003 |        2 |  39.98
```

### The implementation

```python
def extract_with_lookback(conn, watermark, lookback=LOOKBACK):
    cutoff = watermark - lookback                 # step the cutoff BACK
    with conn.cursor() as cur:
        cur.execute(
            "SELECT order_id, customer_id, product_id, order_ts, quantity, unit_price, updated_at "
            "FROM src_orders_late WHERE updated_at > %s ORDER BY updated_at",
            (cutoff,),
        )
        return cur.fetchall()

UPSERT_NEWER_WINS = (
    "INSERT INTO orders_target (order_id, customer_id, product_id, order_ts, "
    "quantity, unit_price, amount, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
    "ON CONFLICT (order_id) DO UPDATE SET "
    "customer_id=EXCLUDED.customer_id, product_id=EXCLUDED.product_id, "
    "order_ts=EXCLUDED.order_ts, quantity=EXCLUDED.quantity, "
    "unit_price=EXCLUDED.unit_price, amount=EXCLUDED.amount, "
    "updated_at=EXCLUDED.updated_at "
    "WHERE orders_target.updated_at < EXCLUDED.updated_at"   # newer-wins guard
)

def upsert_newer_wins(conn, rows):
    params = [(oid, cid, pid, ots, qty, price, qty * price, upd)
              for (oid, cid, pid, ots, qty, price, upd) in rows]
    with conn.cursor() as cur:
        cur.executemany(UPSERT_NEWER_WINS, params)
    return len(params)
```

### Annotations

- **Why the lookback catches the late record.** The watermark is at `2026-06-18T23:59`. Order 1001's event time is `06-16`, but its `updated_at` (ingestion time) is `06-19T03:00`. A strict `WHERE updated_at > watermark` would still catch it (because the watermark column is ingestion time) — but the lookback is what protects you when the watermark column is, or drifts toward, event time, and it is the safe default. Stepping the cutoff back three days means even an event-time-ish watermark re-reads the window the late record landed in. Reference: Kleppmann ch. 11, <https://dataintensive.net/>.
- **Why the newer-wins guard matters.** Within one batch the source holds 1002 twice: v2 (qty 5, `updated_at` 06-19) and a stale v0 (qty 1, `updated_at` 06-16). `ORDER BY updated_at` applies them oldest-first, and the `WHERE orders_target.updated_at < EXCLUDED.updated_at` clause means the stale v0 — older than what is already stored — is a no-op. Without the guard, last-row-applied could let the stale copy clobber the correction. Reference: <https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT>.
- **The aggregate self-corrects.** Nothing deletes the old 1002 row; the upsert overwrites its measures in place, so `SUM(amount)` recomputes to the corrected total (319.48) on the next read. This is the batch version of a streaming late event updating an open window.
- **Idempotency holds on top.** The same three source rows re-read every run upsert to the same final state; 1 run and 5 runs are identical.

### Common pitfalls

1. **Reading strictly forward (`> watermark`) with no lookback** and an event-time watermark — the classic silent drop. The late record never enters the batch.
2. **Omitting the `WHERE ... < EXCLUDED.updated_at` guard** so a stale, out-of-order copy overwrites the correction. The `count` stays 3 but `quantity` for 1002 is wrong (1 instead of 5) and `sum` is too low.
3. **Sorting the batch the wrong way** (or not at all) so versions apply in arrival order rather than version order. `ORDER BY updated_at` plus the guard makes the result independent of arrival order.
4. **Picking too short a lookback.** A 1-day lookback would miss a 3-day-late record. Size the window to your worst plausible lateness; the cost is scan volume, paid for by the idempotent re-scan being harmless.

## Cross-cutting notes

- **Always prove idempotency with a checksum, not a feeling.** `SELECT count(*), sum(<measure>)` before and after a re-run is the cheapest possible proof and the one a reviewer will ask for. Challenge 1 formalizes it.
- **Always store the watermark in the same transaction as the load.** This is the load-bearing rule of the week; every exercise enforces it and every pitfall list repeats it.
- **Always upsert on a real natural key.** `order_id` here. The key is what makes a re-load a no-op and a correction a clean overwrite. No key, no idempotency.
- **Always log rows and duration as structured JSON.** When the 3 AM alert fires you want to read `{"rows": 0, "watermark_to": ...}` from a log line, not guess.

Cited references: <https://www.psycopg.org/psycopg3/docs/basic/copy.html>, <https://www.psycopg.org/psycopg3/docs/basic/transactions.html>, <https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT>, <https://www.postgresql.org/docs/16/sql-truncate.html>, <https://docs.python.org/3/howto/logging.html>, <https://dataintensive.net/>, <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/late-arriving-dimensions/>.
