# Lecture 2 — Watermarks, Incrementality, and Idempotency: The High-Water Mark, the Idempotent Upsert, and the Transactionally-Stored Watermark

> **Time:** ~2.5 hours. **Prerequisites:** Lecture 1 (the staging pattern, COPY, connection/transaction discipline). **Citations:** PostgreSQL 16 INSERT ... ON CONFLICT <https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT>; PostgreSQL 16 MERGE <https://www.postgresql.org/docs/16/sql-merge.html>; PostgreSQL 16 transactions tutorial <https://www.postgresql.org/docs/16/tutorial-transactions.html>; psycopg 3 transactions <https://www.psycopg.org/psycopg3/docs/basic/transactions.html>; psycopg 3 docs <https://www.psycopg.org/psycopg3/docs/>; Kleppmann *DDIA* (idempotency, exactly-once, chs. 10–11) <https://dataintensive.net/>.

## 1. The property we are buying

Lecture 1 built the skeleton of a batch job. This lecture buys the property that makes the job operable: **idempotency** — running it once and running it five times over the same source leave the warehouse in identical state. We get there in four moves: a *watermark* makes the load incremental; an *upsert* makes the merge non-duplicating; *atomic batches* make a crash survivable; and a *transactionally-stored watermark* makes a crash unable to skip data. Miss any one and the 3 AM re-run is a coin flip.

## 2. The high-water mark

A **high-water mark** (or **watermark**) is a single stored value that records how far the last successful run got. The next run reads forward from it. The two common choices:

- **`max(updated_at)`** — a timestamp column the source updates whenever a row changes. The next run reads `WHERE updated_at > last_watermark`. Catches inserts *and* updates, as long as the source bumps `updated_at` on every change.
- **`max(id)`** — a monotonically increasing surrogate the source assigns on insert. The next run reads `WHERE id > last_id`. Catches inserts only (an in-place update does not change `id`), but it is immune to clock skew and to the late-event problem Lecture 3 is about.

Store the watermark in its own small table in the *same database* as the warehouse, so it can participate in the same transaction as the load (section 6):

```sql
CREATE TABLE IF NOT EXISTS etl_watermark (
    source_name   text PRIMARY KEY,   -- e.g. 'orders'
    watermark     timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- Seed it once, far in the past, so the first run reads everything:
INSERT INTO etl_watermark (source_name, watermark)
VALUES ('orders', 'epoch')
ON CONFLICT (source_name) DO NOTHING;
```

Reading and the read-forward extract:

```python
def read_watermark(conn: psycopg.Connection, source: str) -> datetime:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT watermark FROM etl_watermark WHERE source_name = %s",
            (source,),
        )
        row = cur.fetchone()
        return row[0]  # seeded to 'epoch', so never None after the seed

def extract_since(conn: psycopg.Connection, watermark: datetime) -> list[tuple]:
    # Read ONLY rows changed since the last successful run — this is incrementality.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT order_id, customer_id, product_id, order_ts, quantity, "
            "       unit_price, updated_at "
            "FROM   src_orders "
            "WHERE  updated_at > %s "
            "ORDER  BY updated_at",
            (watermark,),
        )
        return cur.fetchall()
```

## 3. How a watermark advances across runs

Picture three daily runs against a source that grows each day. The watermark starts at the epoch and chases the source's newest `updated_at`:

```text
                          src_orders.updated_at  ─────────────────────────────►
                          (each x is a changed row)

  before run 1   wm=epoch │ x x x x x                                  (5 rows new)
  run 1 reads  (> epoch)  │ ───────►                advance wm to ──┐
                          │                                          ▼
  before run 2   wm=day1  │ x x x x x  x x x                        (3 rows new)
  run 2 reads  (> day1)   │           ───────►       advance wm to ──┐
                          │                                           ▼
  before run 3   wm=day2  │ x x x x x  x x x  x x x x                (4 rows new)
  run 3 reads  (> day2)   │                  ───────►   advance wm to ──┐
                          │                                             ▼
  after run 3    wm=day3  │ (all 12 rows loaded, each exactly once)
```

Each run reads only its slice. Total rows read across three runs equals the total rows in the source — no row is read twice, and (crucially) no row is *loaded* twice, because even if a row were re-read, the upsert in section 4 would make the re-load a no-op.

## 4. The idempotent upsert — INSERT ... ON CONFLICT DO UPDATE

The watermark makes the load *incremental*. The upsert makes it *idempotent*. The difference between idempotent and not is the difference between `x = 5` (apply it five times, still 5) and `x += 5` (apply it five times, now 25). A plain `INSERT` is `+=`: re-loading a row appends a duplicate. An upsert is `=`: re-loading a row overwrites it.

PostgreSQL's upsert is `INSERT ... ON CONFLICT ... DO UPDATE` <https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT>. It requires a unique constraint or primary key on the conflict target, and inside the `DO UPDATE` it exposes the proposed row as the special `EXCLUDED` pseudo-table:

```sql
-- The target fact table needs a key the upsert can conflict on.
-- The natural key of an order line is what makes this idempotent.
ALTER TABLE fact_sales
    ADD CONSTRAINT fact_sales_nk UNIQUE (order_id);

INSERT INTO fact_sales
    (order_id, customer_key, product_key, date_key, quantity, unit_price, amount, updated_at)
VALUES
    (%(order_id)s, %(customer_key)s, %(product_key)s, %(date_key)s,
     %(quantity)s, %(unit_price)s, %(amount)s, %(updated_at)s)
ON CONFLICT (order_id) DO UPDATE SET
    customer_key = EXCLUDED.customer_key,
    product_key  = EXCLUDED.product_key,
    date_key     = EXCLUDED.date_key,
    quantity     = EXCLUDED.quantity,
    unit_price   = EXCLUDED.unit_price,
    amount       = EXCLUDED.amount,
    updated_at   = EXCLUDED.updated_at;
```

Read it as: "insert this row; if a row with this `order_id` already exists, overwrite its measures with the new values." Run it once: the row is inserted. Run it five times: the row is inserted once, then overwritten with the same values four times — identical final state. That is idempotency, and it is the entire reason a re-run does not double-count.

Two design choices inside the `DO UPDATE`:

- **Full replace** (shown above): overwrite every measure with `EXCLUDED.*`. Correct when the source row is the complete truth.
- **Conditional update**: add a `WHERE` to skip the update when nothing changed, e.g. `... DO UPDATE SET ... WHERE fact_sales.updated_at < EXCLUDED.updated_at` — avoids writing a row that is already current and prevents an *older* re-arriving copy from overwriting a newer one (this is the out-of-order guard Lecture 3 leans on).

In psycopg 3 you run the upsert per-row in a loop, or — far faster for batches — with `cursor.executemany()` over a list of parameter dicts, or by upserting *from the staging table* in a single set-based statement:

```sql
-- Set-based upsert: stage with COPY (Lecture 1), then merge the whole batch at once.
INSERT INTO fact_sales (order_id, customer_key, product_key, date_key,
                        quantity, unit_price, amount, updated_at)
SELECT s.order_id, c.customer_key, p.product_key, d.date_key,
       s.quantity, s.unit_price, s.quantity * s.unit_price, s.updated_at
FROM   stg_orders s
JOIN   dim_customer c ON c.customer_id = s.customer_id AND c.is_current
JOIN   dim_product  p ON p.product_id  = s.product_id  AND p.is_current
JOIN   dim_date     d ON d.date_actual = s.order_ts::date
ON CONFLICT (order_id) DO UPDATE SET
    customer_key = EXCLUDED.customer_key,
    product_key  = EXCLUDED.product_key,
    date_key     = EXCLUDED.date_key,
    quantity     = EXCLUDED.quantity,
    unit_price   = EXCLUDED.unit_price,
    amount       = EXCLUDED.amount,
    updated_at   = EXCLUDED.updated_at
WHERE  fact_sales.updated_at < EXCLUDED.updated_at;
```

This single statement is the workhorse: COPY the batch into `stg_orders`, then run this one `INSERT ... SELECT ... ON CONFLICT` to merge it. The join to `dim_customer`/`dim_product` on `is_current` resolves the Week-1 surrogate keys; the join to `dim_date` resolves the date dimension.

## 5. MERGE — the SQL-standard alternative

PostgreSQL 15+ also supports the SQL-standard `MERGE` <https://www.postgresql.org/docs/16/sql-merge.html>, which expresses the same intent with `WHEN MATCHED` / `WHEN NOT MATCHED` branches:

```sql
MERGE INTO fact_sales f
USING stg_orders_resolved s          -- staging already joined to dimensions
ON    f.order_id = s.order_id
WHEN MATCHED THEN
    UPDATE SET quantity = s.quantity, unit_price = s.unit_price,
               amount = s.amount, updated_at = s.updated_at
WHEN NOT MATCHED THEN
    INSERT (order_id, customer_key, product_key, date_key,
            quantity, unit_price, amount, updated_at)
    VALUES (s.order_id, s.customer_key, s.product_key, s.date_key,
            s.quantity, s.unit_price, s.amount, s.updated_at);
```

When to reach for which:

| `INSERT ... ON CONFLICT DO UPDATE` | `MERGE` |
| --- | --- |
| Idiomatic in Postgres; widely understood | SQL-standard; portable to other engines (the `MERGE` you will meet in Spark/Iceberg in Phase II) |
| Requires a unique constraint on the conflict target | Matches on an arbitrary join condition; no constraint required |
| Concise for the single-key upsert | Clearer when insert and update logic genuinely differ, or for deletes (`WHEN MATCHED THEN DELETE`) |
| Atomic, single-statement | Atomic, single-statement; note `MERGE` does not allow a row to be touched twice in one run |

For this week's single-key fact load, `ON CONFLICT` is the idiomatic choice. You should be able to read both — Phase II's lakehouse `MERGE` is the same shape.

## 6. Restartability and the transactionally-stored watermark

Here is the bug that ends careers, written the obvious wrong way:

```python
# WRONG — do not do this.
with conn.transaction():
    upsert_batch(conn, batch)        # load commits here...
# ...and the watermark advances in a SEPARATE transaction:
with conn.transaction():
    advance_watermark(conn, new_wm)  # if the process dies in this gap, the
                                     # watermark NEVER advances OR advances past
                                     # data, depending on order — either way, broken.
```

If the process dies in the gap between the two commits, one of two silent disasters happens: either the load committed but the watermark did not (next run re-reads the same slice — survivable *only because* the upsert is idempotent), or, if you advanced the watermark first, the watermark moved past data that was never loaded (next run skips that slice **forever** — unrecoverable without a manual backfill, and nothing errors).

The fix is one line of structure: **advance the watermark in the same transaction as the load it covers.**

```python
# RIGHT — the load and the watermark advance atomically together.
def load_batch_atomically(conn, batch, new_watermark) -> None:
    with conn.transaction():                 # ONE transaction
        upsert_batch(conn, batch)            # load the rows
        advance_watermark(conn, new_watermark)  # advance the watermark
    # Either BOTH commit, or NEITHER does. A crash inside rolls back both.

def advance_watermark(conn, new_watermark) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE etl_watermark SET watermark = %s, updated_at = now() "
            "WHERE source_name = 'orders' AND watermark < %s",
            (new_watermark, new_watermark),  # never move the watermark backwards
        )
```

Now a `kill -9` at any instant leaves the system in one of exactly two consistent states: the batch committed *and* the watermark advanced, or neither did. There is no third state where the watermark lies. The next run reads forward from whatever the last committed watermark is, the upsert makes any re-read rows no-ops, and the result is correct. That is **restartability** — built from nothing but a transaction boundary drawn in the right place.

```text
   ┌──────────────── ONE TRANSACTION ────────────────┐
   │  upsert batch into fact_sales                    │
   │  UPDATE etl_watermark SET watermark = batch_max  │
   └──────────────────────────────────────────────────┘
        crash before COMMIT ──► both roll back ──► re-run reads same slice ──► upsert = no-op ──► correct
        crash after  COMMIT ──► both persisted  ──► re-run reads next slice ──────────────────► correct
```

## 7. Why double-counting happens — and the three ways it does not, here

Double-counting has three classic causes, and this design neutralizes all three:

1. **Re-running after a successful load** (manual re-run, orchestrator retry). *Neutralized by the upsert*: re-loaded rows overwrite, not append.
2. **Re-reading a slice because the watermark did not advance** (crash between load and watermark). *Neutralized by storing the watermark in the load transaction* (section 6) and *by the upsert* if a re-read does happen.
3. **A row appearing in two batches** (overlapping reads, or a lookback window from Lecture 3). *Neutralized by the natural-key upsert*: the same `order_id` in two batches resolves to one row.

Kleppmann's *DDIA* develops exactly this argument for distributed systems: you achieve effective exactly-once not by making delivery exactly-once (you cannot) but by making the *effect* idempotent, so at-least-once delivery plus an idempotent sink equals once <https://dataintensive.net/>. The natural-key upsert is that idempotent sink. Week 9's streaming "exactly-once" is the same trick generalized.

## Exercise pointer

Now wire the watermark and the upsert together. [exercises/exercise-02-incremental-watermark-load.py](../exercises/exercise-02-incremental-watermark-load.py) gives you the `etl_watermark` table and the `fact_sales` target; you write `read_watermark`, the read-forward `extract_since`, the `ON CONFLICT` upsert, and the *single transaction* that advances the watermark with the load. Then run it twice and confirm the second run loads zero new rows.

## Summary

- **A high-water mark is a stored `max(updated_at)` or `max(id)`** the next run reads forward from; it is what makes a load incremental. Store it in a table in the warehouse database so it can join the load transaction.
- **Idempotency is `x = 5`, not `x += 5`.** A plain `INSERT` duplicates on re-run; an upsert overwrites.
- **`INSERT ... ON CONFLICT (key) DO UPDATE`** is the idiomatic Postgres upsert, using `EXCLUDED` for the proposed values; `MERGE` is the SQL-standard equivalent and the shape you will meet again in the lakehouse.
- **Advance the watermark in the same transaction as the load it covers.** A crash then leaves only two states — both committed or neither — never a watermark that lies.
- **Double-counting has three causes, all neutralized here**: re-runs, non-advancing watermarks, and overlapping batches all collapse under a natural-key upsert.
- **At-least-once delivery + an idempotent sink = once.** The natural-key upsert is the idempotent sink; this is the batch ancestor of Week 9's exactly-once streaming.

Cited references: <https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT>, <https://www.postgresql.org/docs/16/sql-merge.html>, <https://www.postgresql.org/docs/16/tutorial-transactions.html>, <https://www.psycopg.org/psycopg3/docs/basic/transactions.html>, <https://www.psycopg.org/psycopg3/docs/>, <https://dataintensive.net/>.
