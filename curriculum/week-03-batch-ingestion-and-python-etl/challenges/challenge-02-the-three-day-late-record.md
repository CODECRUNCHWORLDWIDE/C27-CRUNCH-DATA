# Challenge 2 — The Three-Day-Late Record: Watch an Event-Time Watermark Drop It, Then Catch It

> **Time:** ~2 hours. **Prerequisites:** Exercises 1–3, ideally Challenge 1. **Citations:** Kleppmann *DDIA* ch. 11 (event time vs processing time) <https://dataintensive.net/>; Kimball late-arriving dimensions <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/late-arriving-dimensions/>; PostgreSQL ON CONFLICT <https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT>.

## The premise

The most dangerous data bug is the one that does not error. A record dated three days ago arrives today; a watermark filtered on event time sorts it *behind* the frontier and drops it; the daily revenue number is quietly low; nobody notices until an executive reconciles against the source system in a quarterly review. This challenge makes you reproduce that silent drop *and then fix it*, so you can recognize it on sight and so you can show, with a number, the difference the fix makes.

## Setup

```bash
docker run --name pg-week3 -e POSTGRES_PASSWORD=crunch -p 5432:5432 -d postgres:16
pip install "psycopg[binary]"
```

Build a source `src_orders_late(order_id, customer_id, product_id, order_ts, quantity, unit_price, event_time, ingested_at)` where:

- `event_time` is when the order was placed (can be in the past).
- `ingested_at` is when your pipeline saw the row (monotonic, only ever "now").

Seed a normal history through "yesterday," advance a watermark past yesterday, then inject **two adversarial rows** today:

1. **The three-day-late record.** `event_time` = three days ago, `ingested_at` = now, a fresh `order_id`. It belongs in a window your earlier runs already "closed."
2. **The out-of-order correction.** A *second* version of an order already loaded — same `order_id`, a larger `quantity`, `ingested_at` = now — plus, to make it adversarial, a *stale* re-send of the original version with an older `ingested_at`.

## Part A — reproduce the silent drop

Write `loader_eventtime.py` whose watermark column is `event_time` and whose extract reads strictly forward:

```sql
SELECT ... FROM src_orders_late WHERE event_time > :watermark ORDER BY event_time;
```

Run it. Capture:

- The row count it extracted (the late record is **not** among them).
- The resulting `SUM(amount)` in the target — too low by the late record's amount.
- A log line or print showing the late `order_id` was never seen.

```text
$ python loader_eventtime.py
extracted 1 rows (the correction's event_time is recent; the LATE record is not)
target sum = 4825.00   <- MISSING the late record's 32.00; no error, no warning
late order 9001 present in target? NO
```

This is the bug, reproduced on purpose. Note that nothing failed. Write one sentence in your notes naming *why* it failed: the filter is on event time, and on event time the late record is behind the watermark.

## Part B — catch it three ways

Now write `loader_fixed.py` that combines all three Lecture-3 defenses:

1. **Watermark on `ingested_at`** (or a monotonic `id`) instead of `event_time`, so a late event still sorts *after* the watermark.
2. **A lookback window**: extract `WHERE ingested_at > (watermark - INTERVAL '3 days')` (or your chosen lookback), so even a watermark that drifts toward event time re-reads the window the late record landed in.
3. **A natural-key, newer-wins upsert**: `ON CONFLICT (order_id) DO UPDATE SET ... WHERE target.ingested_at < EXCLUDED.ingested_at`, so the correction wins, the stale re-send loses, and a re-read is a no-op.

Run it. Capture:

```text
$ python loader_fixed.py
extracted 3 rows (late record + correction + stale re-send, all within lookback)
late order 9001 present in target? YES (amount 32.00)
corrected order 7002 quantity = 5 (was 1); stale re-send rejected
target sum = 4857.00   <- corrected: now includes the late record
```

Then prove idempotency on top of the fix, exactly as in Challenge 1: run `loader_fixed.py` five times and show `count(*)` and `SUM(amount)` are unchanged from the first run.

## Part C — report the corrected aggregate

Produce a side-by-side of the two answers to the same business question, "total sales amount," computed three ways:

| Method | `SUM(amount)` | Late record included? | Correct? |
| --- | --- | --- | --- |
| Event-time watermark, forward-only | 4825.00 | no | no — silently low |
| Source ground truth (`SELECT sum(quantity*unit_price) FROM src_orders_late` over the true row set) | 4857.00 | n/a | reference |
| Ingestion watermark + lookback + newer-wins upsert | 4857.00 | yes | yes |

The fixed loader's total matches the source ground truth; the naive loader's does not. That delta, in dollars, is the cost of the silent drop — and the number you would put in an incident postmortem.

## Acceptance criteria

1. `loader_eventtime.py` reproduces the silent drop: the three-day-late record is absent from the target and `SUM(amount)` is demonstrably low, with **no error raised**.
2. `loader_fixed.py` catches the late record (present in target with the correct amount), applies the out-of-order correction (newer version wins), and rejects the stale re-send (older version loses).
3. `loader_fixed.py` is idempotent: 1 run and 5 runs produce identical `count(*)` and `SUM(amount)`.
4. The Part C table reports all three totals; the fixed loader matches source ground truth and the naive loader does not.
5. A short write-up (`LATE.md`) names *why* the event-time watermark dropped the record (one sentence on event vs ingestion time) and *which* of the three defenses was load-bearing for *which* adversarial row.

## Stretch goals

1. **The late-arriving dimension.** Make the late order reference a `customer_id` that does not yet exist in `dim_customer` (the customer registered seconds before ordering, and the dimension feed has not run). Show that an **inner join** in the fact load silently drops the fact (the same class of silent loss as the event-time watermark). Then implement the **inferred-member / placeholder** pattern: insert an `is_inferred = true` placeholder dimension row with the natural key and `'Unknown'` attributes, point the fact at its surrogate key, and show that when the real customer arrives a later run overwrites the placeholder in place while the fact keeps its key. Reference Kimball <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/late-arriving-dimensions/>.
2. **Tune the lookback.** Sweep the lookback window from 1 day to 7 days; for each, report whether the three-day-late record is caught and how many *extra* (already-loaded) rows are re-scanned. Plot caught-vs-cost and pick a defensible window for a source whose 99th-percentile lateness you state explicitly.
3. **Foreshadow Week 9.** Write 250 words mapping every piece of your fix onto its streaming counterpart: the lookback window ↔ allowed lateness / watermark delay, the ingestion watermark ↔ the event-time frontier the engine tracks, the newer-wins upsert ↔ an idempotent/exactly-once sink. Reference Kleppmann ch. 11 <https://dataintensive.net/>. You will reuse this note when you build the Spark Structured Streaming job.

Cited references: <https://dataintensive.net/>, <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/late-arriving-dimensions/>, <https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT>.
