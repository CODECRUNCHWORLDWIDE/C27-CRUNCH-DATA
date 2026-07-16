# Challenge 1 — Prove Idempotency: One Run Equals Five Runs, and a Crash Mid-Batch Skips Nothing

> **Time:** ~2 hours. **Prerequisites:** Exercises 1–3. **Citations:** PostgreSQL ON CONFLICT <https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT>; PostgreSQL transactions <https://www.postgresql.org/docs/16/tutorial-transactions.html>; psycopg 3 transactions <https://www.psycopg.org/psycopg3/docs/basic/transactions.html>; Kleppmann *DDIA* (idempotency / effective exactly-once) <https://dataintensive.net/>.

## The premise

You have a working watermarked, upserting incremental loader (Exercise 2, optionally with Exercise 3's late-record handling). A claim is easy to make — "it's idempotent, it's restartable" — and a claim is worthless without a proof. This challenge makes you produce two proofs an on-call engineer would actually trust:

1. **Idempotency proof.** Running the loader once and running it five times against the *same* source leaves the warehouse in identical state, demonstrated by a checksum, not by eyeballing.
2. **Restartability proof.** Killing the loader in the middle of a batch leaves the watermark pointing at the last *fully committed* slice, never past unloaded data, demonstrated by inspecting the watermark and the row count after a deliberate crash.

## Setup

Start a clean database and load a source large enough that a batch boundary is meaningful:

```bash
docker run --name pg-week3 -e POSTGRES_PASSWORD=crunch -p 5432:5432 -d postgres:16
pip install "psycopg[binary]"
```

Generate ~200,000 source rows across, say, 20 "days" so the loader runs in several batches. A small seeding script (`seed.py`) writing into `src_orders` with a `BATCH_SIZE` of 10,000 is enough; the exact generator is yours to write.

## Part A — the idempotency proof

### The checksum

A trustworthy idempotency checksum captures both *how many* rows and *what they sum to*, so neither a duplicate nor a silent overwrite-with-wrong-value escapes:

```sql
SELECT count(*)                       AS rows,
       coalesce(sum(amount), 0)       AS total_amount,
       coalesce(sum(quantity), 0)     AS total_qty,
       md5(string_agg(order_id::text || ':' || amount::text, ',' ORDER BY order_id)) AS state_hash
FROM   orders_target;
```

The `state_hash` is the strongest form: a single MD5 over the ordered `(order_id, amount)` pairs. Two warehouse states with the same `state_hash` are byte-for-byte identical at the grain that matters.

### The procedure

1. Truncate `orders_target`, reset the watermark to the epoch.
2. Run the loader **once**. Capture the checksum → call it `C1`.
3. Without changing the source, run the loader **four more times**. Capture the checksum → call it `C5`.
4. Assert `C1 == C5` on all four columns, including `state_hash`.

A passing run:

```text
$ python prove_idempotency.py
run 1: rows=200000 total_amount=4825193.50 total_qty=601118 state_hash=8c1f...e3
run 5: rows=200000 total_amount=4825193.50 total_qty=601118 state_hash=8c1f...e3
IDEMPOTENT: state_hash identical after 1 and 5 runs
```

If `rows` grows between run 1 and run 5, your load is appending instead of upserting (a plain `INSERT`, or an upsert on the wrong key). If `rows` is stable but `state_hash` differs, an out-of-order or non-deterministic transform is changing measures between runs.

## Part B — the restartability proof

### The deliberate crash

Add a `--crash-after N` flag to your loader that raises `SystemExit` (or `os._exit(1)` for a harder kill) after committing `N` batches, simulating a process killed mid-run. Then:

1. Reset: truncate `orders_target`, watermark to the epoch.
2. Run with `--crash-after 5` (commits 5 batches, then dies). Record the watermark and `count(*)` immediately after.
3. **Inspect the invariant**: the watermark must equal the `max(updated_at)` of exactly the rows that are in `orders_target`. Not ahead of it, not behind it by more than the lookback window.
4. Re-run the loader normally (no crash). It resumes from the committed watermark, loads the remaining batches, and the final checksum equals the no-crash `C1` from Part A.

The invariant query — the watermark must never point past loaded data:

```sql
SELECT w.watermark                                AS stored_watermark,
       max(t.updated_at)                          AS max_loaded_updated_at,
       w.watermark >= max(t.updated_at)           AS watermark_covers_loaded,
       w.watermark <= (SELECT max(updated_at) FROM src_orders
                       WHERE order_id IN (SELECT order_id FROM orders_target))
                                                  AS watermark_not_past_unloaded
FROM   etl_watermark w, orders_target t
WHERE  w.source_name = 'orders'
GROUP  BY w.watermark;
```

`watermark_not_past_unloaded` must be `true`. If it is `false`, your loader advanced the watermark in a separate transaction from the load (the cardinal sin of Lecture 2 §6) and a crash left the watermark pointing at data that was never loaded — meaning a normal re-run would skip it forever.

A passing run:

```text
$ python loader.py --crash-after 5
... 5 batches committed ...
crash injected after 5 batches

$ docker exec -it pg-week3 psql -U postgres -c "<invariant query>"
 stored_watermark | max_loaded_updated_at | watermark_covers_loaded | watermark_not_past_unloaded
------------------+-----------------------+-------------------------+-----------------------------
 2026-06-23 ...   | 2026-06-23 ...        | t                       | t

$ python loader.py            # resume, no crash
... remaining batches committed ...

$ # final checksum equals Part A's C1
```

## Acceptance criteria

1. `prove_idempotency.py` runs the loader 1× and 5× and asserts that `rows`, `total_amount`, `total_qty`, and `state_hash` are all identical. The script exits 0 only on a match.
2. The checksum includes a `state_hash` (an `md5(string_agg(... ORDER BY ...))`), not merely a row count — a row count alone cannot detect an overwrite-with-wrong-value.
3. The loader accepts `--crash-after N`, commits exactly `N` batches, then dies.
4. After a `--crash-after` run, the invariant query returns `watermark_not_past_unloaded = true`. You include the captured output.
5. A normal re-run after the crash resumes from the committed watermark and reaches the same `state_hash` as the no-crash run.
6. A short write-up (`PROOF.md`) records both proofs with the captured numbers and one paragraph explaining *why* the upsert + transactionally-stored watermark make both properties hold.

## Stretch goals

1. **Break it on purpose, then watch the proof fail.** Change the load from `ON CONFLICT DO UPDATE` to a plain `INSERT` and re-run Part A. Capture the `rows` count exploding from 200,000 to 1,000,000 after five runs. Then move the `advance_watermark` call into its own transaction *after* the load commits, re-run Part B with `--crash-after`, and capture the invariant query returning `false`. Keeping the broken outputs next to the fixed ones is the most convincing artifact you can produce.
2. **Concurrent re-runs.** Start two copies of the loader at the same instant against the same source. Show (via the checksum) that even racing runs converge to the same state — and explain which Postgres mechanism (row locks taken by `ON CONFLICT`, the `watermark < %s` guard) prevents the race from double-counting. Reference Kleppmann's discussion of concurrent idempotent operations <https://dataintensive.net/>.
3. **Quantify the cost.** Measure wall-clock time for the idempotent loader versus a naive full-refresh that truncates and reloads everything each run, at 200k and at 2M source rows. Report the crossover and the per-run byte/row savings of incrementality.

Cited references: <https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT>, <https://www.postgresql.org/docs/16/tutorial-transactions.html>, <https://www.psycopg.org/psycopg3/docs/basic/transactions.html>, <https://dataintensive.net/>.
