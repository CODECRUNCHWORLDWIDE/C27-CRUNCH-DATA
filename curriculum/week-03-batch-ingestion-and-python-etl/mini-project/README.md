# Mini-Project — An Idempotent Incremental Loader You Can Re-Run at 3 AM

> **Time:** 8 hours across Thursday–Saturday–Sunday. **Prerequisites:** Exercises 1–3 and (ideally) both challenges. **Citations:** every URL referenced in the three lecture notes — psycopg 3 (COPY, transactions), PostgreSQL 16 (INSERT/ON CONFLICT, MERGE, COPY), Python logging, Kleppmann *DDIA*, Kimball late-arriving dimensions, the Docker Postgres image.

## The spec

You are building **`crunch-loader`**, a Python ETL job that incrementally loads a daily-growing retail orders source into the Week-1 Postgres warehouse (`dim_customer`, `dim_product`, `dim_date`, `fact_sales`). It is the job an on-call engineer can re-run at 3 AM without thinking, because it is idempotent and restartable by construction. The runtime topology is deliberately small — everything runs in Docker via `docker compose up`:

```text
   ┌──────────────────┐         ┌───────────────────────────────────┐
   │ daily source     │         │  postgres:16 (one container)      │
   │ orders_dayNN.csv │──COPY──►│                                   │
   │ (grows each day) │         │   stg_orders   (staging, raw)     │
   └──────────────────┘         │       │ upsert                    │
            ▲                    │       ▼                           │
            │                    │   fact_sales   (Week-1 target)    │
   crunch-loader (Python 3.12)   │   dim_*        (Week-1 dims)      │
   extract → stage → transform   │   etl_watermark (high-water mark) │
   → load → publish              │                                   │
                                 └───────────────────────────────────┘
```

The loader reads only what changed since the last run (watermark), bulk-loads into staging with `COPY`, upserts idempotently into `fact_sales`, handles a deliberately injected late record, logs structured JSON, and ships a `--full-refresh` mode and an idempotency self-test. `docker compose up` brings up Postgres and runs the loader against a generated multi-day source.

## Functional requirements

### F1 — Incremental extract with a watermark

- Read the stored high-water mark from `etl_watermark` and extract only source rows changed since (`updated_at`/`ingested_at` > watermark, minus the lookback window).
- Seed the watermark to the epoch so the first run reads everything.

### F2 — Bulk staging via COPY

- Truncate `stg_orders` at the start of every run; bulk-load the extracted slice into it with `cursor.copy()` (never single-row `INSERT`s).
- The transform step (cast, dedupe, resolve surrogate keys against `dim_customer`/`dim_product`/`dim_date` on `is_current`) runs against the staging table.

### F3 — Idempotent upsert / merge into `fact_sales`

- Merge staged rows into `fact_sales` with `INSERT ... ON CONFLICT (order_id) DO UPDATE` (or `MERGE`), including a newer-wins guard (`WHERE fact_sales.updated_at < EXCLUDED.updated_at`).
- A re-loaded order overwrites; it never duplicates.

### F4 — Late-record handling

- A lookback window (configurable, default 3 days) so a late-arriving record is re-read and upserted, correcting the aggregate.
- Demonstrate the late-arriving-dimension placeholder pattern for at least one fact whose `customer_id` is not yet in `dim_customer` (inferred member).

### F5 — Structured logging

- Emit one JSON log line per stage (`extract`, `stage`, `transform`, `load`, `publish`) with a per-run `run_id`, `rows`, `duration_ms`, and `watermark_from`/`watermark_to`, via the standard `logging` module.

### F6 — `--full-refresh` mode

- A `--full-refresh` flag truncates `fact_sales` (for the loaded grain), ignores the watermark, reads the entire source, and reloads — for the day the incremental logic is suspect.

### F7 — Idempotency self-test

- A `--self-test` mode (or a `make selftest` target) that, against a fixed source, runs the loader once and five times and asserts identical `count(*)`, `SUM(amount)`, and a `state_hash` (`md5(string_agg(... ORDER BY ...))`). Exits non-zero on any mismatch.

### F8 — Batch-size discipline

- Load in batches of a configurable size (default 10,000), each batch committing in its own transaction *together with* the watermark advance, so a crash loses at most one in-flight batch.

## Non-functional requirements

### NF1 — Build and run

- `docker compose up` brings up Postgres and runs the loader end to end in under 60 seconds on commodity hardware against a generated ~20-day source.
- A single `make run` (or `python -m loader`) runs one incremental load; `make selftest` runs F7.

### NF2 — Code quality

- Python 3.12 with type hints on every function signature; `from __future__ import annotations` where it helps.
- **Every database connection lives in a `with psycopg.connect(...)` block**, and **the watermark advances in the same transaction as the load it covers**. These two are graded explicitly.
- No fire-and-forget; every cursor and transaction is scoped with `with`.

### NF3 — Citations

- Every non-trivial implementation choice carries a citation comment pointing at the psycopg 3 docs, the PostgreSQL 16 docs, or the relevant lecture note.
- The `README.md` lists every dependency with version and license.

## Suggested project layout

```text
crunch-loader/
├── docker-compose.yml          # postgres:16 + the loader service
├── Makefile                    # run, selftest, full-refresh, seed, psql targets
├── pyproject.toml              # deps: psycopg[binary]; python_requires >= 3.12
├── README.md                   # description, build, run, the run write-up
├── RUN.md                      # the measurement write-up (see below)
├── sql/
│   ├── 00_warehouse.sql        # Week-1 dim_* + fact_sales (or import yours)
│   ├── 01_staging.sql          # stg_orders + etl_watermark
│   └── 02_constraints.sql      # UNIQUE (order_id) on fact_sales for ON CONFLICT
├── loader/
│   ├── __init__.py
│   ├── __main__.py             # arg parsing: --full-refresh, --self-test, --lookback
│   ├── config.py               # CONNINFO, batch size, lookback, source path
│   ├── logging_setup.py        # JsonFormatter + run_id LoggerAdapter
│   ├── extract.py              # read_watermark, extract_since / extract_with_lookback
│   ├── stage.py                # truncate + COPY into stg_orders
│   ├── transform.py            # cast, dedupe, resolve surrogate keys, inferred members
│   ├── load.py                 # upsert into fact_sales + advance_watermark (one txn)
│   └── selftest.py             # run 1× vs 5×, compare state_hash
├── data/
│   └── generate.py             # synthesize orders_dayNN.csv across N days + a late record
└── tests/
    └── test_idempotency.py     # pytest: load twice, assert identical checksum
```

## Starter files

A starter scaffold lives in `mini-project/starter/`. Copy it as your starting point. It contains:

- `docker-compose.yml` — `postgres:16` plus a `loader` service that waits for the DB and runs `python -m loader`.
- `loader/logging_setup.py` — the `JsonFormatter` and `run_id` `LoggerAdapter` from Lecture 1, complete.
- `loader/load.py` — the upsert and `advance_watermark` with the transaction block stubbed; you complete the body.
- `data/generate.py` — a generator that writes `orders_dayNN.csv` for N days and injects one three-day-late record and one out-of-order correction.
- `sql/` — the warehouse DDL (you may substitute your own Week-1 schema).

The starter compiles and the database comes up, but the loader does not load end to end until you fill in `extract.py`, `stage.py`, `transform.py`, and the transaction block in `load.py`.

## The run write-up (`RUN.md`)

Treat the write-up as part of the deliverable, not an afterthought. Run the loader and capture:

### M1 — Cold start

`docker compose up` from clean; time until the first incremental load completes. Target: under 60 seconds on commodity hardware.

### M2 — Incremental vs full-refresh

Load 20 days incrementally (one run per day's file), then run `--full-refresh` once over all 20 days. Report rows read and wall-clock for each. Confirm the incremental total rows-read equals the source row count (no row read twice) while full-refresh re-reads everything.

### M3 — Idempotency

Run `--self-test`. Report the `state_hash` after 1 run and after 5 runs and confirm they match. Paste the psql `SELECT count(*), sum(amount) FROM fact_sales` output for both.

### M4 — Late record self-correction

Show the day the late record arrives: the affected day's `SUM(amount)` before the late record, the source ground truth, and the corrected total after the loader picks the late record up via the lookback. The corrected total must match ground truth.

### M5 — Restartability

Run with a `--crash-after N` flag (commit N batches then die). Inspect the watermark with the invariant query from Challenge 1; confirm `watermark_not_past_unloaded = true`. Re-run normally; confirm the final `state_hash` equals M3's.

### M6 — Batch-size sweep

Load the same source at batch sizes 1,000 / 10,000 / 100,000. Report wall-clock and peak memory for each, and state which you would ship and why (the dial between too-many-tiny-transactions and one-transaction-too-large from Lecture 1 §8).

### M7 — Structured-log sample

Paste 5–10 consecutive JSON log lines from one run, showing the `run_id` tying them together and the `watermark_from`/`watermark_to` for the load stage.

## Grading rubric

- **40 points: functional correctness.** Every functional requirement (F1–F8) is implemented and demonstrable: incremental watermark, COPY staging, idempotent upsert, late-record handling, structured logging, `--full-refresh`, the self-test, and batch discipline.
- **20 points: non-functional quality.** Python 3.12 with type hints; every connection in a `with`; **the watermark advances in the same transaction as the load** (this single item is worth 5 of the 20 — get it wrong and you lose them all).
- **15 points: the run write-up.** All seven measurements (M1–M7) reported with captured numbers and a one-sentence interpretation each.
- **10 points: idempotency proof.** The self-test passes with a `state_hash` (not merely a row count) identical after 1 and 5 runs, and the restartability invariant holds after a `--crash-after` run.
- **10 points: citations.** At least 10 distinct citation comments in the source pointing at the psycopg 3 docs, the PostgreSQL 16 docs, or the lecture notes.
- **5 points: tests.** `tests/test_idempotency.py` runs under `pytest`, loads twice, and asserts identical checksum.

Minimum to pass: 70 points **and** no double-counting defect in the self-test.

## Stretch goals

1. **Parallel batches.** Load independent day-partitions concurrently (a process pool or several connections), and show the result is still idempotent and order-independent. Discuss which Postgres mechanism keeps two concurrent upserts on the same key correct (row locks taken by `ON CONFLICT`).
2. **A dead-letter table.** Route rows that fail the transform (unparseable timestamp, negative quantity, unresolvable dimension after the inferred-member attempt) into a `dlq_orders` table with the rejection reason and the `run_id`, instead of failing the whole batch. Report the DLQ count in the structured log. This is the batch ancestor of the quality gates you build in Week 10.
3. **Checkpoint resume within a run.** Persist a per-batch checkpoint (the last committed `order_id` range) so a `--crash-after` run, re-run with `--resume`, picks up at the exact batch boundary rather than re-reading the whole lookback window. Measure the re-scan saved.
4. **A `MERGE` variant.** Re-express the F3 upsert with PostgreSQL `MERGE` (<https://www.postgresql.org/docs/16/sql-merge.html>) behind a `--use-merge` flag; confirm both paths produce identical `state_hash`. Write 150 words on when you would prefer each, and note that the lakehouse `MERGE` in Phase II is the same shape.

## Submission

Push the project on a branch named `week03-mini-project/<your-handle>` and open a PR against the C27 curriculum repository. The PR description must link to `RUN.md` and include the psql output proving `count(*)` and `SUM(amount)` are identical after 1 run and 5 runs.

The teaching staff reviews mini-project PRs within 7 business days. Reviews focus on (a) whether the eight functional requirements are met, (b) whether the watermark advances in the load transaction, (c) whether the self-test genuinely proves idempotency with a `state_hash`, and (d) whether the citations are present and accurate. The single most common review comment is "your watermark advances in a separate transaction — a crash here skips data" — preempt it.

Cited references: <https://www.psycopg.org/psycopg3/docs/basic/copy.html>, <https://www.psycopg.org/psycopg3/docs/basic/transactions.html>, <https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT>, <https://www.postgresql.org/docs/16/sql-merge.html>, <https://www.postgresql.org/docs/16/sql-copy.html>, <https://docs.python.org/3/howto/logging.html>, <https://dataintensive.net/>, <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/late-arriving-dimensions/>, <https://hub.docker.com/_/postgres>.
