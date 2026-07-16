# Lecture 1 — ETL vs ELT and the Shape of a Batch Job: Staging, Bulk Loading with COPY, Connection Discipline, and Structured Logging

> **Time:** ~2.5 hours. **Prerequisites:** Week 1 (the retail star schema in Postgres) and Week 2 (analytical SQL). A working Python 3.12 with `psycopg` 3 installed (`pip install "psycopg[binary]"`) and a `postgres:16` container running. **Citations:** psycopg 3 docs <https://www.psycopg.org/psycopg3/docs/>; psycopg 3 COPY <https://www.psycopg.org/psycopg3/docs/basic/copy.html>; psycopg 3 transactions <https://www.psycopg.org/psycopg3/docs/basic/transactions.html>; PostgreSQL 16 INSERT <https://www.postgresql.org/docs/16/sql-insert.html>, COPY <https://www.postgresql.org/docs/16/sql-copy.html>, TRUNCATE <https://www.postgresql.org/docs/16/sql-truncate.html>; Python logging HOWTO <https://docs.python.org/3/howto/logging.html> and module reference <https://docs.python.org/3/library/logging.html>; Kleppmann *DDIA* ch. 10 <https://dataintensive.net/>.

## 1. The question this whole week answers

Week 1 modeled a retail warehouse. Week 2 queried it. Neither asked how the rows got into `fact_sales` in the first place. That gap is the job of the data engineer, and it is where pipelines actually break. The framing question for the entire week is the one a tired on-call engineer asks at 3 AM when an alert fires: *is it safe to re-run this?* Everything in these three lectures — staging, watermarks, upserts, transaction boundaries — exists to make the answer "yes, obviously" instead of "let me restore a backup."

This lecture builds the skeleton: what an ETL job *is*, the five stages it moves through, why you stage before you load, how you bulk-load fast, how you manage the database connection, and how you log so that you can operate the thing. Lectures 2 and 3 add the hard parts (incrementality/idempotency, then late data) on top of this skeleton.

## 2. ETL vs ELT — what moves where, and why the answer changed

**ETL** — *extract, transform, load* — is the classic shape. A process pulls data out of a source, reshapes it in a separate compute layer, and writes the finished result into the warehouse:

```text
   source ──extract──► [ Python / Spark transform tier ] ──load──► warehouse (clean, conformed)
```

**ELT** — *extract, load, transform* — flips the last two steps. You dump the raw source into the warehouse first, then transform it *in place* with SQL:

```text
   source ──extract──► warehouse (raw) ──transform (SQL)──► warehouse (clean, conformed)
```

ELT won the lakehouse era for one structural reason: cloud warehouses (and DuckDB on a laptop) made compute cheap, elastic, and co-located with storage, so it became cheaper to land raw bytes and transform them with the same engine that serves queries than to operate and scale a separate transform tier. dbt — which you meet in Week 5 — is the tool that made ELT the default; it is "transform" expressed as version-controlled, tested SQL running inside the warehouse. Kleppmann's *Designing Data-Intensive Applications* chapter 10 frames the underlying batch-processing trade-off <https://dataintensive.net/>.

But ETL never died, and you should be able to defend each:

| Use ETL (transform before land) when… | Use ELT (land then transform) when… |
| --- | --- |
| The transform must precede landing — PII redaction, schema enforcement at the boundary | The warehouse compute is cheap and elastic |
| A join requires a system the warehouse cannot reach | The raw data is useful to keep verbatim |
| The source schema is hostile and must be normalized in flight | The transform logic changes often and you want to re-run it without re-extracting |
| You are loading a strict, constrained target (a star-schema fact table) | You are populating a flexible staging area first |

This week you build an **ETL** job, deliberately. Doing the transform in Python first forces you to confront extraction, batching, idempotency, and late data *explicitly*, one stage at a time. Week 5's dbt rebuild will show you the same warehouse populated the ELT way, and the contrast is the lesson.

## 3. The anatomy of a batch job: extract → stage → transform → load → publish

Every batch job, ETL or ELT, decomposes into five stages. Naming them is not pedantry — each stage is a distinct place the job can die, and drawing the boundaries is how you make the death survivable.

```text
   ┌─────────┐   ┌─────────┐   ┌───────────┐   ┌────────┐   ┌──────────┐
   │ EXTRACT │──►│  STAGE  │──►│ TRANSFORM │──►│  LOAD  │──►│ PUBLISH  │
   │ read    │   │ COPY    │   │ cast,     │   │ upsert │   │ advance  │
   │ source  │   │ into a  │   │ dedupe,   │   │ into   │   │ watermark│
   │ rows    │   │ landing │   │ conform   │   │ target │   │ + log    │
   │         │   │ table   │   │ keys      │   │        │   │          │
   └─────────┘   └─────────┘   └───────────┘   └────────┘   └──────────┘
        │             │              │             │             │
   (network)    (crash here =   (bad data =   (crash here =  (commit =
                 only staging    caught in     batch rolls    visible to
                 is dirty)       transform)    back atomically) readers)
```

- **Extract** reads rows from the source (a CSV drop, an API page, a CDC stream, another database). This is where the network lives, so it is where most transient failures happen.
- **Stage** writes the extracted rows into a *staging table* — a plain, unconstrained landing table holding only this run's raw rows. You bulk-load it with `COPY` (section 6). The staging table is disposable: you truncate and refill it every run, so a half-finished extract corrupts only staging, never the warehouse.
- **Transform** reshapes the staged rows — cast types, drop duplicates, conform keys to the warehouse's surrogate keys. In an ETL job this is Python and/or SQL against the staging table.
- **Load** merges the transformed rows into the warehouse target with an **upsert** (Lecture 2), so a re-run overwrites rather than appends.
- **Publish** makes the new state official: advance the watermark *in the same transaction as the load* (Lecture 2), and emit the run's structured log line.

## 4. Full vs incremental loads

A **full load** reads the entire source every run and replaces the entire target. It is gorgeously simple and always correct, and it dies the day the source has a hundred million rows: you do not re-read and re-write the whole warehouse every night.

An **incremental load** reads only the rows that changed since the last successful run and merges just those. This is the production default, and it has exactly one prerequisite: a reliable way to ask the source "what changed since last time?" — the watermark, which Lecture 2 is entirely about.

Keep both. A real loader has a `--full-refresh` flag for the day the incremental logic is suspect (or the target was wiped), and runs incrementally every other day:

```python
if args.full_refresh:
    rows = extract_all(source)        # read everything
    truncate_target(conn)             # replace the target wholesale
else:
    watermark = read_watermark(conn)  # "what did we load last time?"
    rows = extract_since(source, watermark)  # only the new rows
```

## 5. The staging-table pattern — the crash-safe landing zone

The staging table is the single most important structural habit in batch loading. Define it as a plain table with no constraints, no foreign keys, no indexes you do not need — its job is to accept raw rows fast and be thrown away:

```sql
-- A landing table mirroring the raw shape of the source orders feed.
-- No constraints: we want COPY to be as fast as possible, and we will
-- validate during the TRANSFORM step, not at insert time.
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
```

The discipline at the top of every run: `TRUNCATE` staging, then refill it. `TRUNCATE` is far faster than `DELETE` for emptying a whole table because it does not scan rows — it reclaims the storage in one operation <https://www.postgresql.org/docs/16/sql-truncate.html>:

```python
def reset_staging(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE stg_orders")
    # note: not committed yet — we commit at the end of the batch (Lecture 2)
```

Because staging holds only this run's data and is truncated at the start, a crash during extract leaves *only staging* in a dirty state. The next run truncates it anyway. The warehouse target is never touched until the LOAD step, and that step is atomic. This is the structural reason the job is safe to re-run — and it costs you one extra table.

## 6. Bulk loading with COPY — the right way to move a million rows

There is one right way and two wrong ways to move a lot of rows into Postgres.

**Wrong way 1 — a million single-row `INSERT`s.** Each statement is a network round trip and, without an explicit transaction, its own implicit commit. You will measure this at hundreds of rows per second and watch your loader miss its SLA.

**Wrong way 2 — one giant `INSERT` (or an unbatched `executemany`).** This buffers the entire dataset in client memory and in one transaction's WAL before anything commits. It works until the dataset outgrows RAM, then it falls over — and a single failure rolls back *everything*.

**Right way — `COPY`.** `COPY` is Postgres's dedicated bulk-load path. It streams rows into the table with minimal per-row parsing and locking overhead and is typically an order of magnitude faster than `INSERT` for bulk loads. The server-side command is documented at <https://www.postgresql.org/docs/16/sql-copy.html>; psycopg 3 exposes it through `cursor.copy()` <https://www.psycopg.org/psycopg3/docs/basic/copy.html>.

You `COPY` into the **staging** table (fast, raw), then upsert from staging into the constrained target. Bulk-load the raw, merge the clean.

```python
import csv
import psycopg
from pathlib import Path

def copy_csv_into_staging(conn: psycopg.Connection, csv_path: Path) -> int:
    """Bulk-load a CSV into stg_orders with COPY. Returns the row count."""
    rows = 0
    # The COPY ... FROM STDIN statement; psycopg streams rows over the protocol.
    copy_sql = (
        "COPY stg_orders "
        "(order_id, customer_id, product_id, order_ts, quantity, unit_price, updated_at, ingested_at) "
        "FROM STDIN"
    )
    with conn.cursor() as cur, cur.copy(copy_sql) as copy:
        with csv_path.open(newline="") as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            for record in reader:
                # copy.write_row sends one row; psycopg adapts Python types to the
                # COPY wire format. See the psycopg COPY docs for the row-vs-block API.
                copy.write_row(record)
                rows += 1
    return rows
```

`cur.copy(...)` returns a context-managed `Copy` object; `copy.write_row(seq)` sends one row at a time (psycopg handles the binary framing). For pre-formatted byte blocks you would use `copy.write(...)` instead — both are on the COPY docs page <https://www.psycopg.org/psycopg3/docs/basic/copy.html>. For data already in a pandas/Arrow frame there are faster paths, but `write_row` over a `csv.reader` is the clearest starting point and the one Exercise 1 builds.

## 7. Connection and transaction discipline

A connection is expensive to open and carries transaction state, so you open one per job, do all your work on it, and close it deterministically. psycopg 3 gives you two `with` levels, and the distinction matters:

```python
import psycopg

CONNINFO = "host=localhost port=5432 dbname=postgres user=postgres password=crunch"

# The OUTER `with` manages the connection lifetime: it closes on exit, even on exception.
with psycopg.connect(CONNINFO) as conn:
    # The INNER `with conn.transaction()` is an explicit transaction block:
    # it COMMITS on clean exit and ROLLS BACK if the block raises.
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("TRUNCATE stg_orders")
            # ... COPY, upsert, advance watermark ...
        # leaving the transaction block here commits all of the above atomically
# leaving the connection block here closes the connection
```

Two facts that trip up newcomers, both documented on the psycopg transactions page <https://www.psycopg.org/psycopg3/docs/basic/transactions.html>:

1. **The connection `with` block commits on clean exit by default** (`autocommit=False`, the default). If you also use `conn.transaction()`, you get explicit, nested-readable transaction scopes — preferred for ETL because the boundary is visible in the code.
2. **A connection is not a free-threaded shared object.** One job, one connection. A long-lived *service* uses a connection *pool* (`psycopg_pool.ConnectionPool`) to hand out and reclaim connections; a one-shot batch job does not need one. Use a single `with psycopg.connect(...)` and move on.

The payoff of wrapping the load in `conn.transaction()` is restartability: if the process dies anywhere inside the block, Postgres rolls the whole batch back, and the next run starts clean. Lecture 2 makes the watermark part of that same transaction so a crash cannot skip data.

## 8. Batch-size discipline — the dial between two failure modes

For a source too large to load in one transaction, you load in batches. Batch size is a dial with a failure mode at each end:

- **Too small** (e.g., 100 rows): per-transaction overhead dominates; you pay commit cost thousands of times; the job is slow.
- **Too large** (e.g., all 50M rows in one transaction): the WAL and locks grow without bound; a single failure rolls back everything; progress is invisible.

The usual sweet spot is **5,000–50,000 rows per batch**: large enough that commit overhead is amortized, small enough that a failure rolls back cheaply and the logs show forward progress. A batched loop:

```python
def batched(iterable, size):
    """Yield lists of up to `size` items. (Python 3.12 also has itertools.batched.)"""
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch

BATCH_SIZE = 10_000
for batch in batched(extract_since(source, watermark), BATCH_SIZE):
    with conn.transaction():          # each batch is its own atomic unit
        upsert_batch(conn, batch)     # Lecture 2
        advance_watermark(conn, max(r.updated_at for r in batch))  # same txn
    log.info("batch loaded", extra={"rows": len(batch)})
```

Each batch commits independently, so a crash loses at most one in-flight batch (which rolls back) and the next run resumes from the last committed watermark. That is restartability, made of nothing but transaction boundaries.

## 9. Structured logging for pipelines

A pipeline that logs `print("done")` cannot be operated. A pipeline that emits one JSON line per stage — with a `run_id`, row counts, durations, and watermark transitions — can be queried, alerted on, and debugged from a log aggregator without anyone SSHing into a box. The standard-library `logging` module does this with zero third-party dependencies; the HOWTO is at <https://docs.python.org/3/howto/logging.html> and the reference at <https://docs.python.org/3/library/logging.html>.

A minimal JSON formatter and a `run_id`-carrying logger:

```python
import json
import logging
import sys
import time
import uuid

class JsonFormatter(logging.Formatter):
    """Emit each log record as a single JSON object — one line per event."""
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "msg": record.getMessage(),
            "logger": record.name,
        }
        # Anything passed via logging's `extra=` lands as an attribute on the record.
        for key in ("run_id", "stage", "rows", "duration_ms",
                    "watermark_from", "watermark_to"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        return json.dumps(payload)

def build_logger() -> logging.LoggerAdapter:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    base = logging.getLogger("loader")
    base.setLevel(logging.INFO)
    base.handlers = [handler]
    # A LoggerAdapter injects run_id into every record's `extra` automatically.
    return logging.LoggerAdapter(base, {"run_id": uuid.uuid4().hex})

log = build_logger()

t0 = time.perf_counter()
rows = copy_csv_into_staging(conn, csv_path)
log.info("staged", extra={
    "stage": "stage",
    "rows": rows,
    "duration_ms": round((time.perf_counter() - t0) * 1000, 1),
})
```

A run then prints lines a machine can parse:

```text
{"ts": "2026-06-19T03:00:01+0000", "level": "INFO", "msg": "staged", "logger": "loader", "run_id": "a1b2c3d4...", "stage": "stage", "rows": 48213, "duration_ms": 1820.4}
{"ts": "2026-06-19T03:00:03+0000", "level": "INFO", "msg": "loaded", "logger": "loader", "run_id": "a1b2c3d4...", "stage": "load", "rows": 48213, "watermark_from": "2026-06-18T00:00:00+00:00", "watermark_to": "2026-06-19T00:00:00+00:00"}
```

The `run_id` ties every line of one execution together; `rows` and `duration_ms` are the two numbers an operator looks at first; `watermark_from`/`watermark_to` tell you exactly which slice of source this run covered. That last pair is what makes a "did we skip a day?" question answerable from the logs alone.

## Exercise pointer

Now build the STAGE step in isolation. [exercises/exercise-01-bulk-load-with-copy.py](../exercises/exercise-01-bulk-load-with-copy.py) gives you a CSV and a `stg_orders` table; you write the `cursor.copy()` loop and the structured log line reporting rows and duration. Get the bulk load fast and observable before Lecture 2 adds the watermark and the upsert on top.

## Summary

- **ETL transforms before landing; ELT lands then transforms.** ELT won the lakehouse because warehouse compute got cheap; ETL still owns the boundary (PII, schema enforcement, unreachable joins). You build ETL this week to confront every failure mode explicitly.
- **A batch job is extract → stage → transform → load → publish.** Each stage is a place to die; the staging table is what makes the death survivable.
- **Full loads are simple and ruinous at scale; incremental loads are the production default** and require a watermark (Lecture 2). Keep a `--full-refresh` escape hatch.
- **Stage into a disposable, unconstrained table, then merge into the constrained target.** A crash dirties only staging.
- **`COPY` via `cursor.copy()` beats both single-row `INSERT` and one-giant-`INSERT`.** Bulk-load the raw, merge the clean.
- **One connection per job, in a `with` block; wrap the load in `conn.transaction()`** so a crash rolls back atomically. Batch at 5k–50k rows.
- **Emit one JSON log line per stage** with a `run_id`, row counts, durations, and watermark transitions, using the standard `logging` module.

Cited references: <https://www.psycopg.org/psycopg3/docs/>, <https://www.psycopg.org/psycopg3/docs/basic/copy.html>, <https://www.psycopg.org/psycopg3/docs/basic/transactions.html>, <https://www.postgresql.org/docs/16/sql-insert.html>, <https://www.postgresql.org/docs/16/sql-copy.html>, <https://www.postgresql.org/docs/16/sql-truncate.html>, <https://docs.python.org/3/howto/logging.html>, <https://docs.python.org/3/library/logging.html>, <https://dataintensive.net/>.
