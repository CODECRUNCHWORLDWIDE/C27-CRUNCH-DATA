# Week 3 — Quiz

Ten multiple-choice questions covering ETL vs ELT, full vs incremental loads, watermarks, idempotency, restartability, `ON CONFLICT`/`MERGE`, `COPY` vs `INSERT`, late-arriving records, event vs ingestion time, late-arriving dimensions, and structured logging. Treat the quiz as a closed-book check; the answer key with reasoning is at the bottom.

## Question 1 — ETL vs ELT

Which statement best captures why **ELT** became the default in the cloud lakehouse while **ETL** still has a place?

- (A) ELT is always faster than ETL, so ETL is obsolete.
- (B) Cheap, elastic warehouse compute made it economical to land raw data and transform it in place; ETL still owns cases where the transform must precede landing (PII redaction, schema enforcement, an unreachable join).
- (C) ELT does not require a database, whereas ETL does.
- (D) ETL and ELT are the same thing; the letters are interchangeable.

## Question 2 — Full vs incremental

A source has grown to 100 million rows that change slowly. The nightly job currently re-reads and re-writes the entire target. The correct first improvement is to:

- (A) Add more RAM so the full load fits in memory.
- (B) Switch to an incremental load driven by a stored high-water mark, reading only rows changed since the last successful run.
- (C) Run the full load twice a night for safety.
- (D) Drop the target's indexes permanently.

## Question 3 — What a high-water mark is

A high-water mark (watermark) in a batch loader is:

- (A) The maximum number of rows a single transaction may load.
- (B) A stored value (e.g. `max(updated_at)` or `max(id)`) recording how far the last successful run got, which the next run reads forward from.
- (C) A lock held on the source table for the duration of the load.
- (D) The timestamp at which the orchestrator schedules the job.

## Question 4 — Idempotency

A load is idempotent when:

- (A) It runs faster on the second run than the first.
- (B) Running it once and running it five times over the same source leave the warehouse in identical state.
- (C) It never fails.
- (D) It always reads the entire source.

## Question 5 — The upsert

Which SQL makes a re-loaded order overwrite its existing row rather than insert a duplicate, given a unique constraint on `order_id`?

- (A) `INSERT INTO fact_sales (...) VALUES (...);`
- (B) `INSERT INTO fact_sales (...) VALUES (...) ON CONFLICT (order_id) DO UPDATE SET quantity = EXCLUDED.quantity, amount = EXCLUDED.amount;`
- (C) `UPDATE fact_sales SET quantity = ... WHERE order_id = ...;` run before every insert
- (D) `INSERT INTO fact_sales (...) VALUES (...) ON CONFLICT (order_id) DO NOTHING;`

## Question 6 — COPY vs INSERT

Why is `cursor.copy()` the right way to bulk-load a million rows, rather than a loop of single-row `INSERT`s or one giant `INSERT`?

- (A) `COPY` is the only statement that can write to a table.
- (B) Single-row `INSERT`s are round-trip-bound (one network round trip each); one giant `INSERT` buffers everything in memory and one transaction; `COPY` streams rows with minimal per-row overhead, winning on both throughput and memory.
- (C) `COPY` automatically deduplicates rows.
- (D) `COPY` does not require a database connection.

## Question 7 — Restartability and the watermark transaction

A loader advances its watermark in a *separate* transaction, committed *after* the load transaction. A `kill -9` lands between the two commits. What is the danger?

- (A) Nothing; two transactions are always safe.
- (B) Depending on order, the watermark may point past data that was never loaded, so a normal re-run silently skips that slice forever; the fix is to advance the watermark in the same transaction as the load.
- (C) The database will refuse all future connections.
- (D) The source data is deleted.

## Question 8 — Event time vs ingestion time

A record's `event_time` is three days ago; its `ingested_at` is now. A loader filtering `WHERE event_time > watermark` (watermark already past three days ago) will:

- (A) Load the record correctly, because it arrived today.
- (B) Raise an error because the timestamps disagree.
- (C) Silently drop the record, because on event time it sorts behind the watermark — the classic late-data bug.
- (D) Load the record twice.

## Question 9 — Late-arriving dimension

A fact arrives referencing a `customer_id` not yet present in `dim_customer`. With an **inner join** in the fact load, what happens, and what is the standard fix?

- (A) The load errors loudly; fix by retrying.
- (B) The fact row is silently dropped (no matching dimension row → no output); fix with the inferred-member / placeholder pattern — insert a placeholder dimension row, point the fact at its surrogate key, and correct the placeholder when the real data arrives.
- (C) The fact is loaded with a random customer; fix by deleting it.
- (D) Nothing happens; inner joins always keep all fact rows.

## Question 10 — Structured logging

Why emit one JSON line per stage (`{"run_id": ..., "stage": "load", "rows": 48213, "watermark_to": ...}`) instead of `print("done")`?

- (A) JSON is required by PostgreSQL.
- (B) Structured logs are machine-parseable: you can query, alert on, and debug a run from a log aggregator (row counts, durations, watermark transitions, a `run_id` tying the run together) without SSHing into a box; a `print` gives you none of that.
- (C) `print` is removed in Python 3.12.
- (D) JSON logs run faster than text logs.

---

## Answer key

- **Q1: (B).** ELT won because cloud (and laptop, via DuckDB) warehouse compute became cheap and elastic, so landing raw bytes and transforming them in place is cheaper than operating a separate transform tier; ETL still owns transform-before-land cases (PII, schema enforcement, an unreachable join). Citation: Lecture 1 §2 and Kleppmann ch. 10 <https://dataintensive.net/>.
- **Q2: (B).** Incrementality — reading only changed rows via a watermark — is the production answer to a large, slowly-changing source. More RAM (A) and double full loads (C) treat the symptom; dropping indexes (D) harms reads. Citation: Lecture 1 §4, Lecture 2 §2.
- **Q3: (B).** A watermark is a stored progress marker (`max(updated_at)`/`max(id)`) the next run reads forward from; it is what makes a load incremental. Citation: <https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT> (for how the read-forward slice is then merged) and Lecture 2 §2.
- **Q4: (B).** Idempotency is the `x = 5` (not `x += 5`) property: identical state after one run or five. Speed (A), never-failing (C), and full reads (D) are unrelated. Citation: Lecture 2 §1, Kleppmann <https://dataintensive.net/>.
- **Q5: (B).** `ON CONFLICT (order_id) DO UPDATE SET ... = EXCLUDED.*` overwrites on a key collision. Plain `INSERT` (A) duplicates; pre-`UPDATE` (C) races and is not atomic; `DO NOTHING` (D) ignores the new values, so a correction would never be applied. Citation: <https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT>.
- **Q6: (B).** `COPY` streams rows with minimal per-row overhead — an order of magnitude faster than single-row `INSERT` (round-trip-bound) and safer than one giant `INSERT` (memory-bound, all-or-nothing rollback). Citation: <https://www.psycopg.org/psycopg3/docs/basic/copy.html>, <https://www.postgresql.org/docs/16/sql-copy.html>, Lecture 1 §6.
- **Q7: (B).** A separate-transaction watermark advance creates a window where a crash can leave the watermark past unloaded data, silently skipping it forever; advancing the watermark in the same transaction as the load makes the only outcomes "both committed" or "neither." Citation: Lecture 2 §6, <https://www.psycopg.org/psycopg3/docs/basic/transactions.html>.
- **Q8: (C).** The record arrived in time, but the *filter* is on event time, and on event time it is behind the watermark, so it is dropped with no error — the canonical silent late-data bug. Fix with an ingestion/`id` watermark plus a lookback window plus a natural-key upsert. Citation: Lecture 3 §3, Kleppmann ch. 11 <https://dataintensive.net/>.
- **Q9: (B).** An inner join produces no row for an unmatched dimension key, so the fact silently vanishes — the same class of silent loss as Q8. The inferred-member / placeholder pattern keeps the fact (point it at a placeholder surrogate key) and lets the dimension correct itself later. Citation: <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/late-arriving-dimensions/>, Lecture 3 §5.
- **Q10: (B).** Structured JSON logs are queryable and alertable — row counts, durations, watermark transitions, and a `run_id` tying a run together — which is the minimum an operator needs at 3 AM and the maximum a `print` will never give. Citation: <https://docs.python.org/3/howto/logging.html>, Lecture 1 §9.

## Self-assessment

- 9–10: you can ship this week's mini-project without further reading.
- 7–8: re-read the lecture notes on the questions you missed; the citations point to the exact section and reference page.
- 5–6: re-read the lecture notes end to end and redo the exercises.
- 0–4: rewind to Lecture 1 and read all three lecture notes carefully. The mini-project — and the "no double-count" promise — will not make sense without the conceptual foundation.
