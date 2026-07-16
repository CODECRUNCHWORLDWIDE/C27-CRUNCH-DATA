# Week 3 — Homework

Six practice problems that consolidate the week's material. They are sized to ~45 minutes each. Do them after the lectures and the exercises; do them before the mini-project. Cite the URLs you used while solving each one in the commit message of your homework branch.

## Problem 1 — ETL vs ELT for four real constraints

For each of the four scenarios below, declare ETL or ELT and justify the choice in terms of the decision matrix from Lecture 1 (transform-before-land vs land-then-transform, where the compute lives, what the constraint forces).

- **Scenario A:** A healthcare feed where patient identifiers must be tokenized *before* any row is allowed to touch the analytics warehouse, per a data-residency rule.
- **Scenario B:** A cloud warehouse with elastic, cheap compute; the analytics team rewrites the transformation logic weekly and wants to re-run it without re-extracting from the source system.
- **Scenario C:** A nightly load that must join the source orders against a pricing service the warehouse cannot reach over the network.
- **Scenario D:** A clickstream landed as raw JSON that several downstream teams each want to model differently from the same raw bytes.

Cite Kleppmann ch. 10 <https://dataintensive.net/> and at least one of the PostgreSQL `INSERT` <https://www.postgresql.org/docs/16/sql-insert.html> or `COPY` <https://www.postgresql.org/docs/16/sql-copy.html> references.

Deliverable: `homework/01-etl-vs-elt.md` with four declarations and justifications.

## Problem 2 — COPY vs INSERT, measured

Generate a CSV of 500,000 rows. Load it into a staging table three ways and time each: (1) a loop of single-row `INSERT`s, (2) one `executemany` of all 500k rows, (3) `cursor.copy()`. Report rows/second for each and the peak memory of (2) vs (3).

Then write 200 words explaining the result in terms of Lecture 1 §6: why single-row `INSERT` is round-trip-bound, why one giant `INSERT`/`executemany` is memory-bound and rolls back everything on failure, and why `COPY` wins on both axes.

Cite the psycopg COPY page <https://www.psycopg.org/psycopg3/docs/basic/copy.html> and the PostgreSQL COPY reference <https://www.postgresql.org/docs/16/sql-copy.html>.

Deliverable: `homework/02-copy-vs-insert.md` with the timings and the analysis.

## Problem 3 — The transactionally-stored watermark, proven the hard way

Take the Exercise 2 loader. Make a deliberately broken copy that advances the watermark in a *separate* transaction *after* the load commits. Add a `--crash-between` flag that raises immediately after the load commit but before the watermark commit.

Run the broken loader with `--crash-between` against a multi-batch source, then re-run it normally. Show, with a `count(*)` and a `SUM(amount)` against the source ground truth, that a batch was **skipped forever** (the re-run started past the unloaded slice). Then fix it — move the watermark advance into the load transaction — and show the crash-then-rerun now reaches the correct total.

Cite the psycopg transactions page <https://www.psycopg.org/psycopg3/docs/basic/transactions.html> and the PostgreSQL transactions tutorial <https://www.postgresql.org/docs/16/tutorial-transactions.html>.

Deliverable: `homework/03-watermark-transaction.md` with the broken and fixed outputs side by side and a one-paragraph explanation.

## Problem 4 — ON CONFLICT vs MERGE

Implement the same idempotent upsert into a target table twice: once with `INSERT ... ON CONFLICT (key) DO UPDATE` and once with `MERGE`. Load the same batch through each, including a re-load to prove both are idempotent, and confirm both produce an identical `state_hash` (`md5(string_agg(... ORDER BY ...))`).

Then write 250 words on when you would reach for each: the constraint requirement of `ON CONFLICT`, the arbitrary-join and `WHEN MATCHED THEN DELETE` capability of `MERGE`, the portability of `MERGE` to the lakehouse engines you meet in Phase II, and the "a row may not be touched twice in one `MERGE`" caveat.

Cite the ON CONFLICT docs <https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT> and the MERGE docs <https://www.postgresql.org/docs/16/sql-merge.html>.

Deliverable: `homework/04-onconflict-vs-merge.md` with both implementations and the analysis.

## Problem 5 — Event time vs ingestion time, threat-modeled

In one page, work through how a late record gets dropped and how each defense catches it. Cover:

1. **The drop.** With a worked example (give concrete timestamps), show how `WHERE event_time > watermark` filters out a record dated three days ago that arrives today, with no error.
2. **The ingestion-watermark fix.** Show how watermarking on ingestion time (or a monotonic `id`) makes the same record sort *after* the watermark.
3. **The lookback-window fix.** Show how re-reading a trailing window catches it even if the watermark column drifts toward event time, and why the re-read is harmless (the upsert dedupes).
4. **The newer-wins guard.** Show how `WHERE target.updated_at < EXCLUDED.updated_at` prevents an out-of-order older copy from clobbering a newer value.

Cite Kleppmann ch. 11 <https://dataintensive.net/> and the ON CONFLICT docs <https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT>.

Deliverable: `homework/05-event-vs-ingestion-time.md`.

## Problem 6 — The late-arriving dimension

Build a tiny scenario: a fact row references `customer_id = 99812`, which is not yet in `dim_customer`. Show first that an **inner join** in the fact load silently drops the fact (the fact count is one short, no error). Then implement the **inferred-member / placeholder** pattern: insert an `is_inferred = true` placeholder dimension row, point the fact at its surrogate key, and demonstrate that when the real customer 99812 arrives in a later dimension load, overwriting the placeholder in place leaves the fact correct without reprocessing it.

Write 200 words connecting this to the event-time-watermark drop from Problem 5: both are silent losses caused by a filter/join that quietly produces nothing for unmatched data.

Cite the Kimball late-arriving-dimensions technique <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/late-arriving-dimensions/> and the ON CONFLICT docs <https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT>.

Deliverable: `homework/06-late-arriving-dimension.md` with the SQL and the write-up.

## Submission

Push the six deliverables on a branch named `week03-homework/<your-handle>` and open a PR against the C27 curriculum repository. The PR description should link to each of the six files and include a 100-word summary of what you learned.

The teaching staff reviews homework PRs within 5 business days. Reviews focus on whether you have read the citations and whether your reasoning holds together, not on perfect grammar. The single most common review comment is "where is your proof for this claim" — preempt it by including the `count(*)`/`SUM`/`state_hash` evidence and linking the PostgreSQL or psycopg URL for every non-trivial assertion.

Cited references this homework draws from: <https://www.psycopg.org/psycopg3/docs/basic/copy.html>, <https://www.psycopg.org/psycopg3/docs/basic/transactions.html>, <https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT>, <https://www.postgresql.org/docs/16/sql-merge.html>, <https://www.postgresql.org/docs/16/sql-copy.html>, <https://www.postgresql.org/docs/16/tutorial-transactions.html>, <https://dataintensive.net/>, <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/late-arriving-dimensions/>.
