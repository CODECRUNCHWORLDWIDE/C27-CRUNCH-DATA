# Week 11 — Homework

Six practice problems reinforcing the cost model, compaction, pruning, lineage, PII handling, and compliant deletion. These are smaller than the lab and meant to be done alongside the lectures and exercises. Submit per the instructions at the bottom.

---

## Problem 1 — Name the cost

For each scenario, state which of the three cost categories (**scan / shuffle / storage**) is the dominant cost, and the one optimization you would reach for first.

a. A daily dashboard query runs `SELECT sum(amount) FROM events` with no date filter on a table that grows 5 GB/day, kept for two years.
b. A nightly Spark job `GROUP BY customer_id` on a billion-row table hangs on one task at 99% for 40 minutes.
c. An Iceberg table you overwrite hourly has never had `expire_snapshots` run; its MinIO bucket is 80× the logical table size.
d. A query filters `WHERE event_date = '2026-06-01'` but the table is partitioned by `customer_bucket`, and the Spark UI shows partitions pruned = 0.

**Deliverable:** a four-row table — scenario, dominant cost, first optimization, one-sentence justification.

---

## Problem 2 — Predict the compaction numbers

You have an Iceberg table of 12,000 files averaging 90 KB, logically 1.05 GB, that you will `rewrite_data_files` with `target-file-size-bytes = 536870912` (512 MB).

a. Roughly how many files do you expect after compaction? Show the arithmetic.
b. Will the **bytes scanned** by a full-table `count(*)` change materially after compaction? Why or why not?
c. Will the **wall-clock time** of that `count(*)` change materially? Why?
d. Name one writer-side setting that would have prevented the small files in the first place.

**Deliverable:** answers a–d with the arithmetic for (a).

---

## Problem 3 — Fix the unprunable predicate

A teammate's query against an Iceberg table partitioned by `days(event_ts)` scans the whole table even though it only wants one day:

```sql
SELECT count(*) FROM events
WHERE to_date(event_ts) = DATE '2026-06-01'
   OR extract(year FROM event_ts) = 1970;   -- a "data sanity" clause someone added
```

a. Why does partition pruning not fire on this query? Name both reasons.
b. Rewrite it so the first condition prunes correctly.
c. Explain what the `OR ... = 1970` clause does to pruning and how you would handle the sanity check without killing it.

**Deliverable:** the rewritten query plus a–c in prose.

---

## Problem 4 — Read an OpenLineage event

Given this OpenLineage event fragment:

```json
{
  "eventType": "COMPLETE",
  "job":  { "namespace": "dbt", "name": "mart_daily_revenue" },
  "inputs":  [ { "namespace": "duckdb://wh", "name": "int_orders_enriched" },
               { "namespace": "duckdb://wh", "name": "stg_fx_rates" } ],
  "outputs": [ { "namespace": "duckdb://wh", "name": "mart_daily_revenue",
    "facets": { "columnLineage": { "fields": {
      "total_revenue": { "inputFields": [
        { "name": "int_orders_enriched", "field": "amount_usd" } ] } } } } } ]
}
```

a. Distinguish the **job**, the **run** (not shown — where would its id live?), and the **datasets** in this event.
b. From the `columnLineage` facet, what does `total_revenue` depend on, and what does it *not* directly depend on among the inputs?
c. `stg_fx_rates` is an input to the job but does not appear in `total_revenue`'s `inputFields`. Give a plausible reason and why that matters when debugging a wrong `total_revenue`.

**Deliverable:** answers a–c.

---

## Problem 5 — Choose and defend a masking strategy

For each column, choose a masking strategy from {redaction, partial/truncation, deterministic hash, non-deterministic hash, tokenization, column encryption} and justify it in one sentence, given the stated downstream need:

a. `email` — analysts must count distinct customers and join on identity, but must never see an address.
b. `credit_card_number` — the fraud service must be able to recover the real value for chargebacks; analysts must never see it.
c. `full_name` — appears only in an internal report no analyst needs; show nothing.
d. `ip_address` — analysts do coarse geo rollups by network block only.

**Deliverable:** a four-row table — column, strategy, justification.

---

## Problem 6 — Write the deletion runbook

A user with `customer_id = 77` has filed a valid GDPR Article 17 erasure request. Their data lives in an Iceberg table `db.customers` on MinIO, overwritten daily with 30 days of retained snapshots.

a. Write the exact two-step SQL/procedure sequence that *fully* erases them.
b. Write the single verification query that proves the erasure is physically complete, and state the result you expect to see.
c. Your teammate says "I ran `DELETE FROM db.customers WHERE customer_id = 77`, we're compliant." In two sentences, explain precisely why they are not.
d. The table is 4 PB and erasure requests arrive hourly, making the rewrite infeasible. Name the alternative approach and the one thing that must have been true since day one for it to work.

**Deliverable:** answers a–d, with runnable SQL for (a) and (b).

---

## Deliverables and submission

Submit a single `week-11-homework.md` (or a `homework/` directory) containing your answers to all six problems. For problems with SQL (3, 6), include runnable statements; for problems with tables (1, 5), use markdown tables. Where a problem asks "why," one to three precise sentences beat a paragraph.

Push to your cohort repo under `c27/week-11/homework/` and open a PR tagged `week-11`. A TA reviews against the lecture notes and `exercises/SOLUTIONS.md`. The homework is formative — it is not separately graded, but problems 2, 3, and 6 map directly to quiz and lab pass criteria, so do them honestly.
