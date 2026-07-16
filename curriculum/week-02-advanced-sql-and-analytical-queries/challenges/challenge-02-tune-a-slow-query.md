# Challenge 02 — Tune a Slow Query

> **Time:** ~2 hours. **Engine:** PostgreSQL 16 (stretch: compare on DuckDB 1.x). **Dataset:** the Week-1 retail star schema, ideally the mini-project's larger fact table (~5M rows). **Citations:** "Using EXPLAIN" <https://www.postgresql.org/docs/16/using-explain.html>, indexes <https://www.postgresql.org/docs/16/indexes.html>, `CREATE STATISTICS` <https://www.postgresql.org/docs/16/sql-createstatistics.html>.

You are handed a query that runs slowly on the warehouse. Your job is the senior-engineer loop: **read the plan, diagnose the real cause, make one targeted change, and report the before/after with the plan diff.** No guessing. Every claim is backed by a captured plan.

## The slow query

A product analyst wants "the most recent order line for every customer in the *North* region, with the product name and the running total of that customer's spend up to that order." Their query works but takes many seconds:

```sql
WITH customer_lines AS (
    SELECT f.customer_key,
           f.date_key,
           f.product_key,
           f.extended_price,
           ROW_NUMBER() OVER (PARTITION BY f.customer_key ORDER BY f.date_key DESC) AS rn,
           SUM(f.extended_price) OVER (PARTITION BY f.customer_key ORDER BY f.date_key) AS spend_to_date
    FROM   fact_sales f
    JOIN   dim_customer c ON c.customer_key = f.customer_key
    JOIN   dim_store    s ON s.store_key    = f.store_key
    WHERE  s.region = 'North'
)
SELECT cl.customer_key,
       d.full_date,
       p.product_name,
       cl.extended_price,
       cl.spend_to_date
FROM   customer_lines cl
JOIN   dim_date    d ON d.date_key    = cl.date_key
JOIN   dim_product p ON p.product_key = cl.product_key
WHERE  cl.rn = 1
ORDER  BY cl.customer_key;
```

## The loop you must follow

1. **Measure the baseline.** Run `EXPLAIN (ANALYZE, BUFFERS)` on the query as given. Save the full plan text as `before.txt`.
2. **Read the plan inside-out.** Find the node where estimated rows and actual rows diverge most. Look for: a `Seq Scan` on `fact_sales` filtering by region (the region filter is on `dim_store`, so the whole fact table may be scanned and joined before the filter bites); a `Sort` reporting `external merge Disk:` (the window's `PARTITION BY`/`ORDER BY` sort spilled because it exceeded `work_mem`); a `Nested Loop` with large `loops`.
3. **Form a hypothesis and name it.** Write down, in one sentence, what you believe the bottleneck is, citing the specific plan line. Candidate diagnoses: *missing index* (no index supports the region-filtered join into the fact), *spilled sort* (the `WindowAgg`'s sort went to disk), *bad join order / cardinality miss* (a `Nested Loop` chosen on a wrong estimate).
4. **Make ONE change.** Examples — pick what the plan tells you, not what you guessed:
   - Add an index that supports the join + filter, e.g. `CREATE INDEX idx_fact_store_cust ON fact_sales (store_key, customer_key, date_key)`, then `ANALYZE`.
   - Raise `work_mem` for the session (`SET work_mem = '256MB';`) if the sort spilled, and confirm the `Sort Method` changes from `external merge Disk:` to `quicksort Memory:`.
   - Run `ANALYZE` (and consider `CREATE STATISTICS (dependencies) ON store_key, customer_key FROM fact_sales`) if a `Nested Loop` exploded on a stale/correlated estimate.
5. **Re-measure.** Run `EXPLAIN (ANALYZE, BUFFERS)` again. Save as `after.txt`.
6. **Report.** Write the before/after `Execution Time`, the before/after `Buffers`, the node that changed, and the speedup factor.

## Deliverables

1. `before.txt` and `after.txt` — the full captured plans.
2. `tuning-report.md` containing:
   - The baseline timing and buffers.
   - Your diagnosis, citing the exact plan line that proves it (quote it).
   - The single change you made and *why the plan justified that change and not another*.
   - The after timing and buffers, and the speedup factor.
   - A "plan diff" paragraph naming which node(s) changed type or cost (e.g. "`Seq Scan` → `Index Scan`," or "`Sort: external merge Disk:` → `quicksort Memory:`").
3. A one-sentence statement of what you did **not** change and why (resisting the urge to add three indexes "to be safe").

## Acceptance criteria

- The diagnosis quotes a real plan line as evidence; no claim is unsupported.
- Exactly **one** change was made between before and after (so the cause of the improvement is unambiguous).
- The query still returns the same rows after tuning (verify — a tuning change must not alter results).
- The report states a concrete speedup factor and the buffer reduction.
- If you raised `work_mem`, you note that this is a session/query knob, not a free global setting (every concurrent sort can use that much).

## Stretch goal — the same query on DuckDB

Load the same data into DuckDB (it can read directly from a Postgres dump or from Parquet/CSV) and run the *same* query, swapping the Postgres CTE-filter-on-`rn` for DuckDB's `QUALIFY ROW_NUMBER() OVER (...) = 1`. Run `EXPLAIN ANALYZE` (DuckDB syntax, <https://duckdb.org/docs/guides/meta/explain>) and report:

- The DuckDB execution time versus your tuned Postgres time.
- Whether DuckDB needed an index at all (it generally does not for this scan-heavy analytical shape — explain why, referencing columnar storage).
- One sentence on which engine you would choose to run this query in production and why.
