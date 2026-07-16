# Challenge 01 — The Analytical Query Gauntlet

> **Time:** ~2 hours. **Engine:** PostgreSQL 16 (you may also answer in DuckDB and compare). **Dataset:** the Week-1 retail star schema. **This is Lab 02 from the syllabus.**

This is the week's lab. You are handed ten realistic retail business questions in plain English. The deliverable is **ten correct analytical queries** — one per question — using window functions, `GROUPING SETS`, anti-joins, and the rest of the week's toolkit. Two of the questions also ask you to read `EXPLAIN ANALYZE`, find the bottleneck, and make the query measurably faster.

The point of the gauntlet is the skill the syllabus names: **turn a vague business question into correct analytical SQL.** A "correct" answer is one whose result you can defend — right grain, right handling of ties and `NULL`s, right frame on a window. Plausible-looking nonsense is the failure mode you are training against.

## Setup

You need the Week-1 schema loaded in Postgres (`docker exec -it crunch-pg psql -U postgres -d crunch`). If you are using the mini-project's larger dataset, even better — the two tuning questions are more instructive at scale.

## The ten questions

Deliver your answers in a single file `gauntlet.sql`, each query preceded by a comment with the question number and a one-line note on any judgment call you made (e.g. "used DENSE_RANK because the brief said 'top 3 price tiers,' not 'three rows'").

1. **Top 3 products by revenue in each category.** Decide and *state in a comment* whether "top 3" means three rows (`ROW_NUMBER`) or three revenue tiers (`DENSE_RANK`), and answer accordingly. (Window: ranking.)

2. **Each store's running cumulative revenue over the year**, one row per day-with-sales, with the cumulative total to date. (Window: running aggregate, default frame is fine here — justify it.)

3. **The trailing 7-day revenue for each store**, so an analyst can spot a slow week. (Window: explicit `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` frame.)

4. **Month-over-month revenue growth percentage for the whole business.** First and last months handled honestly. (Window: `LAG`, `NULLIF` guard.)

5. **Revenue by region and category, with region subtotals and a grand total, in one result set**, subtotal rows clearly labelled. (`ROLLUP` + `GROUPING()`.)

6. **Customers who signed up but have never placed an order.** (Anti-join — and do *not* use `NOT IN`; explain in a comment why not.)

7. **Customers whose only orders were in a single store** (loyal-to-one-store customers). (Aggregation + `HAVING COUNT(DISTINCT store_key) = 1`.)

8. **Products that have sold in every store.** (Relational division — deliver both the double-`NOT EXISTS` and the `COUNT(DISTINCT)` forms, and say which you'd ship.)

9. **For each customer, their single largest order line and the date of it** — one row per customer. (Window: `ROW_NUMBER` over `extended_price DESC`, then filter to rank 1 via a CTE; in DuckDB show the `QUALIFY` version too.)

10. **The 90th-percentile order-line value within each category** — the threshold above which a sale is "big" for that category. (Window: `PERCENT_RANK`/`CUME_DIST`, or `percentile_cont` as an aggregate — pick one and justify.)

### The two tuning questions

For **question 6** (customers with no orders) and **question 8** (sold in every store):

- Capture `EXPLAIN (ANALYZE, BUFFERS)` for your first working version.
- Read it. Identify the bottleneck node (estimate vs actual rows, any `Seq Scan` with large `Rows Removed by Filter`, any `Nested Loop` with huge `loops`).
- Make **one** change — add an index, switch the anti-join form, switch question 8 from the double-`NOT EXISTS` to the `COUNT(DISTINCT)` form — and re-measure.
- Record the before/after `Execution Time` and `Buffers`.

## Deliverables

1. `gauntlet.sql` — the ten queries, each commented with question number and any judgment call.
2. `tuning-notes.md` — for questions 6 and 8: the before plan, your diagnosis (2–3 sentences), the change you made, the after plan, and the speedup factor.
3. A one-paragraph reflection: which question was hardest to translate from English to SQL, and why.

## Acceptance criteria

- All ten queries run without error on PostgreSQL 16 against the Week-1 schema.
- Each query returns the *correct grain* (e.g. question 9 returns exactly one row per customer).
- Ties, `NULL`s, and frames are handled deliberately and the choice is stated.
- Question 6 uses `NOT EXISTS` or `LEFT JOIN ... IS NULL`, never `NOT IN`, with a comment explaining the trap.
- The two tuning answers include a before plan, an after plan, and a measured speedup — not a guess.
- Every non-obvious clause is labelled with its engine if it is DuckDB-only (`QUALIFY`).

## Citations

- Window functions: <https://www.postgresql.org/docs/16/functions-window.html>
- Grouping sets / `ROLLUP`: <https://www.postgresql.org/docs/16/queries-table-expressions.html#QUERIES-GROUPING-SETS>
- `GROUPING()`: <https://www.postgresql.org/docs/16/functions-aggregate.html#FUNCTIONS-GROUPING-TABLE>
- Subquery expressions (`EXISTS`): <https://www.postgresql.org/docs/16/functions-subquery.html>
- Ordered-set aggregates (`percentile_cont`): <https://www.postgresql.org/docs/16/functions-aggregate.html#FUNCTIONS-ORDEREDSET-TABLE>
- "Using EXPLAIN": <https://www.postgresql.org/docs/16/using-explain.html>
- DuckDB `QUALIFY`: <https://duckdb.org/docs/sql/query_syntax/qualify>

## Stretch goals

- Answer all ten in **DuckDB** as well, using `QUALIFY` where it improves readability, and note any query that needed different SQL between the two engines.
- For question 10, compare `PERCENT_RANK` (a window function, gives every row its percentile) against `percentile_cont(0.9) WITHIN GROUP (ORDER BY extended_price)` (an ordered-set aggregate, gives one threshold per group) and explain when each is the right tool.
- Add an eleventh question of your own that no single clause answers cleanly, and write it as a readable CTE pipeline.
