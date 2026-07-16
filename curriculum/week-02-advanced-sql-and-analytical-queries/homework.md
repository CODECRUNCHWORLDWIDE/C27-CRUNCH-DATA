# Week 2 — Homework

Six practice problems, roughly 45 minutes each, against the Week-1 retail star schema on PostgreSQL 16. Each names a deliverable filename and the references to lean on. These reinforce the lectures and feed directly into the gauntlet and the mini-project; do them before the challenges.

## Problem 1 — Ranking and ties (45 min)

Write a query that, for each store region, lists the top 5 customers by total spend, using `DENSE_RANK`. Then write a second version using `ROW_NUMBER` and, in a comment, explain in one sentence the difference in the result if two customers tie on spend.

- **Deliverable:** `hw1-customer-ranking.sql` (both versions, with the comment).
- **References:** window-functions reference <https://www.postgresql.org/docs/16/functions-window.html>; window tutorial <https://www.postgresql.org/docs/16/tutorial-window.html>.

## Problem 2 — Frames you can defend (45 min)

For one store, produce a daily series with three columns side by side: a trailing-7-day total (`ROWS BETWEEN 6 PRECEDING AND CURRENT ROW`), a centered 3-day average (`ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING`), and a since-start running total (the default frame). In a comment, state which one the default frame produced and why it differs from the trailing-7.

- **Deliverable:** `hw2-frames.sql`.
- **References:** window-frame syntax <https://www.postgresql.org/docs/16/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS>.

## Problem 3 — A recursive date spine (45 min)

Write a recursive CTE that generates every date in a chosen month, left-join it to `fact_sales` (via `dim_date`), and produce a gap-free daily revenue series where days with no sales show `0`, not a missing row. Then rewrite the same result using `generate_series` and confirm the rows match.

- **Deliverable:** `hw3-date-spine.sql` (recursive version + `generate_series` version).
- **References:** `WITH` / recursive queries <https://www.postgresql.org/docs/16/queries-with.html>; `generate_series` <https://www.postgresql.org/docs/16/functions-srf.html>.

## Problem 4 — Subtotals that read clearly (45 min)

Write a `ROLLUP` report of revenue by `year`, then `quarter`, then `month` (a three-level hierarchy from `dim_date`), with `GROUPING()`-based labels so each subtotal row reads `Q-TOTAL` / `YEAR-TOTAL` / `GRAND TOTAL` instead of bare `NULL`s. Order so subtotals sort below their group.

- **Deliverable:** `hw4-rollup.sql`.
- **References:** grouping sets <https://www.postgresql.org/docs/16/queries-table-expressions.html#QUERIES-GROUPING-SETS>; `GROUPING()` <https://www.postgresql.org/docs/16/functions-aggregate.html#FUNCTIONS-GROUPING-TABLE>.

## Problem 5 — The anti-join, three ways, and the trap (45 min)

Find products that have *never* been sold, written three ways: `NOT EXISTS`, `LEFT JOIN ... IS NULL`, and `NOT IN`. Confirm all three agree on the current data. Then insert one `fact_sales` row with a `NULL` `product_key` into a scratch copy, re-run all three, and document which one breaks and exactly why (three-valued logic).

- **Deliverable:** `hw5-anti-joins.sql` (three queries + the documented `NOT IN` failure).
- **References:** subquery expressions <https://www.postgresql.org/docs/16/functions-subquery.html>.

## Problem 6 — Read a plan and fix it (45 min)

Pick any query from Problems 1–5 that touches the full `fact_sales` table with a selective filter (or write one: "all sales for a single product in a single month"). Capture its `EXPLAIN (ANALYZE, BUFFERS)`. If it shows a `Seq Scan` with large `Rows Removed by Filter`, add the index that fixes it, `ANALYZE`, and capture the after plan. Report the before/after time and buffers and the speedup. If the planner already chose an index, explain what made the filter selective enough.

- **Deliverable:** `hw6-plan.md` (before plan, diagnosis, change, after plan, speedup).
- **References:** "Using EXPLAIN" <https://www.postgresql.org/docs/16/using-explain.html>; indexes <https://www.postgresql.org/docs/16/indexes.html>.

## Submission

Put all six deliverables in a `week-02-homework/` folder. Each `.sql` file must run top to bottom without error on PostgreSQL 16 against the Week-1 schema; each `.md` must include the captured plans, not paraphrases of them. Label any DuckDB-only clause (`QUALIFY`) as such. Total expected effort: ~4.5 hours.
