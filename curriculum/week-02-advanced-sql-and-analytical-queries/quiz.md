# Week 2 — Quiz

Ten multiple-choice questions on window frames, ranking functions, recursive CTEs, grouping sets, anti-joins, reading a plan, indexes, `QUALIFY`, and join algorithms. Closed-book; the answer key with reasoning and citations is at the bottom.

## Question 1 — Window vs GROUP BY

What is the defining difference between a window function and a `GROUP BY` aggregate?

- (A) Window functions can only compute `SUM`, never `AVG` or `COUNT`.
- (B) `GROUP BY` collapses rows to one per group; a window function computes an aggregate over a window and attaches it to every input row, preserving detail.
- (C) Window functions run before `WHERE`; `GROUP BY` runs after.
- (D) There is no difference; `OVER ()` is just shorthand for `GROUP BY`.

## Question 2 — The default frame

You write `SUM(amount) OVER (PARTITION BY store ORDER BY day)` with no explicit frame clause. What frame does PostgreSQL apply?

- (A) `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
- (B) `ROWS BETWEEN 1 PRECEDING AND CURRENT ROW`
- (C) `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
- (D) No frame; the function sees only the current row.

## Question 3 — Ranking functions

Within a partition, three rows have ordered values 100, 100, 90. Which function produces the sequence 1, 1, 2?

- (A) `ROW_NUMBER()`
- (B) `RANK()`
- (C) `DENSE_RANK()`
- (D) `NTILE(3)`

## Question 4 — LAG at the boundary

`LAG(revenue) OVER (ORDER BY month)` is evaluated on the first row of the ordered partition. What does it return?

- (A) The value of `revenue` on the last row (it wraps around).
- (B) `0`.
- (C) `NULL`, because there is no preceding row (unless a default argument is supplied).
- (D) An error: `LAG` requires at least two rows.

## Question 5 — Recursive CTEs

A recursive CTE is structured as:

- (A) A single `SELECT` with a `RECURSIVE` keyword in the `WHERE` clause.
- (B) An anchor term, then `UNION ALL`, then a recursive term that references the CTE's own name, repeating until it produces no new rows.
- (C) A `WITH` clause that calls itself with `CALL`, like a stored procedure.
- (D) A `GROUP BY` with `ROLLUP` applied repeatedly.

## Question 6 — ROLLUP

`GROUP BY ROLLUP (region, category)` is equivalent to `GROUP BY GROUPING SETS (...)` with which sets?

- (A) `(region, category), (region), (category), ()`
- (B) `(region, category), (region), ()`
- (C) `(region, category)` only
- (D) `(region), (category)`

## Question 7 — GROUPING()

In a `ROLLUP` result, a row has `NULL` in the `category` column. How do you tell whether that `NULL` is a rolled-up subtotal or a genuine missing value in the data?

- (A) Subtotal `NULL`s are always sorted first, so position tells you.
- (B) Use `GROUPING(category)`: it returns `1` for a rolled-up subtotal and `0` for a real value.
- (C) Use `COALESCE(category, 'subtotal')`.
- (D) You cannot tell them apart in SQL.

## Question 8 — The NOT IN trap

`WHERE customer_key NOT IN (SELECT customer_key FROM fact_sales)` returns zero rows even though many customers clearly have no sales. The most likely cause is:

- (A) `fact_sales` is empty.
- (B) The subquery returns at least one `NULL` `customer_key`, so `NOT IN` evaluates to `NULL` (not `TRUE`) for every row and excludes everything.
- (C) `NOT IN` is not valid SQL in PostgreSQL.
- (D) `customer_key` needs an index for `NOT IN` to work.

## Question 9 — Reading a plan

In `EXPLAIN ANALYZE` output you see `Seq Scan on fact_sales` with `rows=137` (estimated) but `Rows Removed by Filter: 4999863`. What does this tell you?

- (A) The estimate was wrong by a factor of a million.
- (B) The database read ~5,000,000 rows to return 137; the filter column likely lacks a useful index.
- (C) The query has a syntax error in the filter.
- (D) The table needs to be vacuumed.

## Question 10 — QUALIFY availability and join algorithms

Which statement is correct?

- (A) `QUALIFY` is standard SQL and works in PostgreSQL, DuckDB, Snowflake, and BigQuery.
- (B) `QUALIFY` works in DuckDB, Snowflake, and BigQuery but **not** PostgreSQL; the Postgres equivalent wraps the window in a CTE/subquery and filters on its alias. A `Hash Join` builds a hash table on the smaller input and probes with the larger; a `Nested Loop` is cheap only when the outer side is tiny.
- (C) `QUALIFY` works everywhere; a `Nested Loop` is always faster than a `Hash Join`.
- (D) `QUALIFY` is PostgreSQL-only; a `Merge Join` requires no sorting.

---

## Answer key

**Q1 — (B).** A window function decorates every input row with an aggregate computed over its window, while `GROUP BY` reduces rows to one per group. (A) is false — window functions support the full aggregate family. (C) is backwards: window functions run *after* `WHERE`/`GROUP BY`/`HAVING`. See <https://www.postgresql.org/docs/16/tutorial-window.html>.

**Q2 — (C).** With an `ORDER BY` and no explicit frame, PostgreSQL applies `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` — a running total since the start of the partition, with tied `ORDER BY` values lumped together. This is the source of the "I wanted a trailing-7 and got a since-start total" surprise. See <https://www.postgresql.org/docs/16/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS>.

**Q3 — (C).** `DENSE_RANK` ties and does not skip → 1, 1, 2. `RANK` ties and skips → 1, 1, 3. `ROW_NUMBER` never ties → 1, 2, 3. See <https://www.postgresql.org/docs/16/functions-window.html>.

**Q4 — (C).** `LAG` on the first row has no predecessor and returns `NULL`, unless you pass a third default argument like `LAG(revenue, 1, 0)`. Handle the boundary `NULL` deliberately. See <https://www.postgresql.org/docs/16/functions-window.html>.

**Q5 — (B).** Anchor + `UNION ALL` + recursive term referencing the CTE name, iterating until no new rows. Termination is by the recursive term producing nothing (guard it, and use `CYCLE` if cycles are possible). See <https://www.postgresql.org/docs/16/queries-with.html>.

**Q6 — (B).** `ROLLUP(a, b)` gives the *hierarchical* subtotals `(a,b), (a), ()`. Answer (A) is `CUBE(a, b)` (every combination). See <https://www.postgresql.org/docs/16/queries-table-expressions.html#QUERIES-GROUPING-SETS>.

**Q7 — (B).** `GROUPING(category)` returns `1` when the column was rolled up in that row and `0` when it holds a real value, which is the only reliable way to distinguish a subtotal `NULL` from a data `NULL`. See <https://www.postgresql.org/docs/16/functions-aggregate.html#FUNCTIONS-GROUPING-TABLE>.

**Q8 — (B).** A single `NULL` in the `NOT IN` subquery makes `x <> NULL` evaluate to `NULL`; `TRUE AND ... AND NULL` is `NULL`, never `TRUE`, so every row is excluded. Use `NOT EXISTS` instead. See <https://www.postgresql.org/docs/16/functions-subquery.html>.

**Q9 — (B).** The database scanned the whole table and discarded all but 137 rows because there was no index on the filter column; the *estimate* (137) was actually accurate. (A) is wrong — the estimate matched the output. The fix is an index on the filtered column. See <https://www.postgresql.org/docs/16/using-explain.html>.

**Q10 — (B).** `QUALIFY` is a DuckDB / Snowflake / BigQuery feature, absent from PostgreSQL, where you wrap the window in a CTE and filter on its alias. A `Hash Join` builds on the smaller input and probes with the larger (linear); a `Nested Loop` is cheap only with a tiny outer side and an indexed inner side. See <https://duckdb.org/docs/sql/query_syntax/qualify> and <https://www.postgresql.org/docs/16/using-explain.html>.

## Self-assessment

- **9–10 correct:** You can write analytical SQL and read a plan with confidence. Go straight to the gauntlet and tune the hard ones.
- **7–8 correct:** Solid. Re-read the lecture section behind each miss — most misses here are the default frame (Q2) or the `NOT IN` trap (Q8), both of which cause silent wrong answers in production.
- **5–6 correct:** Re-read Lecture 1 (frames, ranking) and Lecture 2 (grouping sets, anti-joins) before the challenges; redo Exercises 1 and 2.
- **Below 5:** Work through all three lectures again with `psql` open, running every example against the schema. The concepts only stick when you watch the rows come back.
