# Week 2 — Resources

Real references only, grouped and annotated. Read the PostgreSQL docs alongside the lectures; they are precise, versioned (these are the **PostgreSQL 16** pages), and the canonical source of truth. Everything here is free and open.

## PostgreSQL 16 — window functions

- **Window Functions tutorial** — <https://www.postgresql.org/docs/16/tutorial-window.html>
  The gentle, example-led introduction. Read it first; it is where the "keep the detail, get the aggregate next to it" mental model comes from.
- **Window Functions reference** — <https://www.postgresql.org/docs/16/functions-window.html>
  The precise list of every built-in window function: `row_number`, `rank`, `dense_rank`, `lag`, `lead`, `first_value`, `last_value`, `nth_value`, `ntile`, `percent_rank`, `cume_dist`. The contract for ties and offsets lives here.
- **Window-function call syntax (frames)** — <https://www.postgresql.org/docs/16/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS>
  The full `ROWS` / `RANGE` / `GROUPS` frame grammar, the default frame, and the `BETWEEN ... AND ...` boundaries. This is the page that explains why the default frame surprises people.
- **`SELECT` — the `WINDOW` clause** — <https://www.postgresql.org/docs/16/sql-select.html#SQL-WINDOW>
  How to name a window once and reuse it across several functions.

## PostgreSQL 16 — CTEs and grouping sets

- **`WITH` Queries (CTEs and recursive CTEs)** — <https://www.postgresql.org/docs/16/queries-with.html>
  Common Table Expressions, `MATERIALIZED` / `NOT MATERIALIZED`, the recursive `WITH RECURSIVE` form (anchor + `UNION ALL` + recursive term), and the `CYCLE` clause for cycle detection.
- **`GROUPING SETS`, `ROLLUP`, `CUBE`** — <https://www.postgresql.org/docs/16/queries-table-expressions.html#QUERIES-GROUPING-SETS>
  Multiple groupings in one pass; the subtotal `NULL`; the relationship between `ROLLUP`/`CUBE` and explicit grouping sets.
- **The `GROUPING()` function** — <https://www.postgresql.org/docs/16/functions-aggregate.html#FUNCTIONS-GROUPING-TABLE>
  The function that distinguishes a subtotal `NULL` from a data `NULL`.
- **Subquery expressions (`EXISTS`, `IN`, `NOT IN`)** — <https://www.postgresql.org/docs/16/functions-subquery.html>
  The semantics of semi-joins and the `NOT IN`-with-`NULL` three-valued-logic trap.
- **Set-returning functions (`generate_series`)** — <https://www.postgresql.org/docs/16/functions-srf.html>
  The simpler alternative to a recursive date spine.
- **Ordered-set aggregates (`percentile_cont`, `percentile_disc`)** — <https://www.postgresql.org/docs/16/functions-aggregate.html#FUNCTIONS-ORDEREDSET-TABLE>
  For the percentile question in the gauntlet.

## PostgreSQL 16 — plans, indexes, and tuning

- **`EXPLAIN` reference** — <https://www.postgresql.org/docs/16/sql-explain.html>
  The command itself: `ANALYZE`, `BUFFERS`, `VERBOSE`, `FORMAT`, and what each option adds.
- **Using `EXPLAIN`** — <https://www.postgresql.org/docs/16/using-explain.html>
  The walkthrough. Read this slowly. It explains cost units, estimated vs actual rows, the node types, and how to read the tree. The single most useful page in the week for plan-reading.
- **Indexes** — <https://www.postgresql.org/docs/16/indexes.html>
  B-tree (the default), when an index helps and when a seq scan is correct, and the cost an index adds to writes.
- **Multicolumn indexes** — <https://www.postgresql.org/docs/16/indexes-multicolumn.html>
  Composite index column order and why the leading column rules.
- **Index-only scans and covering indexes** — <https://www.postgresql.org/docs/16/indexes-index-only-scans.html>
  How `INCLUDE` lets a query be answered from the index without touching the heap.
- **`CREATE STATISTICS`** — <https://www.postgresql.org/docs/16/sql-createstatistics.html>
  Extended statistics for correlated columns, to fix the cardinality miss that makes a join explode.

## DuckDB

- **DuckDB documentation (root)** — <https://duckdb.org/docs/>
  The in-process columnar analytical database used this week for plan comparison and `QUALIFY`. Single binary, no server.
- **`QUALIFY` clause** — <https://duckdb.org/docs/sql/query_syntax/qualify>
  The clause that filters on a window-function result. Works in DuckDB, Snowflake, and BigQuery; **not** PostgreSQL.
- **Window functions in DuckDB** — <https://duckdb.org/docs/sql/window_functions>
  Confirms the `OVER` / `PARTITION BY` / frame syntax matches PostgreSQL, so your window queries port unchanged.
- **`EXPLAIN` / `EXPLAIN ANALYZE` in DuckDB** — <https://duckdb.org/docs/guides/meta/explain>
  How to read a DuckDB plan and see the columnar projection (which columns the scan actually reads).
- **Installing DuckDB** — <https://duckdb.org/docs/installation/>
  Grab the CLI binary for your platform.

## Book — storage and index internals

- **Martin Kleppmann, *Designing Data-Intensive Applications*** — <https://dataintensive.net/>
  Chapter 3 ("Storage and Retrieval") is the canonical explanation of B-tree vs log-structured storage and *why* an index gives log-time lookups; the column-oriented-storage section explains why DuckDB reads fewer bytes for an analytical aggregate. The reference behind every "why is this fast/slow" claim in Lecture 3.

## Infrastructure

- **Docker official Postgres image** — <https://hub.docker.com/_/postgres>
  `docker run --name crunch-pg -e POSTGRES_PASSWORD=crunch -p 5432:5432 -d postgres:16`. The image, its environment variables, and the `docker-entrypoint-initdb.d` auto-load convention the mini-project uses.

## How to use these this week

Keep three tabs open while you read the lectures: the **window-functions reference**, the **"Using EXPLAIN"** walkthrough, and the **DuckDB `QUALIFY`** page. The lectures give you the path; these pages give you the precise contract. When a query surprises you, the answer is almost always on one of these pages — read it before you guess.
