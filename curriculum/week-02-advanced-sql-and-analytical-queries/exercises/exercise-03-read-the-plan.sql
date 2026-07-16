-- =====================================================================
-- C27 · Crunch Data — Week 2 — Exercise 03: Read the Plan
-- =====================================================================
-- ENGINE: PostgreSQL 16
-- DATASET: the Week-1 retail star schema. This exercise is most instructive
--   on a LARGE fact_sales (the mini-project loads ~5M rows; >=100k shows it).
--
-- GOAL: run a deliberately slow query, READ its EXPLAIN ANALYZE plan, find
--   the sequential scan that throws away almost everything it reads, add the
--   correct index, and PROVE the speedup by re-measuring.
--
-- HOW TO RUN: paste each step into psql in order. Capture the plan TEXT from
--   STEP 2 and STEP 5 into your write-up.
-- Full solution with before/after plan excerpts is in SOLUTIONS.md.
-- =====================================================================


-- ---------------------------------------------------------------------
-- STEP 0 — Make sure statistics are fresh, so the planner is not blind.
-- ---------------------------------------------------------------------
ANALYZE fact_sales;
ANALYZE dim_customer;


-- ---------------------------------------------------------------------
-- STEP 1 — The slow query.
-- ---------------------------------------------------------------------
-- Business question: "Total revenue and order count for one specific
-- customer (customer_key = 80421)."
-- This is an OLTP-shaped lookup: a handful of rows out of millions.
-- Run it once to feel the latency (note psql's "Time:" line):
SELECT f.customer_key,
       COUNT(*)              AS order_lines,
       SUM(f.extended_price) AS lifetime_revenue
FROM   fact_sales f
WHERE  f.customer_key = 80421
GROUP  BY f.customer_key;


-- ---------------------------------------------------------------------
-- STEP 2 — Read the plan. CAPTURE THIS OUTPUT.
-- ---------------------------------------------------------------------
EXPLAIN (ANALYZE, BUFFERS)
SELECT f.customer_key,
       COUNT(*)              AS order_lines,
       SUM(f.extended_price) AS lifetime_revenue
FROM   fact_sales f
WHERE  f.customer_key = 80421
GROUP  BY f.customer_key;
--
-- READ IT: you should see a "Seq Scan on fact_sales" with a line like
--   "Rows Removed by Filter: <a huge number>"
-- and "Buffers: shared read=<large>". That is the bottleneck: the database
-- read the WHOLE table to return a few rows because customer_key is not
-- indexed.
--
-- YOUR ANSWER (diagnosis): write 2-3 sentences naming the node, the
-- estimated vs actual rows, the "Rows Removed by Filter" count, and why this
-- is the wrong access path for this query.
--   ...


-- ---------------------------------------------------------------------
-- STEP 3 — Fix it. Add the right index.
-- ---------------------------------------------------------------------
-- YOUR ANSWER: create a B-tree index on the column in the WHERE clause.
-- CREATE INDEX idx_fact_customer ON fact_sales (customer_key);


-- ---------------------------------------------------------------------
-- STEP 4 — Refresh statistics so the planner notices the new index.
-- ---------------------------------------------------------------------
ANALYZE fact_sales;


-- ---------------------------------------------------------------------
-- STEP 5 — Re-measure. CAPTURE THIS OUTPUT and compare to STEP 2.
-- ---------------------------------------------------------------------
EXPLAIN (ANALYZE, BUFFERS)
SELECT f.customer_key,
       COUNT(*)              AS order_lines,
       SUM(f.extended_price) AS lifetime_revenue
FROM   fact_sales f
WHERE  f.customer_key = 80421
GROUP  BY f.customer_key;
--
-- EXPECTATION: the Seq Scan becomes an Index Scan or Bitmap Heap Scan on
-- idx_fact_customer; "Buffers: shared read" collapses to a small "shared
-- hit"; actual time drops by orders of magnitude.
--
-- YOUR ANSWER (result): record the BEFORE and AFTER actual total time and
-- the BEFORE/AFTER buffer counts. State the speedup factor.
--   BEFORE: time = ____ ms, shared read = ____
--   AFTER:  time = ____ ms, shared hit  = ____
--   SPEEDUP: ____x


-- ---------------------------------------------------------------------
-- STEP 6 — Sanity check: when is a Seq Scan CORRECT?
-- ---------------------------------------------------------------------
-- Run the plan for an aggregate over the WHOLE table. You should see the
-- planner CHOOSE a Seq Scan even though idx_fact_customer now exists,
-- because this query needs every row. Confirm and explain in a comment.
EXPLAIN (ANALYZE, BUFFERS)
SELECT SUM(extended_price) FROM fact_sales;
-- YOUR ANSWER (why is the Seq Scan correct here?):
--   ...

-- =====================================================================
-- STRETCH (optional, not graded):
--   * Build a covering index so a customer-revenue lookup never touches the
--     heap:  CREATE INDEX ... ON fact_sales (customer_key) INCLUDE (extended_price);
--     and confirm the plan becomes an "Index Only Scan".
--   * Drop idx_fact_customer when done if this is a shared scratch DB.
-- =====================================================================
