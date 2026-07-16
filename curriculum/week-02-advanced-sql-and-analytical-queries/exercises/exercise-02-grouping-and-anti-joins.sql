-- =====================================================================
-- C27 · Crunch Data — Week 2 — Exercise 02: Grouping Sets & Anti-Joins
-- =====================================================================
-- ENGINE: PostgreSQL 16  (GROUPING SETS and the anti-joins also run on DuckDB)
-- DATASET: the Week-1 retail star schema (see exercise-01 header for columns).
--
-- HOW TO RUN:
--   docker exec -it crunch-pg psql -U postgres -d crunch -f exercise-02-grouping-and-anti-joins.sql
--
-- CONVENTION: fill in each "-- YOUR ANSWER:" slot. Read Lecture 2 first.
-- Full solutions with sample output are in SOLUTIONS.md.
-- =====================================================================


-- ---------------------------------------------------------------------
-- TASK 1 — One-pass subtotal report with GROUPING SETS + GROUPING().
-- ---------------------------------------------------------------------
-- Business question: "Revenue per region per category, PLUS a subtotal per
-- region, PLUS a grand total — all in one result set — with the subtotal
-- and total rows clearly labelled (not ambiguous NULLs)."
--
-- ACCEPTANCE CRITERIA:
--   * One query, one pass over fact_sales (no UNION of three queries).
--   * Grouping sets: (region, category), (region), ().
--   * Subtotal rows show 'ALL CATEGORIES'; the grand total shows
--     'ALL REGIONS' / 'ALL CATEGORIES'. Use GROUPING() to detect subtotals.
--   * Order so subtotal/total rows sort to the bottom of their group.
--
-- SCAFFOLD:
SELECT
    -- YOUR ANSWER: use CASE WHEN GROUPING(s.region)=1 THEN 'ALL REGIONS' ELSE s.region END
    s.region   AS region,
    -- YOUR ANSWER: same idea for category with 'ALL CATEGORIES'
    p.category AS category,
    SUM(f.extended_price) AS revenue
FROM   fact_sales f
JOIN   dim_store   s ON s.store_key   = f.store_key
JOIN   dim_product p ON p.product_key = f.product_key
-- YOUR ANSWER: replace this GROUP BY with GROUPING SETS ((region,category),(region),())
--   (or equivalently ROLLUP(s.region, p.category)).
GROUP  BY s.region, p.category
-- YOUR ANSWER: order by GROUPING(s.region), s.region, GROUPING(p.category), p.category
ORDER  BY s.region, p.category;


-- ---------------------------------------------------------------------
-- TASK 2 — Customers who have NEVER ordered (anti-join). Two correct forms.
-- ---------------------------------------------------------------------
-- Business question: "List every customer who has no row in fact_sales."
--
-- ACCEPTANCE CRITERIA:
--   * Columns: customer_key, customer_name.
--   * Form A: NOT EXISTS. Form B: LEFT JOIN ... WHERE fact key IS NULL.
--   * Both forms must return IDENTICAL rows.
--   * Then write the NOT IN form and OBSERVE the trap (see TASK 2c).
--
-- Form A — NOT EXISTS:
SELECT c.customer_key, c.customer_name
FROM   dim_customer c
-- YOUR ANSWER: WHERE NOT EXISTS (SELECT 1 FROM fact_sales f WHERE f.customer_key = c.customer_key)
;

-- Form B — LEFT JOIN ... IS NULL:
SELECT c.customer_key, c.customer_name
FROM   dim_customer c
-- YOUR ANSWER: LEFT JOIN fact_sales f ON f.customer_key = c.customer_key
-- YOUR ANSWER: WHERE f.customer_key IS NULL
;

-- TASK 2c — Feel the NOT IN trap ON PURPOSE.
-- Run this AS-IS. Then run it after the INSERT below that puts a NULL
-- customer_key into fact_sales (a realistic anonymous/guest sale).
-- Predict the row count BEFORE you run it the second time, then explain.
--
--   -- (in a scratch DB only) INSERT INTO fact_sales(customer_key) VALUES (NULL);
SELECT c.customer_key, c.customer_name
FROM   dim_customer c
WHERE  c.customer_key NOT IN (SELECT customer_key FROM fact_sales);
-- EXPECTATION: with a NULL in the subquery this returns ZERO rows for
-- everyone. Explain why in a comment here. (See Lecture 2 sec. 8.)
-- YOUR ANSWER (explanation):
--   ...


-- ---------------------------------------------------------------------
-- TASK 3 — Products that have sold in EVERY store (relational division).
-- ---------------------------------------------------------------------
-- Business question: "Which products have at least one sale in every single
-- store in dim_store?"
--
-- ACCEPTANCE CRITERIA:
--   * Columns: product_key (and product_name in the count form).
--   * Form A: double NOT EXISTS ("no store where this product did NOT sell").
--   * Form B: COUNT(DISTINCT store_key) = (SELECT COUNT(*) FROM dim_store).
--   * Both forms must return IDENTICAL product_keys.
--
-- Form A — double NOT EXISTS:
SELECT p.product_key, p.product_name
FROM   dim_product p
-- YOUR ANSWER:
-- WHERE NOT EXISTS (
--   SELECT 1 FROM dim_store s
--   WHERE NOT EXISTS (
--     SELECT 1 FROM fact_sales f
--     WHERE f.product_key = p.product_key AND f.store_key = s.store_key
--   )
-- )
;

-- Form B — COUNT(DISTINCT) = total stores:
SELECT f.product_key
FROM   fact_sales f
GROUP  BY f.product_key
-- YOUR ANSWER: HAVING COUNT(DISTINCT f.store_key) = (SELECT COUNT(*) FROM dim_store)
;

-- =====================================================================
-- STRETCH (optional, not graded):
--   * Rewrite TASK 1 with CUBE(s.region, p.category) and explain how many
--     more rows you get and why (region-only subtotals appear too).
--   * Compare the EXPLAIN ANALYZE of TASK 3 Form A vs Form B (Lecture 3).
-- =====================================================================
