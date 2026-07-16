-- =====================================================================
-- C27 · Crunch Data — Week 2 — Exercise 01: Window Functions
-- =====================================================================
-- ENGINE: PostgreSQL 16  (every query here also runs on DuckDB 1.x)
-- DATASET: the Week-1 retail star schema
--   dim_date(date_key, full_date, year, month, day_of_week, ...)
--   dim_product(product_key, product_name, category, brand, unit_cost, ...)
--   dim_store(store_key, store_name, region, ...)
--   dim_customer(customer_key, customer_name, segment, signup_date, ...)
--   fact_sales(sales_key, date_key, product_key, store_key, customer_key,
--              quantity, unit_price, extended_price, discount_amount)
--
-- HOW TO RUN:
--   docker exec -it crunch-pg psql -U postgres -d crunch -f exercise-01-window-functions.sql
--   or paste each task into psql / DBeaver / pgcli.
--
-- CONVENTION: fill in each "-- YOUR ANSWER:" slot. Read Lecture 1 first.
-- Full solutions with sample output are in SOLUTIONS.md.
-- =====================================================================


-- ---------------------------------------------------------------------
-- TASK 1 — Rank products by revenue WITHIN each category.
-- ---------------------------------------------------------------------
-- Business question: "For each category, list its products from highest to
-- lowest revenue, and show the rank of each within its category."
--
-- ACCEPTANCE CRITERIA:
--   * One row per product (detail preserved; do NOT collapse with GROUP BY only).
--   * Columns: category, product_name, revenue, category_rank.
--   * Revenue = SUM(extended_price) for that product.
--   * Ranking RESTARTS at each category (PARTITION BY category).
--   * Use DENSE_RANK so that two products tied on revenue share a rank and
--     the next rank is not skipped. (Then read SOLUTIONS.md for when you'd
--     instead want ROW_NUMBER or RANK.)
--
-- SCAFFOLD:
WITH product_revenue AS (
    SELECT p.category,
           p.product_name,
           SUM(f.extended_price) AS revenue
    FROM   fact_sales f
    JOIN   dim_product p ON p.product_key = f.product_key
    GROUP  BY p.category, p.product_name
)
-- YOUR ANSWER: select from product_revenue and add the DENSE_RANK window.
SELECT category,
       product_name,
       revenue
       -- , DENSE_RANK() OVER ( ... )  AS category_rank
FROM   product_revenue
ORDER  BY category, revenue DESC;


-- ---------------------------------------------------------------------
-- TASK 2 — 7-day running sales total per store, with an explicit frame.
-- ---------------------------------------------------------------------
-- Business question: "For each store, on each calendar day it had sales,
-- show that day's revenue and the trailing 7-day running total."
--
-- ACCEPTANCE CRITERIA:
--   * One row per (store, day-with-sales).
--   * Columns: store_name, full_date, daily_revenue, revenue_7day.
--   * revenue_7day must use an EXPLICIT frame of the current row plus the
--     6 physical rows before it: ROWS BETWEEN 6 PRECEDING AND CURRENT ROW.
--     (Do NOT rely on the default RANGE frame — read Lecture 1 sec. 6.)
--   * PARTITION BY store, ORDER BY date inside the window.
--
-- SCAFFOLD:
WITH daily AS (
    SELECT f.store_key,
           f.date_key,
           SUM(f.extended_price) AS daily_revenue
    FROM   fact_sales f
    GROUP  BY f.store_key, f.date_key
)
SELECT s.store_name,
       d.full_date,
       daily.daily_revenue
       -- YOUR ANSWER: add the 7-day running total window with an explicit ROWS frame.
       -- , SUM(daily.daily_revenue) OVER ( ... ) AS revenue_7day
FROM   daily
JOIN   dim_store s ON s.store_key = daily.store_key
JOIN   dim_date  d ON d.date_key  = daily.date_key
ORDER  BY s.store_name, d.full_date;


-- ---------------------------------------------------------------------
-- TASK 3 — Month-over-month revenue change with LAG.
-- ---------------------------------------------------------------------
-- Business question: "Show total revenue per calendar month, the previous
-- month's revenue, the absolute change, and the percent change."
--
-- ACCEPTANCE CRITERIA:
--   * One row per (year, month), ordered chronologically.
--   * Columns: year, month, revenue, prev_month_revenue, mom_change, mom_pct.
--   * prev_month_revenue uses LAG over (ORDER BY year, month).
--   * The FIRST month's prev/change/pct are NULL (no predecessor) — that is
--     correct, do not fake a zero.
--   * mom_pct must guard against divide-by-zero with NULLIF.
--
-- SCAFFOLD:
WITH monthly AS (
    SELECT d.year,
           d.month,
           SUM(f.extended_price) AS revenue
    FROM   fact_sales f
    JOIN   dim_date d ON d.date_key = f.date_key
    GROUP  BY d.year, d.month
)
SELECT year,
       month,
       revenue
       -- YOUR ANSWER: add prev_month_revenue (LAG), mom_change, and mom_pct.
       -- , LAG(revenue) OVER (ORDER BY year, month) AS prev_month_revenue
       -- , revenue - LAG(revenue) OVER (ORDER BY year, month) AS mom_change
       -- , ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY year, month))
       --         / NULLIF(LAG(revenue) OVER (ORDER BY year, month), 0), 1) AS mom_pct
FROM   monthly
ORDER  BY year, month;

-- =====================================================================
-- STRETCH (optional, not graded):
--   * Redo TASK 1 to keep only the #1 product per category. In PostgreSQL
--     wrap the window in a CTE and filter WHERE rn = 1. In DuckDB you may
--     instead use QUALIFY ROW_NUMBER() OVER (...) = 1. Confirm same result.
--   * In TASK 2, change ROWS to a centered 3-day average:
--     ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING, and explain the difference.
-- =====================================================================
