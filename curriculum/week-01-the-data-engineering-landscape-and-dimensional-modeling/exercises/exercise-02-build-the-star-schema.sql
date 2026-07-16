-- =====================================================================
-- C27 · Crunch Data — Week 1 · Exercise 02: Build the Star Schema
-- =====================================================================
-- ENGINE:  PostgreSQL 16 (run in Docker; see lecture 1 § 4)
--   docker exec -it cc-pg-w1 psql -U postgres -d retail
--   then:  \i exercise-02-build-the-star-schema.sql
--
-- TASK
--   Build the full retail star: the four conformed dimensions
--   (dim_date, dim_product, dim_store, dim_customer) and fact_sales at
--   the line-item grain. Then load a small, hand-written sample dataset
--   and answer one analytical question that joins the fact to ALL FOUR
--   dimensions.
--
-- ACCEPTANCE CRITERIA
--   [1] All four dimensions use surrogate keys (GENERATED ALWAYS AS
--       IDENTITY), except dim_date which uses a smart YYYYMMDD int key.
--   [2] Dimensions are DENORMALIZED (star): category_name, region, etc.
--       are TEXT columns, not foreign keys to sub-dimension tables.
--   [3] fact_sales carries its grain in a comment and references all
--       four dimensions by surrogate key.
--   [4] You load at least 3 dates, 3 products, 2 stores, 3 customers,
--       and at least 6 fact rows spanning multiple orders.
--   [5] Your final analytical query returns weekly revenue by category
--       and region and runs without error.
-- =====================================================================

DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;

-- ---------------------------------------------------------------------
-- dim_date is fully provided — it is mechanical and you should not spend
-- time on it. Smart integer key (YYYYMMDD). This is CORRECT scaffolding.
-- ---------------------------------------------------------------------
CREATE TABLE dim_date (
    date_key      int  PRIMARY KEY,
    full_date     date NOT NULL UNIQUE,
    day_of_week   text NOT NULL,
    day_of_month  int  NOT NULL,
    week_of_year  int  NOT NULL,
    month_num     int  NOT NULL,
    month_name    text NOT NULL,
    quarter       int  NOT NULL,
    year          int  NOT NULL,
    is_weekend    boolean NOT NULL
);

-- Generate a full year of 2026 dates from a single statement:
INSERT INTO dim_date
SELECT  (EXTRACT(YEAR FROM d)*10000
         + EXTRACT(MONTH FROM d)*100
         + EXTRACT(DAY FROM d))::int      AS date_key,
        d                                  AS full_date,
        to_char(d, 'FMDay')               AS day_of_week,
        EXTRACT(DAY  FROM d)::int          AS day_of_month,
        EXTRACT(WEEK FROM d)::int          AS week_of_year,
        EXTRACT(MONTH FROM d)::int         AS month_num,
        to_char(d, 'FMMonth')             AS month_name,
        EXTRACT(QUARTER FROM d)::int       AS quarter,
        EXTRACT(YEAR FROM d)::int          AS year,
        EXTRACT(ISODOW FROM d) IN (6,7)    AS is_weekend
FROM generate_series(DATE '2026-01-01', DATE '2026-12-31', INTERVAL '1 day') AS d;

-- =====================================================================
-- YOUR ANSWER (1): Write CREATE TABLE dim_product (the STAR version).
--   Surrogate key product_key; natural key sku; plus product_name,
--   category_name (TEXT, denormalized), brand_name (TEXT, denormalized).
-- =====================================================================
-- CREATE TABLE dim_product ( ... YOUR ANSWER ... );

-- =====================================================================
-- YOUR ANSWER (2): Write CREATE TABLE dim_store.
--   Surrogate key store_key; natural key store_code; store_name, city,
--   region (TEXT), country (TEXT).
-- =====================================================================
-- CREATE TABLE dim_store ( ... YOUR ANSWER ... );

-- =====================================================================
-- YOUR ANSWER (3): Write CREATE TABLE dim_customer.
--   Surrogate key customer_key; natural key customer_code; full_name,
--   email, city, loyalty_tier (TEXT).
-- =====================================================================
-- CREATE TABLE dim_customer ( ... YOUR ANSWER ... );

-- =====================================================================
-- YOUR ANSWER (4): Write CREATE TABLE fact_sales at the line grain.
--   Put the grain sentence in a comment. FK to all four dimensions by
--   surrogate key; order_number (degenerate); order_line_no; quantity;
--   unit_price (non-additive); extended_amount (additive). UNIQUE
--   (order_number, order_line_no). Add an index per dimension key.
-- =====================================================================
-- GRAIN: ...
-- CREATE TABLE fact_sales ( ... YOUR ANSWER ... );

-- =====================================================================
-- YOUR ANSWER (5): Load sample rows.
--   Insert >=3 products, 2 stores, 3 customers. Then insert >=6
--   fact rows. NOTE: you must look up the surrogate keys you just
--   generated — use a subquery or a CTE; do NOT hardcode the keys,
--   because IDENTITY assigns them.
--
-- Example shape for ONE fact row (uncomment & adapt once tables exist):
--
-- INSERT INTO fact_sales
--   (date_key, product_key, store_key, customer_key,
--    order_number, order_line_no, quantity, unit_price, extended_amount)
-- VALUES (
--   20260612,
--   (SELECT product_key  FROM dim_product  WHERE sku = 'SKU-0001'),
--   (SELECT store_key    FROM dim_store    WHERE store_code = 'STR-MIA'),
--   (SELECT customer_key FROM dim_customer WHERE customer_code = 'CUST-1001'),
--   'ORD-5000', 1, 2, 9.50, 19.00
-- );
-- =====================================================================

-- =====================================================================
-- YOUR ANSWER (6): The analytical query.
--   Weekly revenue by category_name and region for 2026 Q2.
--   Join fact_sales to all four... (you only NEED date, product, store
--   for this one, but write it as a clean star query). GROUP BY week,
--   category, region; ORDER BY revenue DESC. Use SUM(extended_amount).
-- =====================================================================
-- SELECT ... YOUR ANSWER ...;
