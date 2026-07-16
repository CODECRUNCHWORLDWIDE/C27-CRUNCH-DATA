-- =====================================================================
-- C27 · Crunch Data — Week 1 · Exercise 01: Model the Grain
-- =====================================================================
-- ENGINE:  PostgreSQL 16 (run in Docker; see lecture 1 § 4)
--   docker run --name cc-pg-w1 -e POSTGRES_PASSWORD=crunch \
--     -e POSTGRES_DB=retail -p 5432:5432 -d postgres:16
--   docker exec -it cc-pg-w1 psql -U postgres -d retail
--   then:  \i exercise-01-model-the-grain.sql
--
-- TASK
--   A retailer wants a warehouse to answer questions like:
--     - "Weekly revenue by product category, by region, last quarter."
--     - "Units sold per product per store per day."
--     - "Average basket size (lines per order)."
--   The atomic source is a sales-order LINE: each order has one or more
--   lines, each line is one product at a quantity and a unit price.
--
--   Your job in THIS exercise is the single most important modeling
--   decision: declare and DEFEND the grain of the sales fact, then write
--   the CREATE TABLE for fact_sales at that grain.
--
-- ACCEPTANCE CRITERIA
--   [1] You write the grain as ONE sentence, in a comment, with NO "and".
--   [2] fact_sales is at the FINEST grain the source supports (line level).
--   [3] The fact stores SURROGATE-KEY foreign keys to the dimensions,
--       not natural keys.
--   [4] The fact carries the order number as a DEGENERATE dimension
--       (a column on the fact, no separate table).
--   [5] Additive measures (quantity, extended_amount) are stored;
--       the non-additive unit_price is stored but commented as non-additive.
--   [6] A UNIQUE constraint enforces "one row per real order line" so a
--       re-run cannot silently double-count.
-- =====================================================================

-- Clean slate so this file is re-runnable:
DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;

-- ---------------------------------------------------------------------
-- Minimal dimension stubs so the fact's foreign keys resolve. You will
-- flesh these out in Exercise 02; here they only need their surrogate
-- primary keys. These are CORRECT scaffolding — do not change them.
-- ---------------------------------------------------------------------
CREATE TABLE dim_date (
    date_key   int  PRIMARY KEY,
    full_date  date NOT NULL UNIQUE
);

CREATE TABLE dim_product (
    product_key  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku          text   NOT NULL
);

CREATE TABLE dim_store (
    store_key   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_code  text   NOT NULL UNIQUE
);

CREATE TABLE dim_customer (
    customer_key  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_code text   NOT NULL UNIQUE
);

-- =====================================================================
-- YOUR ANSWER (1): Write the grain sentence here, one line, no "and".
--
--   GRAIN: <write it here>
--
-- Hint: the right answer is at the line-item level. State what exactly
-- one row of fact_sales represents.
-- =====================================================================

-- =====================================================================
-- YOUR ANSWER (2): Write the CREATE TABLE for fact_sales at the
-- line-item grain. It must satisfy acceptance criteria [2]-[6].
--
-- Required columns (you decide types, constraints, and order):
--   sale_key         surrogate PK (GENERATED ALWAYS AS IDENTITY)
--   date_key         FK -> dim_date(date_key)
--   product_key      FK -> dim_product(product_key)
--   store_key        FK -> dim_store(store_key)
--   customer_key     FK -> dim_customer(customer_key)
--   order_number     degenerate dimension (text, on the fact)
--   order_line_no    int
--   quantity         additive measure, must be > 0
--   unit_price       NON-additive measure (do not SUM it)
--   extended_amount  additive measure (quantity * unit_price)
-- Plus: a UNIQUE constraint on (order_number, order_line_no).
-- =====================================================================

-- CREATE TABLE fact_sales (
--     ... YOUR ANSWER HERE ...
-- );

-- =====================================================================
-- YOUR ANSWER (3): In a comment, answer the defense question:
--   Why the LINE grain and not the ORDER grain? Name one question the
--   line grain can answer that the order grain cannot, and one risk the
--   order grain avoids that the line grain must guard against.
-- =====================================================================

-- ---------------------------------------------------------------------
-- SELF-CHECK (uncomment after writing fact_sales). Should print the
-- column list of your fact table so you can eyeball the grain + keys.
-- ---------------------------------------------------------------------
-- \d fact_sales
