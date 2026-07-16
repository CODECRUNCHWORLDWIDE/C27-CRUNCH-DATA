-- =====================================================================
-- C27 · Crunch Data — Week 1 · Exercise 03: Type-2 Slowly-Changing Dim
-- =====================================================================
-- ENGINE:  PostgreSQL 16 (run in Docker; see lecture 1 § 4)
--   docker exec -it cc-pg-w1 psql -U postgres -d retail
--   then:  \i exercise-03-type2-scd.sql
--
-- PRECONDITION
--   Run Exercise 02 first (or this file's bootstrap below) so dim_product
--   exists and is loaded. This exercise turns dim_product into a Type-2
--   SCD, processes a re-categorization, and audits it.
--
-- TASK
--   1. Add Type-2 control columns to dim_product.
--   2. Seed two products.
--   3. Process a staged change: SKU-0001 is re-categorized on 2026-06-19.
--      Close the old version, open the new, in ONE transaction.
--   4. Write a point-in-time audit: state of all products on 2026-06-15.
--   5. Write the well-formedness check (exactly one current row per sku).
--
-- ACCEPTANCE CRITERIA
--   [1] valid_from / valid_to / is_current columns exist with sane
--       defaults (valid_to sentinel 9999-12-31, is_current default true).
--   [2] The change is applied with a close-then-open pair in ONE
--       transaction; the close uses MERGE with IS DISTINCT FROM.
--   [3] After the change, SKU-0001 has exactly TWO rows: one closed
--       (is_current=false) and one open (is_current=true) with a fresh
--       surrogate product_key.
--   [4] The point-in-time query for 2026-06-15 returns the OLD category
--       for SKU-0001 (the change is effective 2026-06-19).
--   [5] The well-formedness check returns ZERO rows.
-- =====================================================================

-- ---------- bootstrap (safe to keep; rebuilds a clean dim_product) ----
DROP TABLE IF EXISTS stg_product CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;

CREATE TABLE dim_product (
    product_key   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku           text   NOT NULL,
    product_name  text   NOT NULL,
    category_name text   NOT NULL,
    brand_name    text   NOT NULL
);

-- seed two products as their initial (current) versions:
INSERT INTO dim_product (sku, product_name, category_name, brand_name) VALUES
  ('SKU-0001', 'Trail Mix 200g',   'Snacks',    'CrunchCo'),
  ('SKU-0002', 'Sparkling Water',  'Beverages', 'FizzCo');

-- =====================================================================
-- YOUR ANSWER (1): ALTER dim_product to add the Type-2 control columns.
--   valid_from  date NOT NULL DEFAULT DATE '0001-01-01'
--   valid_to    date NOT NULL DEFAULT DATE '9999-12-31'
--   is_current  boolean NOT NULL DEFAULT true
-- =====================================================================
-- ALTER TABLE dim_product ... YOUR ANSWER ...;

-- ---------------------------------------------------------------------
-- The incoming change arrives in a staging table (provided, correct):
-- SKU-0001 is re-categorized 'Snacks' -> 'Healthy Snacks' on 2026-06-19.
-- SKU-0002 is unchanged (same attributes) and must NOT spawn a new row.
-- ---------------------------------------------------------------------
CREATE TABLE stg_product (
    sku            text NOT NULL,
    product_name   text NOT NULL,
    category_name  text NOT NULL,
    brand_name     text NOT NULL,
    effective_date date NOT NULL
);
INSERT INTO stg_product VALUES
  ('SKU-0001', 'Trail Mix 200g',  'Healthy Snacks', 'CrunchCo', '2026-06-19'),
  ('SKU-0002', 'Sparkling Water',  'Beverages',      'FizzCo',   '2026-06-19');

-- =====================================================================
-- YOUR ANSWER (2): Apply the change in ONE transaction.
--   Step A (MERGE): close every CURRENT row whose tracked attributes
--     differ from staging (use IS DISTINCT FROM on category_name,
--     brand_name, product_name). Set valid_to = effective_date and
--     is_current = false.
--   Step B (INSERT): open a new current row for every staged product
--     that does NOT already have a matching open row.
--   Wrap both in BEGIN; ... COMMIT;
-- =====================================================================
-- BEGIN;
--   MERGE INTO dim_product d USING stg_product s ... ;   -- YOUR ANSWER
--   INSERT INTO dim_product (...) SELECT ... WHERE NOT EXISTS (...);  -- YOUR ANSWER
-- COMMIT;

-- =====================================================================
-- YOUR ANSWER (3): Point-in-time audit.
--   Show sku, category_name as they stood on 2026-06-15.
--   Hint: WHERE DATE '2026-06-15' >= valid_from AND DATE '2026-06-15' < valid_to
-- =====================================================================
-- SELECT ... YOUR ANSWER ...;

-- =====================================================================
-- YOUR ANSWER (4): Full lifecycle of SKU-0001, ordered by valid_from.
--   Expect TWO rows: closed 'Snacks' then open 'Healthy Snacks'.
-- =====================================================================
-- SELECT ... YOUR ANSWER ...;

-- =====================================================================
-- YOUR ANSWER (5): Well-formedness check.
--   Must return ZERO rows: any sku with != 1 current version is a bug.
-- =====================================================================
-- SELECT sku, COUNT(*) ... HAVING COUNT(*) <> 1;   -- YOUR ANSWER
