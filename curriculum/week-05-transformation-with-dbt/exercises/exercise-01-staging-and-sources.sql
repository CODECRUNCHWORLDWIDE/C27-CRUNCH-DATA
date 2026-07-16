-- =============================================================================
-- Exercise 01 — Sources and staging models
-- Course: C27 Crunch Data · Week 5 — Transformation with dbt
-- =============================================================================
--
-- TASK
-- ----
-- Your Week 1 retail extracts have been landed into a `raw` schema in a DuckDB
-- warehouse (one row per customer, one row per order header, one row per order
-- line item). dbt does NOT own these tables — it reads them. Your job:
--
--   (1) Declare the three raw tables as a dbt SOURCE with freshness, so models
--       reference them through {{ source('raw', '<table>') }} and dbt can
--       monitor staleness.
--   (2) Write THREE staging models (stg_customers, stg_orders, stg_order_items),
--       one per source table, that clean + rename ONLY. No joins, no business
--       logic, no aggregation. Staging is the trust boundary.
--
-- This file contains: the sources.yml block (as a comment, you copy it into
-- models/staging/_sources.yml) and the three staging models. Complete every
-- spot marked  >>> YOU COMPLETE <<<  exactly as described.
--
-- ACCEPTANCE CRITERIA
-- -------------------
--   [ ] models/staging/_sources.yml declares source `raw` with tables
--       customers, orders, order_items, a loaded_at_field, and freshness
--       (warn_after 24h / error_after 48h).
--   [ ] stg_customers, stg_orders, stg_order_items each read EXACTLY ONE source
--       via {{ source('raw', ...) }} and contain NO join and NO aggregation.
--   [ ] Column names are snake_case and types are explicit where they matter.
--   [ ] `dbt run --select staging` builds 3 views with PASS=3.
--   [ ] `dbt source freshness` runs and reports PASS/WARN per table.
--
-- RUN COMMANDS
-- ------------
--   dbt debug
--   dbt run --select staging          # builds the three stg_* views
--   dbt source freshness              # checks raw input staleness
--   dbt run --select stg_orders+      # stg_orders and everything downstream
-- =============================================================================


-- -----------------------------------------------------------------------------
-- PART A — sources.yml  (copy this block into models/staging/_sources.yml)
-- -----------------------------------------------------------------------------
-- version: 2
--
-- sources:
--   - name: raw
--     description: "Raw retail extracts landed by the upstream loader (Week 3)."
--     database: crunch_warehouse        # DuckDB catalog
--     schema: raw
--     loaded_at_field: _loaded_at       # timestamp column on every raw table
--     freshness:
--       warn_after:  {count: 24, period: hour}
--       error_after: {count: 48, period: hour}
--     tables:
--       - name: customers
--         description: "One row per customer from the OLTP source."
--         columns:
--           - name: customer_id
--             description: "Natural key from the source system."
--             tests: [not_null, unique]
--       - name: orders
--         description: "One row per order header."
--         # >>> YOU COMPLETE <<<  add a per-table freshness override here:
--         #     warn after 12 hours, error after 24 hours.
--       - name: order_items
--         description: "One row per line item. Grain: (order_id, line_number)."


-- -----------------------------------------------------------------------------
-- PART B — models/staging/stg_customers.sql
-- -----------------------------------------------------------------------------
-- Clean + rename the raw customers table. NO joins, NO business logic.
with source as (

    select * from {{ source('raw', 'customers') }}

),

renamed as (

    select
        customer_id,
        -- >>> YOU COMPLETE <<<  normalize email to trimmed lowercase, aliased `email`.
        -- >>> YOU COMPLETE <<<  cast/standardize country_code to UPPERCASE, aliased `country_code`.
        coalesce(segment, 'unknown')  as segment,        -- default nulls to 'unknown'
        first_name,
        last_name,
        updated_at,
        _loaded_at
    from source

)

select * from renamed


-- -----------------------------------------------------------------------------
-- PART C — models/staging/stg_orders.sql
-- (put this in its OWN file; shown here together for the exercise)
-- -----------------------------------------------------------------------------
-- with source as (
--     select * from {{ source('raw', 'orders') }}
-- ),
-- renamed as (
--     select
--         order_id,
--         customer_id,
--         -- >>> YOU COMPLETE <<<  cast order_ts to a timestamp, aliased `order_ts`.
--         -- >>> YOU COMPLETE <<<  derive `order_date` as order_ts cast to date.
--         status,
--         _loaded_at
--     from source
-- )
-- select * from renamed


-- -----------------------------------------------------------------------------
-- PART D — models/staging/stg_order_items.sql
-- -----------------------------------------------------------------------------
-- with source as (
--     select * from {{ source('raw', 'order_items') }}
-- ),
-- renamed as (
--     select
--         order_id,
--         line_number,
--         product_id,
--         quantity,
--         -- >>> YOU COMPLETE <<<  keep monetary value as INTEGER CENTS, aliased
--         --     `unit_price_cents`. (Never store money as a float — Week 1 rule.)
--         _loaded_at
--     from source
-- )
-- select * from renamed
