-- =============================================================================
-- Exercise 02 — A dimensional mart with refs, plus generic tests
-- Course: C27 Crunch Data · Week 5 — Transformation with dbt
-- =============================================================================
--
-- TASK
-- ----
-- Build the mart layer of the warehouse from the staging models of Exercise 01.
-- You will produce:
--   (1) An INTERMEDIATE model int_orders_enriched that aggregates line items to
--       the order grain (this is where the join + aggregation lives — NOT in
--       staging).
--   (2) A DIMENSION dim_customer with a SURROGATE KEY (use dbt_utils).
--   (3) A FACT fct_orders at grain "one row per order_id", referencing
--       dim_customer by SURROGATE KEY (not natural key).
--   (4) A schema.yml tests block enforcing grain, not-null keys, accepted
--       values, and referential integrity.
--
-- Every input is reached with {{ ref(...) }} — never a hard-coded table name.
-- That is what builds the DAG and the lineage graph.
--
-- ACCEPTANCE CRITERIA
-- -------------------
--   [ ] int_orders_enriched aggregates order_items to ONE row per order_id.
--   [ ] dim_customer has a surrogate key `customer_sk` from generate_surrogate_key.
--   [ ] fct_orders is grain = one row per order_id and joins dim_customer on the
--       NATURAL key to attach the SURROGATE FK customer_sk.
--   [ ] schema.yml: unique+not_null on fct_orders.order_id (grain), not_null on
--       customer_sk, relationships from fct_orders.customer_sk to
--       dim_customer.customer_sk, accepted_values on dim_customer.segment.
--   [ ] `dbt build --select marts+` is green (all models + tests PASS).
--
-- RUN COMMANDS
-- ------------
--   dbt deps                                  # installs dbt_utils
--   dbt run   --select int_orders_enriched dim_customer fct_orders
--   dbt test  --select fct_orders dim_customer
--   dbt build --select +fct_orders            # build fct_orders and all ancestors, with tests
-- =============================================================================


-- -----------------------------------------------------------------------------
-- PART A — models/intermediate/int_orders_enriched.sql
-- Aggregate line items to the ORDER grain, attach order header fields.
-- -----------------------------------------------------------------------------
{{ config(materialized='ephemeral') }}

with orders as (

    select * from {{ ref('stg_orders') }}

),

items as (

    select
        order_id,
        -- >>> YOU COMPLETE <<<  gross_cents = sum(quantity * unit_price_cents)
        -- >>> YOU COMPLETE <<<  line_count  = count(*) of line items
        -- group by the order grain below
    from {{ ref('stg_order_items') }}
    group by order_id

)

select
    o.order_id,
    o.customer_id,
    o.order_ts,
    o.order_date,
    i.gross_cents,
    i.line_count
from orders as o
join items  as i using (order_id)


-- -----------------------------------------------------------------------------
-- PART B — models/marts/dim_customer.sql
-- One row per customer with a SURROGATE KEY.
-- -----------------------------------------------------------------------------
-- {{ config(materialized='table') }}
--
-- with customers as (
--     select * from {{ ref('stg_customers') }}
-- )
-- select
--     -- >>> YOU COMPLETE <<<  customer_sk via
--     --     {{ dbt_utils.generate_surrogate_key(['customer_id']) }}
--     customer_id,
--     email,
--     country_code,
--     segment
-- from customers


-- -----------------------------------------------------------------------------
-- PART C — models/marts/fct_orders.sql
-- Grain: ONE row per order_id. FK to dim_customer by SURROGATE key.
-- -----------------------------------------------------------------------------
{{ config(materialized='table') }}

with orders as (

    select * from {{ ref('int_orders_enriched') }}

),

customers as (

    -- pull the natural+surrogate key pair so we can map natural -> surrogate
    select customer_id, customer_sk from {{ ref('dim_customer') }}

)

select
    o.order_id,
    -- >>> YOU COMPLETE <<<  attach c.customer_sk as the surrogate FK
    o.order_date,
    o.gross_cents,
    o.line_count
from orders as o
join customers as c using (customer_id)


-- -----------------------------------------------------------------------------
-- PART D — schema.yml tests block (copy into models/marts/_marts.yml)
-- -----------------------------------------------------------------------------
-- version: 2
--
-- models:
--   - name: fct_orders
--     description: "Order fact. Grain: one row per order_id."
--     columns:
--       - name: order_id
--         description: "Natural key; the grain of the fact."
--         tests:
--           # >>> YOU COMPLETE <<<  enforce grain: this column is unique AND not_null
--       - name: customer_sk
--         description: "Surrogate FK into dim_customer."
--         tests:
--           - not_null
--           # >>> YOU COMPLETE <<<  relationships test:
--           #   to:    ref('dim_customer')
--           #   field: customer_sk
--       - name: gross_cents
--         tests: [not_null]
--
--   - name: dim_customer
--     columns:
--       - name: customer_sk
--         tests: [unique, not_null]
--       - name: segment
--         tests:
--           # >>> YOU COMPLETE <<<  accepted_values:
--           #   values: ['enterprise', 'smb', 'consumer', 'unknown']
