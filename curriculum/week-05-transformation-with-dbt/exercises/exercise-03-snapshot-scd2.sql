-- =============================================================================
-- Exercise 03 — A dbt snapshot: Type-2 SCD, automated
-- Course: C27 Crunch Data · Week 5 — Transformation with dbt
-- =============================================================================
--
-- TASK
-- ----
-- In Week 1 you hand-built a Type-2 slowly-changing dimension on the customer:
-- when `segment` changed you closed the old row (effective_to) and opened a new
-- one (effective_from), keeping full history. Replace ALL of that hand logic
-- with `dbt snapshot`.
--
--   (1) Write a snapshot `customers_snapshot` using the TIMESTAMP strategy on the
--       source's `updated_at` column.
--   (2) Write a second snapshot `customers_snapshot_check` using the CHECK
--       strategy on [segment, country_code, email], for when no trustworthy
--       updated_at exists.
--   (3) Rebuild dim_customer to be SCD-AWARE: ref the snapshot, expose
--       dbt_valid_from / dbt_valid_to / is_current, and build a surrogate key
--       that is unique PER VERSION (hash natural key + dbt_valid_from).
--
-- dbt manages dbt_valid_from / dbt_valid_to / dbt_scd_id for you. "Current" rows
-- have dbt_valid_to IS NULL.
--
-- ACCEPTANCE CRITERIA
-- -------------------
--   [ ] customers_snapshot uses strategy='timestamp', updated_at='updated_at',
--       unique_key='customer_id', target_schema='snapshots'.
--   [ ] customers_snapshot_check uses strategy='check' with
--       check_cols=['segment','country_code','email'].
--   [ ] dim_customer surrogate key is unique per (customer_id, dbt_valid_from).
--   [ ] After two `dbt snapshot` runs across a changed segment, the snapshot has
--       TWO rows for that customer: one with dbt_valid_to set, one current
--       (dbt_valid_to NULL).
--   [ ] singular test "one current row per customer" PASSES (see SOLUTIONS).
--
-- RUN COMMANDS
-- ------------
--   dbt snapshot                                   # capture current state -> history
--   dbt run --select dim_customer                  # rebuild SCD-aware dimension
--   dbt build --select customers_snapshot+         # snapshot + everything downstream
--   -- audit query (run in DuckDB):
--   --   select customer_id, segment, dbt_valid_from, dbt_valid_to
--   --   from snapshots.customers_snapshot order by customer_id, dbt_valid_from;
-- =============================================================================


-- -----------------------------------------------------------------------------
-- PART A — snapshots/customers_snapshot.sql  (TIMESTAMP strategy)
-- -----------------------------------------------------------------------------
{% snapshot customers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at'
        -- >>> NOTE: with strategy='timestamp', dbt records a new version each
        -- >>> time `updated_at` for a customer advances. No extra code needed.
    )
}}

-- >>> YOU COMPLETE <<<  select the source rows to snapshot.
-- Hint: select * from {{ source('raw', 'customers') }}

{% endsnapshot %}


-- -----------------------------------------------------------------------------
-- PART B — snapshots/customers_snapshot_check.sql  (CHECK strategy)
-- (own file; shown here for the exercise)
-- -----------------------------------------------------------------------------
-- {% snapshot customers_snapshot_check %}
-- {{
--     config(
--         target_schema='snapshots',
--         unique_key='customer_id',
--         strategy='check',
--         -- >>> YOU COMPLETE <<<  check_cols=['segment', 'country_code', 'email']
--     )
-- }}
-- select * from {{ source('raw', 'customers') }}
-- {% endsnapshot %}


-- -----------------------------------------------------------------------------
-- PART C — models/marts/dim_customer.sql  (SCD-aware; replaces Ex.02 version)
-- -----------------------------------------------------------------------------
-- {{ config(materialized='table') }}
--
-- select
--     -- A Type-2 dimension has MANY rows per natural key (one per version), so
--     -- the surrogate key must be unique PER VERSION:
--     -- >>> YOU COMPLETE <<<  customer_sk via
--     --   {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }}
--     customer_id,
--     segment,
--     country_code,
--     email,
--     dbt_valid_from,
--     dbt_valid_to,
--     -- >>> YOU COMPLETE <<<  is_current = (dbt_valid_to is null)
-- from {{ ref('customers_snapshot') }}
