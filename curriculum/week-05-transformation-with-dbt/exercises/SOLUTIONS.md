# Week 5 — Exercise Solutions

**Read this only after you have attempted all three exercises.** The value of the exercises is in the struggle of getting a `dbt build` green against DuckDB; reading the answer first throws that away. Each solution gives the reference code, the expected console output, and the pitfalls that bite people the first time.

These solutions assume the project layout from Lecture 1, dbt-core 1.8, dbt-duckdb 1.8, and `dbt_utils` installed via `packages.yml` + `dbt deps`.

---

## Exercise 01 — Sources and staging models

### What the exercise asks

Declare the three raw retail tables as a dbt source with freshness, then write one staging model per source table that *only* cleans and renames — no joins, no aggregation, no business logic.

### Reference solution

**`models/staging/_sources.yml`** — note the per-table freshness override on `orders`:

```yaml
version: 2

sources:
  - name: raw
    description: "Raw retail extracts landed by the upstream loader (Week 3)."
    database: crunch_warehouse
    schema: raw
    loaded_at_field: _loaded_at
    freshness:
      warn_after:  {count: 24, period: hour}
      error_after: {count: 48, period: hour}
    tables:
      - name: customers
        description: "One row per customer from the OLTP source."
        columns:
          - name: customer_id
            description: "Natural key from the source system."
            tests: [not_null, unique]
      - name: orders
        description: "One row per order header."
        freshness:                              # the completed override
          warn_after:  {count: 12, period: hour}
          error_after: {count: 24, period: hour}
      - name: order_items
        description: "One row per line item. Grain: (order_id, line_number)."
```

**`models/staging/stg_customers.sql`:**

```sql
with source as (
    select * from {{ source('raw', 'customers') }}
),
renamed as (
    select
        customer_id,
        trim(lower(email))            as email,
        upper(country_code)           as country_code,
        coalesce(segment, 'unknown')  as segment,
        first_name,
        last_name,
        updated_at,
        _loaded_at
    from source
)
select * from renamed
```

**`models/staging/stg_orders.sql`:**

```sql
with source as (
    select * from {{ source('raw', 'orders') }}
),
renamed as (
    select
        order_id,
        customer_id,
        order_ts::timestamp           as order_ts,
        order_ts::date                as order_date,
        status,
        _loaded_at
    from source
)
select * from renamed
```

**`models/staging/stg_order_items.sql`:**

```sql
with source as (
    select * from {{ source('raw', 'order_items') }}
),
renamed as (
    select
        order_id,
        line_number,
        product_id,
        quantity,
        unit_price_cents,             -- integer cents, never a float
        _loaded_at
    from source
)
select * from renamed
```

### Expected output

```text
$ dbt run --select staging
14:02:01  Found 3 models, 3 sources
14:02:01  1 of 3 START sql view model main.stg_customers ......... [RUN]
14:02:01  1 of 3 OK created sql view model main.stg_customers .... [OK in 0.05s]
14:02:01  2 of 3 START sql view model main.stg_orders ............ [RUN]
14:02:01  2 of 3 OK created sql view model main.stg_orders ....... [OK in 0.04s]
14:02:01  3 of 3 START sql view model main.stg_order_items ....... [RUN]
14:02:01  3 of 3 OK created sql view model main.stg_order_items .. [OK in 0.04s]
14:02:01  Done. PASS=3 WARN=0 ERROR=0 SKIP=0 TOTAL=3

$ dbt source freshness
14:02:30  1 of 3 START freshness of raw.customers .... [RUN]
14:02:30  1 of 3 PASS freshness of raw.customers ..... [PASS in 0.03s]
14:02:30  2 of 3 WARN freshness of raw.orders ........ [WARN in 0.03s]   <- if older than 12h
14:02:30  3 of 3 PASS freshness of raw.order_items ... [PASS in 0.03s]
```

### Common pitfalls

- **Putting a join in staging.** The single most common mistake. Staging is one-source-per-model. If you find yourself joining `customers` to `orders` in `stg_*`, stop — that belongs in an intermediate model.
- **Forgetting `loaded_at_field`.** Without it, `dbt source freshness` errors ("freshness configured but no loaded_at_field"). It must name a real timestamp column on every raw table.
- **`source()` returns nothing.** Your `schema:` and `database:` in the source block must match where the raw tables actually live in DuckDB. Run `select * from raw.customers` in DuckDB to confirm the schema name.
- **Storing money as a float.** `unit_price_cents` is an integer. Casting to a float here propagates rounding error into every downstream sum.

---

## Exercise 02 — A dimensional mart with refs, plus generic tests

### What the exercise asks

Aggregate line items to the order grain in an intermediate model, build a surrogate-keyed `dim_customer`, build `fct_orders` at one-row-per-order grain referencing the dimension by surrogate key, and attach generic tests that enforce grain, keys, accepted values, and referential integrity.

### Reference solution

**`models/intermediate/int_orders_enriched.sql`:**

```sql
{{ config(materialized='ephemeral') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),
items as (
    select
        order_id,
        sum(quantity * unit_price_cents) as gross_cents,
        count(*)                         as line_count
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
```

**`models/marts/dim_customer.sql`:**

```sql
{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('stg_customers') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,
    customer_id,
    email,
    country_code,
    segment
from customers
```

**`models/marts/fct_orders.sql`:**

```sql
{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('int_orders_enriched') }}
),
customers as (
    select customer_id, customer_sk from {{ ref('dim_customer') }}
)
select
    o.order_id,
    c.customer_sk,
    o.order_date,
    o.gross_cents,
    o.line_count
from orders as o
join customers as c using (customer_id)
```

**`models/marts/_marts.yml`:**

```yaml
version: 2
models:
  - name: fct_orders
    description: "Order fact. Grain: one row per order_id."
    columns:
      - name: order_id
        description: "Natural key; the grain of the fact."
        tests: [unique, not_null]
      - name: customer_sk
        description: "Surrogate FK into dim_customer."
        tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_sk
      - name: gross_cents
        tests: [not_null]
  - name: dim_customer
    columns:
      - name: customer_sk
        tests: [unique, not_null]
      - name: segment
        tests:
          - accepted_values:
              values: ['enterprise', 'smb', 'consumer', 'unknown']
```

### Expected output

```text
$ dbt build --select +fct_orders
14:20:01  Found 5 models, 6 data tests, 3 sources
14:20:01  1 of 11 START sql view model main.stg_customers ........ [OK in 0.04s]
14:20:01  2 of 11 START sql view model main.stg_orders ........... [OK in 0.04s]
14:20:01  3 of 11 START sql view model main.stg_order_items ...... [OK in 0.04s]
14:20:01  4 of 11 START sql table model main.dim_customer ........ [OK in 0.06s]
14:20:01  5 of 11 START test unique_dim_customer_customer_sk ..... [PASS in 0.02s]
14:20:01  6 of 11 START test not_null_dim_customer_customer_sk ... [PASS in 0.02s]
14:20:01  7 of 11 START test accepted_values_dim_customer_segment  [PASS in 0.02s]
14:20:01  8 of 11 START sql table model main.fct_orders .......... [OK in 0.05s]
14:20:01  9 of 11 START test unique_fct_orders_order_id .......... [PASS in 0.02s]
14:20:01 10 of 11 START test not_null_fct_orders_customer_sk ..... [PASS in 0.02s]
14:20:01 11 of 11 START test relationships_fct_orders_customer_sk  [PASS in 0.03s]
14:20:01  Done. PASS=11 WARN=0 ERROR=0 SKIP=0 TOTAL=11
```

(`int_orders_enriched` is ephemeral, so it appears in no model count and is inlined into `fct_orders` — check `target/compiled/.../fct_orders.sql` and you will see its CTE there.)

### Common pitfalls

- **Joining `fct_orders` to `dim_customer` on the surrogate key.** You cannot — the fact only knows the *natural* key (`customer_id`). You join on the natural key to *fetch* the surrogate key. Joining on `customer_sk` is circular.
- **`unique` test fails on `fct_orders.order_id`.** This means your join fanned out — almost always because `int_orders_enriched` did not aggregate items to one row per order, so the order header joined to multiple item rows. Check that `items` groups by `order_id`.
- **`relationships` test fails.** A `customer_sk` in the fact has no match in the dimension. Cause: an order references a `customer_id` not present in `dim_customer` (an orphan). Either the dimension is incomplete or the source has a referential gap — the test is doing its job by surfacing it.
- **`dbt_utils` not found.** Run `dbt deps` after adding it to `packages.yml`. The macro namespace is `dbt_utils.generate_surrogate_key`, not `generate_surrogate_key` bare.

---

## Exercise 03 — A dbt snapshot: Type-2 SCD, automated

### What the exercise asks

Replace your Week 1 hand-built Type-2 SCD with `dbt snapshot`: one snapshot using the `timestamp` strategy, one using `check`, and an SCD-aware `dim_customer` keyed per version.

### Reference solution

**`snapshots/customers_snapshot.sql`:**

```sql
{% snapshot customers_snapshot %}
{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}
select * from {{ source('raw', 'customers') }}
{% endsnapshot %}
```

**`snapshots/customers_snapshot_check.sql`:**

```sql
{% snapshot customers_snapshot_check %}
{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['segment', 'country_code', 'email']
    )
}}
select * from {{ source('raw', 'customers') }}
{% endsnapshot %}
```

**`models/marts/dim_customer.sql`** (SCD-aware version):

```sql
{{ config(materialized='table') }}
select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }} as customer_sk,
    customer_id,
    segment,
    country_code,
    email,
    dbt_valid_from,
    dbt_valid_to,
    (dbt_valid_to is null) as is_current
from {{ ref('customers_snapshot') }}
```

**`tests/assert_one_current_row_per_customer.sql`** (the singular test from Lecture 2):

```sql
select customer_id, count(*) as current_rows
from {{ ref('dim_customer') }}
where is_current
group by customer_id
having count(*) > 1
```

### Expected output

First snapshot run — every row is new, all current:

```text
$ dbt snapshot
14:40:01  1 of 2 START snapshot snapshots.customers_snapshot ........ [RUN]
14:40:01  1 of 2 OK snapshotted snapshots.customers_snapshot ....... [INSERT 0 500 in 0.12s]
14:40:01  2 of 2 START snapshot snapshots.customers_snapshot_check .. [RUN]
14:40:01  2 of 2 OK snapshotted snapshots.customers_snapshot_check . [INSERT 0 500 in 0.10s]
14:40:01  Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
```

Now change one customer's `segment` in the raw source (and bump its `updated_at`), then snapshot again — one row closed, one opened:

```text
$ dbt snapshot
14:45:09  1 of 2 OK snapshotted snapshots.customers_snapshot ....... [INSERT 1 1 in 0.11s]
```

Audit query against the snapshot now shows two rows for that customer:

```text
customer_id | segment    | dbt_valid_from      | dbt_valid_to
------------+------------+---------------------+---------------------
C0042       | smb        | 2026-01-01 00:00:00 | 2026-06-19 14:45:09   <- closed
C0042       | enterprise | 2026-06-19 14:45:09 | NULL                  <- current
```

And the singular test passes (exactly one `is_current` row per customer):

```text
$ dbt test --select assert_one_current_row_per_customer
14:46:00  1 of 1 PASS assert_one_current_row_per_customer ... [PASS in 0.03s]
```

### Common pitfalls

- **Running `dbt snapshot` only once and expecting history.** Snapshots are stateful: history accumulates only across runs. If you change the source and never re-snapshot, the old version is captured but no new version is recorded; if you change it twice between snapshots, you lose the intermediate version permanently.
- **Surrogate key not unique per version.** If you hash only `customer_id` (as in Exercise 02), the SCD-aware `dim_customer` will have duplicate `customer_sk` values — one per version of the same customer — and the `unique` test fails. Hash `customer_id` *plus* `dbt_valid_from`.
- **`check` strategy thrashing.** If you put a frequently-changing or noisy column (a row hash, a `_loaded_at`) in `check_cols`, every snapshot opens a new version even when nothing meaningful changed. Pick the business attributes that define a "real" change.
- **Confusing `is_current` with "mapped".** A snapshot keeps *all* versions. Marts that only want the current state filter `where dbt_valid_to is null`; marts that need point-in-time history keep all rows and join on the validity window.
- **`updated_at` not actually advancing.** The `timestamp` strategy detects a change only when `updated_at` moves. If your source mutates `segment` but leaves `updated_at` stale, the timestamp strategy misses it — that is exactly the situation the `check` strategy exists for.
