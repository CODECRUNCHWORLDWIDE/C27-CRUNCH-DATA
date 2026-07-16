# Challenge 01 — Convert the orders fact to an incremental materialization

> **Time:** ~1.5–2 hours.
> **Prerequisites:** Exercises 01–02 complete (`stg_*`, `int_orders_enriched`, `dim_customer`, `fct_orders` all build). Lecture 2 §1.4 (incremental). Week 3 (idempotency, watermarks, upserts, late data).
> **Citations:** incremental models <https://docs.getdbt.com/docs/build/incremental-models>; materializations <https://docs.getdbt.com/docs/build/materializations>; `is_incremental()` and `{{ this }}` are documented on the incremental page above; dbt-duckdb strategies <https://github.com/duckdb/dbt-duckdb>.

## Premise

`fct_orders` from Exercise 02 is materialized as a `table` — every `dbt run` drops it and rebuilds it from all of history. On laptop data that is fine. In production, the orders fact has hundreds of millions of rows and grows daily; a full rebuild every run is wasteful and eventually impossible. This is exactly the problem you solved by hand in Week 3 with a high-water mark and an idempotent upsert. Now you express it in dbt: convert `fct_orders` to an **incremental** model that processes only new orders, merges them on a `unique_key`, handles a late-arriving order correctly, and — the part that matters — **does not double-count when re-run**.

## Setup

1. Make sure `dbt build --select +fct_orders` is green with the table-materialized version.
2. Add a small batch of "new" orders to the raw `orders` and `order_items` tables (5–10 orders, with `order_ts` later than anything already loaded). Keep one aside as a deliberately **late-arriving** order — give it an `order_ts` from *before* your current high-water mark but a `_loaded_at` of now.

## Steps

1. **Convert `fct_orders` to incremental.** Change its config and add an `is_incremental()` filter:

   ```sql
   {{ config(
       materialized='incremental',
       unique_key='order_id',
       incremental_strategy='delete+insert'
   ) }}

   with orders as (
       select * from {{ ref('int_orders_enriched') }}
   ),
   customers as (
       select customer_id, customer_sk from {{ ref('dim_customer') }}
   ),
   final as (
       select
           o.order_id,
           c.customer_sk,
           o.order_ts,
           o.order_date,
           o.gross_cents,
           o.line_count
       from orders as o
       join customers as c using (customer_id)
   )
   select * from final
   {% if is_incremental() %}
       where order_ts > (select max(order_ts) from {{ this }})
   {% endif %}
   ```

2. **First run (full build).** Run `dbt run --select fct_orders --full-refresh`. Because `is_incremental()` is false on a full refresh, the `where` is omitted and the whole table is built. Record the row count: `select count(*) from fct_orders`.

3. **Incremental run.** Add your batch of new orders to the source, run `dbt run --select fct_orders` (no `--full-refresh`). Now `is_incremental()` is true, the high-water-mark `where` kicks in, and only the new orders flow. Confirm the row count increased by exactly the number of new orders.

4. **Re-run the SAME batch — prove no double-count.** Without adding anything new, run `dbt run --select fct_orders` again. With `delete+insert` on `unique_key='order_id'`, dbt deletes existing rows whose `order_id` matches the incoming batch and re-inserts them — an upsert. The row count must be **unchanged**. This is the idempotency property from Week 3: a re-run produces the same result. If your count grows, you used `append` (which blindly appends) instead of `delete+insert`, or your `where` filter let the same rows through twice.

5. **Handle the late-arriving order.** Your held-aside late order has an `order_ts` *before* the high-water mark, so the naive `where order_ts > max(order_ts)` filter will **miss it** — exactly the late-record failure from Week 3. Fix it with a lookback window so the filter reaches back far enough to catch late data, and let `delete+insert` deduplicate the overlap:

   ```sql
   {% if is_incremental() %}
       where order_ts > (select max(order_ts) - interval 3 day from {{ this }})
   {% endif %}
   ```

   The 3-day lookback reprocesses the last three days every run; because the merge is keyed on `order_id`, reprocessing is safe (it upserts, not duplicates). Confirm the late order now appears and the count is still correct.

6. **Verify with `dbt build`.** Run `dbt build --select fct_orders`. The `unique` test on `order_id` is the proof: if incremental logic ever double-counted, `unique` fails. A green `unique` after the re-run is your evidence.

## Acceptance criteria

- [ ] `fct_orders` is `materialized='incremental'` with `unique_key='order_id'` and `incremental_strategy='delete+insert'`.
- [ ] First run with `--full-refresh` builds the complete table; row count recorded.
- [ ] An incremental run after adding N new orders increases the count by exactly N.
- [ ] **Re-running the same batch leaves the row count unchanged** (no double-count) — captured as before/after counts in your write-up.
- [ ] The late-arriving order (whose `order_ts` predates the high-water mark) is captured via the lookback window.
- [ ] `dbt build --select fct_orders` is green, including the `unique` test on `order_id`.

## Stretch goals

- **Switch the strategy to `append` and reproduce the bug.** Set `incremental_strategy='append'`, re-run the same batch, and watch the row count grow and the `unique` test fail. Then revert. Feeling the failure mode is the lesson; `append` is only safe when your `where` guarantees zero overlap.
- **Use a dedicated watermark column.** Replace `max(order_ts)` with `max(_loaded_at)` and reason about the difference: `order_ts` is event time (when the order happened), `_loaded_at` is processing time (when you ingested it). Which one makes late-data handling correct? (This is the event-time vs processing-time distinction Week 9 builds on.)
- **Add an `on_schema_change` config.** Set `on_schema_change='append_new_columns'` and add a column to `int_orders_enriched`; observe how dbt evolves the incremental table's schema instead of erroring (<https://docs.getdbt.com/docs/build/incremental-models>).
- **Try the `merge` strategy** (if your dbt-duckdb version supports it) and compare the compiled SQL in `target/compiled/` against `delete+insert`. One emits a `MERGE`; the other emits a `DELETE` then `INSERT`. Both achieve the upsert; the trade is atomicity vs portability.

## Cited references

- dbt incremental models (the `is_incremental()` macro, `unique_key`, `{{ this }}`, `--full-refresh`, `on_schema_change`): <https://docs.getdbt.com/docs/build/incremental-models>
- dbt materializations overview: <https://docs.getdbt.com/docs/build/materializations>
- dbt-duckdb adapter (supported incremental strategies): <https://github.com/duckdb/dbt-duckdb>
