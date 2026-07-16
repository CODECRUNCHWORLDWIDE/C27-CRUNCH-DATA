# Challenge 02 — Write a custom generic test and a reusable macro

> **Time:** ~1–1.5 hours.
> **Prerequisites:** Exercises 01–02 complete. Lecture 2 §2 (tests), Lecture 3 §4 (macros). The four built-in tests are not enough — you need your own.
> **Citations:** data tests (custom generic tests) <https://docs.getdbt.com/docs/build/data-tests>; jinja & macros <https://docs.getdbt.com/docs/build/jinja-macros>; how-we-structure <https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview>.

## Premise

The four built-in generic tests (`unique`, `not_null`, `accepted_values`, `relationships`) cover the common cases, but real warehouses have rules they cannot express. "Every order total is positive." "Quantity is between 1 and 10,000." Those are *parameterized assertions over a column* — exactly what a **custom generic test** is. And a custom generic test is, mechanically, just a **macro** that returns the offending rows. This challenge has you build both halves of dbt's extensibility: a reusable macro that returns SQL, and a custom generic test that uses the same mechanism to gate your build.

## Setup

Confirm `dbt build --select +fct_orders` is green. You will add tests to `fct_orders.gross_cents` and `stg_order_items.quantity`.

## Steps

### Part A — A custom generic test: `assert_positive`

A generic test is a macro named `test_<name>` (or placed in `tests/generic/`) that takes `model` and `column_name` and returns a `SELECT` of rows that **violate** the rule. Zero rows returned = pass.

1. Create `tests/generic/assert_positive.sql`:

   ```sql
   {% test assert_positive(model, column_name) %}
   -- Returns rows where the column is NOT strictly positive. Zero rows = pass.
   select
       {{ column_name }} as offending_value
   from {{ model }}
   where {{ column_name }} <= 0
      or {{ column_name }} is null
   {% endtest %}
   ```

2. Apply it in `schema.yml` exactly like a built-in:

   ```yaml
   models:
     - name: fct_orders
       columns:
         - name: gross_cents
           tests:
             - not_null
             - assert_positive          # your custom test
   ```

3. Run `dbt test --select fct_orders`. It should pass if every order total is positive. Now inject a bad row (an order whose `gross_cents` is 0 or negative), re-run, and confirm the test **fails** with the offending value reported. Remove the bad row.

### Part B — A parameterized custom generic test: `accepted_range`

`assert_positive` is fixed. Make a *parameterized* version that takes a `min_value` and `max_value` — the pattern `dbt_utils.accepted_range` uses.

1. Create `tests/generic/accepted_range.sql`:

   ```sql
   {% test accepted_range(model, column_name, min_value, max_value) %}
   select
       {{ column_name }} as offending_value
   from {{ model }}
   where {{ column_name }} < {{ min_value }}
      or {{ column_name }} > {{ max_value }}
   {% endtest %}
   ```

2. Apply it with arguments to `stg_order_items.quantity`:

   ```yaml
   models:
     - name: stg_order_items
       columns:
         - name: quantity
           tests:
             - accepted_range:
                 min_value: 1
                 max_value: 10000
   ```

3. Run `dbt test --select stg_order_items`. Inject a row with `quantity = 0` and one with `quantity = 50000`; confirm both fail the test. Remove them.

### Part C — A reusable macro: `cents_to_dollars`

A macro that returns a *SQL expression* (not offending rows) is the other use of the same tool — DRY-ing up repeated logic.

1. Create `macros/cents_to_dollars.sql`:

   ```sql
   {% macro cents_to_dollars(column_name, precision=2) %}
       round( ({{ column_name }} / 100.0)::numeric, {{ precision }} )
   {% endmacro %}
   ```

2. Use it in a reporting model `models/marts/fct_orders_reported.sql`:

   ```sql
   {{ config(materialized='view') }}
   select
       order_id,
       customer_sk,
       order_date,
       gross_cents,
       {{ cents_to_dollars('gross_cents') }} as gross_dollars
   from {{ ref('fct_orders') }}
   ```

3. Run `dbt run --select fct_orders_reported`, then inspect `target/compiled/.../fct_orders_reported.sql` and confirm the macro expanded to `round((gross_cents / 100.0)::numeric, 2) as gross_dollars`. Verify a value: an order of `gross_cents = 12999` should report `gross_dollars = 129.99`.

### Part D — Tie it together with `dbt build`

Run `dbt build --select +fct_orders_reported`. Every model builds and every test — built-in and custom — runs interleaved by DAG. Capture the final `Done. PASS=… ERROR=0` line as your evidence.

## Acceptance criteria

- [ ] `assert_positive` exists as a custom generic test and is applied to `fct_orders.gross_cents`.
- [ ] `accepted_range(min_value, max_value)` exists, is parameterized, and is applied to `stg_order_items.quantity` with `1`/`10000`.
- [ ] You demonstrated each custom test **failing** on an injected bad row (captured the failure output), then passing after removal.
- [ ] `cents_to_dollars` macro exists, is used in `fct_orders_reported`, and the compiled SQL shows the expansion.
- [ ] `dbt build --select +fct_orders_reported` is green with `ERROR=0`.

## Stretch goals

- **Make `assert_positive` strict-or-nonnegative configurable.** Add an `inclusive` argument so `assert_positive(inclusive=true)` allows zero. Default it to `false`.
- **Override a built-in via a macro.** dbt's `generate_schema_name` macro controls where models are written. Override it in `macros/generate_schema_name.sql` so dev builds land in a `dev_` prefixed schema. This is how teams isolate developer builds — and it is *just a macro*.
- **Write your own `generate_surrogate_key`** instead of using `dbt_utils`. Hash the natural-key columns with `md5(concat(...))`, handling nulls explicitly (coalesce each column to a sentinel before concatenating, so `(null, 'x')` and `('x', null)` do not collide). Compare yours to `dbt_utils`'s output and explain any difference.
- **Add a `severity: warn` to one custom test** and observe that it logs a `WARN` instead of failing the build — useful for advisory rules you do not want to block a release.

## Cited references

- dbt data tests, including custom generic tests (the `{% test %}` block, the `model`/`column_name` signature, parameters): <https://docs.getdbt.com/docs/build/data-tests>
- dbt Jinja & macros (defining macros, calling them, overriding built-ins): <https://docs.getdbt.com/docs/build/jinja-macros>
- dbt "how we structure" (where tests and macros live in a project): <https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview>
