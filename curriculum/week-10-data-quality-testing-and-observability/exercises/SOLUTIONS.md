# Week 10 — Exercise Solutions

Work each exercise yourself before reading these. The point of the week is the *reflex* — see an incident, name the dimension, reach for the check — and you don't build a reflex by reading the answer. These solutions show one correct path; yours may differ in naming and still be right. Every code block below has been written against **Great Expectations 1.x** and **dbt 1.7+** APIs; if a method name differs in your version, the project docs are authoritative (and re-read Lecture 2 §1 on the GX 0.x/1.x break before you debug).

There are five solutions: the two GX exercises (suite, then checkpoint + Data Docs), the dbt mart tests, the dbt source freshness, and a bonus volume-anomaly singular test that you'll reuse in the mini-project.

---

## Solution 1 — Ingestion GX suite (`exercise-01-ingestion-gx-suite.py`)

The completed `build_context_and_batch`, `build_suite`, and `validate`:

```python
def build_context_and_batch():
    context = gx.get_context(mode="file")
    try:
        data_source = context.data_sources.get("raw_orders_source")
    except Exception:
        data_source = context.data_sources.add_pandas(name="raw_orders_source")
    try:
        data_asset = data_source.get_asset("raw_orders")
    except Exception:
        data_asset = data_source.add_dataframe_asset(name="raw_orders")
    try:
        batch_definition = data_asset.get_batch_definition("nightly_batch")
    except Exception:
        batch_definition = data_asset.add_batch_definition_whole_dataframe("nightly_batch")
    return context, batch_definition


def build_suite(context):
    try:
        return context.suites.get("orders_ingestion")
    except Exception:
        pass
    suite = context.suites.add(gx.ExpectationSuite(name="orders_ingestion"))

    # Completeness
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="order_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="customer_id", mostly=0.99))
    # Validity
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(
            column="status", value_set=["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]
        )
    )
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(column="total_cents", min_value=0, max_value=10_000_000)
    )
    suite.add_expectation(
        gxe.ExpectColumnValuesToMatchRegex(column="currency_code", regex="^[A-Z]{3}$")
    )
    # Uniqueness
    suite.add_expectation(
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["order_id", "line_number"])
    )
    # Volume (small band to match the tiny sample; 30_000–50_000 in production)
    suite.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=1, max_value=100))

    suite.save()
    return suite


def validate(context, batch_definition, suite, df):
    name = "orders_ingestion_validation"
    try:
        vd = context.validation_definitions.get(name)
    except Exception:
        vd = context.validation_definitions.add(
            gx.ValidationDefinition(name=name, data=batch_definition, suite=suite)
        )
    return vd.run(batch_parameters={"dataframe": df})
```

Run output (abridged):

```
=== CLEAN load (expect success=True) ===
success: True

=== CORRUPTED load (expect success=False) ===
success: False
GATE WOULD HALT THE PIPELINE — the corrupted load is rejected.
```

**Which corruption tripped which expectation** (this is the deliverable):

| Corruption in `corrupted_orders()` | Expectation that failed | Dimension |
|---|---|---|
| `order_id` has a `None` | `ExpectColumnValuesToNotBeNull(order_id)` | Completeness |
| `(1003, 1)` appears twice | `ExpectCompoundColumnsToBeUnique` | Uniqueness |
| `status = "PLCAED"` | `ExpectColumnValuesToBeInSet(status)` | Validity (set) |
| `total_cents = -8200` | `ExpectColumnValuesToBeBetween(total_cents)` | Validity (range) |
| `currency_code = "usd"` | `ExpectColumnValuesToMatchRegex` | Validity (format) |

Five failures, four dimensions — exactly the taxonomy of Lecture 1, doing its job at the ingestion boundary.

---

## Solution 2 — Checkpoint + Data Docs (`exercise-02-gx-checkpoint-and-datadocs.py`)

```python
def build_validation_definition(context, batch_definition, suite):
    name = "orders_ingestion_validation"
    try:
        return context.validation_definitions.get(name)
    except Exception:
        return context.validation_definitions.add(
            gx.ValidationDefinition(name=name, data=batch_definition, suite=suite)
        )


def build_checkpoint(context, validation_definition):
    name = "orders_ingestion_checkpoint"
    try:
        return context.checkpoints.get(name)
    except Exception:
        pass
    return context.checkpoints.add(
        gx.Checkpoint(
            name=name,
            validation_definitions=[validation_definition],
            actions=[UpdateDataDocsAction(name="refresh_docs")],
            result_format={"result_format": "SUMMARY"},
        )
    )


def gate(result):
    if result.success:
        return
    failed = [
        r.expectation_config.type
        for run in result.run_results.values()
        for r in run.results
        if not r.success
    ]
    raise RuntimeError(f"orders_ingestion checkpoint FAILED: {failed}")
```

Run output:

```
=== CLEAN batch ===
success: True

=== CORRUPTED batch ===
success: False

Data Docs at: gx/uncommitted/data_docs/local_site/index.html
Traceback (most recent call last):
  ...
RuntimeError: orders_ingestion checkpoint FAILED: ['expect_column_values_to_not_be_null',
  'expect_compound_columns_to_be_unique', 'expect_column_values_to_be_in_set',
  'expect_column_values_to_be_between', 'expect_column_values_to_match_regex']
```

The `RuntimeError` on the last line is the entire week in one stack trace: the checkpoint ran, `.success` was `False`, and the gate **raised**. In the mini-project this exact `raise` lives in an Airflow `PythonOperator`, and the raise is what fails the task and halts the DAG. Open `gx/uncommitted/data_docs/local_site/index.html` to see the failing run rendered: each red expectation, with `SUMMARY` result format showing sample offending values (the `None` order_id, the `PLCAED` status). That HTML page is the DQ report artifact from Lecture 1 §6 — for free, on every run, because of the `UpdateDataDocsAction`.

**Common mistake:** stopping at `checkpoint.run(...)` and never calling `gate(bad)`. That is a monitor, not a gate — the validation ran and Data Docs updated, but nothing stopped the pipeline. Running the check is not gating; raising on the result is gating.

---

## Solution 3 — dbt mart tests (`exercise-03-dbt-mart-tests.yml`)

```yaml
version: 2

models:
  - name: fct_orders
    description: "One row per order line. Grain: (order_id, line_number)."
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: ['order_id', 'line_number']
      - dbt_utils.expression_is_true:
          expression: "total_cents >= 0"
    columns:
      - name: order_id
        data_tests: [not_null]
      - name: line_number
        data_tests: [not_null]
      - name: customer_id
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_id
              config:
                severity: warn
      - name: status
        data_tests:
          - accepted_values:
              values: ['PLACED', 'SHIPPED', 'DELIVERED', 'CANCELLED']
      - name: total_cents
        data_tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 10000000
              config:
                severity: error
                error_if: ">100"
                warn_if: ">0"

  - name: daily_revenue
    description: "Daily revenue rollup. One row per revenue_date."
    data_tests:
      - dbt_expectations.expect_row_values_to_have_recent_data:
          column: revenue_date
          datepart: day
          interval: 1
    columns:
      - name: revenue_date
        data_tests: [unique, not_null]
      - name: revenue_cents
        data_tests:
          - dbt_utils.accepted_range:
              min_value: 0
              config:
                store_failures: true
```

Verification:

```
$ dbt deps
$ dbt build --select fct_orders daily_revenue
...
14:41:02  PASS not_null_fct_orders_order_id ........................ [PASS in 0.09s]
14:41:02  PASS dbt_utils_unique_combination_... .................... [PASS in 0.12s]
14:41:02  WARN relationships_fct_orders_customer_id ............... [WARN 3 in 0.10s]
14:41:02  PASS accepted_values_fct_orders_status ................. [PASS in 0.08s]
14:41:02  PASS dbt_expectations_expect_column_values_to_be_between [PASS in 0.11s]
14:41:02  Done. PASS=11 WARN=1 ERROR=0 SKIP=0 TOTAL=12
```

Note the `WARN 3` on `relationships` — three orders reference a `customer_id` not in `dim_customer`. Because we set `severity: warn`, the build **continued** (it did not halt). That is the deliberate call from Lecture 1 §5.2: a missing FK is a soft signal worth a human's eyes, not a reason to stop the trains. Had we left it at the default `severity: error`, those 3 rows would have failed `dbt build` and halted downstream models. Flip it to `error`, re-run, and watch `dbt build` stop — that's the difference between warn and fail, demonstrated.

The graduated severity on `total_cents` (`error_if: ">100"`, `warn_if: ">0"`) is Lecture 1 §5.3's band: one stray out-of-range value warns; a flood (more than 100 rows) halts. And `store_failures: true` on `revenue_cents` wrote any negative-revenue days to `dbt_test__audit.accepted_range_daily_revenue_revenue_cents` — query that table to see *which* days, not just how many.

---

## Solution 4 — dbt source freshness (`exercise-04-dbt-source-freshness.yml`)

```yaml
version: 2

sources:
  - name: raw
    database: analytics
    schema: raw
    loaded_at_field: loaded_at
    freshness:
      warn_after: {count: 1, period: hour}
      error_after: {count: 2, period: hour}
    tables:
      - name: orders
        freshness:
          warn_after: {count: 30, period: minute}
          error_after: {count: 90, period: minute}
      - name: products
        freshness: null
      - name: customers
        loaded_at_field: snapshot_at
        freshness:
          warn_after: {count: 18, period: hour}
          error_after: {count: 26, period: hour}
```

Verification — fresh, then aged:

```
$ dbt source freshness
14:50:01  1 of 3 START freshness of raw.customers ........ [RUN]
14:50:01  1 of 3 PASS freshness of raw.customers ......... [PASS in 0.20s]
14:50:01  2 of 3 START freshness of raw.orders ........... [RUN]
14:50:01  2 of 3 PASS freshness of raw.orders ............ [PASS in 0.19s]
14:50:01  Skipping raw.products (freshness disabled)
$ echo $?
0

# now age the source: stop loading orders for 2 hours (or set loaded_at back)
$ dbt source freshness
14:50:01  2 of 3 ERROR STALE freshness of raw.orders ..... [ERROR in 0.18s]
  Source freshness error for raw.orders: 102 minutes since the most recent record
$ echo $?
1
```

The non-zero exit code (`1`) is the gate. An Airflow `BashOperator` running `dbt source freshness` fails its task on that exit code, which halts the DAG — the freshness gate at the source boundary. Note `products` was skipped entirely (`freshness: null`): turning the check *off* for a slow dimension is a real design decision, not a missing config — a weekly-updated table would false-alarm under any hourly SLA, and a gate that false-alarms gets disabled (Lecture 1 §5.3).

---

## Solution 5 — Volume-anomaly singular test (bonus, reused in the mini-project)

A `tests/assert_orders_volume_within_baseline.sql` that fails when today's load is outside the rolling-7-day band (Lecture 3 §3). This is the mart-boundary volume gate as a dbt singular test — it returns the offending row, so zero rows = pass.

```sql
-- tests/assert_orders_volume_within_baseline.sql
-- Fails if today's row_count is below 50% or above 200% of the trailing-7-day mean.
with baseline as (
    select
        load_date,
        row_count,
        avg(row_count) over (
            order by load_date rows between 7 preceding and 1 preceding
        ) as rolling_mean
    from {{ source('meta', 'load_metrics') }}
    where table_name = 'raw.orders'
)
select
    load_date,
    row_count,
    rolling_mean
from baseline
where load_date = current_date
  and rolling_mean is not null                 -- skip the first week (no baseline yet)
  and (row_count < 0.5 * rolling_mean or row_count > 2.0 * rolling_mean)
```

Verification against the §1 truncated-load scenario:

```
$ dbt test --select assert_orders_volume_within_baseline
15:02:11  1 of 1 START test assert_orders_volume_within_baseline [RUN]
15:02:11  1 of 1 FAIL 1 assert_orders_volume_within_baseline ... [FAIL 1 in 0.14s]
  Got 1 result, configured to fail if != 0
  | load_date  | row_count | rolling_mean |
  | 2026-06-19 |     16000 |     40250.00 |
```

One failing row: today's 16,000 against a ~40,250 baseline trips the 50% floor, the test fails, and `dbt build` halts before the bad number reaches the dashboard. This is the truncated-load incident from Lecture 1 §1, caught in 0.14 seconds at 01:05 instead of in 8 hours at a board meeting. That single test is the whole point of the week.
