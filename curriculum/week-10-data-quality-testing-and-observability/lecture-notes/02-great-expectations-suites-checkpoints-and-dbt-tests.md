# Lecture 2 — Great Expectations Suites & Checkpoints, and dbt Tests Revisited

> **Duration:** ~1.5 hours of reading + hands-on.
> **Outcome:** You can build a Great Expectations (GX Core 1.x) validation end to end — Data Context, Datasource, Data Asset, Batch Definition, Expectation Suite, Validation Definition, Checkpoint, Actions, Data Docs — write real expectations for schema, nulls, ranges, value sets, uniqueness, and compound keys, and you can write dbt tests in depth (generic, singular, `dbt_utils`, `dbt_expectations`, severity, `store_failures`) and pick the right test and severity for a rule.

Lecture 1 gave you the taxonomy and the requirement that a gate must halt. This lecture gives you the two tools that implement the gates: **Great Expectations** at the ingestion boundary, and **dbt tests** inside the transformation layer. They overlap — both can check nulls and ranges — and knowing *which to reach for* is part of the craft, addressed in §7.

---

## 1. A blunt note on GX versions: 0.x vs 1.x

Before any code: **Great Expectations had a hard API break.** Tutorials and Stack Overflow answers from before late 2024 use the **0.x** API, which is *not compatible* with the **1.x** API this course uses. If you copy a 0.x snippet into a 1.x project, it will not run. Two anchors so you can tell them apart instantly:

| | GX 0.x (legacy — avoid) | GX 1.x (GX Core — this course) |
|---|---|---|
| Suite creation | `context.create_expectation_suite("name")`, add expectations via a `Validator` | `context.suites.add(ExpectationSuite(name=...))`, add `gxe.Expect...` objects |
| Expectations | called as methods on a validator: `validator.expect_column_values_to_not_be_null("col")` | imported as **classes**: `gxe.ExpectColumnValuesToNotBeNull(column="col")` |
| Running | `checkpoint = context.add_or_update_checkpoint(...)` with a big `validations` config | a `ValidationDefinition` (suite × batch) wrapped in a `Checkpoint` |
| Datasource API | `context.sources.add_pandas(...)` | `context.data_sources.add_pandas(...)` (Fluent) |

`pip install great_expectations` today gives you 1.x (1.x is current as of this writing). The mental model below is **1.x only**. When you read the docs at <https://docs.greatexpectations.io/docs/>, confirm the version selector says 1.x. This honesty matters because the version trap is the single most common reason a learner's GX code "just doesn't work."

---

## 2. The GX object model

GX 1.x is a set of nested objects. Build them in this order; each depends on the one before:

```
Data Context                 # the entry point; holds all config (file-backed or ephemeral)
└── Data Source              # how to reach the data (pandas, postgres, spark, ...)
    └── Data Asset           # a queryable thing within the source (a dataframe, a table)
        └── Batch Definition # how to slice the asset into a Batch (whole table, by date, ...)
            └── Batch        # the concrete chunk of data validated at run time

Expectation Suite           # a named bundle of Expectations (the rules)
└── Expectation             # one rule: ExpectColumnValuesToNotBeNull, ExpectColumnValuesToBeBetween, ...

Validation Definition       # binds ONE Suite to ONE Batch Definition ("run these rules on this data")
└── Checkpoint              # runs one or more Validation Definitions + Actions (alert, update Data Docs)
    └── Action              # what to do with the result (UpdateDataDocsAction, alerting, ...)
```

The two halves — **(suite of expectations)** and **(data to run them on)** — stay separate until the **Validation Definition** marries them, and the **Checkpoint** is the runnable unit that produces a result you can act on (the `result.success` your Airflow task raises on). Hold that shape; the code below just fills it in.

---

## 3. Building a suite over the raw orders ingestion (GX 1.x)

The ingestion gate from Lecture 1 §4: validate the raw `orders` data the nightly ETL is about to load. We use a pandas Data Source because the raw load is a dataframe; for a Postgres asset the only change is `add_postgres(...)` and `add_table_asset(...)`.

```python
import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd

# --- 1. Data Context (file-backed so suites/checkpoints persist; ephemeral for tests) ---
context = gx.get_context(mode="file")   # creates ./gx/ on first run; use mode="ephemeral" in CI

# --- 2. Data Source + Data Asset + Batch Definition ---
data_source = context.data_sources.add_pandas(name="raw_orders_source")
data_asset = data_source.add_dataframe_asset(name="raw_orders")
batch_definition = data_asset.add_batch_definition_whole_dataframe("nightly_batch")

# --- 3. Expectation Suite: one rule per data-quality dimension (Lecture 1 taxonomy) ---
suite = context.suites.add(gx.ExpectationSuite(name="orders_ingestion"))

# Completeness — order_id is never null; customer_id tolerated 1% null (mostly)
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="order_id"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="customer_id", mostly=0.99))

# Validity — schema presence, ranges, value set, format
suite.add_expectation(gxe.ExpectColumnToExist(column="status"))
suite.add_expectation(
    gxe.ExpectColumnValuesToBeBetween(column="total_cents", min_value=0, max_value=10_000_000)
)
suite.add_expectation(
    gxe.ExpectColumnValuesToBeInSet(
        column="status", value_set=["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]
    )
)
suite.add_expectation(
    gxe.ExpectColumnValuesToMatchRegex(column="currency_code", regex="^[A-Z]{3}$")
)

# Uniqueness — single key and compound key
suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="order_id"))
suite.add_expectation(
    gxe.ExpectCompoundColumnsToBeUnique(column_list=["order_id", "line_number"])
)

# Volume — this nightly load should be in the normal range (Lecture 1 §3.5)
suite.add_expectation(
    gxe.ExpectTableRowCountToBeBetween(min_value=30_000, max_value=50_000)
)

suite.save()   # persist the suite to the file context
```

Every line maps to a dimension from Lecture 1. That is the point: **the suite is the taxonomy, instantiated.** Read it top to bottom and you can recite which dimension each rule guards.

The expectations you will use most, by dimension:

| Expectation (GX 1.x class) | Dimension | Checks |
|---|---|---|
| `ExpectColumnValuesToNotBeNull(column, mostly=)` | Completeness | column populated (with tolerance) |
| `ExpectColumnToExist(column)` | Validity (schema) | the column is present |
| `ExpectColumnValuesToBeOfType(column, type_)` | Validity (schema) | dtype matches |
| `ExpectColumnValuesToBeBetween(column, min_value, max_value)` | Validity (range) | numeric/date in range |
| `ExpectColumnValuesToBeInSet(column, value_set)` | Validity (set) | value in allowed set |
| `ExpectColumnValuesToMatchRegex(column, regex)` | Validity (format) | string matches pattern |
| `ExpectColumnValuesToBeUnique(column)` | Uniqueness | no duplicate values |
| `ExpectCompoundColumnsToBeUnique(column_list)` | Uniqueness | no duplicate tuples |
| `ExpectTableRowCountToBeBetween(min_value, max_value)` | Volume | row count in range |
| `ExpectColumnMeanToBeBetween(column, min_value, max_value)` | Distribution | mean drift |
| `ExpectColumnUniqueValueCountToBeBetween(column, min_value, max_value)` | Distribution | cardinality drift |

The full set is the **Expectations Gallery**: <https://greatexpectations.io/expectations/>. When you need a check you don't know, search the gallery before writing a custom expectation — there are several hundred.

---

## 4. Validation Definition, Checkpoint, Actions, and running it

The suite is the rules; now bind it to data and make it runnable.

```python
# --- 4. Validation Definition: this suite, on this batch ---
validation_definition = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="orders_ingestion_validation",
        data=batch_definition,
        suite=suite,
    )
)

# --- 5. Checkpoint: run the validation + Actions (update Data Docs on every run) ---
from great_expectations.checkpoint import UpdateDataDocsAction
checkpoint = context.checkpoints.add(
    gx.Checkpoint(
        name="orders_ingestion_checkpoint",
        validation_definitions=[validation_definition],
        actions=[UpdateDataDocsAction(name="refresh_docs")],
        result_format={"result_format": "SUMMARY"},   # SUMMARY gives unexpected-value samples
    )
)

# --- 6. Run it against the actual nightly dataframe ---
raw_df: pd.DataFrame = load_tonights_orders()          # your ETL's extracted frame
result = checkpoint.run(batch_parameters={"dataframe": raw_df})

# --- 7. THE GATE (Lecture 1 §5): act on the result ---
if not result.success:
    failed = [
        r.expectation_config.type
        for run in result.run_results.values()
        for r in run.results
        if not r.success
    ]
    raise RuntimeError(f"orders_ingestion checkpoint FAILED: {failed}")
```

Line 7 is the whole lesson of Week 10 in one block. `checkpoint.run(...)` returns a `CheckpointResult`; `.success` is the boolean the gate hangs on. If you stop at `checkpoint.run(...)` and never inspect `.success`, you have a monitor, not a gate (Lecture 1 §5.1). In the mini-project this exact block lives inside an Airflow `PythonOperator` (or you use the `GreatExpectationsOperator`, which raises for you).

**Actions** run after validation regardless of pass/fail: `UpdateDataDocsAction` regenerates the Data Docs HTML; you can add alerting actions (Slack, email) so a failure pages someone. Actions are *how the report and the alert happen*; the `raise` is *how the halt happens*. Different jobs, both wired into the checkpoint.

---

## 5. Data Docs: the report artifact

`UpdateDataDocsAction` builds **Data Docs** — a static HTML site, one page per validation run, showing every expectation, whether it passed, and (in `SUMMARY`/`COMPLETE` result format) samples of the offending values. It is the human-readable DQ report from Lecture 1 §6, generated for free on every checkpoint run.

```python
context.build_data_docs()                  # rebuild on demand
context.open_data_docs()                    # open the index in a browser (local dev)
# the site lands under gx/uncommitted/data_docs/local_site/index.html by default
```

In the mini-project you serve this site (or copy it to MinIO) so anyone can answer "is the orders data good right now?" with a link instead of a question. Data Docs is GX's strongest selling point over a hand-rolled checker: the *evidence* is a first-class, versioned artifact.

---

## 6. dbt tests revisited — gates inside the transformation layer

GX gates the *ingestion boundary*. Inside the transformation layer — your Week 5 dbt project on DuckDB — the gates are **dbt tests**, and `dbt build` runs models and their tests together so a failing test halts the build. You met these in Week 5; here is the depth.

### 6.1 Generic tests (the built-in four)

Declared in YAML under a column. These are the four dbt ships with:

```yaml
# models/marts/schema.yml
models:
  - name: fct_orders
    columns:
      - name: order_id
        data_tests:
          - unique          # uniqueness
          - not_null        # completeness
      - name: status
        data_tests:
          - accepted_values:                     # validity (value set)
              values: ['PLACED', 'SHIPPED', 'DELIVERED', 'CANCELLED']
      - name: customer_id
        data_tests:
          - not_null
          - relationships:                       # referential integrity
              to: ref('dim_customer')
              field: customer_id
```

`unique`, `not_null`, `accepted_values`, `relationships` cover four of the six dimensions out of the box. Note `relationships` — dbt does cross-table referential integrity cleanly, which is exactly where pure GX is weak (Lecture 1 §3.3). This is one reason the *transformation-layer* checks lean dbt.

### 6.2 Singular tests (arbitrary SQL)

When a rule doesn't fit a generic test, write a `.sql` file in `tests/`. A singular test is **a query that returns the failing rows** — zero rows returned means pass; any rows returned means fail:

```sql
-- tests/assert_no_future_order_dates.sql
-- Validity: an order can't be created in the future. Fails if any row is.
select order_id, created_at
from {{ ref('fct_orders') }}
where created_at > current_date
```

```sql
-- tests/assert_revenue_reconciles.sql
-- A business-logic invariant: mart daily revenue must equal the sum of its order lines.
with mart as (select sum(revenue_cents) as r from {{ ref('daily_revenue') }}),
     base as (select sum(total_cents)  as r from {{ ref('fct_orders') }})
select mart.r as mart_total, base.r as base_total
from mart, base
where mart.r <> base.r
```

Singular tests are where your *business invariants* live — the rules no package ships because they're specific to your data.

### 6.3 dbt_utils and dbt_expectations packages

Two community packages extend dbt's tests far beyond the built-in four. Add them to `packages.yml` and `dbt deps`:

```yaml
# packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.1.0", "<2.0.0"]
  - package: calogica/dbt_expectations
    version: [">=0.10.0", "<0.11.0"]
```

`dbt_utils` adds workhorse generic tests:

```yaml
      - name: total_cents
        data_tests:
          - dbt_utils.accepted_range:           # validity: range, like GX between
              min_value: 0
              max_value: 10000000
    # a model-level expression invariant:
    data_tests:
      - dbt_utils.expression_is_true:
          expression: "total_cents >= 0"
      - dbt_utils.unique_combination_of_columns: # uniqueness: compound key
          combination_of_columns: ['order_id', 'line_number']
```

`dbt_expectations` is a near-direct port of Great Expectations *into dbt* — so the same expectation vocabulary works inside the transformation layer:

```yaml
      - name: total_cents
        data_tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 10000000
    # freshness as a test: the model must contain a row within the last day
    data_tests:
      - dbt_expectations.expect_row_values_to_have_recent_data:
          column: created_at
          datepart: day
          interval: 1
```

`dbt_expectations.expect_row_values_to_have_recent_data` is a *freshness* check expressed as a test — useful at the mart boundary alongside `dbt source freshness` (Lecture 3 §2).

### 6.4 Severity, error_if / warn_if, and store_failures

dbt encodes Lecture 1 §5.2's fail-vs-warn directly:

```yaml
      - name: customer_id
        data_tests:
          - not_null:
              config:
                severity: error          # default: any failing row halts the build
          - relationships:
              to: ref('dim_customer')
              field: customer_id
              config:
                severity: warn            # missing FK: warn, don't halt (a soft signal)

      # graduated severity: warn at a small failure, error at a large one
      - name: total_cents
        data_tests:
          - dbt_utils.accepted_range:
              min_value: 0
              config:
                severity: error
                error_if: ">100"          # error only if more than 100 rows fail
                warn_if: ">0"             # warn if any fail
```

- `severity: error` (default) — failing rows fail the test and halt `dbt build`.
- `severity: warn` — failing rows are reported but the build continues. This is the dbt equivalent of "monitor, don't gate."
- `error_if` / `warn_if` — thresholds on the *number of failing rows*, so you can say "warn on any, but only halt if it's bad enough." This is the graduated band from Lecture 1 §5.3.
- `store_failures: true` — persist the failing rows to a table (`dbt_test__audit` schema) so you can inspect *which* rows failed, not just the count. This is your forensics artifact at the dbt layer.

```yaml
      - name: order_id
        data_tests:
          - unique:
              config:
                store_failures: true      # write the duplicates to a table for inspection
```

Run it:

```bash
dbt deps                    # install dbt_utils + dbt_expectations
dbt build                   # run models AND tests; a severity:error failure halts here
dbt test --select fct_orders   # run just this model's tests
dbt test --store-failures      # force-store failing rows for all tests this run
```

`dbt build` (not `dbt run`) is the command that gates: it interleaves models and their tests so a failing test stops downstream models from building — the dbt-layer equivalent of the Airflow `raise`.

---

## 7. GX or dbt? Picking the tool per boundary

Both can check nulls, ranges, and sets. The honest division of labor:

| Use **Great Expectations** when… | Use **dbt tests** when… |
|---|---|
| Checking **raw data before it enters dbt** (the ingestion boundary, pre-staging). | Checking **models dbt builds** (staging, intermediate, marts). |
| The data is a **file / dataframe / non-dbt source** (CSV, Parquet, a pandas frame from an API). | The data is a **dbt model or source** already in your warehouse. |
| You want **rich Data Docs** as a standalone validation artifact. | You want tests **co-located with the models** and run by `dbt build`. |
| You need **distribution expectations** (mean, cardinality) out of the box. | You need **referential integrity** (`relationships`) — dbt does it cleanly. |

The two-boundary pattern from Lecture 1 §4 maps onto this directly: **GX at ingestion, dbt tests in transformation, and source-freshness (Lecture 3) at the source boundary.** Don't pick one tool for everything; pick the tool that owns the boundary.

---

## 8. Where this goes

You now have both gates: a GX suite + checkpoint for the ingestion boundary, and dbt tests (with severity and `store_failures`) for the transformation layer. Lecture 3 adds the third boundary — **source freshness** — plus the data contract that *defines* what these checks should assert, and the volume/distribution anomaly detection and observability that turn checks into early warning. The mini-project wires the GX checkpoint into Airflow so it halts the DAG.

---

## References

- **Great Expectations — documentation home** (GX Core 1.x; confirm the version selector): <https://docs.greatexpectations.io/docs/>
- **Great Expectations — Expectations Gallery** (the full catalog of expectation classes): <https://greatexpectations.io/expectations/>
- **dbt — Add data tests to your DAG** (generic + singular tests, severity, `store_failures`): <https://docs.getdbt.com/docs/build/data-tests>
- **dbt — documentation home**: <https://docs.getdbt.com/>
- **dbt-utils** (the `expression_is_true`, `accepted_range`, `unique_combination_of_columns` tests): <https://github.com/dbt-labs/dbt-utils>
- **dbt-expectations** (the GX expectation vocabulary ported into dbt): <https://github.com/calogica/dbt-expectations>
