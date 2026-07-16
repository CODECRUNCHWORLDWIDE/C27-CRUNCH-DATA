# Lecture 3 — Docs, lineage, seeds, macros, and dbt-core vs dbt Cloud

> **Time:** ~2.5 hours of reading + a `dbt docs generate` / `dbt docs serve` loop and writing one macro.
> **Prerequisites:** Lectures 1 and 2 (project anatomy, refs/sources, layering, materializations, tests, snapshots).
> **Citations:** documentation <https://docs.getdbt.com/docs/build/documentation>; jinja & macros <https://docs.getdbt.com/docs/build/jinja-macros>; seeds <https://docs.getdbt.com/docs/build/seeds>; how-we-structure <https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview>; dbt-core GitHub <https://github.com/dbt-labs/dbt-core>; DuckDB docs <https://duckdb.org/docs/>.

If you only remember one thing from this lecture, remember this:

> **Documentation and lineage are not a separate deliverable — they are a *projection* of the metadata you already wrote. The `description:` fields in your `schema.yml` plus the `ref`/`source` DAG you built in Lecture 1 compile, via `dbt docs generate`, into a static site with a clickable lineage graph. Seeds and macros round out the project: a seed is a version-controlled CSV lookup table, and a macro is a Jinja function that returns SQL so you stop copy-pasting the same cast or hash. All of it is dbt-core, the open-source CLI; dbt Cloud is the same compiler with a hosted operator surface bolted on.**

This lecture is the part of dbt that turns a working transformation into a *legible, governed* one — the difference between "the models run" and "a new engineer can understand and trust the warehouse without asking you."

---

## 1. Documentation — descriptions live next to the data

You have already been writing documentation: every `description:` in the `sources.yml` and `schema.yml` from Lectures 1–2 is a doc string. dbt collects all of it. The discipline is to describe the *model's grain and meaning* and *each non-obvious column*, right where the tests live, so docs never drift from the code (<https://docs.getdbt.com/docs/build/documentation>).

```yaml
# models/marts/_marts.yml
version: 2

models:
  - name: fct_orders
    description: >
      Order fact table. **Grain: one row per `order_id`.** One order may contain
      many line items; those are aggregated to the order header here. Built from
      `int_orders_enriched` joined to `dim_customer` on the natural key to attach
      the surrogate FK.
    columns:
      - name: order_id
        description: "Natural key from the source OLTP system; the grain of this table."
        tests: [unique, not_null]
      - name: customer_sk
        description: "Surrogate FK into `dim_customer`. Resolved from `customer_id`."
        tests:
          - not_null
          - relationships: {to: ref('dim_customer'), field: customer_sk}
      - name: gross_cents
        description: "Order total in integer cents. Sum of `quantity * unit_price_cents` across line items."
```

### 1.1 Doc blocks — write prose once, reference it many times

For longer descriptions reused across models (a metric definition, a grain explanation), use a **doc block** in a `.md` file and reference it with the `doc()` Jinja function:

```markdown
<!-- models/docs.md -->
{% docs gross_cents %}
Order total expressed in **integer cents** to avoid floating-point rounding.
Computed as `sum(quantity * unit_price_cents)` over all line items of the order.
Divide by 100 for dollars; never store dollars as a float.
{% enddocs %}
```

```yaml
      - name: gross_cents
        description: '{{ doc("gross_cents") }}'   # reference the block
```

Now the definition lives in one place and every model that uses `gross_cents` shows the same authoritative description.

---

## 2. `dbt docs generate` and the lineage graph

Two commands turn all that metadata into a website:

```bash
dbt docs generate    # compiles models + descriptions + the DAG into target/catalog.json + manifest.json
dbt docs serve       # serves the static site on http://localhost:8080
```

`dbt docs generate` does two things: it reads your project metadata (descriptions, tests, configs) and it queries the warehouse for the *actual* column names and types of every built model (the "catalog"). `dbt docs serve` opens a browser site where every model has a page showing its description, its columns and their docs, its tests, its compiled SQL, and — the centerpiece — a **lineage graph** (<https://docs.getdbt.com/docs/build/documentation>).

### 2.1 Reading the lineage graph

The lineage graph is the `ref`/`source` DAG from Lecture 1, rendered visually. You did not draw it; it is *inferred* from your `ref` and `source` calls. For the warehouse we have been building it reads left to right:

```text
source(raw,customers) ──> stg_customers ──┐
                                          ├──> customers_snapshot ──> dim_customer ──┐
source(raw,orders) ──────> stg_orders ────┤                                         ├──> fct_orders
source(raw,order_items) ─> stg_order_items┘──> int_orders_enriched ─────────────────┘
```

The skills you practice reading it:

- **Trace upstream.** Click `fct_orders`; the graph highlights its ancestors. The answer to "where does this number come from" is now a click, not a code archaeology session. This is the lineage axis from Lecture 1's analytics-as-code argument, made literal.
- **Trace downstream.** Click `stg_orders`; the graph highlights descendants — everything that breaks if you change this model. This is the *blast radius* of a change, the thing you check before merging a staging refactor.
- **Map graph selectors to the picture.** `dbt run --select stg_orders+` builds `stg_orders` and the highlighted-downstream subgraph; `dbt run --select +fct_orders` builds `fct_orders` and its highlighted-upstream ancestors. The `+` operator and the graph are the same object viewed two ways.
- **Spot structural smells.** A mart that `ref`s a source directly (skipping staging) shows as an edge that jumps a layer — a layering violation you can see. A staging model with many incoming source edges is doing joins it should not (staging is one-source-per-model).

The lineage graph is the artifact you screenshot for the mini-project and the first tool you reach for in Week 11's incident drills.

---

## 3. Seeds — version-controlled CSV lookups

A **seed** is a small CSV file in `seeds/` that dbt loads into the warehouse as a table with `dbt seed` (<https://docs.getdbt.com/docs/build/seeds>). Seeds are for **static reference data you control and want in version control** — not for loading production data (that is what your loader/sources are for).

Good seed candidates: a country-code-to-region mapping, a segment-rename lookup, a list of test customer IDs to exclude, a manually-curated date spine. Bad seed candidates: anything large, anything that changes outside your control, anything with PII.

```text
# seeds/country_region.csv
country_code,region
US,North America
CA,North America
GB,Europe
DE,Europe
JP,Asia Pacific
```

```yaml
# seeds/_seeds.yml — type and document seeds like any model
version: 2
seeds:
  - name: country_region
    description: "Static map from ISO country code to sales region. Edit in git, reload with dbt seed."
    config:
      column_types:
        country_code: varchar
        region: varchar
    columns:
      - name: country_code
        tests: [unique, not_null]
```

```bash
dbt seed     # loads seeds/*.csv into the warehouse as tables
```

Reference a seed exactly like a model, with `{{ ref() }}`:

```sql
-- models/intermediate/int_customers_with_region.sql
select
    c.customer_id,
    c.country_code,
    r.region
from {{ ref('stg_customers') }} as c
left join {{ ref('country_region') }} as r using (country_code)
```

Because the CSV is in git, a change to the region mapping is a reviewable diff, and `dbt seed && dbt build` reproduces the whole warehouse deterministically. That is the seed's whole value: small lookup data that is *code*, not a manual `INSERT`.

---

## 4. Macros — Jinja functions that return SQL

dbt compiles models by expanding Jinja. A **macro** is a reusable Jinja function, defined in `macros/`, that returns a string of SQL (<https://docs.getdbt.com/docs/build/jinja-macros>). Macros are how you stop copy-pasting the same expression across models.

### 4.1 A simple macro — DRY up a repeated cast

```sql
-- macros/cents_to_dollars.sql
{% macro cents_to_dollars(column_name, precision=2) %}
    round( ({{ column_name }} / 100.0)::numeric, {{ precision }} )
{% endmacro %}
```

Call it in any model:

```sql
select
    order_id,
    {{ cents_to_dollars('gross_cents') }} as gross_dollars
from {{ ref('fct_orders') }}
```

It compiles to `round((gross_cents / 100.0)::numeric, 2) as gross_dollars`. Change the rounding rule once, in the macro, and every model follows. This is the same DRY discipline as a Python helper function — now for SQL.

### 4.2 A surrogate-key macro — and the `dbt_utils` package

Generating a deterministic surrogate key from natural-key columns is so common that the community package `dbt_utils` ships a macro for it. Add the package:

```yaml
# packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.1.0", "<2.0.0"]
```

```bash
dbt deps     # install declared packages into dbt_packages/
```

Then `{{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }}` produces a hash of those columns — exactly the surrogate key you used in Lecture 2's SCD-aware `dim_customer`. It is just a macro; you could write it yourself with `md5(concat(...))`, but `dbt_utils` handles null-safety and cross-warehouse hashing for you. Writing your own version is Challenge 02.

### 4.3 Macros power custom generic tests

A generic test (Lecture 2) *is* a macro with a special signature — it takes `model` and `column_name` and returns a `SELECT` of offending rows. Challenge 02 has you write `assert_positive` and an `accepted_range` test this way; the mechanism is the macro you just learned. A macro that returns "the rows that violate my rule" is a custom test; a macro that returns "a SQL expression" is a helper. Same tool, two uses.

---

## 5. dbt-core vs dbt Cloud — the honest trade

We use **dbt-core** in this course, exclusively. It is worth being precise about what that means and what dbt Cloud adds, so you can make the call yourself later.

**dbt-core** is the open-source command-line tool you `pip install` (Apache-2.0 licensed; source at <https://github.com/dbt-labs/dbt-core>). It does the entire job: compile, run, test, snapshot, build, seed, docs generate. It runs anywhere Python runs — your laptop, a CI runner, a container in your Week 4 Airflow DAG. Everything in C27 is dbt-core. There is no feature in this course that dbt-core cannot do.

**dbt Cloud** is dbt Labs' commercial hosted product. It runs the *same* dbt-core engine under the hood and adds an operator surface around it:

- A **hosted scheduler** (run jobs on a cron without standing up your own orchestrator).
- A **browser-based IDE** with autocomplete and an integrated lineage view.
- A **hosted docs site** (no `dbt docs serve` on your machine).
- A **CI integration** that runs `dbt build` on pull requests against a temporary schema.
- A **metadata/semantic layer** API for downstream BI tools.

The honest framing: dbt Cloud is **convenience and managed infrastructure, not capability**. Every transformation, test, snapshot, and doc you can express, you express in dbt-core; Cloud spares you from running the scheduler, the IDE, the docs host, and the CI plumbing yourself. For a team without a platform group, that convenience is worth paying for. For C27 — laptop-local, open-source-first, no vendor lock-in, and you already *have* an orchestrator from Week 4 — dbt-core is the right call. You will run `dbt build` from Airflow exactly as you would run any other CLI tool, and host the docs as static files. When you join a team on dbt Cloud, nothing you learned changes; you just stop typing the commands yourself.

This is the same pattern C27 takes with every cloud product (Snowflake, Databricks, Confluent): learn the open-source engine, name the managed product, understand the trade, refuse the lock-in.

### 5.1 Running dbt-core in CI — the part dbt Cloud automates

The one piece of dbt Cloud worth reproducing yourself is **CI on pull requests**: when someone opens a PR that changes a model, you want `dbt build` to run against a temporary schema and block the merge if any test fails. With dbt-core this is a few lines in your CI runner (GitHub Actions, GitLab CI, the same place your Week 15-style pipelines live):

```yaml
# .github/workflows/dbt-ci.yml (sketch)
steps:
  - run: pip install "dbt-core==1.8.*" "dbt-duckdb==1.8.*"
  - run: dbt deps
  - run: dbt build --target ci        # ci target writes to a throwaway schema/file
```

The principle: a PR that breaks a test never merges, because `dbt build` exits non-zero and the CI job fails. That is the analytics-as-code loop closed — code review *plus* automated tests gate every change to the warehouse, exactly as they gate application code. dbt Cloud gives you this with a checkbox; dbt-core gives you this with a workflow file you own. For C27, owning the workflow file is the point.

A second hygiene habit: `dbt-core` ships a `state:` selection method. After saving a `manifest.json` from your production run, `dbt build --select state:modified+` builds *only the models a PR changed and their descendants* — a "slim CI" that does not rebuild the whole warehouse on every commit. This is how large dbt projects keep CI fast, and it is dbt-core, not a Cloud feature.

---

## 6. Putting it together — the full command surface

The complete dbt-core loop you will run this week and in the mini-project:

```bash
dbt deps               # install packages (dbt_utils)
dbt seed               # load static CSV lookups
dbt source freshness   # check raw inputs are not stale (Lecture 1)
dbt snapshot           # capture Type-2 SCD history (Lecture 2)
dbt build              # run + test all models, interleaved by DAG (Lecture 2)
dbt docs generate      # compile docs + catalog + lineage
dbt docs serve         # browse the site and the lineage graph
```

`dbt build` is the workhorse; the others bracket it — `deps`/`seed`/`snapshot` prepare inputs, `docs` projects the result. In your Week 4 Airflow DAG these become tasks with the same dependency edges dbt already knows, and a non-zero exit from `dbt build` fails the DAG. That is the whole pipeline: analytics-as-code, tested, snapshotted, documented, and orchestrated, running against DuckDB on your laptop with no cloud account.

---

## 7. Summary

- **Documentation is a projection of metadata you already wrote.** `description:` fields in `schema.yml`/`sources.yml`, plus reusable `{% docs %}` blocks referenced with `doc()`, compile into the docs site. Describe grain and non-obvious columns where the tests live so docs cannot drift.
- **`dbt docs generate` + `dbt docs serve`** build a static site whose centerpiece is the **lineage graph** — the `ref`/`source` DAG rendered visually, not drawn by hand. Use it to trace upstream ("where did this number come from"), trace downstream ("what breaks if I change this"), map graph selectors to the picture, and spot layering smells.
- **Seeds** are version-controlled CSV lookups loaded with `dbt seed` and referenced with `{{ ref() }}` — static reference data as code, not a manual `INSERT`. Not for production data.
- **Macros** are Jinja functions that return SQL. Use them to DRY up repeated expressions (`cents_to_dollars`) and to generate surrogate keys (`dbt_utils.generate_surrogate_key`). A custom *generic test* is just a macro that returns offending rows (Challenge 02). Install community packages with `packages.yml` + `dbt deps`.
- **dbt-core vs dbt Cloud:** same compiler, different operator surface. Core is the open-source CLI that does everything in this course; Cloud adds a hosted scheduler, browser IDE, hosted docs, and CI — convenience, not capability. C27 uses core: laptop-local, lock-in-free, orchestrated by your own Week 4 Airflow DAG.
- The full loop: `dbt deps` → `dbt seed` → `dbt source freshness` → `dbt snapshot` → `dbt build` → `dbt docs generate`/`serve`.

*Cited pages: documentation <https://docs.getdbt.com/docs/build/documentation>; jinja & macros <https://docs.getdbt.com/docs/build/jinja-macros>; seeds <https://docs.getdbt.com/docs/build/seeds>; how-we-structure <https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview>; dbt-core GitHub <https://github.com/dbt-labs/dbt-core>; DuckDB docs <https://duckdb.org/docs/>.*
