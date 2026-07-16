# Lecture 1 — Analytics-as-code: models, sources, refs, and the layering DAG

> **Time:** ~3 hours of reading + a `dbt init` / `dbt run` loop against DuckDB.
> **Prerequisites:** Week 1 (star schema, grain, SCD-2 in Postgres), Week 2 (analytical SQL), Week 4 (DAGs and idempotent re-runs). dbt-core 1.8 and dbt-duckdb 1.8 installed (`pip install "dbt-core==1.8.*" "dbt-duckdb==1.8.*"`).
> **Citations:** dbt projects <https://docs.getdbt.com/docs/build/projects>; models <https://docs.getdbt.com/docs/build/models>; sources <https://docs.getdbt.com/docs/build/sources>; `ref` <https://docs.getdbt.com/reference/dbt-jinja-functions/ref>; how-we-structure <https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview>; dbt-duckdb <https://github.com/duckdb/dbt-duckdb>.

If you only remember one thing from this lecture, remember this:

> **dbt is not a database. It is a compiler and an orchestrator for SQL. You write `SELECT` statements that declare their inputs with `{{ source() }}` and `{{ ref() }}`; dbt parses every file, builds a dependency DAG from those calls, compiles each model into a plain `CREATE TABLE`/`CREATE VIEW` statement, and runs them against your warehouse in topological order. Everything dbt gives you — build ordering, lineage, the docs site, incremental builds, graph-based selection — comes from that one act of declaring dependencies instead of hard-coding table names.**

In Week 3 your transformation SQL was a Python string. In Week 4 you wrapped that string in an Airflow task and drew the dependency edges by hand in the DAG file. dbt removes both pain points: the SQL becomes a versioned file, and the edges are *inferred* from the `ref`s you already have to write to make the query work. You do not maintain a DAG; you maintain models, and the DAG is a consequence.

---

## 1. Why analytics-as-code wins — the four-axis argument

"Analytics-as-code" is the claim that your transformation logic should be treated exactly like application code: in version control, reviewed, tested, and documented. The argument is easiest to make by listing what hand-run SQL lacks, and you have felt each absence already.

**Version control.** In Week 3, the SQL that built your fact table was a string literal inside `loader.py`. If you changed `COALESCE(amount, 0)` to `amount`, the diff was buried in a Python file and the *intent* of the change was invisible. As a dbt model, that logic is `models/marts/fct_orders.sql` — a file whose entire history is `git log`, whose every change is a reviewable diff, and whose blame points at a person and a commit. The most valuable code in the platform finally lives where the rest of your code lives.

**Code review.** Nobody reviewed your Week 3 SQL the way they reviewed your DAG, because it was not in a place a reviewer naturally reads. dbt models are files in the repo; a pull request that changes `dim_customer.sql` shows up in review like any other code, and a reviewer can comment on a join condition the same way they comment on a function.

**Tests.** When your Week 1 Type-2 SCD logic was subtly wrong — say it opened a new row on *every* load instead of only on attribute change — nothing failed. The bad rows landed and an analyst found them three weeks later. With dbt you attach `unique` to the natural key and a singular test for "no customer has two current rows," and the next `dbt build` exits non-zero the moment the logic breaks. Bad data is caught at build time, by the build, not by a human at report time.

**Lineage.** When an analyst asked "where does `dim_customer.segment` come from," your only answer in Week 3 was "read `loader.py`." dbt knows the answer because the `ref`/`source` graph *is* the lineage: `dim_customer` ← `int_customer_enriched` ← `stg_customers` ← `source('raw','customers')`. `dbt docs serve` renders that as a clickable graph (§7), and during an incident it is the first thing you open.

The dbt docs frame this as "transformation as a software-engineering workflow" (<https://docs.getdbt.com/docs/build/projects>). The four axes above are why every serious analytics team adopts it.

---

## 2. The dbt mental model — compile, then run

dbt has exactly two jobs.

1. **Compile.** dbt reads every `.sql` file in `models/`, expands the Jinja (`{{ ref() }}`, `{{ source() }}`, `{{ config() }}`, macros) into a final SQL string, and wraps it in the DDL appropriate to the model's materialization. A `view`-materialized model `stg_orders.sql` containing `select * from {{ source('raw','orders') }}` compiles to `create or replace view "main"."stg_orders" as (select * from "raw"."orders");`. The compiled SQL lands in `target/compiled/` — read it when something surprises you.
2. **Run.** dbt executes those compiled statements against the warehouse named in your profile, **in dependency order** derived from the DAG.

dbt never touches a byte of your data itself. It hands SQL to DuckDB (or Postgres, Snowflake, BigQuery, Spark — the *adapter* differs, the project does not) and the warehouse does the work. This is why the same project runs against any backend with a one-line profile change, and why dbt scales with your warehouse rather than being a bottleneck itself (<https://docs.getdbt.com/docs/build/models>).

This week the warehouse is **DuckDB**, an in-process columnar analytical database — think "SQLite for analytics." The `dbt-duckdb` adapter (<https://github.com/duckdb/dbt-duckdb>) runs the whole project against a single `.duckdb` file on disk, no server. The develop loop is seconds.

---

## 3. Project anatomy — `dbt init`, `dbt_project.yml`, `profiles.yml`

Scaffold a project:

```bash
dbt init crunch_warehouse
# prompts for adapter -> choose duckdb
cd crunch_warehouse
```

A dbt project is a directory with a specific shape. The pieces that matter this week:

```text
crunch_warehouse/
├── dbt_project.yml         # project config: name, paths, default materializations
├── models/                 # the .sql models, organized into layer subfolders
│   ├── staging/
│   ├── intermediate/
│   └── marts/
├── seeds/                  # static CSVs loaded with `dbt seed`
├── snapshots/              # SCD-2 snapshot definitions
├── macros/                 # reusable Jinja/SQL functions
├── tests/                  # singular (.sql) tests
└── target/                 # compiled SQL + run artifacts (gitignored)
```

`dbt_project.yml` is the project's control file. A minimal, opinionated one for this course:

```yaml
name: 'crunch_warehouse'
version: '1.0.0'
config-version: 2
profile: 'crunch_warehouse'        # which profile in profiles.yml to use

model-paths: ["models"]
seed-paths: ["seeds"]
snapshot-paths: ["snapshots"]
macro-paths: ["macros"]
test-paths: ["tests"]
target-path: "target"

models:
  crunch_warehouse:
    staging:
      +materialized: view          # staging is cheap, recompute on read
    intermediate:
      +materialized: ephemeral     # intermediate inlines into marts
    marts:
      +materialized: table         # marts are queried often, materialize them
```

Note the layered defaults: every model under `models/staging/` is a view unless overridden, every model under `models/marts/` is a table. You set materialization *once per layer* here and only override per model when a model is special. We cover materializations in Lecture 2.

`profiles.yml` lives in `~/.dbt/profiles.yml` (outside the repo, because it can hold credentials) and tells dbt *where* to run. For DuckDB it is trivial — the "warehouse" is a file:

```yaml
crunch_warehouse:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: 'crunch_warehouse.duckdb'   # the whole warehouse, one file
      threads: 4
      # optional: attach a Postgres source DB read-only, or load extensions
      extensions:
        - httpfs
        - parquet
```

Verify the wiring before writing a single model:

```bash
dbt debug
# Checks: dbt version, profile found, profile valid, connection OK, project parses.
```

If `dbt debug` is green, the connection works and the project parses. If it is not, fix it now — every later command depends on it.

---

## 4. Sources — declaring the raw tables dbt does not own

Your raw data — the retail extracts from Week 1 — is loaded into the warehouse by something *upstream* of dbt (your Week 3 Python loader, or for this lab a `seed`/`read_parquet` into a `raw` schema). dbt does not create these tables; it *reads* them. You declare them as **sources** so that (a) models reference them through a stable name, (b) dbt can monitor their freshness, and (c) they appear in lineage as the true origins (<https://docs.getdbt.com/docs/build/sources>).

A source is defined in a YAML file under `models/staging/` (convention: `_sources.yml` or `src_*.yml`):

```yaml
version: 2

sources:
  - name: raw                          # the source "group"
    description: "Raw retail extracts landed by the upstream loader."
    database: crunch_warehouse         # DuckDB catalog
    schema: raw                        # the schema the raw tables live in
    freshness:                         # default freshness for all tables here
      warn_after: {count: 24, period: hour}
      error_after: {count: 48, period: hour}
    loaded_at_field: _loaded_at        # the column that timestamps each load

    tables:
      - name: customers
        description: "One row per customer as extracted from the OLTP system."
        columns:
          - name: customer_id
            description: "Natural key from the source system."
            tests:
              - not_null
              - unique
      - name: orders
        description: "One row per order header."
        freshness:                     # override per-table if needed
          warn_after: {count: 12, period: hour}
      - name: order_items
        description: "One row per line item; grain is (order_id, line_number)."
```

In a model you reference a source like this:

```sql
select * from {{ source('raw', 'orders') }}
```

which compiles to the fully-qualified `"crunch_warehouse"."raw"."orders"`. You never type the schema name in a model again — change where `raw` lives once, in YAML, and every model follows.

**Freshness** is a quality signal you get for free. `loaded_at_field` names a timestamp column; `warn_after` / `error_after` define how stale is too stale. Then:

```bash
dbt source freshness
# 13:02:11  1 of 3 START freshness of raw.customers ...
# 13:02:11  1 of 3 PASS freshness of raw.customers  [PASS in 0.04s]
# 13:02:11  2 of 3 WARN freshness of raw.orders     [WARN in 0.03s]   <- stale beyond warn_after
```

A `WARN` or `ERROR` here tells you the *input* is stale before you waste a build on old data. This is the dbt complement to the high-water-mark thinking from Week 3 — there you watched the watermark advance; here dbt watches the source's `loaded_at` for you.

---

## 5. Refs and the DAG — how dbt knows the build order

The other Jinja function — and the more important one — is `{{ ref() }}`. When one model reads another model, you never write its table name; you write `{{ ref('stg_orders') }}`:

```sql
-- models/intermediate/int_order_items_joined.sql
select
    oi.order_id,
    oi.line_number,
    oi.product_id,
    o.customer_id,
    o.order_ts,
    oi.quantity,
    oi.unit_price_cents
from {{ ref('stg_order_items') }} as oi
join {{ ref('stg_orders') }}      as o using (order_id)
```

This does two things at once. First, it compiles to the correct fully-qualified name for `stg_order_items` *in the current target* — so the same model points at `dev` tables when you run dev and `prod` tables when you run prod, with no edits (<https://docs.getdbt.com/reference/dbt-jinja-functions/ref>). Second, and crucially, it **declares an edge in the DAG**: dbt now knows `int_order_items_joined` depends on `stg_order_items` and `stg_orders`. dbt parses every `ref` and `source` across the whole project and assembles a directed acyclic graph. That DAG drives everything:

- **`dbt run`** builds models in topological order — a model never runs before its inputs exist.
- **Graph selectors** let you build slices of the DAG:

```bash
dbt run --select stg_orders          # just this one model
dbt run --select stg_orders+         # stg_orders and everything DOWNSTREAM of it
dbt run --select +fct_orders         # fct_orders and everything UPSTREAM of it
dbt run --select staging             # everything in the staging folder
dbt run --select tag:nightly         # everything tagged nightly
```

The `+` is a graph operator: `model+` is "model and its descendants," `+model` is "model and its ancestors." This is how you rebuild exactly the affected subgraph after a change instead of the whole warehouse.

A circular `ref` is a hard error — dbt detects the cycle at parse time and refuses to run, because a DAG with a cycle has no valid build order. This is the same acyclic constraint you met in Week 4's Airflow DAGs, enforced here automatically.

---

## 6. The staging / intermediate / mart layering pattern

This is the most important convention in the week, and the reason every serious warehouse looks structurally similar. dbt Labs documents it as the reference structure (<https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview>). The pattern decomposes the monolithic transformation you wrote in Week 1 into three layers, each with one job.

### 6.1 Staging (`stg_*`) — one model per source table, the cleanup layer

A staging model does exactly one thing: take **one** source table and clean it. Rename columns to your conventions, cast types, apply trivial standardizations (uppercase a country code, coalesce a null to a sensible default). **No joins. No business logic. No aggregation.** One staging model per source table, one to one.

```sql
-- models/staging/stg_customers.sql
with source as (
    select * from {{ source('raw', 'customers') }}
),

renamed as (
    select
        customer_id,
        trim(lower(email))            as email,
        initcap(first_name)           as first_name,
        initcap(last_name)            as last_name,
        upper(country_code)           as country_code,
        coalesce(segment, 'unknown')  as segment,
        updated_at,
        _loaded_at
    from source
)

select * from renamed
```

Why so disciplined? Because staging is the layer the rest of the project trusts. If every downstream model can assume `country_code` is uppercase and `segment` is never null, you write that logic *once*, here, and never again. Staging models are cheap to recompute, so they materialize as **views** by default — no storage, always reflecting the latest source.

### 6.2 Intermediate (`int_*`) — compose staging into business concepts

Intermediate models are where joins and reusable business logic live. They take staging models as inputs (via `ref`) and produce a concept that more than one mart will want: an enriched order, a customer with their lifetime metrics, a de-duplicated event stream. Name them after the *transformation* (`int_orders_joined_to_customers`), not the output table.

```sql
-- models/intermediate/int_orders_enriched.sql
with orders as (
    select * from {{ ref('stg_orders') }}
),
items as (
    select
        order_id,
        sum(quantity * unit_price_cents) as gross_cents,
        count(*)                         as line_count
    from {{ ref('stg_order_items') }}
    group by 1
)

select
    o.order_id,
    o.customer_id,
    o.order_ts,
    i.gross_cents,
    i.line_count
from orders as o
join items  as i using (order_id)
```

Intermediate models often materialize as **ephemeral** — they are inlined as CTEs into the marts that use them and never become a database object. They exist for code organization and reuse, not as something you query directly. (More on ephemeral in Lecture 2.)

### 6.3 Marts (`dim_*`, `fct_*`) — the dimensional layer an analyst queries

Marts are the final, dimensional shapes — the star schema from Week 1, now built by dbt. Dimensions get surrogate keys; facts reference dimensions by those keys at a declared grain.

```sql
-- models/marts/dim_customer.sql
with customers as (
    select * from {{ ref('stg_customers') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,  -- macro, Lecture 3
    customer_id,
    email,
    first_name,
    last_name,
    country_code,
    segment
from customers
```

```sql
-- models/marts/fct_orders.sql  (grain: one row per order)
with orders as (
    select * from {{ ref('int_orders_enriched') }}
),
customers as (
    select customer_sk, customer_id from {{ ref('dim_customer') }}
)

select
    o.order_id,
    c.customer_sk,                 -- FK to dim_customer by surrogate key
    o.order_ts::date as order_date,
    o.gross_cents,
    o.line_count
from orders as o
join customers as c using (customer_id)
```

Marts materialize as **tables** because analysts query them constantly and you do not want to recompute the joins on every query. The grain is fixed and documented — exactly the discipline from Week 1, now enforced by a `unique` test on `order_id` (Lecture 2).

### 6.4 Why the layering pays off

Each layer is independently testable: you test that staging cleaned the data, that intermediate composed it correctly, and that the mart has the right grain and keys. Each transformation is a small, reviewable diff instead of one 200-line monster. And the DAG reads top to bottom — sources → staging → intermediate → marts — so the lineage graph is legible. A new engineer can open `dbt docs serve`, click `fct_orders`, and trace it back to the raw source without reading a line of Python. That legibility is the entire return on the discipline.

---

## 7. A first end-to-end run

With sources declared and a few models written, the full loop is three commands:

```bash
dbt seed     # load any static CSV lookups (Lecture 3)
dbt run      # build all models in DAG order
dbt test     # run all tests (Lecture 2)
```

`dbt run` output reads like a build log, in dependency order:

```text
13:40:02  Running with dbt=1.8.0
13:40:02  Found 7 models, 2 seeds, 14 data tests, 3 sources
13:40:03  1 of 7 START sql view  model main.stg_customers ........ [RUN]
13:40:03  1 of 7 OK created sql view model main.stg_customers ... [OK in 0.05s]
13:40:03  2 of 7 START sql view  model main.stg_orders .......... [RUN]
...
13:40:03  6 of 7 START sql table model main.dim_customer ........ [RUN]
13:40:03  7 of 7 START sql table model main.fct_orders .......... [RUN]
13:40:03  Finished running 4 view models, 3 table models in 0.9s.
13:40:03  Completed successfully
13:40:03  Done. PASS=7 WARN=0 ERROR=0 SKIP=0 TOTAL=7
```

Note that staging views build before the marts that `ref` them — you wrote no ordering, dbt derived it from the graph. The compiled SQL for any model is in `target/compiled/crunch_warehouse/models/...`; read it the first time `{{ ref() }}` surprises you, and you will see it resolved to a plain qualified name.

---

## 8. Summary

- **dbt is a compiler/orchestrator for SQL, not a database.** It expands Jinja, wraps each model in DDL, and runs it against your warehouse (DuckDB this week) in DAG order. The same project runs anywhere with a profile change.
- **Analytics-as-code wins on four axes** hand-run SQL lacks: version control, code review, tests, and lineage. You have felt the absence of each in Weeks 1–4.
- **`{{ source() }}`** declares raw inputs dbt reads but does not own, gives them stable names, and enables freshness checks (`dbt source freshness`).
- **`{{ ref() }}`** points at another model *and* declares a DAG edge. dbt infers the entire dependency graph from refs and sources — you never draw the edges by hand.
- **Graph selectors** (`model+`, `+model`, `staging`, `tag:`) let you build exactly the affected subgraph.
- **The staging / intermediate / mart layering** decomposes one monolithic transform into small, testable layers: `stg_*` cleans one source (views, no joins), `int_*` composes business concepts (often ephemeral), `dim_*`/`fct_*` are the dimensional star (tables).
- `dbt_project.yml` sets per-layer materialization defaults; `profiles.yml` (in `~/.dbt/`) names the warehouse; `dbt debug` verifies the wiring.
- The DAG you build from refs is the same acyclic dependency idea as Week 4's Airflow — but here it is inferred, not authored.

*Cited pages: dbt projects <https://docs.getdbt.com/docs/build/projects>; models <https://docs.getdbt.com/docs/build/models>; sources <https://docs.getdbt.com/docs/build/sources>; ref <https://docs.getdbt.com/reference/dbt-jinja-functions/ref>; how-we-structure <https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview>; dbt-duckdb <https://github.com/duckdb/dbt-duckdb>.*
