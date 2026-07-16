# Week 5 — Transformation with dbt (analytics-as-code: models, sources, refs, layering, materializations, tests, snapshots, docs, lineage)

Phase I ended with a pipeline you could be proud of operationally: an Airflow DAG that lands a daily extract into a Postgres star schema, runs incrementally off a high-water mark, survives a re-run without double-counting, and backfills thirty days cleanly. But look closely at *what that pipeline actually transforms*. The dimension conformance, the surrogate-key assignment, the Type-2 SCD logic, the fact-table grain enforcement — in Weeks 1 through 4 all of that lived in hand-written SQL strings inside Python tasks, or in `psql` scripts you ran by hand and pasted into a runbook. That SQL is the most valuable code in the whole platform and it is also the least governed: it is not under code review the way your Python is, it has no automated tests, its dependencies on other tables are implicit and discoverable only by reading every file, and the only documentation is whatever you remembered to write in a comment. This week we fix that, and the tool that fixes it is **dbt** (the data build tool). dbt is the industry-standard answer to a single question — *how do you make analytical SQL a first-class software artifact?* — and the answer it gives is **analytics-as-code**: every transformation is a versioned `.sql` file, dependencies are declared with `{{ ref() }}` and `{{ source() }}` so dbt can build a dependency graph for you, correctness is enforced by tests that fail a build, and documentation plus column-level lineage fall out of the same metadata you were already writing.

This is the opening week of Phase II — *The Lakehouse & Distributed Compute* — and the order is deliberate. Before we touch Parquet, Iceberg, Spark, or Kafka, we settle the question of *how transformation is expressed*, because that question is independent of where the bytes live. The same dbt project you write this week against DuckDB will, with a one-line profile change, run against Postgres, Snowflake, BigQuery, or Spark. dbt is a **compiler and an orchestrator for SQL**, not a database; it does not store or move data, it generates SQL and hands it to whatever warehouse you point it at. That separation is the whole point, and it is why dbt sits at the center of the modern data stack. We run it against **DuckDB** via the open-source `dbt-duckdb` adapter, which means the entire transformation layer runs in a single embedded analytical database on your laptop, no server, no cloud account, no credit card — exactly the constraint C27 holds itself to.

Here are the things to internalize this week, each connecting to something you already built.

**One — analytics-as-code beats hand-run SQL on four axes, and you have already felt the absence of each.** Version control: in Week 3 your transform SQL was a string literal; if you changed it, the diff was invisible and the old version was gone. Code review: nobody reviewed that SQL the way they reviewed your DAG. Tests: when the SCD logic was subtly wrong, nothing failed — the bad rows just landed. Lineage: when an analyst asked "where does `dim_customer.segment` come from," the only answer was "read the loader." dbt gives you all four for free, because every model is a file in git, every dependency is declared, and every assertion is a test that exits non-zero when violated. The lecture spine of this week is exactly this argument, made concretely.

**Two — `{{ source() }}` and `{{ ref() }}` are not cosmetic; they are how dbt knows the build order.** A model never hard-codes a table name. It declares its inputs: raw tables come in through `{{ source('raw', 'orders') }}`, and other dbt models come in through `{{ ref('stg_orders') }}`. dbt parses every model, extracts these calls, and builds a **directed acyclic graph** of the entire project. That DAG is what lets `dbt run` build models in dependency order, what lets `dbt run --select stg_orders+` build a model and everything downstream of it, and what renders as the lineage graph in the docs site. This is the same DAG idea you met in Week 4's Airflow — but here you never write the edges by hand; they are inferred from the `ref`s.

**Three — the staging / intermediate / mart layering is the single most important convention in the warehouse, and it solves a problem you hit in Week 1.** In Week 1 you wrote SQL that simultaneously renamed columns, cast types, joined three tables, applied business logic, and shaped a star schema — all in one statement that was impossible to test or reuse. The layering pattern decomposes that: **staging** models (`stg_*`) do one thing — clean and rename one source, one model per source table, no joins, no business logic. **Intermediate** models (`int_*`) compose staging models into reusable business concepts. **Mart** models (`dim_*`, `fct_*`) are the dimensional tables an analyst queries. Every serious warehouse uses some version of this because it makes each layer independently testable and each transformation a small, reviewable diff. dbt Labs documents this as the reference structure (<https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview>).

**Four — materialization is a deployment decision, decoupled from the model's SQL.** The same `SELECT` can be a **view** (default, no storage, recomputed on read), a **table** (rebuilt fully each `dbt run`), an **ephemeral** model (inlined as a CTE into its consumers, never materialized), or **incremental** (only new/changed rows processed each run, via `is_incremental()` and a `unique_key`). You change the materialization in a `config` block or in `dbt_project.yml` without touching the query. Incremental is the dbt expression of exactly the idempotent-incremental discipline you built by hand in Week 3 — the high-water-mark filter becomes an `is_incremental()` block and the upsert becomes a `unique_key` merge.

**Five — tests are the gate, and a failing test exits non-zero on purpose.** dbt ships four built-in **generic** tests — `unique`, `not_null`, `accepted_values`, `relationships` — that you attach to columns in YAML. You also write **singular** tests, which are just `.sql` files that return rows when something is wrong (zero rows returned = pass). When `dbt test` or `dbt build` finds a failure, the process exits non-zero, which is what lets Airflow (or any orchestrator) treat a data-quality failure as a pipeline failure and *halt* rather than land bad data. This is the foundation Week 10's data-quality layer builds on.

**Six — snapshots are dbt's built-in Type-2 SCD, and they are the same idea you hand-built in Week 1, automated.** In Week 1 you wrote effective-dated rows by hand: an `effective_from`, an `effective_to`, a current-flag, surrogate keys, and a `MERGE` that closed the old row and opened a new one when an attribute changed. `dbt snapshot` does exactly that, managing `dbt_valid_from`, `dbt_valid_to`, and a hashed `dbt_scd_id` for you, using either a `timestamp` strategy (trust an `updated_at` column) or a `check` strategy (diff a list of columns). The history table it builds is queryable for "what was this customer's segment on 2026-03-01?" — the audit query you wrote by hand in Week 1, now maintained automatically (<https://docs.getdbt.com/docs/build/snapshots>).

**Seven — documentation and lineage are not a separate chore; they fall out of metadata you already wrote.** The same `schema.yml` that holds your tests holds `description:` fields for models and columns. `dbt docs generate` compiles all of it plus the inferred DAG into a static site, and `dbt docs serve` opens it with a clickable lineage graph. The lineage is not something you draw — it is the `ref`/`source` DAG rendered visually. When an incident hits in Week 11 and someone asks where a number came from, this graph is the first thing you open.

**Eight — `dbt build` interleaves run and test by DAG order, and that ordering is the safety property.** `dbt run` builds models; `dbt test` runs tests; `dbt build` does both *interleaved in dependency order* — it builds a model, runs its tests, and only proceeds to downstream models if those tests pass. That means a bad staging model never silently feeds a mart: the test on the staging model fails first and downstream models are skipped. This is the single command you will wire into your orchestrator.

**Nine — seeds and macros round out the project.** A **seed** is a small CSV you check into the repo and load with `dbt seed` — perfect for the static lookup tables (country codes, segment mappings, a date spine) you do not want to manage as a source. A **macro** is a Jinja function that returns SQL, letting you DRY up repeated logic (a `cents_to_dollars()` cast, a `generate_surrogate_key()` hash) and even override dbt's built-in behavior. Both are version-controlled, reviewable, and testable like everything else.

**Ten — dbt-core (CLI, open-source, what we use) and dbt Cloud (hosted, commercial) are the same compiler with a different operator surface.** dbt-core is the GPL-… actually Apache-2.0-licensed CLI you `pip install`; it does everything in this course. dbt Cloud adds a hosted scheduler, a browser IDE, a managed docs site, and a CI integration — convenience, not capability. We use dbt-core because it runs on your laptop, fits in your own orchestrator (the Airflow DAG from Week 4), and has no vendor lock-in. We name dbt Cloud honestly and tell you exactly what it adds.

**Eleven — DuckDB is the perfect dbt teaching warehouse, and it is not a toy.** DuckDB is an in-process columnar analytical database — SQLite for analytics. The `dbt-duckdb` adapter lets a whole dbt project run against a single `.duckdb` file with zero server. Everything you learn transfers directly to a "real" warehouse because the dbt project is identical; only the profile changes. And DuckDB is genuinely fast on laptop-scale data, so the develop-run-test loop is seconds, not minutes.

**Twelve — by Friday you will have re-expressed the Phase I warehouse as a dbt project.** Lab 05 takes the raw retail tables you have been carrying since Week 1 and rebuilds the entire transformation layer in dbt against DuckDB: `stg_*` staging models over declared sources, `int_*` intermediate models, a dimensional mart (`dim_customer`, `dim_product`, `fct_orders`), generic tests (`unique`, `not_null`, `relationships`) plus at least one singular test, a `dbt snapshot` implementing the customer SCD, a seed, a macro, and a generated docs site whose lineage graph you can read aloud. That artifact is the transformation layer your Phase II lakehouse and your capstone will sit on top of.

## Learning objectives

By the end of this week you will be able to:

- **Initialize** a dbt-core project against DuckDB with a working `dbt_project.yml` and a `profiles.yml`, and explain what `dbt debug` checks (project structure docs: <https://docs.getdbt.com/docs/build/projects>; dbt-duckdb adapter: <https://github.com/duckdb/dbt-duckdb>).
- **Declare** raw inputs as sources with `{{ source() }}` and freshness thresholds, and reference other models with `{{ ref() }}`, so dbt infers the build DAG (sources: <https://docs.getdbt.com/docs/build/sources>; `ref`: <https://docs.getdbt.com/reference/dbt-jinja-functions/ref>).
- **Structure** a project into staging (`stg_*`), intermediate (`int_*`), and mart (`dim_*` / `fct_*`) layers and defend why each layer exists (how we structure projects: <https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview>).
- **Choose** the right materialization — view, table, ephemeral, or incremental — for a given model and set it in a `config` block or `dbt_project.yml` (materializations: <https://docs.getdbt.com/docs/build/materializations>; incremental: <https://docs.getdbt.com/docs/build/incremental-models>).
- **Add** generic tests (`unique`, `not_null`, `accepted_values`, `relationships`) in `schema.yml` and write a singular test as a `.sql` file, and explain how a failing test gates a pipeline (data tests: <https://docs.getdbt.com/docs/build/data-tests>).
- **Build** a `dbt snapshot` implementing a Type-2 SCD with the `timestamp` and `check` strategies, and relate `dbt_valid_from` / `dbt_valid_to` / `dbt_scd_id` to your Week 1 hand-built SCD (snapshots: <https://docs.getdbt.com/docs/build/snapshots>).
- **Generate** documentation and a lineage graph with `dbt docs generate` + `dbt docs serve`, and read the DAG to reason about a model's upstream and downstream dependencies (documentation: <https://docs.getdbt.com/docs/build/documentation>).
- **Write** a reusable macro and load a seed, and explain when each is the right tool (jinja & macros: <https://docs.getdbt.com/docs/build/jinja-macros>; seeds: <https://docs.getdbt.com/docs/build/seeds>).
- **Run** the full toolchain — `dbt seed`, `dbt run`, `dbt test`, `dbt snapshot`, `dbt build`, `dbt source freshness` — and interpret each command's console output (models: <https://docs.getdbt.com/docs/build/models>).

## Prerequisites

This week assumes Weeks 1–4 are **done and committed**. Specifically:

- You have a **retail star schema** (a customer dimension with a Type-2 SCD, a product dimension, and a sales/orders fact) modeled in Postgres from **Week 1**, and you understand fact-table grain, surrogate keys, and the Type-2 SCD pattern (`effective_from` / `effective_to` / current-flag). dbt snapshots this week are the automated version of what you hand-built then.
- You can write **advanced analytical SQL** from **Week 2** — window functions, CTEs, anti-joins — because dbt models *are* SQL `SELECT` statements; dbt adds the dependency graph and the lifecycle, not a new query language.
- You internalized **idempotency and incrementality** from **Week 3** — high-water marks, upserts, late-arriving records. The dbt `incremental` materialization (Challenge 01) is exactly this discipline, expressed in `is_incremental()` and `unique_key`.
- You know what a **DAG** is and why **idempotent re-runs** matter from **Week 4** (Airflow). dbt builds a DAG from your `ref`s and every `dbt run` is idempotent by design.

You need **Docker** (for the optional Postgres source) and **Python 3.11+**. Install dbt-core and the DuckDB adapter with `pip install dbt-core==1.8.* dbt-duckdb==1.8.*` (pin to the 1.8 line for this course). DuckDB itself comes in with the adapter — no separate server. Total disk for the week's project is under 500 MB; DuckDB stores the whole warehouse in a single `.duckdb` file.

You do **not** need cloud anything. dbt Cloud is named and described for honesty; every command in this week runs in dbt-core on your laptop.

## Topics covered

- **Analytics-as-code** — why versioned, reviewed, tested, lineage-aware SQL beats hand-run SQL on the four axes (version control, code review, tests, lineage).
- **The dbt mental model** — dbt is a compiler/orchestrator for SQL, not a database; it generates SQL and runs it against whatever warehouse the profile names. DuckDB via `dbt-duckdb` this week.
- **Project anatomy** — `dbt_project.yml`, `profiles.yml`, `models/`, `seeds/`, `snapshots/`, `macros/`, `tests/`; `dbt init`, `dbt debug`.
- **Sources** — `{{ source() }}`, the `sources.yml` block, source freshness (`loaded_at_field`, `warn_after`, `error_after`), `dbt source freshness`.
- **Refs and the DAG** — `{{ ref() }}`, how dbt infers the dependency graph, graph selectors (`stg_orders+`, `+fct_orders`, `tag:nightly`).
- **Layering** — staging (`stg_*`), intermediate (`int_*`), marts (`dim_*` / `fct_*`); one staging model per source, no joins in staging, business logic in intermediate, dimensional shapes in marts.
- **Materializations** — view (default, no storage), table (full rebuild), ephemeral (CTE-inlined, no object), incremental (`is_incremental()` + `unique_key`, append/merge); when each is right.
- **Tests** — generic (`unique`, `not_null`, `accepted_values`, `relationships`) vs singular (`.sql` returning offending rows); non-zero exit gates a pipeline; severity (`warn` vs `error`).
- **Snapshots** — dbt's Type-2 SCD; `timestamp` vs `check` strategies; `dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`; tied back to Week 1.
- **Documentation & lineage** — `description:` in YAML, doc blocks, `dbt docs generate`, `dbt docs serve`, reading the lineage graph.
- **Seeds & macros** — `dbt seed` for static CSV lookups; macros as Jinja functions that return SQL, plus `generate_surrogate_key` and the `dbt_utils` ecosystem.
- **`dbt build` ordering** — run + test interleaved by DAG, so a failed test skips downstream models.
- **dbt-core vs dbt Cloud** — the honest trade; we use core for laptop-local, lock-in-free work.

## Weekly schedule

The schedule below adds up to approximately **33 hours**. Treat it as a target. Monday's lecture on layering and the DAG is the hour that decides whether the rest of the week makes structural sense — if your project is laid out wrong, every later step fights you.

| Day       | Focus                                                       | Lectures | Exercises | Challenges | Quiz/Read | Homework | Mini-Project | Self-Study | Daily Total |
|-----------|-------------------------------------------------------------|---------:|----------:|-----------:|----------:|---------:|-------------:|-----------:|------------:|
| Monday    | Analytics-as-code; models, sources, refs; the layering DAG  |    2h    |    1.5h   |     0h     |    0.5h   |   1h     |     0h       |    0.5h    |     5.5h    |
| Tuesday   | Materializations; generic + singular tests; `dbt build`     |    2h    |    1.5h   |     0h     |    0.5h   |   1h     |     0h       |    0.5h    |     5.5h    |
| Wednesday | Snapshots (SCD-2); docs, lineage, seeds, macros; core vs Cloud |  1.5h  |    1.5h   |     1h     |    0.5h   |   1h     |     0.5h     |    0.5h    |     6.5h    |
| Thursday  | Incremental materialization; custom test + macro            |    0.5h  |    1h     |     1.5h   |    0.5h   |   1h     |     2h       |    0.5h    |     7h      |
| Friday    | Mini-project build: layers, tests, snapshot, docs           |    0h    |    0.5h   |     0h     |    0.5h   |   1h     |     2.5h     |    0h      |     4.5h    |
| Saturday  | Mini-project: freshness, lineage graph, write-up            |    0h    |    0h     |     0h     |    0h     |   1h     |     2h       |    0h      |     3h      |
| Sunday    | Quiz, review, polish                                        |    0h    |    0h     |     0h     |    0.5h   |   0h     |     0h       |    0h      |     0.5h    |
| **Total** |                                                             | **8h**   | **6.5h**  | **3.5h**   | **3h**    | **6h**   | **9h**       | **2.5h**   | **32.5h**   |

## How to navigate this week

| File | What's inside |
|------|---------------|
| [README.md](./README.md) | This overview (you are here) |
| [resources.md](./resources.md) | dbt docs, the dbt-duckdb adapter, DuckDB docs, the "how we structure" guide |
| [lecture-notes/01-analytics-as-code-models-sources-refs-and-layering.md](./lecture-notes/01-analytics-as-code-models-sources-refs-and-layering.md) | Why analytics-as-code wins; the dbt mental model; `dbt_project.yml` / `profiles.yml`; sources, refs, the DAG; staging / intermediate / mart layering |
| [lecture-notes/02-materializations-tests-and-snapshots.md](./lecture-notes/02-materializations-tests-and-snapshots.md) | View / table / ephemeral / incremental; generic vs singular tests; the four built-ins; `dbt build` ordering; snapshots as SCD-2 (`timestamp` and `check` strategies) |
| [lecture-notes/03-docs-lineage-seeds-macros-and-dbt-core-vs-cloud.md](./lecture-notes/03-docs-lineage-seeds-macros-and-dbt-core-vs-cloud.md) | `dbt docs generate`/`serve`, reading the lineage graph, doc blocks; seeds; macros (including a surrogate-key macro); the honest dbt-core vs dbt Cloud trade |
| [exercises/exercise-01-staging-and-sources.sql](./exercises/exercise-01-staging-and-sources.sql) | Declare sources and write `stg_*` staging models over the raw retail tables |
| [exercises/exercise-02-marts-and-tests.sql](./exercises/exercise-02-marts-and-tests.sql) | Build a dimensional mart with `{{ ref() }}` and attach generic tests in `schema.yml` |
| [exercises/exercise-03-snapshot-scd2.sql](./exercises/exercise-03-snapshot-scd2.sql) | Write a `dbt snapshot` implementing the customer Type-2 SCD |
| [exercises/SOLUTIONS.md](./exercises/SOLUTIONS.md) | Reference solutions, expected console output, and common pitfalls (read after attempting) |
| [challenges/challenge-01-incremental-materialization.md](./challenges/challenge-01-incremental-materialization.md) | Convert the orders fact to an incremental model with `is_incremental()`, `unique_key`, and late-data handling; prove no double-count on re-run |
| [challenges/challenge-02-custom-generic-test-and-macro.md](./challenges/challenge-02-custom-generic-test-and-macro.md) | Write a custom generic test (`assert_positive` / `accepted_range`) and a reusable macro, and apply both via `schema.yml` |
| [mini-project/README.md](./mini-project/README.md) | **Crunch Warehouse, as Code** — rebuild the Phase I warehouse as a full dbt project with layers, tests, a snapshot, a seed, a macro, freshness, and a generated lineage graph |
| [quiz.md](./quiz.md) | 10 multiple-choice questions on refs vs sources, materializations, tests, snapshots, layering, incremental, and lineage |
| [homework.md](./homework.md) | Six practice problems for the week |

## A note on tone

C27 is written in **engineer's voice**, and dbt amplifies the discipline the rest of the course already demands. We pin versions ("dbt-core 1.8, dbt-duckdb 1.8"). We do not say "add some tests" — we say *which* test (`unique` on the primary key, `not_null` on the foreign key, `relationships` from `fct_orders.customer_sk` to `dim_customer.customer_sk`) and *why* a failing one must exit non-zero. We do not say "the mart looks right" — we say `dbt build` ran 11 models and 23 tests, all passed, and the lineage graph shows `fct_orders` depending on exactly `stg_orders`, `int_order_items_joined`, and `dim_customer`. A green build is a claim with evidence; if your write-up says "the models worked" with no command output, you have not produced one yet.

## Up next

Continue to **Week 6 — File Formats, Columnar Storage & the Lakehouse** once you have pushed your dbt project. Week 6 takes the dimensional mart you build this week and lands it as partitioned Parquet on MinIO object storage, wraps it in an Apache Iceberg table, and queries it with DuckDB using predicate pushdown. The clean, tested, layered transformation you write this week is what makes that lakehouse worth building — there is no point storing untrusted data efficiently.

---

*If you find errors in this material, please open an issue or send a PR to <https://github.com/CODE-CRUNCH-CLUB>. Future learners will thank you. Licensed GPL-3.0.*
