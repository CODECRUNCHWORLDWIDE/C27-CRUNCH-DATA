# Week 5 — Resources

Every resource here is **free** and **publicly accessible**. Pin to the versions this course uses — **dbt-core 1.8** and **dbt-duckdb 1.8** (`pip install "dbt-core==1.8.*" "dbt-duckdb==1.8.*"`) — so your commands and output match the lecture notes. If a link breaks, please open an issue.

## Start here (read before the lab)

- **dbt documentation home** — the canonical reference for everything this week:
  <https://docs.getdbt.com/>
- **About dbt projects** — what a project is, the directory layout, `dbt_project.yml`:
  <https://docs.getdbt.com/docs/build/projects>
- **How we structure our dbt projects** — the staging / intermediate / mart layering guide, the single most important convention of the week:
  <https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview>

## Core concepts (one page per concept)

- **Models** — what a dbt model is, how it compiles, materialization basics:
  <https://docs.getdbt.com/docs/build/models>
- **Sources** — declaring raw tables dbt reads but does not own, plus source freshness:
  <https://docs.getdbt.com/docs/build/sources>
- **The `ref` function** — how `{{ ref() }}` resolves names and builds the DAG:
  <https://docs.getdbt.com/reference/dbt-jinja-functions/ref>
- **Materializations** — view, table, ephemeral, incremental, and when to use each:
  <https://docs.getdbt.com/docs/build/materializations>
- **Incremental models** — `is_incremental()`, `unique_key`, `{{ this }}`, `--full-refresh`, `on_schema_change` (Challenge 01):
  <https://docs.getdbt.com/docs/build/incremental-models>
- **Data tests** — generic and singular tests, the four built-ins, custom generic tests, severity (Lecture 2, Challenge 02):
  <https://docs.getdbt.com/docs/build/data-tests>
- **Snapshots** — dbt's Type-2 SCD; `timestamp` and `check` strategies; `dbt_valid_from`/`dbt_valid_to`/`dbt_scd_id` (Lecture 2, Exercise 03):
  <https://docs.getdbt.com/docs/build/snapshots>
- **Jinja & macros** — defining macros, calling them, overriding built-ins (Lecture 3, Challenge 02):
  <https://docs.getdbt.com/docs/build/jinja-macros>
- **Seeds** — loading static CSV lookups with `dbt seed` (Lecture 3, F8):
  <https://docs.getdbt.com/docs/build/seeds>
- **Documentation** — descriptions, doc blocks, `dbt docs generate`/`serve`, the lineage graph (Lecture 3, F10):
  <https://docs.getdbt.com/docs/build/documentation>

## Adapter and warehouse (the laptop-local stack)

- **dbt-core (source)** — the open-source CLI you `pip install`; everything in this course runs on it:
  <https://github.com/dbt-labs/dbt-core>
- **dbt-duckdb adapter** — the adapter that runs the whole project against a single DuckDB file; read its README for the supported incremental strategies (`append`, `delete+insert`, `merge`), extensions, and profile options:
  <https://github.com/duckdb/dbt-duckdb>
- **DuckDB documentation** — the in-process analytical database that is our warehouse this week; useful for the SQL dialect (`read_csv`, `read_parquet`, type casts) you will land raw data with:
  <https://duckdb.org/docs/>

## Command cheat-sheet (the dbt-core surface for this week)

| Command | Purpose |
|---------|---------|
| `dbt init <project>` | Scaffold a new project (choose the `duckdb` adapter) |
| `dbt debug` | Verify connection, profile, and that the project parses |
| `dbt deps` | Install packages declared in `packages.yml` (e.g. `dbt_utils`) |
| `dbt seed` | Load `seeds/*.csv` into the warehouse |
| `dbt source freshness` | Check declared sources against their freshness thresholds |
| `dbt run` | Build models in DAG order |
| `dbt run --select stg_orders+` | Build `stg_orders` and everything downstream |
| `dbt run --select +fct_orders` | Build `fct_orders` and everything upstream |
| `dbt test` | Run all tests |
| `dbt snapshot` | Capture Type-2 SCD history |
| `dbt build` | Run + test interleaved by DAG; the workhorse pipeline command |
| `dbt run --select fct_orders --full-refresh` | Rebuild an incremental model from scratch |
| `dbt compile` | Compile Jinja to SQL into `target/compiled/` without running |
| `dbt docs generate` | Compile docs, catalog, and lineage |
| `dbt docs serve` | Serve the docs site + lineage graph on `localhost:8080` |

## Community packages

- **dbt_utils** — the standard utility macro package; this week we use `generate_surrogate_key`. Declare it in `packages.yml`, install with `dbt deps`:
  <https://github.com/dbt-labs/dbt-utils>

## How this connects to the rest of C27

- **Week 1 (dimensional modeling, Type-2 SCD).** Your hand-built SCD becomes a `dbt snapshot`; your star schema becomes the mart layer. Snapshots' `dbt_valid_from`/`dbt_valid_to` map onto your `effective_from`/`effective_to`.
- **Week 2 (advanced SQL).** dbt models *are* SQL `SELECT`s — window functions, CTEs, and anti-joins all live inside models unchanged.
- **Week 3 (idempotent incremental ETL).** The `incremental` materialization (Challenge 01) is your high-water mark + upsert, expressed in `is_incremental()` + `unique_key`.
- **Week 4 (orchestration).** dbt builds a DAG from your refs, just as Airflow has a DAG; you will run `dbt build` as an Airflow task, and a failing test fails the DAG.
- **Week 6 (lakehouse).** The dimensional mart you build this week is what you will land as partitioned Parquet in an Iceberg table on MinIO.
- **Week 10 (data quality).** dbt tests are the first quality gate; Great Expectations extends the same "fail the pipeline on bad data" idea.
- **Week 11 (lineage & governance).** The `dbt docs` lineage graph is the source-to-dashboard lineage you will reach for during an incident.

---

*Found a broken link or a better reference? Open an issue or send a PR to <https://github.com/CODE-CRUNCH-CLUB>. Licensed GPL-3.0.*
