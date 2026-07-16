# Mini-Project — Crunch Warehouse, as Code

**Lab 05.** Re-express the entire Phase I retail warehouse as a dbt project against DuckDB. By the end you will have a versioned, tested, documented transformation layer — staging, intermediate, and mart models; sources with freshness; generic and singular tests; a Type-2 snapshot; a seed; a macro; and a generated docs site whose lineage graph you can read aloud. This is the artifact every later week of Phase II and your capstone will sit on top of.

This is the week's largest deliverable (~9 hours across Wed–Sat). It is graded. Read the rubric before you start.

---

## Runtime / topology

Everything runs on your laptop, no server, no cloud:

```text
            ┌─────────────────────────────────────────────────────┐
            │  crunch_warehouse.duckdb  (one file, in-process)     │
            │                                                      │
   raw  ──► │  raw.customers / raw.orders / raw.order_items        │  <- landed by you
            │       │                                              │     (seed or read_*)
            │       ▼   {{ source('raw', ...) }}                   │
            │  stg_customers · stg_orders · stg_order_items (views)│  <- staging layer
            │       │                                              │
            │       ▼   {{ ref(...) }}                             │
            │  int_orders_enriched · int_customers_with_region     │  <- intermediate (ephemeral)
            │       │                                              │
            │       ▼                                              │
            │  dim_customer · dim_product · fct_orders   (tables)  │  <- mart layer (star)
            │       ▲                                              │
            │  customers_snapshot  (snapshots schema, SCD-2)       │  <- snapshot
            │  country_region  (seed)                              │  <- seed
            └─────────────────────────────────────────────────────┘
                              │
                  dbt docs serve  →  lineage graph @ localhost:8080
```

- **dbt-core 1.8** + **dbt-duckdb 1.8**, installed with `pip install "dbt-core==1.8.*" "dbt-duckdb==1.8.*"`.
- **dbt_utils** via `packages.yml` + `dbt deps` (for `generate_surrogate_key`).
- The "warehouse" is a single `crunch_warehouse.duckdb` file. The raw retail data is the same dataset you carried from Week 1 — land it into a `raw` schema (via a `dbt seed`, a DuckDB `read_csv`/`read_parquet`, or your Week 3 loader pointed at DuckDB).

---

## Functional requirements

- **F1 — Sources with freshness.** Declare `raw.customers`, `raw.orders`, `raw.order_items` as a dbt source in `models/staging/_sources.yml`, each with a `loaded_at_field` and freshness thresholds (`warn_after` / `error_after`). `dbt source freshness` must run and report per-table status.
- **F2 — Staging layer.** One `stg_*` model per source table (`stg_customers`, `stg_orders`, `stg_order_items`), materialized as views, cleaning and renaming **only** — no joins, no aggregation. Money stays in integer cents.
- **F3 — Intermediate layer.** At least two `int_*` models (materialized ephemeral): `int_orders_enriched` (line items aggregated to order grain) and `int_customers_with_region` (customers joined to the `country_region` seed).
- **F4 — Mart layer (the star).** `dim_customer`, `dim_product`, and `fct_orders`. Dimensions carry surrogate keys (via `generate_surrogate_key`); `fct_orders` is at grain *one row per order_id* and references dimensions by surrogate key. Materialized as tables.
- **F5 — Generic tests.** At minimum: `unique` + `not_null` on every dimension surrogate key and on `fct_orders.order_id` (the grain); `not_null` on every foreign key; `relationships` from `fct_orders.customer_sk` → `dim_customer.customer_sk` and `fct_orders.product_sk` → `dim_product.product_sk`; `accepted_values` on `dim_customer.segment`.
- **F6 — At least one singular test.** A `.sql` file in `tests/` expressing a rule the built-ins cannot — e.g. "exactly one current row per customer in the snapshot-backed dimension," or "no order_date in the future."
- **F7 — A snapshot (SCD-2).** `customers_snapshot` implementing the Type-2 customer SCD with the `timestamp` strategy (and document why, vs `check`). The mart `dim_customer` should be snapshot-aware: surrogate key unique per version, exposing `dbt_valid_from` / `dbt_valid_to` / `is_current`.
- **F8 — A seed.** `seeds/country_region.csv` (country code → region), typed and documented in `seeds/_seeds.yml`, referenced by `int_customers_with_region` via `{{ ref() }}`.
- **F9 — A macro.** At least one reusable macro in `macros/` (e.g. `cents_to_dollars`) used in at least one model, with the expansion verifiable in `target/compiled/`.
- **F10 — Generated docs + lineage graph.** `dbt docs generate` then `dbt docs serve`; capture a screenshot of the lineage graph showing the full source → staging → intermediate → mart flow. Every model and every non-obvious column has a `description:`.

## Non-functional requirements

- **NF1 — `dbt build` is the single command** that runs every model and every test interleaved by DAG; it must exit zero (`ERROR=0`).
- **NF2 — Idempotent.** Running `dbt build` twice in a row produces identical results (no double-count, no schema drift). DuckDB file may be deleted and rebuilt from scratch with `dbt seed && dbt snapshot && dbt build`.
- **NF3 — Layered materializations** set per-folder in `dbt_project.yml` (staging=view, intermediate=ephemeral, marts=table), overridden per model only with a documented reason.
- **NF4 — No hard-coded table names** anywhere in `models/`. Every input is `{{ source() }}` or `{{ ref() }}`. (Grep your models for the raw schema name — it should appear only in `_sources.yml`.)
- **NF5 — Documentation discipline.** Grain of `fct_orders` stated in its model description; every surrogate key and FK documented.

## Suggested dbt project layout

```text
crunch_warehouse/
├── dbt_project.yml
├── packages.yml                       # dbt_utils
├── profiles.yml                       # (in ~/.dbt/; duckdb target)
├── seeds/
│   ├── _seeds.yml
│   └── country_region.csv
├── snapshots/
│   └── customers_snapshot.sql
├── macros/
│   └── cents_to_dollars.sql
├── tests/
│   └── assert_one_current_row_per_customer.sql
└── models/
    ├── staging/
    │   ├── _sources.yml
    │   ├── _staging.yml               # staging model docs + tests
    │   ├── stg_customers.sql
    │   ├── stg_orders.sql
    │   └── stg_order_items.sql
    ├── intermediate/
    │   ├── int_orders_enriched.sql
    │   └── int_customers_with_region.sql
    └── marts/
        ├── _marts.yml                 # mart docs + the bulk of the tests
        ├── docs.md                    # reusable doc blocks
        ├── dim_customer.sql
        ├── dim_product.sql
        └── fct_orders.sql
```

## Validation plan

Run, in order, and capture the console output of each into your write-up:

```bash
dbt deps                 # install dbt_utils
dbt debug                # connection + project parse OK
dbt seed                 # load country_region.csv
dbt source freshness     # raw inputs not stale (F1)
dbt snapshot             # capture SCD-2 history (F7)
dbt build                # run + test ALL models/tests by DAG (NF1) -> must be ERROR=0
dbt docs generate        # compile docs + lineage
dbt docs serve           # screenshot the lineage graph (F10)
```

Then prove the SCD works: change one customer's `segment` (and bump `updated_at`) in the raw source, `dbt snapshot` again, and run the audit query showing two rows for that customer (one closed, one current). Re-run `dbt build` and confirm it is still green and counts are unchanged (NF2).

## Grading rubric (100 points)

| Criterion | Points |
|-----------|-------:|
| F1 — sources declared with working freshness (`dbt source freshness` runs) | 8 |
| F2 — staging layer: one view per source, clean/rename only, no joins | 10 |
| F3 — intermediate layer: ≥2 `int_*` models, correct aggregation/joins | 8 |
| F4 — mart layer: `dim_customer`, `dim_product`, `fct_orders`, surrogate keys, correct grain | 16 |
| F5 — generic tests: unique/not_null/relationships/accepted_values all present and passing | 12 |
| F6 — at least one meaningful singular test | 6 |
| F7 — snapshot implements SCD-2; dim is snapshot-aware; audit query demonstrated | 14 |
| F8 — seed created, typed, documented, referenced | 5 |
| F9 — macro written and used; expansion verified in compiled SQL | 5 |
| F10 — docs generated; lineage graph screenshot; descriptions present | 8 |
| NF — `dbt build` green & idempotent; no hard-coded table names; layered materializations | 8 |
| **Total** | **100** |

A `dbt build` that exits non-zero caps the score at 60 regardless of other work — a transformation layer that does not pass its own tests is not done.

## Stretch goals

- Add a **second snapshot** with the `check` strategy on `dim_product` (products with no reliable `updated_at`) and document the trade-off vs `timestamp`.
- Add **`dbt_utils.equal_rowcount`** or a custom singular test asserting `fct_orders` row count equals the source order count (a reconciliation test).
- Wire `dbt build` into your **Week 4 Airflow DAG** as a `BashOperator` task so the warehouse rebuilds on schedule and a failing test fails the DAG.
- Convert `fct_orders` to **incremental** (Challenge 01) and prove no double-count on re-run.
- Add a **custom generic test** (Challenge 02, `accepted_range`) on `quantity` and `gross_cents`.
- Add an **exposure** in YAML naming the dashboard that consumes `fct_orders`, so the lineage graph extends to the downstream consumer.

## Submission

Push the dbt project to your `crunch-data-portfolio-<yourhandle>/week-05/` directory and include in the PR:

1. The full dbt project (everything except `target/` and `dbt_packages/`, both gitignored).
2. A `RUN.md` with the console output of the full validation plan (every command, real output — `Done. PASS=… ERROR=0` lines included).
3. A screenshot of the `dbt docs serve` **lineage graph** showing source → staging → intermediate → mart.
4. The SCD audit query output (two rows for the changed customer) and a one-paragraph write-up: which materialization you chose per layer and why, why `timestamp` vs `check` for the snapshot, and what the `unique` test on `order_id` proves about your join.

Licensed GPL-3.0. PRs back to <https://github.com/CODE-CRUNCH-CLUB>.
