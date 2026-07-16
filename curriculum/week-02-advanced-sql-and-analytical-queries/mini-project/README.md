# Mini-Project — An Analytics Query Library + Tuning Report for the Retail Warehouse

> **Time budget:** ~10 hours across the week. **Engine:** PostgreSQL 16 in Docker (`postgres:16`), with an optional DuckDB 1.x comparison. **Dataset:** the Week-1 retail star schema, loaded at a size large enough to make plans interesting (~5M `fact_sales` rows). **License:** GPL-3.0.

## The brief

The analysts on your team keep re-writing the same ten queries slightly differently, getting slightly different answers, and pasting slow ad-hoc SQL into the production warehouse. Your job is to give them a **trusted analytics query library** — a set of parameterized, reviewed, documented analytical views and queries against the retail warehouse — plus a **tuning report** that proves the heavy ones are fast and explains why.

This is the artifact a data engineer actually ships: not one clever query, but a *library* a team can rely on, with the plans captured so the next person does not have to rediscover why a view is fast. It runs entirely on a laptop with `docker compose up`.

## What you build

A repository that, after `docker compose up`, brings up Postgres 16 with the star schema loaded, creates a library of analytical views, and ships a tuning report with captured `EXPLAIN ANALYZE` plans.

### Functional requirements

- **F1 — A loadable warehouse.** `docker compose up` starts `postgres:16`, creates the Week-1 schema (`dim_date`, `dim_product`, `dim_store`, `dim_customer`, `fact_sales`), and loads a dataset of at least 1,000,000 `fact_sales` rows (5M preferred). A `sql/00_schema.sql` and a data-load step (generated rows or a seed file) make this reproducible from nothing.
- **F2 — A ranking view.** `v_product_rank_by_category`: each product with its revenue and its `DENSE_RANK` within its category. Parameterizable by year via a function `top_products(p_year int, p_n int)` returning the top *N* per category for a given year.
- **F3 — A time-series view.** `v_store_daily_revenue` with daily revenue and a trailing 7-day total per store using an explicit `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` frame.
- **F4 — A period-over-period view.** `v_monthly_growth`: revenue per month with `LAG`-based month-over-month absolute and percent change, `NULLIF`-guarded.
- **F5 — A subtotal report view.** `v_region_category_rollup`: revenue by region and category with region subtotals and a grand total via `ROLLUP`, with `GROUPING()`-derived labels.
- **F6 — Two set-membership views.** `v_customers_without_orders` (anti-join, `NOT EXISTS`) and `v_products_in_all_stores` (relational division, the `COUNT(DISTINCT)` form).
- **F7 — A recursive-CTE view or function.** Either a `v_category_tree` walking the category hierarchy with depth and path, **or** a `gap_free_daily_revenue(p_from date, p_to date)` function that left-joins a generated date spine so zero-sales days appear. State which you chose and why.
- **F8 — A tuning report.** `TUNING.md` documenting at least **three** of the views above: the `EXPLAIN (ANALYZE, BUFFERS)` plan as first written, the bottleneck you found, the index (or rewrite) you applied, and the re-measured plan. At least one must show a `Seq Scan` → index-based scan transition with a measured speedup; at least one must show a *correct* `Seq Scan` you chose **not** to index, with the reasoning.
- **F9 — A catalog.** `QUERIES.md` listing every view/function with a one-line description, its parameters, the business question it answers, and an example call with expected output shape.

### Non-functional requirements

- **NF1 — Reproducible.** A reviewer runs `docker compose up`, waits for healthy, and the schema + data + views exist with no manual steps. Document the one command.
- **NF2 — Engine-labelled.** Every query/view is PostgreSQL. If you include any DuckDB comparison, it lives in a clearly separated `duckdb/` folder and is labelled DuckDB-only (e.g. anything using `QUALIFY`).
- **NF3 — Reviewed-readable.** Every view uses CTEs to name its steps; no triple-nested anonymous subqueries. A teammate must be able to read each view top to bottom.
- **NF4 — Evidence-backed.** No tuning claim in `TUNING.md` exists without a before/after plan capture. "Made it faster" without a plan is not accepted.
- **NF5 — Correct grain.** Each view returns the grain its name implies (`v_monthly_growth` is one row per month; `v_customers_without_orders` is one row per customer).

## Project layout

```text
analytics-query-library/
├── docker-compose.yml          # postgres:16 + a load step
├── README.md                   # how to run, what each piece is
├── sql/
│   ├── 00_schema.sql           # the Week-1 star schema DDL
│   ├── 01_load.sql             # generate/seed >=1M fact rows
│   ├── 10_views.sql            # F2..F7 view + function definitions
│   └── 20_indexes.sql          # the indexes your tuning report justifies
├── QUERIES.md                  # the catalog (F9)
├── TUNING.md                   # the tuning report with captured plans (F8)
├── plans/                      # before_*.txt / after_*.txt plan captures
└── duckdb/                     # OPTIONAL: the QUALIFY comparison, labelled
    └── compare.sql
```

## Verification and measurement

Before you submit, run and record:

1. **Correctness pass.** For each view, run it and eyeball the grain and a few values against a hand-checked spot query. Record one sample output block per view in `QUERIES.md`.
2. **Plan captures.** For the three tuned views, save `plans/before_<view>.txt` and `plans/after_<view>.txt` from `EXPLAIN (ANALYZE, BUFFERS)`.
3. **Timing table.** In `TUNING.md`, a table: view | before ms | after ms | before buffers (read) | after buffers (hit) | speedup | change made.
4. **Negative control.** Show one query where you *left* a `Seq Scan` in place and explain why an index would have been the wrong call (the query needs most of the table).

## Grading rubric (100 points)

| Criterion | Points |
|---|---|
| **F1** — `docker compose up` builds the warehouse with ≥1M fact rows, no manual steps | 12 |
| **F2–F4** — ranking, 7-day-frame, and `LAG` views correct (right grain, explicit frame, NULL/divide guards) | 18 |
| **F5** — `ROLLUP` subtotal view with correct `GROUPING()` labels | 8 |
| **F6** — anti-join and relational-division views correct (no `NOT IN` trap) | 10 |
| **F7** — recursive-CTE view/function correct and terminates | 8 |
| **F8** — tuning report: ≥3 views, before/after plans, ≥1 real seq-scan→index speedup, ≥1 justified seq scan | 22 |
| **F9** — catalog complete and accurate (params, question, sample output) | 8 |
| **NF2/NF3** — engine-labelled, CTE-readable, reviewable | 6 |
| **NF4** — every tuning claim backed by a captured plan | 8 |
| **Total** | **100** |

Minimum to pass: **70**, AND no view that returns the wrong grain or relies on `NOT IN` over a nullable column.

## Stretch goals

- **DuckDB twin.** Load the same data into DuckDB and re-express the ranking and "latest order per customer" views with `QUALIFY`. Add an `EXPLAIN ANALYZE` comparison to `TUNING.md` and state, per query, which engine you would run it on and why.
- **Covering index.** Add an `INCLUDE` covering index that turns one of your customer-lookup views into an `Index Only Scan`, and prove it in the plan (no `Heap Fetches`, or near zero).
- **Parameterized as functions.** Convert `v_region_category_rollup` into a function taking a date range, and confirm the planner pushes the date predicate down (`NOT MATERIALIZED` CTE or inlined).
- **`CREATE STATISTICS`.** Find a query where a correlated pair of columns causes a cardinality miss, add extended statistics, and show the estimate improve in the plan.

## Submission

Push the repo (public, GPL-3.0) with the README, `docker-compose.yml`, the `sql/` library, `QUERIES.md`, `TUNING.md`, and the `plans/` captures. A reviewer must be able to clone, run `docker compose up`, and reproduce every view and every plan in your report. C27 · Crunch Data is part of **Code Crunch Club** — <https://github.com/CODE-CRUNCH-CLUB>.
