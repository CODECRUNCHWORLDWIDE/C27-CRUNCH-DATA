# Week 5 — Homework

Six problems, roughly **45 minutes each**, designed to deepen the muscles the lab assumes. Each names a deliverable filename and the doc pages worth reading first. Do these against the same `crunch_warehouse` DuckDB project you build in the lab — they reinforce it, they do not duplicate it. Submit all six in your `week-05/homework/` directory.

---

## Problem 1 — Compile and read the SQL dbt actually runs

dbt is a compiler; the proof is in `target/`. Take any three of your models — one view (`stg_orders`), one ephemeral (`int_orders_enriched`), one table that refs the ephemeral (`fct_orders`) — and run `dbt compile`. Open the compiled SQL in `target/compiled/crunch_warehouse/models/...` for each.

Write a short note (`p1_compiled.md`) answering: (a) What did `{{ source('raw','orders') }}` compile to? (b) Where did the ephemeral `int_orders_enriched` *go* in the compiled `fct_orders` — show the CTE it became. (c) What DDL wraps the view vs the table model?

**Read first:** models <https://docs.getdbt.com/docs/build/models>; ref <https://docs.getdbt.com/reference/dbt-jinja-functions/ref>.
**Deliverable:** `p1_compiled.md` with the three compiled snippets and your answers.

---

## Problem 2 — A materialization decision table for your own models

For every model in your project, decide and justify its materialization. Build a table with columns: model, layer, chosen materialization (view/table/ephemeral/incremental), and a one-sentence justification grounded in *query frequency*, *compute cost*, and *data size*. Then change one model's materialization (e.g. make `dim_customer` a view) and measure the difference: time `dbt run --select dim_customer` and time a representative query against it, before and after. Revert.

**Read first:** materializations <https://docs.getdbt.com/docs/build/materializations>.
**Deliverable:** `p2_materializations.md` with the decision table and the before/after timing.

---

## Problem 3 — Break a test on purpose and read the failure

Pick one `relationships` test in your project (e.g. `fct_orders.customer_sk` → `dim_customer.customer_sk`). Inject an orphan: add an order whose `customer_id` does not exist in `dim_customer`. Run `dbt test --select fct_orders` with `--store-failures`, then query the stored-failures table to see exactly which rows violated the rule. Document the failure output, the non-zero exit code (`echo $?`), and how `dbt build` would have *skipped* downstream models. Remove the orphan and confirm green.

**Read first:** data tests <https://docs.getdbt.com/docs/build/data-tests>.
**Deliverable:** `p3_test_failure.md` with the failing output, the exit code, and the stored-failure rows.

---

## Problem 4 — Two snapshot strategies, side by side

Build both `customers_snapshot` (`timestamp`) and `customers_snapshot_check` (`check` on `segment`, `country_code`, `email`) over the same source. Then perform two source mutations: (a) change `segment` AND bump `updated_at`; (b) change `email` but DO NOT bump `updated_at`. Run `dbt snapshot` after each. Show which strategy captured which change and explain why the `timestamp` strategy missed mutation (b).

**Read first:** snapshots <https://docs.getdbt.com/docs/build/snapshots>.
**Deliverable:** `p4_snapshots.md` with both snapshots' row histories for the mutated customer and the explanation. Relate it explicitly to your Week 1 hand-built SCD.

---

## Problem 5 — A seed-driven lookup and a documented metric

Create the `country_region` seed (`seeds/country_region.csv`), type it in `seeds/_seeds.yml`, and build `int_customers_with_region` that left-joins it. Then define a metric — "gross revenue by region" — as a small mart model `agg_revenue_by_region`, and document `gross_cents` once as a reusable doc block (`{% docs %}`) referenced from the column's `description`. Run `dbt docs generate` and confirm the doc block renders on both the model page and anywhere `gross_cents` is documented.

**Read first:** seeds <https://docs.getdbt.com/docs/build/seeds>; documentation <https://docs.getdbt.com/docs/build/documentation>.
**Deliverable:** the seed, the two models, the doc block, and `p5_notes.md` describing what the doc block does and why a left join (not inner) is correct here.

---

## Problem 6 — Read your own lineage and reason about blast radius

Run `dbt docs generate && dbt docs serve`. Screenshot the full lineage graph. Then answer, using only the graph (no reading model files): (a) If you change `stg_customers`, which models must you rebuild, and what is the exact graph selector to rebuild precisely that set? (b) If `dim_customer` is wrong, which upstream models are the candidates, and what selector builds just its ancestors? (c) Identify one structural smell if present (a mart refing a source directly, a staging model with multiple source inputs) or confirm there is none.

**Read first:** documentation/lineage <https://docs.getdbt.com/docs/build/documentation>; how-we-structure <https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview>.
**Deliverable:** `p6_lineage.md` with the screenshot, the two graph selectors (`stg_customers+` and `+dim_customer`), and the smell assessment.

---

## Submission note

Commit all six deliverables to `crunch-data-portfolio-<yourhandle>/week-05/homework/`. Each `.md` must contain **real command output**, not paraphrase — a homework write-up that says "the test failed as expected" without the actual `FAIL N` line is not evidence. If a command errored and you fixed it, show both the error and the fix; the debugging is the learning. Licensed GPL-3.0; PRs to <https://github.com/CODE-CRUNCH-CLUB>.
