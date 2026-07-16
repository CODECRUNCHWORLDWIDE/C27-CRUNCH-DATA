# Mini-Project — Lab 10: The Quality-Gated Pipeline

> Build the full data-quality lab from the syllabus: a Great Expectations suite at the **ingestion boundary** (schema, null thresholds, ranges, referential integrity) and a **freshness + volume check** at the **mart boundary**, both wired into your Airflow DAG so that a malformed load **fails the pipeline and alerts** instead of landing silently — and the pipeline emits a **human-readable data-quality report** as an artifact on every run.

This is the artifact that proves the week's thesis: your pipeline now has a conscience. Before this lab, your DAG moved data and trusted it. After it, a corrupted file gets *stopped at the door*, a stale or short mart never gets published, every run leaves a legible record of what it checked, and the on-call engineer is paged in minutes — not the executive in hours. "We have data quality" stops being a slide and becomes a running pipeline you can deliberately break and watch refuse to ship the break.

**Estimated time:** ~15 hours, split across Thursday, Friday, Saturday, and Sunday in the suggested schedule.

**Builds on and closes:** this consumes the Week 1–3 star schema + ETL, the Week 4 Airflow DAG, and the Week 5 dbt project on DuckDB directly — the gates wrap the pipeline you already built. It is the quality layer that Week 11 (governance, lineage, cost) and the Week 12 capstone both assume is present.

---

## What you will build

A repository `quality-gated-pipeline` with five deliverables:

1. **`gx/`** — a Great Expectations Core 1.x project with the `orders_ingestion` suite (schema, null thresholds, ranges, value sets, uniqueness, referential integrity) and the `orders_ingestion_checkpoint`.
2. **`dbt/`** — your dbt project extended with: mart tests (generic + `dbt_expectations`), a `sources.yml` with `dbt source freshness` on `raw.orders`, and a `tests/assert_orders_volume_within_baseline.sql` singular test (the rolling-baseline volume gate).
3. **`dags/quality_gated_orders.py`** — the Airflow DAG wiring both boundaries as **halting gates**: the GX checkpoint before the load, and `dbt source freshness` + the volume test after the mart build, with an alert on any failure.
4. **`meta/`** — a `load_metrics` table (DDL + the task that populates it each run) recording run metadata, row counts, latency, and the distribution metrics for anomaly detection.
5. **`REPORT.md` + the generated DQ report** — the human-readable data-quality report: GX Data Docs plus a per-run summary (what was checked, what passed, what failed, the row counts and freshness for the run), emitted as an artifact every time the DAG runs.

By the end you have a public repo of ~600–800 lines (GX + dbt + Airflow + SQL) demonstrating a complete, gated, observable pipeline — a strong portfolio piece and the literal answer to "how does your pipeline stop a bad load?"

---

## Why both boundaries, not one

You could put all your checks at ingestion and call it done. Don't — the two boundaries catch *different* failures (Lecture 1 §4):

- The **ingestion gate (GX)** catches *malformed rows*: a null `order_id`, a `PLCAED` status, a negative total, a duplicate key, an `order_id` with no matching `dim_customer`. Per-row, intrinsic problems.
- The **mart gate (freshness + volume)** catches *untrustworthy results* even when every row is valid: a load that's stale (the DAG didn't run), short (an upstream truncated — every one of the 16,000 rows is valid, there are just too few), or shifted. Per-load, contextual problems.

The headline incident — the truncated load from Lecture 1 §1 — **passes the ingestion gate cleanly** and is caught *only* by the volume check at the mart boundary. A pipeline gated at one boundary would have shipped it. You need both.

---

## Architecture

```
                          INGESTION BOUNDARY                                MART BOUNDARY
                                                                      ┌─ dbt source freshness (raw.orders)
   ┌──────────┐   ┌──────────┐   ┌──────────────┐   ┌──────────┐   ┌──┴───────────┐   ┌──────────────────┐
   │  upstream │──▶│ extract  │──▶│  GX GATE     │──▶│  load +  │──▶│  dbt build   │──▶│ VOLUME GATE      │──▶ dashboard
   │  orders   │   │ to frame │   │ checkpoint   │   │  stage   │   │ (mart tests) │   │ (rolling baseline│
   └──────────┘   └──────────┘   └──────┬───────┘   └──────────┘   └──────┬───────┘   │  singular test)  │
                                        │ raise on                        │ severity   └────────┬─────────┘
                                        │ not success                     │ error               │ fail if
                                        ▼                                 ▼ halts build          ▼ outside band
                                   ┌────────────────────────────────────────────────────────────────────┐
                                   │   on failure: HALT the DAG  +  ALERT (Slack/email)  +  status='gated'│
                                   └────────────────────────────────────────────────────────────────────┘
                                        │ every run (pass or fail)
                                        ▼
        ┌──────────────────────────────────────────────────────────────────────────────────────┐
        │  meta.load_metrics  (run_id, rows_read/written, latency, newest_loaded_at, drift cols)  │
        │  GX Data Docs (HTML)  +  per-run DQ report  ──▶  the artifact a human reads              │
        └──────────────────────────────────────────────────────────────────────────────────────┘
```

The DAG dependency chain: `extract >> gx_gate >> load >> stage >> dbt_build >> [source_freshness, volume_gate] >> publish`. A failure anywhere upstream of `publish` leaves `publish` un-run — the bad result never reaches the dashboard. Every task, pass or fail, writes a row to `meta.load_metrics` and contributes to the DQ report.

---

## Repository layout

```
quality-gated-pipeline/
├── README.md
├── gx/                                 # Great Expectations Core 1.x project
│   ├── great_expectations.yml
│   ├── expectations/orders_ingestion.json
│   └── checkpoints/orders_ingestion_checkpoint.json
├── dbt/
│   ├── dbt_project.yml
│   ├── packages.yml                    # dbt_utils + dbt_expectations
│   ├── models/
│   │   ├── sources.yml                 # dbt source freshness (warn_after/error_after)
│   │   └── marts/schema.yml            # generic + dbt_expectations tests
│   └── tests/
│       └── assert_orders_volume_within_baseline.sql   # rolling-baseline volume gate
├── dags/
│   └── quality_gated_orders.py         # the gated DAG + alert + metrics-write
├── meta/
│   ├── load_metrics.sql                # DDL for the observability table
│   └── write_metrics.py                # task that records rows/latency/drift each run
├── fixtures/
│   ├── orders_clean.csv
│   └── orders_corrupt.csv              # the deliberate corruption to prove the gate
└── REPORT.md                           # the placement of every check + the run evidence
```

---

## Deliverables

1. **Ingestion gate (GX).** The `orders_ingestion` suite with checks for: schema (column presence/type), null thresholds (`not_null` on keys, `mostly=0.99` on tolerant columns), ranges (`total_cents` 0–10M), value sets (`status`), uniqueness (compound `(order_id, line_number)`), and referential integrity (every `customer_id` in `dim_customer` — a singular GX/SQL check or pushed to dbt `relationships`). Wired as a checkpoint that the DAG `raise`s on.
2. **Mart gate (freshness + volume).** `dbt source freshness` on `raw.orders` (warn 1h / error 2h) and the rolling-baseline volume singular test, both run after `dbt build` and both able to halt.
3. **The gated DAG.** `quality_gated_orders.py` with the dependency chain above, the GX gate as a `GreatExpectationsOperator` or `PythonOperator` that raises, the mart gates as tasks that fail on non-zero/returned-rows, and an `on_failure_callback` alert.
4. **Observability.** `meta.load_metrics` populated every run with `rows_read`, `rows_written`, `latency_seconds`, `newest_loaded_at`, and the distribution columns; the rolling-baseline test reads it.
5. **The DQ report.** GX Data Docs (auto-generated by the checkpoint's `UpdateDataDocsAction`) plus a per-run summary written as an artifact (e.g. `reports/run_<run_id>.md`) listing every check, its verdict, the run's row counts, and the freshness.
6. **The proof.** A run on `orders_clean.csv` that fully succeeds and publishes; a run on `orders_corrupt.csv` that **halts, alerts, and does not publish**; and a separate run with a *valid but truncated* load (8,000 rows) that passes the ingestion gate but is **caught by the volume gate** — proving you need both boundaries.

---

## Grading rubric

| Dimension | Weight | What "excellent" looks like |
|---|---:|---|
| **Ingestion gate correctness** | 20% | The GX suite covers all of schema/null/range/set/uniqueness/referential-integrity; the checkpoint raises; a corrupt file is rejected at the door. |
| **Mart gate correctness** | 20% | Freshness (`dbt source freshness`) and volume (rolling baseline) both halt; the truncated-but-valid load is caught here and *not* at ingestion. |
| **Halting (not logging)** | 20% | Failures genuinely **halt** the DAG (downstream un-run, bad data never published) and **alert** — not merely log. Demonstrated, not claimed. |
| **Observability + report** | 20% | `meta.load_metrics` is populated every run; the DQ report (Data Docs + per-run summary) is a real artifact a non-engineer could read. |
| **Severity discipline** | 10% | `warn` vs `error` chosen deliberately per check (a soft volume dip warns; a hard one fails; distribution drift warns), with the reasoning in `REPORT.md`. |
| **`REPORT.md` quality** | 10% | Each check is placed on a boundary with its dimension and severity justified; the before/after proof is complete. |

## Pass criteria

- [ ] On `orders_corrupt.csv`, the GX gate fails, the DAG halts, the alert fires, and the corrupt rows are **not** in the target/mart.
- [ ] On the truncated-but-valid load, the ingestion gate **passes** and the **volume gate** catches it at the mart boundary (proving both boundaries are needed).
- [ ] On `orders_clean.csv`, the full DAG succeeds and publishes.
- [ ] `dbt source freshness` fails (and halts) when the source is aged past `error_after`.
- [ ] Every run writes a `meta.load_metrics` row and produces a DQ report artifact.
- [ ] `REPORT.md` justifies each check's boundary and severity and shows the three proof runs.

## References

- **Great Expectations — documentation** (suites, checkpoints, actions, Data Docs): <https://docs.greatexpectations.io/docs/>
- **dbt — data tests** and **source freshness**: <https://docs.getdbt.com/docs/build/data-tests> · <https://docs.getdbt.com/docs/build/sources#snapshotting-source-data-freshness>
- **`airflow-provider-great-expectations`** (`GreatExpectationsOperator`): <https://github.com/great-expectations/airflow-provider-great-expectations>
- **dbt-expectations** and **dbt-utils**: <https://github.com/calogica/dbt-expectations> · <https://github.com/dbt-labs/dbt-utils>
- **Joe Reis & Matt Housley, *Fundamentals of Data Engineering*** (O'Reilly, 2022), ISBN 978-1-098-10830-4: <https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/>
- Lecture notes 01 (taxonomy + halting gate), 02 (GX + dbt tests), 03 (contracts, anomalies, observability).
