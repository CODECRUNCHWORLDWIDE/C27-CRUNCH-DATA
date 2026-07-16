# Week 10 — Homework

Six problems that extend the week beyond the exercises and the lab. They are meant to be done *after* the lectures and exercises, and several feed directly into the mini-project — do them in order and you'll have built half the lab as a side effect. Budget ~4 hours across the week.

Everything runs locally against your existing Postgres / DuckDB / Airflow / dbt stack. No new infrastructure.

---

## Problem 1 — Classify ten incidents

For each of the ten data incidents below, name the **dimension** (completeness / validity / uniqueness / freshness / volume / distribution), the **right check** (a specific GX expectation, dbt test, or `dbt source freshness`), and the **severity** you'd assign (`warn` or `error`) with one sentence of reasoning.

1. The `order_id` column has 4,000 nulls this morning.
2. `avg(total_cents)` jumped from 3,000 to 290,000 overnight.
3. Two rows share `(order_id, line_number) = (5501, 1)`.
4. Last night's load wrote 9,200 rows; the trailing-7-day mean is 41,000.
5. The raw `orders` source's newest row is 4 hours old; the SLA is 2 hours.
6. `status` contains the value `"PEDNING"`.
7. `currency_code` contains `"us"` (two letters).
8. The `customer_id` null-rate went from 0.5% to 31%.
9. A `created_at` of `2103-04-01`.
10. `count(distinct status)` dropped from 4 to 1.

**Deliverable:** a table with columns `# | dimension | check | severity | reasoning`.

---

## Problem 2 — Add the referential-integrity gate

Your ingestion GX suite checks completeness, validity, uniqueness, and volume but **not** referential integrity. Add a check that every `customer_id` in the raw orders load exists in `dim_customer`. Because pure GX is weak at cross-table FK checks (Lecture 1 §3.3, Lecture 2 §7), do it the right way: push it to dbt as a `relationships` test on `fct_orders`, *and* write the equivalent as a GX/SQL singular check at ingestion for defense in depth. Show both, and a run where a planted orphan `customer_id` is caught by each.

**Deliverable:** the dbt `relationships` config + the GX/SQL singular check, plus the failing output from both when an orphan is present.

---

## Problem 3 — Tune a gate so it stops false-halting

You're given a volume gate `ExpectTableRowCountToBeBetween(min_value=30000, max_value=50000)`. It false-halted three times last quarter: twice on Black Friday weekend (legitimate spikes to ~95,000) and once on a holiday Monday (legitimate dip to ~12,000). Replace the static band with a **rolling-baseline** check (Lecture 3 §3) that would have *passed* all three legitimate cases but still caught the 16,000-row truncated load against a ~41,000 baseline. Show the SQL and a back-test against fabricated history that demonstrates all four outcomes (three passes, one fail).

**Deliverable:** the rolling-baseline singular test + a back-test table showing each historical day's verdict.

---

## Problem 4 — Emit the run metadata

Implement `meta.load_metrics` (DDL in Lecture 3 §5) and a task that, on every pipeline run, writes one row capturing `rows_read`, `rows_written`, `latency_seconds`, `newest_loaded_at`, and the three distribution metrics (`mean_total_cents`, `null_rate_customer`, `cardinality_status`). Then write three queries against it: (a) the rolling-baseline volume check, (b) a latency trend (`latency_seconds` over the last 14 runs), and (c) a null-rate-drift check on `customer_id`.

**Deliverable:** the DDL, the metrics-write task, and the three queries with sample output.

---

## Problem 5 — Author a data contract for a *second* feed

Pick a second source from your stack (e.g. `products` or `customers`) and write a full data contract for it (Lecture 3 §1): schema, grain, semantics, freshness + volume SLAs appropriate to that feed (a slow dimension's SLA is *not* the orders SLA), ownership, change policy, and PII flags. Then generate at least the freshness check (`dbt source freshness`) and the schema/`not_null` checks from it. The contract must make a *different* set of SLA choices than the orders contract, with the reasoning written down.

**Deliverable:** `contracts/<feed>.yaml` + the generated freshness and schema checks + a paragraph on why this feed's SLAs differ from orders'.

---

## Problem 6 — The "monitor vs gate" audit

Audit your Week 4 Airflow DAG and any dbt tests you already had. For each existing check, classify it as a **gate** (it can halt the pipeline) or a **monitor** (it only logs/warns). Find at least one check that *should* be a gate but is only a monitor, convert it, and prove the conversion (show the pipeline now halting on the failure it used to ignore). Write up what you found.

**Deliverable:** an audit table (`check | gate-or-monitor | should-be | action`) + the proof of one monitor→gate conversion halting the pipeline.

---

## Submission

Push a `week-10-homework/` directory to your C27 repo containing:

- `problem-1-classification.md` — the ten-incident table.
- `problem-2-referential-integrity/` — both checks + failing output.
- `problem-3-rolling-baseline/` — the tuned check + back-test.
- `problem-4-observability/` — the DDL, the write task, the three queries.
- `problem-5-contract/` — the second feed's contract + generated checks.
- `problem-6-audit.md` — the gate/monitor audit + the conversion proof.

Open a PR titled `week-10 homework — <your name>`. In the PR description, answer in two sentences: **which of your existing checks were monitors masquerading as gates, and how do you know the difference now?** That question is the whole week, and if you can answer it crisply you've got it.
