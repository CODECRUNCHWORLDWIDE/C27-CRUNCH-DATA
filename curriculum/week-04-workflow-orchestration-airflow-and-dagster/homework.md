# Week 4 Homework

Six practice problems that revisit the week's topics. Each is scoped to about **45 minutes**. The full set is ~4.5 hours. Work in your `crunch-data-portfolio-<yourhandle>/week-04/homework/` directory so each problem produces at least one commit you can point to later.

Each problem includes:

- A short **problem statement**.
- A **deliverable** (the filename to commit).
- **Acceptance criteria** so you know when you are done.
- Real **citation URLs**.

---

## Problem 1 — Predict the run times from a schedule

**Problem statement.** A DAG has `schedule="@daily"`, `start_date=pendulum.datetime(2026, 6, 1, tz="UTC")`, and `catchup=True`. It is deployed and unpaused at `2026-06-10 09:00 UTC`. Without running anything, answer in writing: (a) how many DAG runs does the scheduler create on deploy, and for which data intervals; (b) for the run that owns the interval `[2026-06-05, 2026-06-06)`, what are `data_interval_start`, `data_interval_end`, `logical_date`, and `ds`; (c) approximately when (wall-clock) did that run's interval *close*, and why does the run fire after that, not during the 5th. Then change `catchup` to `False` and re-answer (a).

**Deliverable.** `homework/p1-schedule-reasoning.md`.

**Acceptance criteria.**

- (a) names the exact count and the interval range for `catchup=True` (intervals from `06-01` up to the last *closed* interval before deploy) and for `catchup=False` (only the most recent).
- (b) gives all four values for the `06-05` run, with `ds = "2026-06-05"`.
- (c) explains the "run owns the interval, fires after it closes" rule in your own words.

**Citations.** Airflow DAG runs & data intervals: <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dag-run.html>.

**Estimated time.** 45 minutes.

---

## Problem 2 — TaskFlow vs classic, same graph

**Problem statement.** Write the identical three-task pipeline (`extract >> transform >> load`) **twice**: once with classic operators (`PythonOperator` + `>>`) and once with the TaskFlow API (`@task` + the call graph). Each task should print `data_interval_start`. Confirm in the Airflow UI that both DAGs render the same graph and produce the same logs for one run.

**Deliverable.** `homework/p2_classic.py` and `homework/p2_taskflow.py`.

**Acceptance criteria.**

- Both DAGs parse with no import errors and appear in the UI.
- Both render an `extract -> transform -> load` graph.
- In TaskFlow, the dependency is created by passing one task's return as the next's argument (no `>>`). In classic, it is created with `>>`.
- A run of each prints the same `data_interval_start` in all three tasks.

**Citations.** TaskFlow tutorial: <https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html>; core concepts (DAGs): <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html>.

**Estimated time.** 45 minutes.

---

## Problem 3 — Choose and justify the wait mode

**Problem statement.** You have three sensors: (S1) waits ~5 seconds for an in-process flag; (S2) waits up to 3 hours for a daily file; (S3) you must run 200 of, concurrently, each waiting up to 6 hours for a partition. For each, state which `mode` (`poke` / `reschedule` / deferrable) you would use and *why*, in terms of worker-slot cost and scheduler overhead. Then implement S2 as a real `FileSensor` with the mode you chose, a templated path, and a finite timeout, and S1 as a `@task.sensor` returning a `PokeReturnValue`.

**Deliverable.** `homework/p3-wait-modes.md` (the reasoning) and `homework/p3_sensors.py` (S1 and S2).

**Acceptance criteria.**

- Each sensor's chosen mode is named with a one-sentence justification grounded in slot cost / scheduler overhead.
- S2 is a `FileSensor` with `mode` set, a `{{ ds }}`-templated `filepath`, `poke_interval`, and a finite `timeout`.
- S1 is a `@task.sensor` that returns `PokeReturnValue(is_done=...)`.

**Citations.** Airflow sensors: <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html>.

**Estimated time.** 45 minutes.

---

## Problem 4 — Retry math and the idempotency precondition

**Problem statement.** A task is configured with `retries=5`, `retry_delay=timedelta(minutes=1)`, `retry_exponential_backoff=True`, `max_retry_delay=timedelta(minutes=15)`. (a) Write out the approximate gap before each of the five retries and the total wall-clock span of all retries. (b) In two paragraphs, explain why a non-idempotent `INSERT`-only load makes retries *dangerous* — walk through the worker-dies-after-commit scenario and the resulting double-count. (c) Show the one-transaction delete-then-insert rewrite that makes the same retries *safe*.

**Deliverable.** `homework/p4-retry-and-idempotency.md` (with the code snippet inline).

**Acceptance criteria.**

- (a) lists the five gaps (≈ 1, 2, 4, 8, 15 min — capped) and the total span.
- (b) names the exact failure: commit succeeds, worker dies before success is recorded, Airflow retries, `INSERT`-only doubles the window.
- (c) shows delete-then-insert keyed off the interval, delete + insert in one transaction.

**Citations.** TaskFlow tutorial (retries/backoff): <https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html>; DAG runs & backfill: <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dag-run.html>.

**Estimated time.** 45 minutes.

---

## Problem 5 — The assertion task that catches the lie

**Problem statement.** Given a `load` task that can exit zero while loading partial or zero rows, write a downstream `assert_load` task that performs three checks against the just-loaded window: (1) warehouse row count equals the expected source count; (2) row count is non-zero; (3) a `SUM(amount)` reconciliation between the warehouse window and the source total. It must raise `AirflowFailException` (not a plain exception) on any violation. Then deliberately feed a truncated source for one window and capture the task log showing the assertion firing.

**Deliverable.** `homework/p5_assert_load.py` and `homework/p5-assertion-evidence.txt` (the red-task log line).

**Acceptance criteria.**

- `assert_load` implements all three checks and raises `AirflowFailException`.
- The evidence file shows the specific mismatch message (e.g. `row-count mismatch for 2026-06-12: warehouse=59 expected=118`).
- A one-sentence note explains why `AirflowFailException` (no retry) is correct here.

**Citations.** Airflow core concepts (tasks, exceptions): <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html>.

**Estimated time.** 45 minutes.

---

## Problem 6 — Dagster assets and a pick-one memo

**Problem statement.** Re-express a minimal `raw_sales -> fact_sales` loader as two Dagster `@asset`s partitioned by a `DailyPartitionsDefinition`, with the dependency inferred from the function argument and an `@asset_check` on `fact_sales`. Run `dagster dev`, materialize one partition, and screenshot the asset graph. Then write a one-page memo recommending Airflow or Dagster for a specific team you describe (size, existing stack, how they think about their data), using the Lecture 3 §4 framework.

**Deliverable.** `homework/p6_dagster/assets.py` + `homework/p6-asset-graph.png` + `homework/p6-pick-one-memo.md`.

**Acceptance criteria.**

- Two partitioned assets with the dependency inferred from the argument (no explicit wiring) and an `@asset_check`.
- The screenshot shows the rendered asset graph with the materialized partition.
- The memo describes a concrete team and justifies the pick against at least three framework axes (ecosystem, asset-vs-task thinking, developer experience, hiring pool, operational maturity).

**Citations.** Dagster software-defined assets: <https://docs.dagster.io/concepts/assets/software-defined-assets>; partitions & backfills: <https://docs.dagster.io/concepts/partitions-schedules-sensors/partitions>.

**Estimated time.** 45 minutes.

---

## Submission

Commit each deliverable under `week-04/homework/` with a descriptive message (e.g. `p4: retry math + idempotent rewrite`). When all six are committed, update your portfolio root `README.md` with a Week 4 homework checklist. PRs and corrections to <https://github.com/CODE-CRUNCH-CLUB>. Licensed GPL-3.0.
