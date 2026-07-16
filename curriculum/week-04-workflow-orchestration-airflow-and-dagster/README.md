# Week 4 — Workflow Orchestration (Airflow & Dagster)

In Week 3 you built a Python ETL job that incrementally loads a daily-growing source into the Postgres warehouse using a watermark, performs an idempotent upsert, and produces the same result whether you run it once or five times. That job is correct. It is also a script. Somebody — you — has to remember to run it every morning, has to notice when yesterday's file did not arrive, has to re-run the right date range when the source vendor re-issues three days of corrected history, and has to do all of that at 06:00 without double-counting a single row. This week is about the machine that does the remembering: the **orchestrator**. By Friday you will have wrapped Week 3's loader in an Airflow DAG running in Docker, given it a sensor that waits for the daily file, retries with exponential backoff, an SLA alert that fires when the load is late, and a backfill of 30 days of history that does not double-count — and you will have read the same pipeline expressed as Dagster software-defined assets, so you can argue for one or the other in an interview without hand-waving.

The spine of the week is one sentence: **orchestration is where pipelines actually break.** Not in the transformation logic — you tested that in Week 3. Pipelines break in the seams: the schedule that fired before the upstream data landed, the retry that ran the same load twice because the first attempt timed out *after* it committed, the backfill of 30 days that launched 30 task instances simultaneously and pinned the warehouse at 100% CPU until the on-call engineer killed it, the task that exited zero and turned green on the dashboard while quietly loading half the rows. An orchestrator is not a `cron` with a nicer UI. It is a system that models *dependencies between units of work over a sequence of time intervals*, and the entire value of that model — retries, backfills, catchup, SLAs — is only safe if the units of work are **idempotent**. That is why this week sits where it does, at the end of Phase I, immediately after the week that taught you to write an idempotent load. The orchestrator multiplies idempotency into a superpower; it multiplies non-idempotency into a data-corruption incident.

Here are the ideas to internalize this week, in the order you will meet them. **First, the DAG.** A pipeline is a directed acyclic graph of tasks. The acyclic part is not a technicality — it is the guarantee that the orchestrator can compute a valid execution order and that "wait for X before running Y" terminates. You will learn to express dependencies two ways: the classic operator style (`extract >> transform >> load`) and the TaskFlow style where the data flow *is* the dependency graph (the return value of one `@task` becomes the argument of the next). Both compile to the same graph; the second is closer to how you already think in Week 3 Python.

**Second, the schedule and the interval.** This is the concept that trips up everyone the first week. An Airflow DAG does not run "at" a time; it runs *for a time interval*, and it runs *after that interval has closed*. A DAG with `schedule="@daily"` and a data interval of 2026-06-18 fires shortly after midnight on 2026-06-19, because only then is the 18th's data complete. The variables `data_interval_start` and `data_interval_end` (and the older `logical_date`) name *which slice of time this run is responsible for*. Your loader must key off that slice — not off `now()` — or catchup and backfill will load the wrong windows. This is the single most important mechanical idea in the week, and Lecture 1 hammers it.

**Third, sensors.** Real sources do not arrive on a schedule; they arrive *around* a schedule. The vendor promises the daily extract by 05:00 and delivers it at 05:00, or 05:40, or — twice a quarter — not at all. A sensor is a task whose only job is to wait for a condition (a file exists, a partition landed, an upstream marker appeared) and to do so without burning a worker slot the whole time. You will write a `FileSensor` and a deferrable `@task.sensor`, and you will learn the difference between `poke` mode (cheap to reason about, expensive in worker slots) and `reschedule`/deferrable mode (frees the slot between checks).

**Fourth, retries, backoff, and SLAs.** Networks blip. Databases hit a lock. The right response to a transient failure is to try again — but *try again immediately* turns one blip into a thundering-herd retry storm, so you set `retries`, `retry_delay`, and `retry_exponential_backoff` so the gaps grow. The right response to a *late* run — one that is still trying but has blown past the time it should have finished — is to alert a human while it keeps trying; that is what an SLA and an SLA-miss callback are for. Retries and SLAs answer two different questions: "should this run again?" and "is this run late?" Confusing them is a classic mistake.

**Fifth, backfill and catchup — and the failure mode that defines the week.** When you deploy a DAG with a `start_date` in the past and `catchup=True`, Airflow will schedule a run for *every* interval between the start date and now. That is catchup, and it is how you fill in history. A manual `airflow dags backfill` over an explicit date range does the same thing on demand. Both are wonderful when your tasks are idempotent and disciplined, and both are how you melt the warehouse when they are not. Two ways to melt it: (1) launch all 30 historical runs at once with no concurrency cap, so 30 loads hammer Postgres simultaneously — fixed by `max_active_runs`; (2) write a loader that *appends* its window instead of replacing it, so the backfill double-counts every row it touches — fixed by idempotency, which brings us to the last idea.

**Sixth, idempotency for catchup, and "the task that lied."** Each run must key off `data_interval_start` and operate *only on its own partition* — delete-then-insert (or merge/upsert) for exactly that window — so that re-running interval N, whether by retry, by catchup, or by backfill, produces the identical end state. This is the direct continuation of Week 3's watermark-and-upsert: the watermark told you *where to start*; the interval tells you *which window you own*; the upsert makes owning it safe to repeat. And then there is the honest, ugly failure: **a task can exit zero and be wrong.** The Python returned, the operator reported success, the box went green — and the load committed 4,000 of 10,000 rows because the source file was truncated, or loaded yesterday's window because of a date-math bug. Exit code is a statement about *the process*, not about *the data*. The only defense is to make a separate downstream task *assert the data* — a row-count check, a checksum, a freshness comparison — and fail loudly when the assertion is violated. You will build exactly that.

**Seventh, Airflow's architecture, honestly.** Airflow is three moving parts plus a database: the **scheduler** (reads your DAG files, decides which task instances are runnable, and queues them), the **executor** (the bridge that hands queued tasks to workers — `LocalExecutor`, `CeleryExecutor`, `KubernetesExecutor`), and the **metadata database** (a Postgres or MySQL that stores every DAG run, task instance, XCom, and connection — it is the source of truth, and if it is slow, everything is slow). On your laptop you will run the `LocalExecutor` against a Postgres metadata DB in Docker — the same Postgres engine you use for the warehouse, in a separate database. Understanding that the scheduler and the executor are different things, and that the metadata DB is where state lives, is what lets you debug "why is my task stuck in `queued`."

**Eighth, Dagster as the modern alternative, argued fairly.** Airflow models *tasks*: imperative units of work wired into a graph. Dagster models *assets*: the *things your pipeline produces* (a table, a partition, a file), declared with `@asset`, with dependencies inferred from which assets reference which. The mental shift is from "run these steps in this order" to "these data assets exist and here is how each is computed; keep them up to date." Dagster's `DailyPartitionsDefinition` makes the time-interval model first-class and its partitioned backfills make "rebuild June" a one-click, idempotent-by-construction operation. Neither tool is universally better. By the end of the week you will have a real decision framework — team size, existing investment, whether your team thinks in tasks or in data products, operational maturity — and you will be able to pick one for a given team and defend the pick.

The Phase I gate lands at the end of this week. To pass it you will demo an orchestrated, idempotent, incremental batch pipeline that loads a modeled star schema in Postgres (the schema from Week 1), survives a re-run with no double-counting (the discipline from Week 3), and backfills 30 days cleanly (the new skill from this week). Everything runs on your laptop in Docker — Postgres is both the warehouse and Airflow's metadata DB. No cloud account, no managed Airflow, no hand-waving. If your backfill double-counts a single row, you have not passed; orchestration is exactly the layer where "almost correct" is indistinguishable from "wrong" until an executive asks why revenue is double what finance reported.

## Learning objectives

By the end of this week, you will be able to:

- **Author** an Airflow DAG using both the TaskFlow API (`@dag`/`@task`) and classic operators, wiring task dependencies and setting `schedule`, `start_date`, `catchup`, and `max_active_runs` deliberately — [Airflow core concepts: DAGs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html).
- **Explain** Airflow's data-interval model — why a `@daily` DAG for 2026-06-18 fires *after* the 18th closes, and how `data_interval_start`/`data_interval_end`/`logical_date` name the window a run owns — [Airflow DAG runs & data intervals](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dag-run.html).
- **Build** a sensor that waits for the daily file before the load proceeds, choosing `poke` vs `reschedule`/deferrable mode for the right reason — [Airflow sensors](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html).
- **Configure** retries with exponential backoff (`retries`, `retry_delay`, `retry_exponential_backoff`) and distinguish them from SLAs and SLA-miss callbacks (`sla`, `on_failure_callback`) — [Airflow TaskFlow tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html).
- **Run** a safe, idempotent backfill over a 30-day range with `airflow dags backfill`, keyed off `data_interval_start`, that produces identical results on a re-run with no double-counting — [Airflow backfill & catchup](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dag-run.html).
- **Diagnose** the "task succeeded but lied" failure — a task that exits zero while loading wrong or partial data — and add a downstream row-count / checksum assertion task that fails loudly — [Airflow core concepts: tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html).
- **Describe** Airflow's architecture — scheduler, executor (`LocalExecutor` vs Celery/Kubernetes), and metadata DB — and run it locally with the official Docker Compose against a Postgres metadata DB — [Airflow scheduler](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html), [executor types](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html), [running in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).
- **Re-express** the same pipeline as Dagster software-defined assets with a `DailyPartitionsDefinition` and a partitioned backfill, contrasting the asset-oriented and task-oriented mental models — [Dagster software-defined assets](https://docs.dagster.io/concepts/assets/software-defined-assets), [Dagster partitions & backfills](https://docs.dagster.io/concepts/partitions-schedules-sensors/partitions).
- **Choose** Airflow or Dagster for a given team and justify the choice against a concrete decision framework (team size, existing investment, task- vs asset-oriented thinking, operational maturity).

## Prerequisites

This week assumes Weeks 1–3 are **done and committed**. Specifically:

- You have the **Week 1 star schema** loaded in Postgres in Docker: a `fact_sales` table at a defined grain, conformed dimensions, and a Type-2 SCD you can audit with one query. The orchestrated pipeline lands into this schema.
- You have the **Week 3 idempotent incremental loader**: a Python ETL job that reads a daily source, applies a watermark, and performs an idempotent upsert (delete-then-insert or merge) so a re-run never double-counts. This week wraps *that exact job* in an orchestrator. If your Week 3 loader was not idempotent, fix it before Wednesday — catchup and backfill are unsafe without it.
- You are fluent with **Week 2 analytical SQL**, because the assertion tasks (`COUNT(*)`, checksums, `GROUP BY` reconciliation) are SQL, and you will read at least one `EXPLAIN ANALYZE` when a backfill is slow.
- You have **Docker Desktop (or Docker Engine + Compose v2)** with at least **4 GB of RAM allocated** — the official Airflow Compose file runs a scheduler, a webserver, a triggerer, and a Postgres metadata DB, and it is unhappy under 4 GB. You have Python 3.11+ on the host for the Dagster reading portion.

You do not need any cloud account, any managed-Airflow subscription, or any prior orchestration experience. You do need to accept one mental tax up front: **the data interval is not "now."** Read Lecture 1 §3 twice if it does not click the first time; every backfill bug in the week traces back to a run that used `now()` instead of `data_interval_start`.

## Topics covered

- DAGs as directed acyclic graphs of tasks; the TaskFlow API (`@dag`/`@task`) vs classic operators (`PythonOperator`, `EmptyOperator`); dependency wiring with `>>` and with TaskFlow data flow; XCom and why you push small values, not DataFrames.
- Schedules and intervals: `schedule="@daily"`/cron/`timedelta`/dataset-driven; `start_date`; why a run for interval N fires *after* N closes; `data_interval_start`, `data_interval_end`, `logical_date`, and the deprecated `execution_date`.
- `catchup=True` vs `catchup=False`; `max_active_runs` and `max_active_tasks` to cap concurrency; `depends_on_past` for strictly-sequential intervals.
- Sensors: `FileSensor`, the `@task.sensor` decorator, `poke` vs `reschedule` mode, deferrable operators and the triggerer, `timeout`, `poke_interval`, `mode`, `soft_fail`.
- Retries and resilience: `retries`, `retry_delay`, `retry_exponential_backoff`, `max_retry_delay`; idempotency as the precondition for safe retries; `execution_timeout`.
- SLAs and alerting: the `sla` parameter, `sla_miss_callback`, `on_failure_callback`, `on_retry_callback`; the difference between "this failed" and "this is late."
- Backfill and catchup mechanics: `airflow dags backfill -s START -e END DAG_ID`; how Airflow enumerates intervals; running a backfill safely with concurrency caps; reprocessing corrected history.
- Idempotent, windowed loads: keying every write off `data_interval_start`; delete-then-insert vs merge/upsert per partition; tying this back to Week 3's watermark and upsert.
- The "task succeeded but lied" failure: why exit code ≠ data correctness; partial loads, wrong-window loads, silent truncation; the downstream assertion task (row-count, checksum, freshness) that converts a silent wrong answer into a loud failure.
- Airflow architecture: the scheduler loop, the executor abstraction (`SequentialExecutor`/`LocalExecutor`/`CeleryExecutor`/`KubernetesExecutor`), the metadata database, the webserver, the triggerer; what lives where and how to debug a task stuck in `queued`.
- Running Airflow locally: the official `docker-compose.yaml`, `LocalExecutor` + Postgres metadata DB, `airflow.cfg` essentials, the DAGs folder bind-mount, `AIRFLOW__CORE__` environment overrides.
- Dagster's asset-oriented model: `@asset`, dependencies inferred from function arguments, `DailyPartitionsDefinition`, partitioned backfills, schedules and sensors in Dagster, the asset graph vs the task graph.
- The decision framework: when Airflow's maturity and ecosystem win, when Dagster's asset model and developer experience win, and how to read a team to pick.

## Weekly schedule

The schedule below adds up to approximately **33 hours**. Treat it as a target. Monday's lecture on the data-interval model is the single hour that decides whether the rest of the week — catchup, backfill, idempotency — makes sense. If only one section sticks, make it that one.

| Day       | Focus                                                        | Lectures | Exercises | Challenges | Quiz/Read | Homework | Mini-Project | Self-Study | Daily Total |
|-----------|--------------------------------------------------------------|---------:|----------:|-----------:|----------:|---------:|-------------:|-----------:|------------:|
| Monday    | DAGs, tasks, schedules, intervals, Airflow architecture      |    2h    |    1.5h   |     0h     |    0.5h   |   1h     |     0h       |    0.5h    |     5.5h    |
| Tuesday   | Sensors, retries, backoff, SLAs, alerting                    |    2h    |    1.5h   |     0h     |    0.5h   |   1h     |     0h       |    0.5h    |     5.5h    |
| Wednesday | Backfill, catchup, idempotent windowed loads                 |    1h    |    1.5h   |     1h     |    0.5h   |   1h     |     1h       |    0h      |     6h      |
| Thursday  | The task that lied; Dagster assets & the decision            |    1h    |    1h     |     1h     |    0.5h   |   1h     |     1.5h     |    0h      |     6h      |
| Friday    | Mini-project deep work; Phase I gate dry-run                 |    0h    |    0.5h   |     0h     |    0.5h   |   0h     |     3h       |    0h      |     4h      |
| Saturday  | Mini-project finish, backfill validation                     |    0h    |    0h     |     0h     |    0h     |   0h     |     2.5h     |    0h      |     2.5h    |
| Sunday    | Quiz, review, polish, push                                   |    0h    |    0h     |     0h     |    0.5h   |   0h     |     2h       |    0.5h    |     3h      |
| **Total** |                                                              | **6h**   | **6.5h**  | **3h**     | **3.5h**  | **4h**   | **10h**      | **1.5h**   | **34.5h**   |

## How to navigate this week

| File | What's inside |
|------|---------------|
| [README.md](./README.md) | This overview (you are here) |
| [resources.md](./resources.md) | Airflow docs (DAGs, TaskFlow, sensors, scheduler, executors, backfill, Docker), Dagster docs (assets, partitions, schedules, sensors), and both GitHub repos |
| [lecture-notes/01-dags-tasks-schedules-and-the-airflow-architecture.md](./lecture-notes/01-dags-tasks-schedules-and-the-airflow-architecture.md) | What a DAG is, TaskFlow vs operators, the data-interval model, `schedule`/`start_date`/`catchup`/`max_active_runs`, and the scheduler/executor/metadata-DB architecture with the Docker Compose that runs it all locally |
| [lecture-notes/02-sensors-retries-slas-backfills-and-catchup.md](./lecture-notes/02-sensors-retries-slas-backfills-and-catchup.md) | Sensors (`FileSensor`, `@task.sensor`, poke vs reschedule), retries with exponential backoff, SLAs vs failures, and running a safe `airflow dags backfill` over a date range |
| [lecture-notes/03-idempotency-the-task-that-lied-and-dagster-assets.md](./lecture-notes/03-idempotency-the-task-that-lied-and-dagster-assets.md) | Idempotent windowed loads keyed off `data_interval_start`, the "task succeeded but lied" failure and the assertion task that catches it, and the same pipeline as Dagster assets with a partitioned backfill — plus a pick-one decision framework |
| [exercises/exercise-01-first-dag.py](./exercises/exercise-01-first-dag.py) | Author your first TaskFlow DAG: extract → transform → load, with `schedule`, `start_date`, `catchup`, and a window keyed off `data_interval_start` |
| [exercises/exercise-02-sensor-retries-sla.py](./exercises/exercise-02-sensor-retries-sla.py) | Add a file sensor, retries with exponential backoff, an SLA, and an `on_failure_callback` to the DAG |
| [exercises/exercise-03-safe-backfill.py](./exercises/exercise-03-safe-backfill.py) | Make the load idempotent (delete-then-insert per partition) and run a safe 30-day backfill that does not double-count |
| [exercises/SOLUTIONS.md](./exercises/SOLUTIONS.md) | Worked solutions, expected output, and common pitfalls — read **after** attempting |
| [challenges/challenge-01-backfill-that-double-counts.md](./challenges/challenge-01-backfill-that-double-counts.md) | A DAG that double-counts on backfill is handed to you broken; find the bug, fix the idempotency, and prove it with a re-run |
| [challenges/challenge-02-dagster-assets-rewrite.md](./challenges/challenge-02-dagster-assets-rewrite.md) | Re-express the orchestrated loader as Dagster software-defined assets with a daily partition and a partitioned backfill |
| [quiz.md](./quiz.md) | 10 multiple-choice questions on orchestration concepts and failure modes, with a reasoned answer key |
| [homework.md](./homework.md) | Six practice problems for the week |
| [mini-project/README.md](./mini-project/README.md) | **Crunch Loader Orchestrated** — the Phase I gate: an orchestrated, idempotent, incremental pipeline into the star schema, with a sensor, retries, an SLA, an assertion task, and a clean 30-day backfill |

## A note on tone

C27 is written in **operator voice**. We pin versions ("Apache Airflow 2.9", "Postgres 16", "Dagster 1.7"). We say "the backfill launched 30 task instances against `max_active_runs=16`, queued the rest, and finished in 4m12s without the metadata DB exceeding 40% CPU" — not "the backfill ran fine." We name the failure precisely: not "the load broke" but "task `load_sales` exited zero having committed 4,127 of an expected 10,000 rows because the source file was truncated, and the downstream `assert_rowcount` task caught it." If your runbook says "it usually works," you have not written a runbook yet. Numbers, intervals, exit codes, row counts.

## A note on running Airflow on a laptop

Airflow is a real distributed system pretending to be a single binary. On a laptop, respect that:

- The official Docker Compose runs **five** containers (scheduler, webserver, triggerer, Postgres metadata DB, and a one-shot `airflow-init`). Give Docker **at least 4 GB of RAM** or the scheduler will be OOM-killed and your tasks will sit in `queued` forever with no obvious error. The Compose file warns you about this on startup; read the warning.
- The **metadata DB is Postgres**, the same engine as your warehouse, but a *separate database*. Do not point Airflow at your warehouse database — a runaway scheduler will fill it with task-instance rows. Keep them isolated (separate database, ideally separate container).
- First boot runs `airflow-init` (DB migrations + the admin user) and can take a couple of minutes. Watch `docker compose logs -f airflow-init` until it exits 0 before you open the webserver at `http://localhost:8080`.
- The DAGs folder is **bind-mounted** from the host. Edit a DAG file on the host, and the scheduler picks it up within ~30 seconds (the `dag_dir_list_interval`). You do not rebuild the image to ship a DAG change.

If your laptop genuinely cannot spare 4 GB for Docker, fall back to the `SequentialExecutor` with a SQLite metadata DB via `airflow standalone` — it runs everything in one process, is fine for the first two exercises, and cannot run tasks in parallel (which means the backfill exercise will be slow but still correct). The mini-project assumes the full Compose with `LocalExecutor`.

## Stretch goals

If you finish early and want to push further, try any of the following:

- Swap the `LocalExecutor` for the `CeleryExecutor` with a Redis broker (the official Compose has a `CeleryExecutor` variant). Re-run the 30-day backfill and watch tasks distribute across two worker containers. Read [executor types](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html).
- Replace the time-based schedule with a **dataset-driven** schedule: have the producing DAG declare it `outlets` a dataset and the consuming DAG `schedule` on that dataset. Observe the consumer fire as soon as the producer updates, with no clock involved.
- Wire a real alert: configure an `on_failure_callback` that posts to a local webhook receiver (run a tiny Flask app in Docker) so you can see the alert payload an SLA miss produces, end to end.
- Build the **same** pipeline a third way — plain `cron` calling your Week 3 script — and write down, in two paragraphs, every property you lose: dependency modeling, retries, backfill, observability, the assertion gate. This is the argument for why orchestrators exist.

## Up next

Continue to [Week 5 — Transformation with dbt](../week-05-transformation-with-dbt/) once you have passed the Phase I gate and pushed your mini-project. Week 5 takes the hand-written SQL inside your loader and turns it into versioned, tested, lineage-aware dbt models — and you will wire dbt back into this week's Airflow DAG, so the orchestration you learned here becomes the thing that runs your transformations from Week 5 onward.

---

*If you find errors in this material, please open an issue or send a PR to <https://github.com/CODE-CRUNCH-CLUB>. Future learners will thank you. Licensed GPL-3.0.*
