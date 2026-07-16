# Week 4 — Resources

Every resource on this page is **free** and **publicly accessible**. Where we name a version (Apache Airflow 2.9, Postgres 16, Dagster 1.7, Python 3.11), use that exact version when running locally — it pins your reproducibility. If a link breaks, please open an issue.

## Required reading (work it into your week)

- **Apache Airflow documentation (stable)** — the home page for everything below; bookmark it:
  <https://airflow.apache.org/docs/apache-airflow/stable/>
- **Airflow — Core concepts: DAGs (and tasks)** — what a DAG is, tasks, dependencies, the basics you build on all week:
  <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html>
- **Airflow — TaskFlow tutorial** — the `@dag`/`@task` API, passing data, the recommended modern authoring style; also shows `retries`/`retry_delay`:
  <https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html>
- **Airflow — DAG runs, data intervals, catchup & backfill** — the most important page of the week: how a run owns an interval, `data_interval_start`/`data_interval_end`/`logical_date`, `catchup`, and `airflow dags backfill`:
  <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dag-run.html>
- **Airflow — running Airflow in Docker (Docker Compose)** — the official local-dev stack you run all week (LocalExecutor + Postgres metadata DB):
  <https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html>

## Sensors, scheduler & executors (the mechanisms)

- **Airflow — Sensors** — `FileSensor`, the `@task.sensor` decorator, `poke` vs `reschedule` vs deferrable, `timeout`, `soft_fail`:
  <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html>
- **Airflow — Scheduler** — the scheduler loop, DAG parsing, why a dead scheduler leaves tasks stuck in `queued`:
  <https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html>
- **Airflow — Executor types** — `SequentialExecutor`, `LocalExecutor`, `CeleryExecutor`, `KubernetesExecutor`, and when to use each (you use `LocalExecutor`):
  <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html>
- **Airflow — GitHub** — source, issues, providers; a good open-source repo to read and (stretch) PR back to:
  <https://github.com/apache/airflow>

## Dagster (the asset-oriented alternative)

- **Dagster documentation** — the home page:
  <https://docs.dagster.io/>
- **Dagster — Software-defined assets** — `@asset`, dependencies inferred from arguments, asset metadata, asset checks; the core of Lecture 3 §3:
  <https://docs.dagster.io/concepts/assets/software-defined-assets>
- **Dagster — Partitions & backfills** — `DailyPartitionsDefinition`, `context.partition_key`, partitioned (and idempotent) backfills:
  <https://docs.dagster.io/concepts/partitions-schedules-sensors/partitions>
- **Dagster — Schedules** — cron/partitioned schedules, `build_schedule_from_partitioned_job`:
  <https://docs.dagster.io/concepts/automation/schedules>
- **Dagster — Sensors** — event-driven runs, `RunRequest`, file-arrival and upstream-asset triggers:
  <https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors>
- **Dagster — GitHub** — source, issues, examples:
  <https://github.com/dagster-io/dagster>

## Airflow CLI cheat-sheet (the commands you use this week)

All commands run inside the scheduler container: `docker compose exec airflow-scheduler airflow ...`.

| Command | Purpose |
|---------|---------|
| `airflow dags list` | List all parsed DAGs |
| `airflow dags trigger <dag_id>` | Manually trigger one run (current interval) |
| `airflow dags backfill -s <start> -e <end> <dag_id>` | Run a date range of intervals (the 30-day backfill) |
| `airflow dags pause / unpause <dag_id>` | Stop / start automatic scheduling |
| `airflow tasks test <dag_id> <task_id> <logical_date>` | Run one task in isolation, no scheduler, for debugging |
| `airflow dags list-runs -d <dag_id>` | List DAG runs and their states |
| `airflow db migrate` | Apply metadata-DB migrations (run by `airflow-init`) |
| `airflow connections add <id> --conn-uri <uri>` | Register a connection (e.g. the warehouse) from the CLI |

## Key parameters reference (Lectures 1–3)

| Parameter | Where | What it controls |
|-----------|-------|------------------|
| `schedule` | `@dag` | Cadence: `@daily`, cron, `timedelta`, dataset, or `None` |
| `start_date` | `@dag` | The first interval the DAG can run |
| `catchup` | `@dag` | Whether to auto-run all intervals since `start_date` on deploy |
| `max_active_runs` | `@dag` | Cap on concurrent DAG runs (the backfill throttle) |
| `data_interval_start` / `_end` | injected | The window this run owns — key all reads/writes off these |
| `poke_interval` / `timeout` / `mode` | sensor | How often to check / when to give up / slot strategy |
| `retries` / `retry_delay` | task / `default_args` | How many re-runs / base gap |
| `retry_exponential_backoff` / `max_retry_delay` | task / `default_args` | Grow the gap each retry / cap it |
| `execution_timeout` | task / `default_args` | Kill a hung task into a retry |
| `sla` / `sla_miss_callback` | task / `@dag` | Lateness threshold / lateness alert |
| `on_failure_callback` / `on_retry_callback` | `default_args` | Page after retries exhausted / quiet retry telemetry |

## Connecting back / forward in the course

- **Week 1 — Dimensional modeling:** the star schema this week's DAG loads into. <https://airflow.apache.org/docs/apache-airflow/stable/> assumes you have it.
- **Week 3 — Python ETL:** the idempotent watermark-and-upsert loader you wrap this week. Idempotency there is the precondition for safe backfill here.
- **Week 5 — dbt:** you will run dbt *from* an Airflow DAG, so this week's orchestration becomes the runner for next week's transformations.
- **Week 10 — Data quality:** the `assert_load` row-count/checksum gate from Lecture 3 §2 is the seed of the Great Expectations quality layer.

## A note on versions and the laptop

The official Airflow Docker Compose warns you if Docker has less than ~4 GB of RAM — heed it; under that, the scheduler is OOM-killed and tasks hang in `queued` (Quiz Q9). Pin `apache/airflow:2.9.x` and `postgres:16` in your compose file so a reviewer reproduces your exact stack. Keep Airflow's metadata Postgres and your warehouse Postgres as **separate databases** so a runaway scheduler never bloats your facts.

---

*All Week 4 materials are licensed under **GPL-3.0**. Fork, teach, remix; PR improvements back to <https://github.com/CODE-CRUNCH-CLUB>.*
