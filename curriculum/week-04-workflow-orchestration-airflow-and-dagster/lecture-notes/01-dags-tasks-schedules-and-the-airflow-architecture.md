# Lecture 1 — DAGs, Tasks, Schedules, and the Airflow Architecture

> **Time:** ~2 hours of reading + a working Airflow stack on your laptop.
> **Prerequisites:** Week 3's idempotent incremental loader committed; Docker with ≥ 4 GB RAM; Python 3.11+; you can read SQL fluently (Week 2).
> **Citations:** [Airflow DAGs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html), [TaskFlow tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html), [DAG runs & data intervals](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dag-run.html), [scheduler](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html), [executor types](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html), [running in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

If you only remember one thing from this lecture, remember this:

> **An Airflow DAG does not run *at* a time. It runs *for a time interval*, and it runs *after that interval has closed*.** Every retry, catchup, and backfill behavior in the entire system is a consequence of that one sentence. The variables `data_interval_start` and `data_interval_end` tell each run which slice of time it is responsible for. Your load must key off that slice — never off `now()`.

This is the lecture where you build the mental model. Lecture 2 is where you make it resilient (sensors, retries, SLAs, backfills); Lecture 3 is where you make it correct under repetition (idempotency, the assertion gate, Dagster). Without this lecture the scheduler is a black box; with it, every "why did my task run / not run / run twice" question has a mechanical answer.

---

## 1. Why an orchestrator and not `cron`

You already have a working loader from Week 3. The temptation is to put one line in a crontab:

```cron
0 6 * * *  cd /opt/crunch && python load_sales.py
```

This runs your loader at 06:00 every day. It also gives you *nothing else*. `cron` cannot express "run `load` only after `extract` succeeds." It cannot retry a transient failure. It cannot tell you the job is late. It cannot re-run last Tuesday. It has no record of whether yesterday's run succeeded — if the box was down at 06:00, that day is simply gone. And if `load_sales.py` exits zero having loaded half the rows, `cron` reports success, because `cron` only knows about exit codes, never about data.

An **orchestrator** adds exactly the things `cron` lacks: it models **dependencies** between units of work, it tracks the **state** of every run of every task in a database, it **retries** on failure, it **alerts** when work is late, and it can **re-run** any past interval on demand. Airflow ([Apache Airflow docs](https://airflow.apache.org/docs/apache-airflow/stable/)) is the most widely deployed open-source orchestrator; it has been the default in data engineering for a decade. Dagster (Lecture 3) is the modern asset-oriented challenger. Both are far more than `cron`. The cost of that power is that you must understand the model, starting with the DAG.

---

## 2. The DAG: a directed acyclic graph of tasks

A **DAG** (Directed Acyclic Graph) is your pipeline expressed as a graph: nodes are **tasks** (units of work), edges are **dependencies** (run order). "Directed" means edges point from upstream to downstream. "Acyclic" means no cycles — you cannot have A depend on B depend on A. The acyclic property is not pedantry: it is the guarantee that a valid execution order exists and that "wait for upstream" terminates ([Airflow DAGs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)).

A DAG is *defined in a Python file* that lives in Airflow's `dags/` folder. The scheduler imports that file, builds the graph, and schedules runs. There are two ways to write the graph.

### 2.1 Classic operators

The original style wires pre-built **operators** with the `>>` (downstream) operator:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum

def _extract(**context):
    # context["data_interval_start"] is a pendulum.DateTime
    print("extracting for", context["data_interval_start"])

def _load(**context):
    print("loading for", context["data_interval_start"])

with DAG(
    dag_id="classic_example",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")
    extract = PythonOperator(task_id="extract", python_callable=_extract)
    load = PythonOperator(task_id="load", python_callable=_load)

    start >> extract >> load   # this line *is* the dependency graph
```

`start >> extract >> load` reads "start, then extract, then load." The `>>` builds edges; it does not call the functions. Airflow calls each callable later, once per scheduled interval, passing a `context` dict full of run metadata. An `EmptyOperator` does nothing — it is a structural placeholder, useful as a fan-in/fan-out join point.

### 2.2 The TaskFlow API

The modern style ([TaskFlow tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)) uses the `@dag` and `@task` decorators, and the *data flow defines the dependency graph*:

```python
from airflow.decorators import dag, task
import pendulum

@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    tags=["crunch-data", "week-04"],
)
def taskflow_example():

    @task
    def extract(data_interval_start=None):
        # Airflow injects data_interval_start by name. Return a small value.
        return {"window": data_interval_start.to_date_string(), "rows": 1000}

    @task
    def transform(extracted: dict):
        return {**extracted, "rows": extracted["rows"]}  # toy transform

    @task
    def load(transformed: dict):
        print(f"loading {transformed['rows']} rows for {transformed['window']}")

    load(transform(extract()))   # the call graph *is* the dependency graph

taskflow_example()
```

Here `load(transform(extract()))` does **not** run the functions inline. The decorators turn each `@task` into a factory that returns an *XCom reference*; passing that reference as an argument creates the dependency edge `extract >> transform >> load`. This reads almost exactly like the Week 3 Python you already wrote, which is why TaskFlow is the recommended style for new DAGs and the one we use for the rest of the week.

### 2.3 XCom: pass references, not payloads

The values passed between TaskFlow tasks travel through **XCom** ("cross-communication"), which is stored in the metadata database. That has a hard implication: **XCom is for small values** — a row count, a file path, a date window, a dict of a few keys. Do *not* return a pandas DataFrame or a 50 MB list from a task; you will bloat the metadata DB and slow the scheduler. The correct pattern is to write the bulk data to a real store (Postgres, a file, MinIO) and pass the *location* through XCom. Our loader passes a window and a row count; the rows themselves go straight into Postgres.

---

## 3. Schedules and the data-interval model (read this twice)

This is the section everyone gets wrong the first week, so we go slowly.

### 3.1 A run is responsible for an interval, and runs after it closes

Set `schedule="@daily"` and `start_date=2026-06-01`. Airflow defines a sequence of **data intervals**: `[2026-06-01 00:00, 2026-06-02 00:00)`, `[2026-06-02 00:00, 2026-06-03 00:00)`, and so on. Each interval is a half-open window — start inclusive, end exclusive. The **DAG run** for the interval `[06-18, 06-19)` does **not** fire on the 18th. It fires shortly after `2026-06-19 00:00`, because *that is the first moment the 18th's data is complete* ([DAG runs & data intervals](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dag-run.html)).

This is the right behavior, and once you see why, it is obvious: if you are loading "all of June 18th's sales," you cannot do that *during* June 18th — the day is not over. So Airflow waits for the interval to close, then runs the job that processes the just-closed interval. The job for the 18th runs on the 19th.

```mermaid
flowchart LR
  A["Interval opens Jun 18 00:00"] --> B["Interval closes Jun 19 00:00"]
  B --> C["Run fires after close"]
  C --> D["Run owns the Jun 18 window"]
```
*A run does not fire during its interval — it fires once that interval has closed.*

### 3.2 The variables that name the window

Inside any task, Airflow injects these (in `context`, or by name in TaskFlow):

| Variable | Meaning | For the `[06-18, 06-19)` run |
|----------|---------|------------------------------|
| `data_interval_start` | Start of the window this run owns (inclusive) | `2026-06-18 00:00 UTC` |
| `data_interval_end` | End of the window (exclusive) | `2026-06-19 00:00 UTC` |
| `logical_date` | The "label" of the run; for cron/preset schedules it equals `data_interval_start` | `2026-06-18 00:00 UTC` |
| `ds` | `logical_date` as `YYYY-MM-DD` string | `"2026-06-18"` |
| `run_id` | Unique id of this DAG run | `scheduled__2026-06-18T00:00:00+00:00` |

> `logical_date` was called `execution_date` before Airflow 2.2. You will see `execution_date` in old code and Stack Overflow answers; it is the same idea and is deprecated. Older tutorials also said "Airflow runs at the *end* of the interval" — true, but the cleaner way to think is "the run *owns* the interval `[start, end)` and fires after `end`."

**The load-bearing rule:** your loader must filter the source and key its writes off `data_interval_start` / `data_interval_end`, *never* off `datetime.now()`. If you write `WHERE event_date = CURRENT_DATE`, a backfill of June 1st running today loads *today's* data into June 1st's partition — silent, total corruption. If instead you write `WHERE event_date = '{{ data_interval_start | ds }}'`, the same run correctly loads June 1st. This single discipline is what makes catchup and backfill (Lecture 2) safe.

### 3.3 Schedule syntaxes

`schedule` accepts several forms ([DAG runs](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dag-run.html)):

```python
schedule="@daily"                       # preset: 00:00 every day
schedule="@hourly"                       # preset: top of every hour
schedule="0 6 * * *"                     # cron: 06:00 every day (interval is still daily)
schedule=datetime.timedelta(hours=6)     # every 6 hours, relative to start_date
schedule=None                            # manual / triggered only — no automatic runs
schedule=[Dataset("s3://.../sales")]     # dataset-driven: run when an upstream dataset updates
```

A subtlety with cron: `schedule="0 6 * * *"` does **not** mean "the data interval is 06:00-to-06:00." Airflow infers the interval from the cron cadence (daily here) and fires the run for interval `[day N, day N+1)` at 06:00 on day N+1. The cron expression controls *when the run launches*, the cadence controls *what window it owns*. For our daily loads, `@daily` and `0 6 * * *` own the same windows; the second just launches at 06:00 instead of 00:00.

---

## 4. `start_date`, `catchup`, and `max_active_runs`

These four parameters decide *which runs exist* and *how many run at once*. Getting them wrong is how you melt the warehouse (Lecture 2).

### 4.1 `start_date` and `catchup`

```python
@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    catchup=True,
)
```

Deploy this DAG on 2026-06-19 with `catchup=True`, and the scheduler immediately enumerates *every* daily interval from 2026-05-01 to now — about 49 intervals — and schedules a run for each. That is **catchup**: it fills in history automatically. It is exactly what you want when you ship a new pipeline and need to backfill the past month. It is also exactly how you launch 49 simultaneous loads against a Postgres box that can handle three.

Set `catchup=False` and the scheduler creates *only the most recent* interval on deploy; history is not filled. New DAGs in development almost always want `catchup=False` to avoid surprise floods. Production DAGs that must own a full history want `catchup=True` **plus** a concurrency cap.

> Rule of thumb: never deploy a DAG with `catchup=True` *and* an unbounded `max_active_runs` *and* a past `start_date` you have not thought about. That combination is the single most common "the new hire took down the warehouse" story in data engineering.

### 4.2 `max_active_runs` and `max_active_tasks`

```python
@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    catchup=True,
    max_active_runs=3,        # at most 3 DAG runs (3 intervals) concurrently
    default_args={"depends_on_past": False},
)
```

`max_active_runs` caps how many *DAG runs* (intervals) execute at the same time. With `catchup=True` and 49 pending intervals, `max_active_runs=3` runs three, queues the other 46, and feeds them in as slots free up. This is the throttle that keeps a backfill from being a denial-of-service attack on your own warehouse. `max_active_tasks` (formerly `concurrency`) caps the number of *tasks* across all runs of the DAG. For our loader — one heavy `load` task per run — `max_active_runs` is the knob that matters.

`depends_on_past=True` is a stronger constraint: a task instance will not run until *the same task in the previous interval* has succeeded. Use it when interval N genuinely depends on interval N-1 (e.g., a running balance). It serializes the backfill (no parallelism across intervals) and means one stuck day blocks all later days — powerful but heavy. Our loader is per-partition independent, so we leave it `False`.

---

## 5. The Airflow architecture, honestly

Airflow looks like one binary; it is four cooperating components plus a database. Knowing which is which is what lets you debug "my task is stuck in `queued`."

```text
                         +-------------------------+
        DAG files  --->  |       SCHEDULER         |
        (dags/)          |  - parses DAG files      |
                         |  - computes which task   |
                         |    instances are runnable |
                         |  - writes their state to  |
                         |    the metadata DB and    |
                         |    hands them to the      |
                         |    EXECUTOR               |
                         +-----------+-------------+
                                     |
                       (queues task instances)
                                     v
                         +-----------+-------------+         +-------------------+
                         |        EXECUTOR          | <-----> |   WORKERS         |
                         |  Sequential / Local /    |  runs   |  (run task code,  |
                         |  Celery / Kubernetes     |  tasks  |   e.g. your load) |
                         +-----------+-------------+         +-------------------+
                                     |
                          (read/write state)
                                     v
                         +-----------+-------------+
                         |   METADATA DATABASE      |   <-- the source of truth
                         |   (Postgres / MySQL)     |       DAG runs, task instances,
                         |                          |       XComs, connections, vars
                         +-----------+-------------+
                                     ^
                          (reads state to render)
                                     |
                         +-----------+-------------+      +-------------------+
                         |        WEBSERVER         |      |    TRIGGERER       |
                         |  (the UI at :8080)       |      |  (runs deferrable  |
                         +--------------------------+      |   sensors/ops)     |
                                                           +-------------------+
```

### 5.1 The scheduler

The **scheduler** ([scheduler docs](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html)) is the heart. In a loop it: parses the DAG files in `dags/` (every `dag_dir_list_interval`, default 5 minutes for the file list, but file *contents* are re-parsed more often), creates DAG runs for intervals that have closed, evaluates each task instance's dependencies, and when a task is runnable, sets it to `scheduled` then `queued` and hands it to the executor. The scheduler does **not** run your task code. If the scheduler is dead (often OOM-killed on a memory-starved laptop), tasks never leave `queued` and there is no obvious error — the first thing to check when tasks hang.

### 5.2 The executor

The **executor** ([executor types](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html)) is the strategy for *where task code actually runs*:

| Executor | Parallelism | Where tasks run | Use it for |
|----------|-------------|-----------------|------------|
| `SequentialExecutor` | None (one at a time) | In the scheduler process; SQLite metadata DB | `airflow standalone` quick start only |
| `LocalExecutor` | Yes (subprocesses) | On the scheduler host, as subprocesses | **A single laptop / single box — this week** |
| `CeleryExecutor` | Yes (distributed) | On separate worker machines via a broker (Redis/RabbitMQ) | Multi-machine clusters |
| `KubernetesExecutor` | Yes (one pod per task) | A fresh Kubernetes pod per task | Cloud-native, isolated, elastic |

For everything this week, `LocalExecutor` against a Postgres metadata DB is the right answer: real parallelism (so a 30-day backfill is not glacial), no broker to manage, runs on one laptop.

### 5.3 The metadata database

The **metadata DB** is the source of truth. Every DAG run, every task-instance state transition, every XCom, every connection and variable lives there. It is Postgres (or MySQL) — **never SQLite for `LocalExecutor`**, because SQLite cannot handle concurrent writers and will serialize you back to one task at a time. If the metadata DB is slow, the *whole orchestrator* is slow, because the scheduler reads and writes it constantly. On your laptop it is a Postgres container — the same engine as your warehouse, in a **separate database** so a runaway scheduler cannot bloat your facts.

### 5.4 The webserver and the triggerer

The **webserver** is the UI at `http://localhost:8080`: the grid view, the graph view, logs, manual triggers, and the backfill controls. It only *reads and renders* metadata-DB state and lets you issue commands; it runs no task code. The **triggerer** is a separate process that runs *deferrable* operators and sensors efficiently (Lecture 2 §2.3) — it lets a sensor "wait" without holding a worker slot.

```mermaid
flowchart LR
  DAGFiles["DAG files"] --> Scheduler
  Scheduler --> Executor
  Executor --> Workers
  Scheduler --> DB["Metadata DB"]
  Executor --> DB
  DB --> Webserver["Webserver UI"]
  Triggerer --> DB
```
*Five cooperating components, one source of truth: the metadata database.*

---

## 6. Running Airflow locally in Docker

Airflow ships an official Docker Compose for local development ([running in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)). Fetch it and run it:

```bash
# 1. Get the official compose file (pin the version — we use 2.9).
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.3/docker-compose.yaml'

# 2. Make the host folders Airflow bind-mounts, and set the host UID
#    so files Airflow writes are owned by you, not root.
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env

# 3. Initialize the metadata DB and create the admin user (one-shot).
docker compose up airflow-init

# 4. Bring up the stack: scheduler, webserver, triggerer, postgres.
docker compose up -d

# 5. Watch it become healthy, then open http://localhost:8080 (airflow/airflow).
docker compose ps
```

The shape of that Compose file (trimmed to the load-bearing parts; the official one is longer and you should read it):

```yaml
# docker-compose.yaml (excerpt — official Airflow local-dev compose)
x-airflow-common: &airflow-common
  image: apache/airflow:2.9.3
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    # The metadata DB connection: a SEPARATE Postgres, not your warehouse.
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"
  volumes:
    - ./dags:/opt/airflow/dags        # bind-mount your DAGs from the host
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    - ./config:/opt/airflow/config
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on: &airflow-common-depends-on
    postgres:
      condition: service_healthy

services:
  postgres:                            # Airflow's METADATA DB (not the warehouse)
    image: postgres:16
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    depends_on: *airflow-common-depends-on

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports: ["8080:8080"]
    depends_on: *airflow-common-depends-on

  airflow-triggerer:
    <<: *airflow-common
    command: triggerer
    depends_on: *airflow-common-depends-on

  airflow-init:                        # one-shot: db migrate + create admin user
    <<: *airflow-common
    entrypoint: /bin/bash
    command:
      - -c
      - airflow db migrate && airflow users create --username airflow
        --password airflow --firstname a --lastname b --role Admin --email a@b.c

volumes:
  postgres-db-volume:
```

Two notes that save you an hour each:

- **The warehouse is a *different* Postgres.** In this course you have one Postgres container for the warehouse (your star schema) and a second for Airflow's metadata. To let a DAG write to the warehouse, define an Airflow **Connection** (UI → Admin → Connections, or `AIRFLOW_CONN_*` env var) pointing at the warehouse container, and use `PostgresHook(postgres_conn_id="warehouse")` in your tasks. Never reuse the metadata-DB connection for warehouse writes.
- **Drop your DAG file in `./dags/` on the host.** The bind-mount makes it appear inside the containers; the scheduler parses it within ~30s and it shows up (paused) in the UI. Unpause it to let it schedule. No image rebuild.

---

## 7. A first end-to-end DAG against the warehouse

Pulling §2–§6 together, here is the skeleton you will flesh out in Exercise 1 — a daily, interval-keyed load into the Week 1 star schema:

```python
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

@dag(
    dag_id="crunch_sales_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,                 # flip to True only with max_active_runs set
    max_active_runs=3,
    default_args={"owner": "crunch-data"},
    tags=["crunch-data", "week-04"],
)
def crunch_sales_daily():

    @task
    def extract(data_interval_start=None) -> dict:
        # Read ONLY this interval's slice of the source. Key off the interval.
        window = data_interval_start.to_date_string()      # 'YYYY-MM-DD'
        # ... read source file / table filtered to `window` ...
        return {"window": window, "row_count": 0}          # small XCom payload

    @task
    def load(extracted: dict) -> dict:
        hook = PostgresHook(postgres_conn_id="warehouse")
        window = extracted["window"]
        # Idempotent load comes in Lecture 3 / Exercise 3 (delete-then-insert
        # for exactly this window). For now, a plain insert keyed off the window.
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO fact_sales_staging (sales_date, amount) "
                "SELECT %s, 0 WHERE FALSE",                # placeholder shape
                (window,),
            )
            conn.commit()
        return {"window": window, "loaded": True}

    load(extract())

crunch_sales_daily()
```

The shape is the lesson: `extract` and `load` both take `data_interval_start` (directly or via the dict), both operate on *one window*, and the call `load(extract())` declares the dependency. In Lecture 3 you make `load` delete-then-insert so a re-run is safe; in Lecture 2 you add the sensor, retries, SLA, and backfill around it.

---

## Summary

- An orchestrator gives you what `cron` cannot: dependency modeling, state tracking, retries, alerting on lateness, and on-demand re-runs of any past interval ([Airflow docs](https://airflow.apache.org/docs/apache-airflow/stable/)).
- A DAG is a directed acyclic graph of tasks; write it with classic operators (`a >> b`) or the TaskFlow API (`@dag`/`@task`, where the call graph is the dependency graph) ([DAGs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html), [TaskFlow](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)).
- **A run owns a data interval `[start, end)` and fires after `end` closes.** Key every read and write off `data_interval_start`/`data_interval_end`, never `now()` — this is the discipline that makes catchup and backfill safe ([data intervals](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dag-run.html)).
- XCom is for small values (counts, paths, windows); put bulk data in a real store and pass its location.
- `catchup=True` fills history automatically and will flood your warehouse unless `max_active_runs` throttles it; `catchup=False` plus an explicit backfill is the safer default for new DAGs.
- Airflow is a scheduler (decides what runs), an executor (decides where it runs — use `LocalExecutor` on a laptop), a metadata DB (Postgres, the source of truth), a webserver (the UI), and a triggerer (deferrable waits) ([scheduler](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html), [executors](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html)).
- The official Docker Compose runs the whole stack locally with `LocalExecutor` + Postgres; keep Airflow's metadata DB separate from your warehouse and bind-mount `./dags` from the host ([Docker Compose](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)).

*Cited pages: Airflow DAGs, TaskFlow tutorial, DAG runs & data intervals, scheduler, executor types, and running Airflow in Docker (all linked inline above), Apache Airflow 2.9 documentation.*
