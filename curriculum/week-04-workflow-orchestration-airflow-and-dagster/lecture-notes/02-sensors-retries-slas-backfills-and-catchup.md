# Lecture 2 — Sensors, Retries, SLAs, Backfills, and Catchup

> **Time:** ~2 hours of reading + extending your Lecture 1 DAG.
> **Prerequisites:** Lecture 1 (the data-interval model, `catchup`, `max_active_runs`, the Docker stack running); Week 3's idempotent loader.
> **Citations:** [Airflow sensors](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html), [TaskFlow tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html), [DAG runs, backfill & catchup](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dag-run.html), [DAGs / tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html), [scheduler](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html).

If you only remember one thing from this lecture, remember this:

> **Retries answer "should this run again?" SLAs answer "is this run late?"** They are different questions with different mechanisms. A task can be failing-and-retrying (not late yet) or succeeding-but-slow (late but never failed). Wire both, and never let one stand in for the other. And remember the throttle: a backfill is safe only when its tasks are idempotent *and* its concurrency is capped.

Lecture 1 gave you a DAG that runs the right window at the right time. This lecture makes that DAG survive the real world: sources that arrive late, networks that blip, runs that overshoot their deadline, and the day you must reprocess a month of corrected history.

---

## 1. Sensors: waiting for the world

Your daily extract is promised by 05:00. Sometimes it lands at 05:00, sometimes 05:40, sometimes — twice a quarter — never. A **sensor** is a task whose whole job is to *wait for a condition to become true* before downstream tasks proceed ([Airflow sensors](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html)). The condition can be "a file exists," "a partition landed," "an upstream marker appeared," "an HTTP endpoint returns 200." The sensor blocks the path until the condition holds or a timeout fires.

### 1.1 A `FileSensor`

The canonical case for our loader: wait for today's source file to appear before loading it.

```python
from airflow.sensors.filesystem import FileSensor

wait_for_file = FileSensor(
    task_id="wait_for_daily_file",
    fs_conn_id="fs_default",                          # a filesystem connection
    filepath="/data/incoming/sales_{{ ds }}.csv",     # templated with the run's date
    poke_interval=60,        # check every 60 seconds
    timeout=60 * 60 * 3,     # give up after 3 hours
    mode="reschedule",       # free the worker slot between checks (see §1.3)
    soft_fail=False,         # on timeout, FAIL the task (do not skip)
)
```

Note `{{ ds }}` — the file path is **templated** with this run's date (`logical_date` as `YYYY-MM-DD`). The run that owns the `2026-06-18` interval waits for `sales_2026-06-18.csv`, the next day's run waits for `sales_2026-06-19.csv`. Templating is how a single DAG definition produces date-specific behavior per run; the `{{ ... }}` Jinja is rendered by Airflow at task execution time using the run's context.

### 1.2 A custom sensor with `@task.sensor`

For conditions a built-in sensor does not cover — "the source table has at least N rows for this window," "an upstream API marked the extract complete" — write your own with the TaskFlow `@task.sensor` decorator. It returns a `PokeReturnValue`: keep waiting, or stop and (optionally) pass a value downstream ([TaskFlow tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html), [sensors](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html)).

```python
from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook

@task.sensor(poke_interval=60, timeout=60 * 60 * 3, mode="reschedule")
def wait_for_source_rows(data_interval_start=None) -> PokeReturnValue:
    window = data_interval_start.to_date_string()
    hook = PostgresHook(postgres_conn_id="source_db")
    (count,) = hook.get_first(
        "SELECT count(*) FROM raw_sales WHERE sales_date = %s", parameters=(window,)
    )
    # is_done True -> stop poking and proceed; xcom_value flows downstream.
    return PokeReturnValue(is_done=count > 0, xcom_value={"window": window, "available": count})
```

### 1.3 `poke` vs `reschedule` vs deferrable — the slot question

A sensor that "waits for three hours" must not *hold a worker slot* for three hours; on a laptop with a handful of slots, a few greedy sensors starve everything else. There are three waiting strategies ([sensors](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html)):

| `mode` | What it does | Cost | When |
|--------|--------------|------|------|
| `poke` (default) | Holds a worker slot the entire time, sleeping `poke_interval` between checks | One slot occupied for the whole wait | Short waits (seconds to a couple of minutes) |
| `reschedule` | Releases the slot between checks; the scheduler re-queues the sensor each `poke_interval` | No slot held while sleeping; tiny scheduler overhead per check | **Long waits — our 3-hour file wait** |
| deferrable (`deferrable=True`) | Hands the wait to the **triggerer** process via an async trigger; frees the slot entirely | Most efficient at scale; needs the triggerer running | Many concurrent long waits |

For waits measured in minutes-to-hours, use `mode="reschedule"`. The triggerer/deferrable path is the most efficient (it is why Lecture 1's stack runs a triggerer container) and is worth knowing, but `reschedule` is the right default for this week.

### 1.4 `soft_fail` and `timeout`

`timeout` is mandatory thinking: a sensor with no timeout that waits on a file that never arrives waits *forever*, and your pipeline is wedged with no alert. Set a `timeout` that reflects your real deadline (3 hours past the promised delivery, say). When the timeout fires, `soft_fail=False` (default) **fails** the sensor — which is correct, because a missing source is a real problem someone must see. `soft_fail=True` instead **skips** the sensor and its downstream tasks — use it only when "the file legitimately may not exist today, and that is fine" (e.g., an optional weekly file on non-Mondays).

---

## 2. Retries and exponential backoff

Networks blip. A database hits a lock and times out. The right response to a *transient* failure is to try again — but a retry that fires *immediately* turns one blip into a hammering retry storm against an already-struggling dependency. So you space retries out, growing the gap each time.

### 2.1 The parameters

These go in `default_args` (applied to every task) or on an individual task ([core concepts](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html), [TaskFlow tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)):

```python
import datetime, pendulum
from airflow.decorators import dag

@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    default_args={
        "retries": 4,                                       # try up to 4 times after the first failure
        "retry_delay": datetime.timedelta(minutes=2),       # base gap before the first retry
        "retry_exponential_backoff": True,                  # double the gap each retry
        "max_retry_delay": datetime.timedelta(minutes=30),  # cap the gap so it never explodes
        "execution_timeout": datetime.timedelta(minutes=20),# kill a task that runs too long
    },
)
def resilient_dag():
    ...
```

With `retry_exponential_backoff=True` and a 2-minute base, the gaps grow roughly 2, 4, 8, 16 minutes (jittered), capped at `max_retry_delay=30m`. So the four retries span ~30 minutes of patience, not four instant re-hammers. Without exponential backoff, all four retries fire 2 minutes apart — fine for some cases, dangerous when the dependency needs time to recover.

`execution_timeout` is the other half: a task that *hangs* (a query that never returns, a download stalled on a half-open socket) will otherwise occupy its slot indefinitely. `execution_timeout` kills it, which then counts as a failure and triggers a retry — exactly what you want for a hung task.

### 2.2 Retries are only safe if the task is idempotent

Here is the trap, and it is the bridge to Lecture 3. Suppose `load` runs, **commits 10,000 rows**, and *then* the worker dies before reporting success — a timeout, an OOM, a network partition between worker and metadata DB. Airflow sees no success, marks the task failed, and **retries it**. The retry runs `load` again. If `load` is a plain `INSERT`, you now have **20,000 rows** — double-counted, silently. The retry that was supposed to heal a transient failure has corrupted your data.

The fix is not "fewer retries." The fix is **idempotency**: `load` must delete-then-insert (or merge) its own window, so running it once or five times yields the identical state. That is Lecture 3 and Exercise 3. Internalize the dependency now: **you cannot safely turn on retries, catchup, or backfill until your tasks are idempotent.** Retries make a correct pipeline resilient and an incorrect pipeline corrupt.

---

## 3. SLAs and alerting: "is this run late?"

A retry asks whether to run again. An **SLA** (Service-Level Agreement) asks a different question: *is this run taking longer than it should?* A task can be merrily succeeding and still be unacceptably late — the daily mart is supposed to be ready by 07:00 and it is now 09:00 and the load is still grinding. Nothing failed; the business is still blind. That is an SLA miss.

### 3.1 The `sla` parameter and the miss callback

```python
import datetime

def sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    # Called by the scheduler when one or more tasks miss their SLA.
    # `slas` is the list of missed SLAs; alert a human here.
    print(f"SLA MISS on {dag.dag_id}: tasks {[s.task_id for s in slas]} ran past their SLA")
    # -> post to Slack / PagerDuty / a webhook in real life

@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    sla_miss_callback=sla_miss_alert,        # fired when any task misses its SLA
)
def sla_dag():

    @task(sla=datetime.timedelta(hours=2))   # this task should finish within 2h of the run start
    def load(...):
        ...
```

The `sla` is a `timedelta` measured **from the DAG run's expected start**. If the task has not completed within that window, the scheduler records an SLA miss and invokes `sla_miss_callback`. Crucially, **the task keeps running** — an SLA miss is a *notification*, not an interruption. You want the human alerted *while the load is still trying to catch up*, not after it finally finishes at noon.

> The SLA semantics are historically Airflow's weakest corner — the timer is relative to the schedule, not to "when the task actually started," which surprises people whose DAGs have long upstream sensors. Know the gotcha: if a 3-hour sensor precedes a 2-hour-SLA load, the load's SLA window may already be blown before the load even starts. For our pipeline we set the SLA on the *whole critical path* or on the final task accordingly, and we keep the timeout on the sensor as the safety net for "source never arrived."

### 3.2 Failure and retry callbacks

For the *failed loudly* path (as opposed to *late*), use task-level callbacks ([core concepts](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)):

```python
def on_failure(context):
    ti = context["task_instance"]
    print(f"ALERT: {ti.dag_id}.{ti.task_id} failed for {context['data_interval_start']} "
          f"after {ti.try_number - 1} retries. Log: {ti.log_url}")

def on_retry(context):
    ti = context["task_instance"]
    print(f"retrying {ti.task_id}, attempt {ti.try_number}")

@dag(
    default_args={
        "on_failure_callback": on_failure,   # fires after the LAST retry fails
        "on_retry_callback": on_retry,       # fires on each retry
    },
    ...
)
```

`on_failure_callback` fires only when the task has **exhausted its retries and finally failed** — that is the moment to page a human. `on_retry_callback` fires on each transient retry; use it for low-noise telemetry, not for paging (you do not want to wake someone for a self-healing blip). Putting the two together: retries + `on_retry_callback` handle the transient blip quietly; `on_failure_callback` pages when retries are exhausted; `sla`/`sla_miss_callback` alert when the run is late even though nothing has failed. Three signals, three mechanisms.

---

## 4. Backfill and catchup: reprocessing history

A **backfill** runs a DAG over a range of *past* intervals. You backfill when you ship a new pipeline and must populate the last 30 days, when a source vendor re-issues corrected history and you must reprocess it, or when a bug meant last week's loads were wrong and you must redo them. Catchup (Lecture 1 §4.1) is *automatic* backfill on deploy; `airflow dags backfill` is *manual, on-demand* backfill over an explicit range. The mechanics are identical: enumerate the intervals in range, schedule a run for each ([DAG runs, backfill & catchup](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dag-run.html)).

### 4.1 The CLI

```bash
# Backfill 30 days, inclusive of the start, exclusive of the end interval.
# Run it INSIDE the scheduler (or any) container so it shares the metadata DB.
docker compose exec airflow-scheduler \
  airflow dags backfill \
    --start-date 2026-05-20 \
    --end-date   2026-06-19 \
    crunch_sales_daily
```

This enumerates the daily intervals from 2026-05-20 through 2026-06-18 (the interval owning `06-19` is excluded as its `end` has not closed) and runs each. Each run gets its own correct `data_interval_start`, so each loads *its* window — provided your loader keys off the interval (Lecture 1 §3.2).

### 4.2 The two ways a backfill melts the cluster

**Melt #1 — no concurrency cap.** A naive backfill of 30 days with no `max_active_runs` launches up to 30 runs concurrently. Thirty simultaneous `load` tasks hammer the warehouse Postgres; CPU pins at 100%, connections exhaust, the metadata DB (also Postgres) slows because the scheduler is fighting for the same machine, healthchecks start failing, the scheduler gets restarted, tasks stick in `queued`, and the on-call engineer is paged at 02:00 to a pipeline that is "running" but accomplishing nothing. **Fix:** set `max_active_runs` (Lecture 1 §4.2). With `max_active_runs=16`, the backfill runs 16 at a time and queues the rest — bounded, predictable load. This is the "anatomy of a backfill that melts the cluster" from the week's lecture spine, and the fix is one parameter.

**Melt #2 — non-idempotent tasks.** A backfill of 30 days against a `load` that *appends* loads every window — and if any of those windows were *already loaded* (by the original scheduled run, or by an earlier backfill attempt), it loads them *again*. Now every double-loaded day has doubled rows, your revenue numbers are wrong, and you cannot tell which days are affected without an audit. **Fix:** idempotency — delete-then-insert per window (Lecture 3). A correctly idempotent backfill is *safe to run twice*; a non-idempotent one corrupts on the first re-run.

### 4.3 Catchup vs backfill — which to use

| | Catchup (`catchup=True`) | Manual backfill (`airflow dags backfill`) |
|---|---|---|
| Trigger | Automatic on deploy / unpause | You run the CLI |
| Range | `start_date` → now | Explicit `--start-date` / `--end-date` |
| When | New DAG that must own a full history | Reprocess a specific corrected range; one-off fixes |
| Risk | Floods if `start_date` is far back + no cap | You control the range, but still need the cap |
| Our default | `catchup=False` for dev safety | Run a deliberate, capped 30-day backfill on demand |

For the mini-project you will deploy with `catchup=False` (no surprise flood), then run a deliberate, capped, idempotent 30-day backfill from the CLI and *prove* it does not double-count by re-running it and diffing row counts.

### 4.4 Proving a backfill is safe

The proof is mechanical and is exactly what the Phase I gate asks for:

```sql
-- 1. Capture the per-day fact counts after the first backfill.
SELECT sales_date, count(*) AS n
FROM   fact_sales
GROUP  BY sales_date ORDER BY sales_date;
```

Run the **same** 30-day backfill a second time. Re-run the query. If the per-day counts are **identical**, the load is idempotent and the backfill is safe. If any day's count grew, you have a double-count bug — almost always a `load` that appends instead of replacing its window, or a window keyed off `now()` instead of `data_interval_start`. The challenge `challenge-01-backfill-that-double-counts.md` hands you exactly this bug to find.

---

## 5. Putting it together: the resilient daily DAG

Here is the Lecture 1 skeleton, now with a sensor, retries, an SLA, and callbacks — the shape you build across Exercises 1–2 and harden in Exercise 3:

```python
import datetime, pendulum
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

def on_failure(context):
    ti = context["task_instance"]
    print(f"ALERT {ti.dag_id}.{ti.task_id} failed for {context['data_interval_start']}")

def sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"SLA MISS {dag.dag_id}: {[s.task_id for s in slas]}")

@dag(
    dag_id="crunch_sales_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=16,                                  # cap concurrency for safe backfills
    sla_miss_callback=sla_miss,
    default_args={
        "owner": "crunch-data",
        "retries": 4,
        "retry_delay": datetime.timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": datetime.timedelta(minutes=30),
        "execution_timeout": datetime.timedelta(minutes=20),
        "on_failure_callback": on_failure,
    },
    tags=["crunch-data", "week-04"],
)
def crunch_sales_daily():

    wait_for_file = FileSensor(
        task_id="wait_for_daily_file",
        filepath="/data/incoming/sales_{{ ds }}.csv",
        poke_interval=60,
        timeout=60 * 60 * 3,
        mode="reschedule",
    )

    @task(sla=datetime.timedelta(hours=2))
    def load(data_interval_start=None) -> dict:
        window = data_interval_start.to_date_string()
        hook = PostgresHook(postgres_conn_id="warehouse")
        # Idempotent delete-then-insert for THIS window — built in Lecture 3.
        # (placeholder shape; see Exercise 3 for the real upsert)
        return {"window": window}

    wait_for_file >> load()

crunch_sales_daily()
```

Read the dependency: the sensor must succeed (file present) before `load` runs; `load` retries on transient failure with growing backoff, alerts on final failure, and trips an SLA-miss alert if it runs past two hours. The one thing still missing — the thing that makes the backfill *safe* — is idempotent load. That is Lecture 3.

---

## Summary

- A **sensor** waits for an external condition before downstream tasks run; `FileSensor` waits for a templated file path, `@task.sensor` waits for any condition you can check in Python ([sensors](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html)).
- Choose the wait mode by duration: `poke` for short waits (holds a slot), `reschedule` for long waits (frees the slot — our default), deferrable for many concurrent waits (uses the triggerer). Always set a `timeout`.
- **Retries** (`retries`, `retry_delay`, `retry_exponential_backoff`, `max_retry_delay`) heal transient failures; exponential backoff prevents a retry storm; `execution_timeout` kills a hung task into a retry ([TaskFlow tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)).
- Retries are **only safe on idempotent tasks** — a non-idempotent `load` that commits and then has the worker die will double-count on retry (Lecture 3 fixes this).
- **SLAs** answer "is this run late?" — `sla` plus `sla_miss_callback` alert a human while the task keeps running. `on_failure_callback` pages only after retries are exhausted; `on_retry_callback` is quiet telemetry. Three signals, three mechanisms ([core concepts](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)).
- A **backfill** reprocesses past intervals; `airflow dags backfill -s START -e END DAG_ID` runs an explicit range, catchup does it automatically on deploy ([backfill & catchup](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dag-run.html)).
- A backfill melts the cluster two ways: no concurrency cap (fix with `max_active_runs`) and non-idempotent tasks (fix with delete-then-insert per window). Prove safety by re-running and diffing per-day row counts — identical counts mean idempotent.

*Cited pages: Airflow sensors, TaskFlow tutorial, DAG runs / backfill / catchup, core concepts (DAGs & tasks), and scheduler (all linked inline above), Apache Airflow 2.9 documentation.*
