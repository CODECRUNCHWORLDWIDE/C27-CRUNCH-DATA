"""
Exercise 2 — Add a file sensor, retries with backoff, an SLA, and failure alerting
==================================================================================

GOAL
----
Take the Exercise 1 DAG and make it survive the real world (Lecture 2):
  1. A FileSensor that waits for the day's source file before loading, in
     `mode="reschedule"` so it does not hold a worker slot while it waits, with a
     real `timeout`.
  2. Retries with EXPONENTIAL BACKOFF so a transient failure does not retry-storm.
  3. An `execution_timeout` so a hung task is killed into a retry.
  4. An `sla` on the load plus an `sla_miss_callback` -- the "is this run late?"
     signal, distinct from "did this fail?".
  5. An `on_failure_callback` that fires only after retries are exhausted -- the
     "page a human" signal.

KEY DISTINCTION (Lecture 2 section 3): retries answer "should this run again?";
SLAs answer "is this run late?". You are wiring BOTH, plus the failure alert. Three
signals, three mechanisms. Do not let one stand in for another.

RUN INSTRUCTIONS
----------------
1. Same Airflow + warehouse stack as Exercise 1.
2. The sensor waits for `/data/incoming/sales_<ds>.csv` INSIDE the Airflow
   containers. Make sure that path is bind-mounted (add `./incoming:/data/incoming`
   to the airflow-common volumes) so you can drop files in from the host.
3. Drop this file in `./dags/`. Unpause `crunch_w4_ex02_sensor_retries_sla`.
4. Trigger a run. The sensor will WAIT. Drop a file for that run's date:
     mkdir -p ./incoming && touch ./incoming/sales_2026-06-18.csv
   Watch the sensor turn green and `load` proceed. To see an SLA miss, give the
   load an artificially tiny `sla` and a `time.sleep` longer than it.

ACCEPTANCE CRITERIA
-------------------
[ ] A FileSensor (`wait_for_daily_file`) gates the load; its `filepath` is templated
    with `{{ ds }}` so each run waits for ITS date's file.
[ ] The sensor uses `mode="reschedule"` and a finite `timeout` (a few hours).
[ ] `default_args` set `retries`, `retry_delay`, `retry_exponential_backoff=True`,
    `max_retry_delay`, and `execution_timeout`.
[ ] The `load` task has an `sla`; the DAG has an `sla_miss_callback`.
[ ] An `on_failure_callback` is wired and prints the dag/task/window/log url.
[ ] When you drop the awaited file, the sensor completes and `load` runs. When you
    delete/withhold it past the timeout, the sensor FAILS (soft_fail=False) and the
    failure callback fires.

Tested against: Apache Airflow 2.9, Postgres 16, Python 3.11.
"""

from __future__ import annotations

import datetime

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor

WAREHOUSE_CONN_ID = "warehouse"
INCOMING_DIR = "/data/incoming"  # bind-mounted into the Airflow containers


# --------------------------------------------------------------------------- #
# Callbacks: the alerting surface. In production these post to Slack/PagerDuty/
# a webhook. Here they print structured lines you can grep in the task logs.
# --------------------------------------------------------------------------- #
def on_failure(context) -> None:
    """Fires AFTER retries are exhausted -- the 'page a human' signal."""
    ti = context["task_instance"]
    print(
        f"[ALERT-FAILURE] dag={ti.dag_id} task={ti.task_id} "
        f"window={context['data_interval_start']} "
        f"attempts={ti.try_number - 1} log={ti.log_url}"
    )


def on_retry(context) -> None:
    """Quiet telemetry on each transient retry -- do NOT page on this."""
    ti = context["task_instance"]
    print(f"[RETRY] task={ti.task_id} attempt={ti.try_number} window={context['data_interval_start']}")


def sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """Fires when a task runs past its SLA -- the 'is this late?' signal.

    The task KEEPS RUNNING; this is a notification, not an interruption. Alert a
    human while the load is still trying to catch up.
    """
    late = [s.task_id for s in slas]
    print(f"[ALERT-SLA-MISS] dag={dag.dag_id} late_tasks={late}")


@dag(
    dag_id="crunch_w4_ex02_sensor_retries_sla",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=3,
    sla_miss_callback=sla_miss,
    default_args={
        "owner": "crunch-data",
        # --- Retries: heal transient failures without a retry storm. ---
        "retries": 4,
        "retry_delay": datetime.timedelta(minutes=2),
        "retry_exponential_backoff": True,            # gaps grow ~2,4,8,16 min
        "max_retry_delay": datetime.timedelta(minutes=30),  # capped so it never explodes
        # --- Kill a hung task into a retry. ---
        "execution_timeout": datetime.timedelta(minutes=20),
        # --- Page only after retries are exhausted. ---
        "on_failure_callback": on_failure,
        "on_retry_callback": on_retry,
    },
    tags=["crunch-data", "week-04", "exercise-02"],
)
def crunch_w4_ex02_sensor_retries_sla():

    # ------------------------------------------------------------------ #
    # YOU IMPLEMENT: a FileSensor named `wait_for_daily_file`.
    #   - filepath templated with {{ ds }}:  f"{INCOMING_DIR}/sales_{{{{ ds }}}}.csv"
    #   - poke_interval=60
    #   - timeout = 3 hours (in seconds)
    #   - mode="reschedule"  (free the slot while waiting)
    #   - soft_fail=False    (a missing source is a real failure to surface)
    # ------------------------------------------------------------------ #
    wait_for_daily_file = FileSensor(
        task_id="wait_for_daily_file",
        filepath=f"{INCOMING_DIR}/sales_{{{{ ds }}}}.csv",
        poke_interval=60,
        timeout=60 * 60 * 3,
        mode="reschedule",
        soft_fail=False,
    )

    @task(
        # SLA: the load should finish within 2 hours of the run's expected start.
        # If it does not, sla_miss fires while the task keeps running.
        sla=datetime.timedelta(hours=2),
    )
    def load(data_interval_start: pendulum.DateTime | None = None) -> dict:
        """Load this window's file into fact_sales_staging.

        Reuse the Exercise 1 loader shape. Still a plain INSERT (not idempotent yet
        -- that is Exercise 3). The new thing here is that this task only runs AFTER
        the sensor confirms the file exists, and it inherits retries + SLA from the
        DAG defaults.
        """
        assert data_interval_start is not None
        window = data_interval_start.to_date_string()
        rows = _read_or_synthesize(window)

        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO fact_sales_staging (sales_date, store_key, product_key, amount) "
                "VALUES (%(sales_date)s, %(store_key)s, %(product_key)s, %(amount)s)",
                rows,
            )
            conn.commit()
        print(f"[load] window={window} loaded_rows={len(rows)}")
        return {"window": window, "loaded_rows": len(rows)}

    # Dependency: the sensor must pass before the load runs.
    wait_for_daily_file >> load()


def _read_or_synthesize(window: str) -> list[dict]:
    """Read the day's CSV if present; otherwise synthesize a deterministic source.

    A real loader reads f"{INCOMING_DIR}/sales_{window}.csv". For the exercise we
    fall back to a deterministic synthetic source so the load is reproducible even
    when the dropped file is an empty `touch`ed placeholder.
    """
    import csv
    import os

    path = f"{INCOMING_DIR}/sales_{window}.csv"
    if os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, newline="") as fh:
            return [
                {
                    "sales_date": window,
                    "store_key": int(r["store_key"]),
                    "product_key": int(r["product_key"]),
                    "amount": float(r["amount"]),
                }
                for r in csv.DictReader(fh)
            ]
    day = int(window[-2:])
    n = 100 + day
    return [
        {"sales_date": window, "store_key": (i % 5) + 1, "product_key": (i % 11) + 1,
         "amount": round(10.0 + (i % 7) * 1.5, 2)}
        for i in range(n)
    ]


crunch_w4_ex02_sensor_retries_sla()
