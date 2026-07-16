"""
Exercise 1 — Your first TaskFlow DAG (extract -> transform -> load), keyed off the data interval
================================================================================================

GOAL
----
Author a daily Airflow DAG, using the TaskFlow API, that loads one day of sales
into the Postgres warehouse. The whole point of the exercise is the DATA-INTERVAL
DISCIPLINE from Lecture 1 section 3: every task must operate on the window
`data_interval_start` -> `data_interval_end`, never on `datetime.now()`. If you key
off `now()`, this DAG cannot be backfilled safely in Exercise 3.

You will:
  1. Build a three-task pipeline: extract -> transform -> load.
  2. Set `schedule`, `start_date`, `catchup`, and `max_active_runs` deliberately.
  3. Make every task derive its window from the injected `data_interval_start`.
  4. Pass only SMALL values through XCom (a window string + a row count), never
     the bulk rows.

This exercise does NOT yet make the load idempotent (that is Exercise 3) and does
NOT yet add a sensor / retries / SLA (that is Exercise 2). Keep it focused: get the
graph, the schedule, and the interval keying right.

RUN INSTRUCTIONS
----------------
1. Stand up the official Airflow Docker Compose stack with LocalExecutor + Postgres
   (Lecture 1 section 6). Confirm http://localhost:8080 is up (airflow / airflow).
2. Stand up your WAREHOUSE Postgres (the Week 1 star schema) as a SEPARATE container
   from Airflow's metadata Postgres. Define an Airflow Connection named "warehouse"
   pointing at it (UI -> Admin -> Connections, or env var AIRFLOW_CONN_WAREHOUSE).
3. Drop this file into the bind-mounted `./dags/` folder. Wait ~30s; it appears in
   the UI (paused). Unpause `crunch_w4_ex01_first_dag`.
4. Trigger one run manually, or let `@daily` schedule it. Inspect the task logs.

PREREQUISITES IN THE WAREHOUSE
------------------------------
A landing table to load into. For this exercise a simple staging table is enough:

    CREATE TABLE IF NOT EXISTS fact_sales_staging (
        sales_date  date    NOT NULL,
        store_key   integer NOT NULL,
        product_key integer NOT NULL,
        amount      numeric(12, 2) NOT NULL
    );

ACCEPTANCE CRITERIA
-------------------
[ ] The DAG parses with no import errors (check `docker compose logs airflow-scheduler`).
[ ] `schedule="@daily"`, an explicit `start_date`, `catchup=False`, and a finite
    `max_active_runs` are all set.
[ ] `extract`, `transform`, and `load` each receive the run's window from
    `data_interval_start` (NOT `datetime.now()` / `CURRENT_DATE`).
[ ] The dependency graph is `extract >> transform >> load`, expressed via the
    TaskFlow call `load(transform(extract()))`.
[ ] XCom payloads are small dicts (window + count), never bulk rows.
[ ] A manual run succeeds and the `load` task log prints the window it loaded and a
    row count, and rows for THAT window appear in `fact_sales_staging`.

Tested against: Apache Airflow 2.9, Postgres 16, Python 3.11.
"""

from __future__ import annotations

import datetime

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Connection id pointing at the WAREHOUSE Postgres (NOT Airflow's metadata DB).
WAREHOUSE_CONN_ID = "warehouse"


@dag(
    dag_id="crunch_w4_ex01_first_dag",
    schedule="@daily",
    # Pick a start_date a few days in the past so you have intervals to run.
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    # catchup=False so deploying does NOT flood the warehouse with historical runs.
    # You will turn on catchup / run a manual backfill safely in Exercise 3.
    catchup=False,
    # Cap concurrency now so the habit is in place before the backfill exercise.
    max_active_runs=3,
    default_args={"owner": "crunch-data"},
    tags=["crunch-data", "week-04", "exercise-01"],
)
def crunch_w4_ex01_first_dag():

    @task
    def extract(data_interval_start: pendulum.DateTime | None = None) -> dict:
        """Read ONLY this interval's slice of the source.

        Airflow injects `data_interval_start` by name. We derive the window from it
        and return a SMALL payload (window string + a synthetic source row count).

        In a real pipeline you would read `/data/incoming/sales_<window>.csv` or a
        source table filtered to the window. Here we synthesize a deterministic row
        count so the exercise runs with no external file, while still proving the
        interval keying. Replace `_synthesize_source_rows` with your real reader.
        """
        assert data_interval_start is not None, "Airflow must inject data_interval_start"
        window = data_interval_start.to_date_string()  # 'YYYY-MM-DD'

        rows = _synthesize_source_rows(window)
        print(f"[extract] window={window} source_rows={len(rows)}")

        # XCom carries the window + count ONLY. The bulk rows go to a real store.
        # For this exercise we stash the rows on the task instance's local scratch by
        # returning them is tempting -- DO NOT. Instead, write them where `load` can
        # re-read them deterministically. The simplest deterministic store for the
        # exercise is to regenerate them in `load` from the same window (pure fn).
        return {"window": window, "source_row_count": len(rows)}

    @task
    def transform(extracted: dict) -> dict:
        """Apply the (toy) transform for THIS window.

        Real transforms clean / conform / map natural keys to surrogate keys. Here we
        keep the contract: take the small dict, return a small dict, never inflate
        XCom. The window is preserved end to end so `load` keys off the same window.
        """
        window = extracted["window"]
        # ------------------------------------------------------------------
        # YOU IMPLEMENT: nothing heavy. Just confirm the window flows through and
        # (optionally) record a 'transformed_row_count'. For the toy source the
        # transformed count equals the source count. Return the small dict.
        # ------------------------------------------------------------------
        transformed_row_count = extracted["source_row_count"]
        print(f"[transform] window={window} transformed_rows={transformed_row_count}")
        return {"window": window, "row_count": transformed_row_count}

    @task
    def load(transformed: dict) -> dict:
        """Load THIS window into fact_sales_staging.

        NOTE: this is a plain INSERT for Exercise 1. It is NOT idempotent yet -- a
        re-run of the same window will double the rows. You will fix that in
        Exercise 3 with a delete-then-insert. For now, prove the interval keying and
        the end-to-end graph work.
        """
        window = transformed["window"]
        rows = _synthesize_source_rows(window)  # deterministic regeneration for the toy source

        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        with hook.get_conn() as conn, conn.cursor() as cur:
            # ------------------------------------------------------------------
            # YOU IMPLEMENT: a bulk INSERT of `rows` into fact_sales_staging.
            # Use cur.executemany(...) with a parameterized INSERT. Every row's
            # sales_date MUST equal `window` -- prove the data lands in the right
            # partition. Commit at the end.
            # ------------------------------------------------------------------
            cur.executemany(
                "INSERT INTO fact_sales_staging (sales_date, store_key, product_key, amount) "
                "VALUES (%(sales_date)s, %(store_key)s, %(product_key)s, %(amount)s)",
                rows,
            )
            conn.commit()

        print(f"[load] window={window} loaded_rows={len(rows)} into fact_sales_staging")
        return {"window": window, "loaded_rows": len(rows)}

    # The call graph IS the dependency graph: extract >> transform >> load.
    load(transform(extract()))


def _synthesize_source_rows(window: str) -> list[dict]:
    """Deterministic synthetic source for one day, derived purely from the window.

    Pure function of `window` so `extract` and `load` agree without shipping rows
    through XCom. A real exercise replaces this with a CSV / source-table reader.
    The row count varies by day-of-month so different windows look different (and so
    Exercise 3's per-day count diff is meaningful).
    """
    day = int(window[-2:])  # day-of-month 1..31, just to vary the volume per window
    n = 100 + day  # 101..131 rows; deterministic per window
    return [
        {
            "sales_date": window,
            "store_key": (i % 5) + 1,
            "product_key": (i % 11) + 1,
            "amount": round(10.0 + (i % 7) * 1.5, 2),
        }
        for i in range(n)
    ]


crunch_w4_ex01_first_dag()
