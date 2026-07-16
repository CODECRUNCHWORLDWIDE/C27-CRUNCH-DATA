"""
Exercise 3 — Make the load idempotent and run a safe 30-day backfill
====================================================================

GOAL
----
This is the exercise that earns the Phase I gate skill: a backfill that does NOT
double-count. Two things have to be true (Lectures 2 and 3):

  1. The load is IDEMPOTENT: it deletes-then-inserts EXACTLY this run's window
     inside one transaction, keyed off `data_interval_start` / `data_interval_end`.
     Running a window once or five times converges to the same rows.
  2. The backfill is THROTTLED: `max_active_runs` caps how many historical runs hit
     the warehouse at once, so 30 days do not melt Postgres.

Then you add the gate from Lecture 3 section 2: an `assert_load` task that catches
"the task that lied" -- a load that exits zero having loaded partial / wrong-window /
zero rows -- by checking the warehouse row count for the window and failing loudly.

Finally you run the backfill from the CLI, then RUN IT AGAIN, and prove the per-day
counts are identical.

RUN INSTRUCTIONS
----------------
1. Same Airflow + warehouse stack. Create the real fact table:

       CREATE TABLE IF NOT EXISTS fact_sales (
           sales_date  date    NOT NULL,
           store_key   integer NOT NULL,
           product_key integer NOT NULL,
           amount      numeric(12, 2) NOT NULL
       );
       CREATE INDEX IF NOT EXISTS ix_fact_sales_date ON fact_sales (sales_date);

2. Drop this file in `./dags/`. Unpause `crunch_w4_ex03_safe_backfill`. Keep
   catchup=False so deploying does not auto-flood; you will backfill ON PURPOSE.

3. Run the 30-day backfill from inside the scheduler container:

       docker compose exec airflow-scheduler airflow dags backfill \
           --start-date 2026-05-20 --end-date 2026-06-19 \
           crunch_w4_ex03_safe_backfill

4. Capture per-day counts (this is the proof query):

       SELECT sales_date, count(*) AS n
       FROM   fact_sales GROUP BY sales_date ORDER BY sales_date;

5. Run the SAME backfill a second time. Re-run the proof query. The counts MUST be
   identical. If any day grew, the load is not idempotent -- find the bug.

ACCEPTANCE CRITERIA
-------------------
[ ] `load` deletes-then-inserts THIS window inside ONE transaction (delete + insert
    commit together), keyed off `data_interval_start`/`data_interval_end`.
[ ] No use of `datetime.now()` / `CURRENT_DATE` anywhere a window is decided.
[ ] `max_active_runs` is set (e.g. 16) so the backfill is throttled.
[ ] An `assert_load` task downstream of `load` checks the warehouse window row count
    against the expected count and raises `AirflowFailException` on mismatch / zero.
[ ] Running the 30-day backfill twice yields IDENTICAL per-day counts in fact_sales.

Tested against: Apache Airflow 2.9, Postgres 16, Python 3.11.
"""

from __future__ import annotations

import datetime

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook

WAREHOUSE_CONN_ID = "warehouse"


def on_failure(context) -> None:
    ti = context["task_instance"]
    print(f"[ALERT-FAILURE] {ti.dag_id}.{ti.task_id} window={context['data_interval_start']} "
          f"log={ti.log_url}")


@dag(
    dag_id="crunch_w4_ex03_safe_backfill",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    catchup=False,                 # backfill on purpose; do not auto-flood on deploy
    max_active_runs=16,            # THROTTLE the backfill: at most 16 windows at once
    default_args={
        "owner": "crunch-data",
        "retries": 4,
        "retry_delay": datetime.timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": datetime.timedelta(minutes=30),
        "execution_timeout": datetime.timedelta(minutes=20),
        "on_failure_callback": on_failure,
    },
    tags=["crunch-data", "week-04", "exercise-03"],
)
def crunch_w4_ex03_safe_backfill():

    @task
    def extract(
        data_interval_start: pendulum.DateTime | None = None,
        data_interval_end: pendulum.DateTime | None = None,
    ) -> dict:
        """Extract exactly [data_interval_start, data_interval_end). Return small dict."""
        assert data_interval_start is not None and data_interval_end is not None
        window_start = data_interval_start.to_date_string()
        window_end = data_interval_end.to_date_string()
        rows = _synthesize_source_rows(window_start)
        print(f"[extract] [{window_start}, {window_end}) source_rows={len(rows)}")
        return {"window_start": window_start, "window_end": window_end,
                "source_row_count": len(rows)}

    @task
    def load(extracted: dict) -> dict:
        """IDEMPOTENT load: delete-then-insert THIS window in one transaction.

        This is the heart of the exercise. The DELETE scrubs any rows previously
        loaded for this window (by the original scheduled run, a retry, or an earlier
        backfill). The INSERT re-loads the window. Because delete + insert commit
        together, running this window N times leaves the identical final state, and
        an interrupted run rolls back to the pre-run state.
        """
        window_start = extracted["window_start"]
        window_end = extracted["window_end"]
        rows = _synthesize_source_rows(window_start)

        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                # ----------------------------------------------------------------
                # YOU IMPLEMENT the idempotent body:
                #   1. DELETE FROM fact_sales WHERE sales_date >= window_start
                #                                AND sales_date <  window_end;
                #   2. INSERT the freshly-extracted `rows` for this window.
                #   3. conn.commit()  -- delete + insert in ONE transaction.
                # Reference implementation shown; study why each line matters.
                # ----------------------------------------------------------------
                cur.execute(
                    "DELETE FROM fact_sales WHERE sales_date >= %s AND sales_date < %s",
                    (window_start, window_end),
                )
                cur.executemany(
                    "INSERT INTO fact_sales (sales_date, store_key, product_key, amount) "
                    "VALUES (%(sales_date)s, %(store_key)s, %(product_key)s, %(amount)s)",
                    rows,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        print(f"[load] window={window_start} replaced -> loaded_rows={len(rows)}")
        return {"window_start": window_start, "window_end": window_end,
                "expected_rows": extracted["source_row_count"], "loaded_rows": len(rows)}

    @task
    def assert_load(loaded: dict) -> dict:
        """Catch 'the task that lied'.

        `load` can exit zero having loaded partial / zero / wrong-window data. Exit
        code is about the process, not the data. So we ASSERT the data: the warehouse
        row count for this window must equal what we expected to load, and must not be
        zero. Fail LOUDLY (AirflowFailException -> no retry) on violation.
        """
        window_start = loaded["window_start"]
        window_end = loaded["window_end"]
        expected = loaded["expected_rows"]

        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        (warehouse_n,) = hook.get_first(
            "SELECT count(*) FROM fact_sales WHERE sales_date >= %s AND sales_date < %s",
            parameters=(window_start, window_end),
        )
        # ------------------------------------------------------------------ #
        # YOU IMPLEMENT the assertions:
        #   - warehouse_n == 0          -> raise (empty/truncated source)
        #   - warehouse_n != expected   -> raise (partial / double load)
        # Use AirflowFailException so a still-bad load is not retried pointlessly.
        # ------------------------------------------------------------------ #
        if warehouse_n == 0:
            raise AirflowFailException(f"zero rows for {window_start}: empty/truncated source")
        if warehouse_n != expected:
            raise AirflowFailException(
                f"row-count mismatch for {window_start}: warehouse={warehouse_n} expected={expected} "
                f"(partial load, double load, or wrong-window load)")
        print(f"[assert_load] window={window_start} OK rows={warehouse_n}")
        return {"window_start": window_start, "asserted_rows": warehouse_n}

    # extract -> load -> assert_load. The assertion GATES anything downstream.
    assert_load(load(extract()))


def _synthesize_source_rows(window: str) -> list[dict]:
    """Deterministic synthetic source for one day (pure function of the window).

    Deterministic so that re-running a window produces the SAME rows, which is what
    lets the per-day count diff prove idempotency. Replace with your real reader.
    """
    day = int(window[-2:])
    n = 100 + day
    return [
        {"sales_date": window, "store_key": (i % 5) + 1, "product_key": (i % 11) + 1,
         "amount": round(10.0 + (i % 7) * 1.5, 2)}
        for i in range(n)
    ]


crunch_w4_ex03_safe_backfill()
