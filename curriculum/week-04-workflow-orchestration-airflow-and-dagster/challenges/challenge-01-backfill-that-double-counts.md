# Challenge 1 — The Backfill That Double-Counts

> **Time:** ~90 minutes.
> **Prerequisites:** Exercises 1–3 attempted; Lectures 2 (backfill/catchup) and 3 (idempotency, the assertion gate); the Airflow Docker stack + warehouse Postgres running.
> **Citations:** [Airflow DAG runs, backfill & catchup](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dag-run.html), [Airflow core concepts (tasks)](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html), [Airflow scheduler](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html).

## Premise

You join a team on Monday. On Tuesday the analytics lead pings you: "Revenue for the last two weeks is showing roughly double what finance reports. The numbers were fine until someone re-ran a backfill on Friday. Can you look?" You open the DAG. It is a daily sales loader, it has retries, it has an SLA, the runs are all green. Nothing *failed*. And yet the warehouse is wrong.

This is the failure mode at the center of the week: a backfill that double-counts, on a pipeline that looks healthy. Your job is to reproduce it, find the bug, fix the idempotency, and *prove* the fix with a re-run that does not change the counts. Then you will add the assertion gate that would have caught it before finance did.

## Setup

Create the warehouse table (separate Postgres from Airflow's metadata DB):

```sql
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_date  date    NOT NULL,
    store_key   integer NOT NULL,
    product_key integer NOT NULL,
    amount      numeric(12, 2) NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_fact_sales_date ON fact_sales (sales_date);
```

Here is the **broken DAG** you inherited. Drop it in `./dags/` as `broken_sales_loader.py`. It is deliberately wrong in two ways — find both.

```python
import datetime, pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    dag_id="broken_sales_loader",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    catchup=False,
    # BUG-HINT #1: what is missing here that a 30-day backfill needs?
    default_args={"owner": "crunch-data", "retries": 4,
                  "retry_delay": datetime.timedelta(minutes=1),
                  "retry_exponential_backoff": True},
    tags=["crunch-data", "week-04", "challenge-01", "BROKEN"],
)
def broken_sales_loader():

    @task
    def load(data_interval_start=None) -> dict:
        window = data_interval_start.to_date_string()
        day = int(window[-2:])
        rows = [{"sales_date": window, "store_key": (i % 5) + 1,
                 "product_key": (i % 11) + 1, "amount": round(10.0 + (i % 7) * 1.5, 2)}
                for i in range(100 + day)]
        hook = PostgresHook(postgres_conn_id="warehouse")
        with hook.get_conn() as conn, conn.cursor() as cur:
            # BUG-HINT #2: read this INSERT carefully. What happens the SECOND
            # time this window runs (retry / catchup / second backfill)?
            cur.executemany(
                "INSERT INTO fact_sales (sales_date, store_key, product_key, amount) "
                "VALUES (%(sales_date)s, %(store_key)s, %(product_key)s, %(amount)s)",
                rows,
            )
            conn.commit()
        return {"window": window, "loaded_rows": len(rows)}

    load()

broken_sales_loader()
```

The compose addition you need (so `airflow dags backfill` runs inside the cluster) is already in your Lecture 1 stack: run the CLI via `docker compose exec airflow-scheduler ...`.

## Steps

1. **Reproduce the incident.** Run a 10-day backfill, capture per-day counts, then run the *same* backfill again and capture again:

   ```bash
   docker compose exec airflow-scheduler airflow dags backfill \
     --start-date 2026-05-20 --end-date 2026-05-30 broken_sales_loader
   ```

   ```sql
   SELECT sales_date, count(*) AS n FROM fact_sales
   WHERE sales_date >= '2026-05-20' AND sales_date < '2026-05-30'
   GROUP BY sales_date ORDER BY sales_date;
   ```

   Record the counts after run 1 and after run 2 in `notes/incident.md`. You should see every day roughly double. That is the bug, reproduced.

2. **Find both defects.** In `notes/incident.md`, name them precisely:
   - **Defect A (idempotency):** the `load` is `INSERT`-only, so a second run of any window *appends* a second copy instead of replacing. Quote the offending lines.
   - **Defect B (throttle):** `max_active_runs` is unset, so the backfill can launch all intervals at once and hammer the warehouse — a latent denial-of-service even when the data is correct. Explain why "it happened to be fine" is luck, not safety.

3. **Fix the idempotency.** Rewrite `load` to delete-then-insert this window in one transaction, keyed off `data_interval_start`/`data_interval_end` (Lecture 3 §1.1). Save as `dags/fixed_sales_loader.py` with `dag_id="fixed_sales_loader"` and `max_active_runs=16`.

4. **Prove the fix.** `TRUNCATE fact_sales;` to start clean. Run the 10-day backfill on `fixed_sales_loader` **twice**. Capture per-day counts after each run. They must be **identical**. Paste both count tables into `notes/incident.md` and state, in one sentence, why the second run did not change them.

5. **Add the gate that would have caught it earlier.** Add an `assert_load` task downstream of `load` (Lecture 3 §2.2) that compares the warehouse window count to the expected count and raises `AirflowFailException` on mismatch. Then deliberately re-introduce Defect A in a throwaway copy and confirm the gate turns **red** on the double-count instead of staying green. Document what the assertion caught.

## Acceptance criteria

- [ ] `notes/incident.md` shows the reproduced double-count (run-1 vs run-2 counts on the broken DAG).
- [ ] Both defects (idempotency *and* missing throttle) are named with quoted offending lines.
- [ ] `dags/fixed_sales_loader.py` does delete-then-insert per window in one transaction, keyed off the interval, with `max_active_runs` set.
- [ ] Running the fixed backfill twice yields **identical** per-day counts, with both tables pasted in `notes/incident.md`.
- [ ] An `assert_load` gate exists and is shown turning red when the double-count bug is re-introduced.

## Stretch goals

- **Catchup version.** Reproduce the same double-count via `catchup=True` (deploy with a past `start_date`, pause/unpause) instead of a manual backfill, to prove the two paths share the mechanism.
- **Partial-load lie.** Make the source return half its rows for one window (simulate a truncated file) and confirm `assert_load` catches the *expected != warehouse* mismatch even though `load` exited zero (Lecture 3 §2.1).
- **Concurrency evidence.** Run the broken (unthrottled) and fixed (throttled) backfills while watching `docker stats` on the warehouse Postgres container. Record peak CPU for each and write two sentences on why the throttle matters even when the data is correct.
- **`depends_on_past`.** Add a running-balance column whose value for day N depends on day N-1, set `depends_on_past=True`, and observe how it serializes the backfill. Note the trade-off versus parallel windows.

## References

- Airflow — DAG runs, backfill, and catchup: <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dag-run.html>
- Airflow — core concepts (tasks, `AirflowFailException`): <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html>
- Airflow — scheduler (why an under-resourced scheduler stalls a backfill): <https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html>

*Submit `dags/fixed_sales_loader.py` and `notes/incident.md` in your portfolio's `week-04/challenge-01/`. PRs to <https://github.com/CODE-CRUNCH-CLUB>. Licensed GPL-3.0.*
