# Exercise Solutions — Week 4

These are the worked solutions and annotations for the three exercises. **Read them after attempting the exercises, not before.** The reference implementations are inlined in the `.py` files themselves (the "YOU IMPLEMENT" blocks show the exact shape), so this document focuses on *why* each shape is the correct one and on the canonical mistakes that cost a backfill its idempotency.

All solutions are written against **Apache Airflow 2.9, Postgres 16, Python 3.11**.

---

## Exercise 1 — Your first TaskFlow DAG

### What the exercise asks

Author a daily `extract -> transform -> load` DAG with the TaskFlow API, set `schedule`/`start_date`/`catchup`/`max_active_runs` deliberately, key every task off `data_interval_start`, and keep XCom payloads small.

### Reference solution (with real code)

The load-bearing decisions:

```python
@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,            # do not flood on deploy
    max_active_runs=3,        # the habit, in place early
)
def crunch_w4_ex01_first_dag():
    @task
    def extract(data_interval_start=None) -> dict:
        window = data_interval_start.to_date_string()   # NOT datetime.now()
        rows = _synthesize_source_rows(window)
        return {"window": window, "source_row_count": len(rows)}   # small XCom
```

The dependency graph is the single line `load(transform(extract()))`. That call does not run the functions inline — each `@task` returns an XCom reference, and passing it as an argument creates the edge. The `load` body is a `cur.executemany(...)` over the deterministic rows for the window, committed once.

The one subtlety learners miss: **how do you get the bulk rows from `extract` to `load` without shipping them through XCom?** The exercise's answer is to make the source a *pure function of the window* (`_synthesize_source_rows(window)`), so `load` regenerates the same rows from the same window string. In a real pipeline you would instead have `extract` write rows to a file or a staging table and pass the *path/location* through XCom. Either way the rule holds: **XCom carries the window and the count, never the rows.**

### Expected output

After one manual run of the `2026-06-18` interval, the `load` log shows:

```
[extract] window=2026-06-18 source_rows=118
[transform] window=2026-06-18 transformed_rows=118
[load] window=2026-06-18 loaded_rows=118 into fact_sales_staging
```

and `SELECT count(*) FROM fact_sales_staging WHERE sales_date='2026-06-18';` returns `118` (`100 + 18`).

### Common pitfalls

- **Keying off `now()`.** `window = pendulum.now().to_date_string()` "works" for a manual run today and silently breaks every backfill — the run for `06-01` would load *today's* data. The whole point of the exercise is to take the window from `data_interval_start`. If you used `now()`, Exercise 3 will fail and you will not know why.
- **Returning the rows from `extract`.** `return rows` bloats XCom (stored in the metadata DB) and slows the scheduler. Return `{"window": ..., "row_count": ...}`.
- **Calling the task functions directly.** Writing `extract()` then `transform(extract())` *twice* creates two `extract` task instances. Build the graph exactly once: `load(transform(extract()))`.
- **`catchup` left at its default.** The default is `True`. Deploy with a past `start_date` and unbounded concurrency and you flood the warehouse. Set `catchup=False` explicitly for development DAGs.

---

## Exercise 2 — Sensor, retries, SLA, alerting

### What the exercise asks

Add a `FileSensor` (reschedule mode, real timeout) that gates the load, retries with exponential backoff, an `execution_timeout`, an `sla` plus `sla_miss_callback`, and an `on_failure_callback`. Understand that retries, SLAs, and failure alerts are three different signals.

### Reference solution (with real code)

```python
wait_for_daily_file = FileSensor(
    task_id="wait_for_daily_file",
    filepath=f"{INCOMING_DIR}/sales_{{{{ ds }}}}.csv",   # templated per run's date
    poke_interval=60,
    timeout=60 * 60 * 3,        # 3 hours, then FAIL
    mode="reschedule",          # free the slot while waiting
    soft_fail=False,
)
```

Note the quadrupled braces in the f-string: `{{{{ ds }}}}` renders to the literal `{{ ds }}` that Airflow's Jinja layer then templates at run time. If you build the filepath without an f-string, write `"/data/incoming/sales_{{ ds }}.csv"` directly.

`default_args` carry the resilience:

```python
default_args={
    "retries": 4,
    "retry_delay": datetime.timedelta(minutes=2),
    "retry_exponential_backoff": True,           # gaps ~2,4,8,16 min
    "max_retry_delay": datetime.timedelta(minutes=30),
    "execution_timeout": datetime.timedelta(minutes=20),
    "on_failure_callback": on_failure,           # page AFTER retries exhausted
    "on_retry_callback": on_retry,               # quiet telemetry per retry
}
```

The `sla=datetime.timedelta(hours=2)` goes on the `load` task; the `sla_miss_callback` goes on the `@dag`. The dependency `wait_for_daily_file >> load()` makes the load wait for the file.

### Expected output

Trigger the `2026-06-18` interval with no file present: the sensor sits in `up_for_reschedule`, re-checking every 60 s, holding **no** worker slot. Drop the file:

```bash
mkdir -p ./incoming && touch ./incoming/sales_2026-06-18.csv
```

Within ~60 s the sensor turns green and `load` runs. To see an SLA miss, temporarily set `sla=timedelta(seconds=30)` and add `time.sleep(60)` to `load`: the task still succeeds, but `[ALERT-SLA-MISS]` appears from the scheduler. To see a failure alert, withhold the file past the 3-hour timeout (or set `timeout=120`): the sensor fails and `[ALERT-FAILURE]` fires.

### Common pitfalls

- **`mode="poke"` for a multi-hour wait.** Poke holds a worker slot the entire time. A couple of poke sensors on a laptop's handful of slots starve every other task. Use `reschedule` (or deferrable) for waits over a minute or two.
- **No `timeout`.** A sensor with no timeout waiting on a file that never arrives waits *forever* and wedges the pipeline with no alert. Always set one.
- **Confusing SLA with timeout.** The SLA does **not** stop the task; it notifies. The `execution_timeout` is what kills a runaway. They answer different questions: "is it late?" vs "is it hung?".
- **Paging on `on_retry_callback`.** That fires on every transient blip; wiring it to a pager means you get woken for self-healing failures. Page on `on_failure_callback` (retries exhausted) only.
- **SLA timer surprise.** The SLA is measured from the run's *expected start*, not from when the task actually began. A long upstream sensor can consume the SLA window before the load even starts. Set the SLA on the critical path with the sensor's worst case in mind, and rely on the sensor's `timeout` for "source never came."

---

## Exercise 3 — Idempotent load and a safe 30-day backfill

### What the exercise asks

Make `load` idempotent (delete-then-insert this window in one transaction, keyed off the interval), throttle the backfill with `max_active_runs`, add an `assert_load` gate that catches "the task that lied," run a 30-day backfill, then run it again and prove identical per-day counts.

### Reference solution (with real code)

The idempotent body is the whole exercise:

```python
conn = hook.get_conn()
try:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM fact_sales WHERE sales_date >= %s AND sales_date < %s",
            (window_start, window_end),
        )
        cur.executemany(
            "INSERT INTO fact_sales (sales_date, store_key, product_key, amount) "
            "VALUES (%(sales_date)s, %(store_key)s, %(product_key)s, %(amount)s)",
            rows,
        )
    conn.commit()            # delete + insert commit TOGETHER
except Exception:
    conn.rollback()          # interrupted run -> back to pre-run state
    raise
finally:
    conn.close()
```

Three properties, each load-bearing: (1) the window comes from `data_interval_start`/`data_interval_end`, so the run for `06-01` touches only `06-01`; (2) it *replaces* (delete then insert), so two runs equal one run; (3) delete and insert commit in one transaction, so a crash rolls back cleanly and a retry re-replaces.

The gate:

```python
if warehouse_n == 0:
    raise AirflowFailException(f"zero rows for {window_start}: empty/truncated source")
if warehouse_n != expected:
    raise AirflowFailException(f"row-count mismatch for {window_start}: "
                               f"warehouse={warehouse_n} expected={expected}")
```

`AirflowFailException` fails *without* retrying — re-asserting a still-bad load just burns retries.

### Expected output

Run the backfill once:

```bash
docker compose exec airflow-scheduler airflow dags backfill \
  --start-date 2026-05-20 --end-date 2026-06-19 crunch_w4_ex03_safe_backfill
```

Proof query after the first run:

```
 sales_date | n
------------+-----
 2026-05-20 | 120
 2026-05-21 | 121
 ...
 2026-06-18 | 118
(30 rows)
```

Each day's count is `100 + day-of-month`. Run the **same** backfill again, re-run the proof query: **the 30 rows are byte-for-byte identical.** That identical re-run is the Phase I gate evidence. With a naive `INSERT`-only `load`, the second backfill would *double* every count (240, 242, ...) — the exact failure `challenge-01` makes you debug.

### Common pitfalls

- **`INSERT` without the preceding `DELETE`.** The classic double-count. The second time a window runs (retry, catchup, or second backfill), you append a second copy. Always replace the window.
- **Delete and insert in separate transactions.** If you `commit()` after the delete and then crash before the insert, the window is now *empty* until the next run — a silent data hole. Commit them together.
- **A wider/narrower delete than insert.** Deleting `WHERE sales_date = window_start` (a single day) while the source actually spans more, or deleting the whole table — both corrupt. The delete predicate must match the window exactly: `>= start AND < end`.
- **No `max_active_runs`.** 30 simultaneous loads pin the warehouse and the metadata DB (also Postgres). The backfill "runs" but stalls and pages on-call. Cap it.
- **A non-deterministic source.** If `_synthesize_source_rows` used `random` without a seed, the re-run would legitimately differ and you could not prove idempotency. Keep the source deterministic (or compare against the *source's own* per-window count, not a remembered number).
- **Letting the gate retry.** If `assert_load` raises a generic `Exception`, Airflow retries it four times against the same bad data. Use `AirflowFailException` to fail immediately and surface the problem.

---

*If a solution here disagrees with the live Airflow behavior you observe, trust the docs (linked in `resources.md`) and open a PR to <https://github.com/CODE-CRUNCH-CLUB>. Licensed GPL-3.0.*
