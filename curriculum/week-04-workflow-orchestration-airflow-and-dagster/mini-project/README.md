# Mini-Project — Crunch Loader Orchestrated (the Phase I Gate)

> Wrap your Week 3 idempotent incremental loader in an Airflow DAG running in Docker that lands a daily source into the Week 1 star schema in Postgres, waits for the file with a sensor, retries with backoff, alerts on an SLA miss, gates the load with a row-count/checksum assertion, and survives a **30-day backfill that does not double-count**. This is the Phase I gate: an orchestrated, idempotent, incremental batch pipeline that loads a modeled star schema, survives a re-run with no double-counting, and backfills 30 days cleanly.

This is the capstone of Phase I. Everything you learned in Weeks 1–4 converges here: the **star schema** (Week 1), the **analytical SQL** of the assertion checks (Week 2), the **idempotent incremental load** (Week 3), and the **orchestration** of this week. By the end you will have a runnable `docker compose up` system and an Airflow DAG you can demo live to a reviewer, plus a one-page report that states — in numbers, intervals, and row counts — exactly how you proved the backfill is safe.

**Estimated time:** 10 hours (Friday/Saturday/Sunday in the suggested schedule).

---

## Runtime topology

```text
   host:  ./incoming/sales_<YYYY-MM-DD>.csv   (the daily source file lands here)
                         |
                         |  bind-mounted into the Airflow containers
                         v
   +----------------------------------------------------------------+
   |                    AIRFLOW (docker compose)                     |
   |                                                                 |
   |   scheduler ----> LocalExecutor ----> task subprocesses         |
   |       |                                     |                   |
   |   metadata DB (Postgres #1: airflow)        |                   |
   |   webserver :8080      triggerer            |                   |
   +-------------------------------------|---------------------------+
                                         |
                  DAG: crunch_loader_orchestrated
                  +----------------------------------------------+
                  | wait_for_file (FileSensor, reschedule, 3h)   |
                  |        v                                     |
                  | extract (window = data_interval_start)       |
                  |        v                                     |
                  | load   (delete-then-insert THIS window,      |
                  |         one transaction, idempotent)         |
                  |        v                                     |
                  | assert_load (row-count + volume + checksum;  |
                  |         AirflowFailException on violation)   |
                  |        v                                     |
                  | publish_mart (only if assertion passed)      |
                  +----------------------|-----------------------+
                                         |
                                         v
   +----------------------------------------------------------------+
   |        WAREHOUSE (Postgres #2: warehouse) -- the Week 1 star    |
   |   dim_store  dim_product  dim_date  ...  +  fact_sales          |
   +----------------------------------------------------------------+
```

Two Postgres containers: **#1** is Airflow's metadata DB, **#2** is the warehouse holding your star schema. They are isolated on purpose (Lecture 1 §5.3). The source files land on the host in `./incoming/` and are bind-mounted into the Airflow containers.

---

## What you will produce

In your portfolio repo (`crunch-data-portfolio-<yourhandle>`), add a `week-04/` directory:

```
crunch-data-portfolio-<yourhandle>/
├── README.md                          (updated, with a Week 4 section)
└── week-04/
    ├── README.md                      one-page report (~900 words)
    ├── docker-compose.yaml            Airflow (LocalExecutor + metadata Postgres) + warehouse Postgres
    ├── .env                           AIRFLOW_UID + connection env vars
    ├── Makefile                       `make up`, `make backfill`, `make prove`, `make down`
    ├── dags/
    │   └── crunch_loader_orchestrated.py   the DAG: sensor -> extract -> load -> assert -> publish
    ├── sql/
    │   ├── 00_warehouse_schema.sql    the Week 1 star schema (dims + fact_sales)
    │   └── 10_proof_queries.sql       per-day count + checksum reconciliation queries
    ├── loader/
    │   └── load_window.py             the idempotent delete-then-insert (ported from Week 3)
    ├── data/
    │   └── generate_source.py         emits 35 daily source files for the backfill window
    ├── incoming/                      where daily files land (gitignored except a sample)
    └── evidence/
        ├── backfill_run1_counts.txt   per-day counts after the first backfill
        ├── backfill_run2_counts.txt   per-day counts after the second (must match run1)
        ├── assertion_caught.txt       proof the gate caught an injected bad load
        └── grid_view.png              screenshot of the green DAG grid across 30 days
```

---

## Functional requirements

- **F1 — Star-schema target.** The DAG lands into the Week 1 star schema in the warehouse Postgres: conformed dimensions plus a `fact_sales` table at a defined grain. `sql/00_warehouse_schema.sql` creates it. The load resolves natural keys to surrogate keys against the dimensions (reuse your Week 1/3 logic).
- **F2 — Interval-keyed extract.** `extract` reads only the window `[data_interval_start, data_interval_end)` from the daily source file `incoming/sales_<ds>.csv`. No `now()` / `CURRENT_DATE` anywhere a window is decided.
- **F3 — File sensor.** A `FileSensor` (`mode="reschedule"`, finite `timeout` ≈ 3h, templated `{{ ds }}` path) gates the extract on the day's file existing.
- **F4 — Idempotent load.** `load` delete-then-inserts (or merges) exactly this window in one transaction, keyed off the interval. Running a window once or five times converges to the same rows.
- **F5 — Retries with backoff.** `retries`, `retry_delay`, `retry_exponential_backoff`, `max_retry_delay`, and `execution_timeout` are set in `default_args`.
- **F6 — SLA + alerting.** An `sla` on the critical path plus an `sla_miss_callback`, and an `on_failure_callback` that fires after retries are exhausted. Both print structured alert lines (a real webhook is a stretch goal).
- **F7 — Assertion gate.** `assert_load` checks (a) warehouse window row count == expected source count, (b) non-zero, (c) a volume sanity check against the trailing 7-day average, and (d) one checksum/sum reconciliation (e.g. `SUM(amount)` matches the source total). It raises `AirflowFailException` on any violation, blocking `publish_mart`.
- **F8 — Throttled 30-day backfill.** `max_active_runs` is set so a 30-day backfill is bounded. The backfill runs from the CLI over `[today-30, today]` and completes without melting either Postgres.
- **F9 — Proven idempotency.** Running the **same** 30-day backfill twice yields **identical** per-day counts and identical `SUM(amount)` per day. The two count files in `evidence/` must match.
- **F10 — Demonstrated lie-catching.** Inject a truncated source for one window (half the rows), run that window, and capture the assertion task turning red with the mismatch message in `evidence/assertion_caught.txt`.

## Non-functional requirements

- **N1 — One-command bring-up.** `make up` (or `docker compose up`) brings up Airflow (LocalExecutor + metadata Postgres) and the warehouse Postgres on a fresh checkout, with Docker given ≥ 4 GB RAM. A reviewer runs it during the demo.
- **N2 — Reproducible.** `make backfill` runs the 30-day backfill; `make prove` runs the proof queries and writes the count files; a second `make backfill && make prove` produces matching counts. Versions pinned (Airflow 2.9, Postgres 16, Python 3.11).
- **N3 — Isolated state.** Airflow's metadata DB and the warehouse are separate databases (ideally separate containers). The warehouse never holds Airflow task-instance rows.
- **N4 — No bulk data in XCom.** XCom carries windows, counts, and a checksum — never DataFrames or row lists.
- **N5 — Operator-voice report.** The report states results in numbers ("backfill of 30 intervals at `max_active_runs=16` finished in N min; peak warehouse CPU M%; run-1 and run-2 per-day counts identical"), not adjectives.

---

## Validation & measurement plan

You must *measure*, not assert. Produce each of these:

1. **Idempotency proof (F9).** `sql/10_proof_queries.sql` contains:
   ```sql
   SELECT sales_date, count(*) AS n, sum(amount) AS total
   FROM   fact_sales
   WHERE  sales_date >= :start AND sales_date < :end
   GROUP  BY sales_date ORDER BY sales_date;
   ```
   Run after backfill #1 → `evidence/backfill_run1_counts.txt`. Run after backfill #2 → `evidence/backfill_run2_counts.txt`. `diff` them — the diff must be empty. Paste the `diff` result (empty) into the report.
2. **Concurrency bound (F8/N1).** While the backfill runs, capture `docker stats` peak CPU/RAM for the warehouse Postgres. Report the peak. Show it stayed bounded because of `max_active_runs`.
3. **Backfill duration.** Time the 30-day backfill (`time make backfill` or the run-duration column in the UI). Report it.
4. **Assertion evidence (F10).** The red-task log line showing `row-count mismatch for <window>: warehouse=<n> expected=<m>` → `evidence/assertion_caught.txt`.
5. **Grid screenshot.** The Airflow grid view showing 30 green DAG runs → `evidence/grid_view.png`.

---

## Suggested order of operations

1. **Stack up (~1h).** Write `docker-compose.yaml` (official Airflow local-dev compose + a `warehouse` Postgres service). `make up`. Confirm the UI, both Postgres containers, and an Airflow `warehouse` connection.
2. **Schema + source (~1h).** Apply `sql/00_warehouse_schema.sql` (Week 1 star schema). Write `data/generate_source.py` to emit 35 deterministic daily files into `incoming/`.
3. **Port the loader (~2h).** Move your Week 3 idempotent `load_window.py` into `loader/`. Confirm delete-then-insert keyed off the window, in one transaction, resolving surrogate keys.
4. **Author the DAG (~2h).** `dags/crunch_loader_orchestrated.py`: sensor → extract → load → assert_load → publish_mart, with retries/backoff, SLA, callbacks, and `max_active_runs`.
5. **Backfill + prove (~2h).** Run the 30-day backfill, capture counts; run it again, capture counts; `diff`. Iterate until the diff is empty.
6. **Break it on purpose (~1h).** Inject the truncated-source window; capture the assertion firing.
7. **Write the report (~1h).** ~900 words in operator voice, with the measurements above.

---

## Grading rubric (100 points)

| Criterion | Points |
|-----------|-------:|
| **Idempotency proven** — same 30-day backfill twice yields identical per-day counts *and* sums; empty `diff` shown (F4, F9) | 25 |
| **Orchestration correctness** — sensor, retries+backoff, SLA, callbacks all present and wired correctly (F3, F5, F6) | 15 |
| **Interval discipline** — every window keyed off `data_interval_start`/`data_interval_end`, no `now()` (F2) | 10 |
| **Assertion gate catches the lie** — `assert_load` checks row count + volume + checksum, raises `AirflowFailException`, and is shown catching an injected bad load (F7, F10) | 15 |
| **Lands the star schema** — correct grain, surrogate-key resolution, into the Week 1 schema (F1) | 10 |
| **Throttled, bounded backfill** — `max_active_runs` set; measured peak warehouse CPU reported and bounded (F8) | 10 |
| **Runs from one command** — `make up` / `docker compose up` brings the whole stack up on a fresh checkout; isolated metadata vs warehouse DBs (N1, N3) | 8 |
| **Report quality** — operator voice, numbers not adjectives, all measurements present (N5) | 7 |

Minimum to pass the Phase I gate: **70 points, AND no double-count in the final run** (an empty backfill `diff` is non-negotiable — a pipeline that double-counts does not pass regardless of total score).

---

## Stretch goals

- **Real alert sink.** Point `on_failure_callback` / `sla_miss_callback` at a tiny Flask webhook receiver in Docker and capture the alert payload end to end.
- **Dataset-driven downstream.** Have `publish_mart` declare a dataset `outlet`; add a second DAG that `schedule`s on that dataset and fires only when the mart updates — no clock.
- **Celery executor.** Switch to the `CeleryExecutor` compose variant and re-run the backfill across two worker containers; report the duration change.
- **Late-record window.** Inject a record dated three days in the past into a daily file; show that re-running that past window via backfill correctly absorbs it (tying back to Week 3's late-record handling) without double-counting the days in between.

---

*Submit by pushing `week-04/` to your public portfolio repo and opening the Phase I gate demo. The reviewer will run `make up`, trigger a run, and run your backfill twice. Make the diff empty. PRs and improvements to <https://github.com/CODE-CRUNCH-CLUB>. Licensed GPL-3.0.*
