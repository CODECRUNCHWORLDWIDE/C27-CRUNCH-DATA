# Week 4 — Quiz

Ten multiple-choice questions on orchestration concepts and failure modes: the data-interval model, sensors, retries vs SLAs, backfill safety, idempotency, "the task that lied," Airflow architecture, and Dagster assets. Take it with the lecture notes closed. Aim for 9/10 before the mini-project. Answer key at the bottom — do not peek.

---

**Q1.** A DAG has `schedule="@daily"` and `start_date=2026-06-01`. The DAG run responsible for the data interval `[2026-06-18, 2026-06-19)` fires:

- A) At the start of 2026-06-18.
- B) At the midpoint of 2026-06-18 (noon).
- C) Shortly after 2026-06-19 00:00, because only then is the 18th's data complete.
- D) Only when manually triggered; `@daily` does not auto-run.

---

**Q2.** Inside a task, which value should a windowed, backfillable load key its reads and writes off of?

- A) `datetime.now()`.
- B) `CURRENT_DATE` evaluated in the warehouse.
- C) `data_interval_start` (and `data_interval_end`).
- D) The DAG's `start_date`.

---

**Q3.** You deploy a DAG with `start_date` 60 days in the past, `catchup=True`, and no `max_active_runs`. What is the most likely immediate consequence?

- A) Nothing runs until you manually trigger it.
- B) The scheduler enumerates ~60 intervals and launches them with unbounded concurrency, hammering the warehouse (and the metadata DB).
- C) Only the most recent interval runs; history is skipped.
- D) Airflow refuses to deploy a DAG with a past `start_date`.

---

**Q4.** A sensor must wait up to 3 hours for a daily file. Which `mode` is the right default, and why?

- A) `poke`, because it is simplest and the 3 hours is fine to hold a worker slot.
- B) `reschedule`, because it releases the worker slot between checks so the wait does not starve other tasks.
- C) `soft_fail`, because the file might not arrive.
- D) No mode is needed; sensors never hold slots.

---

**Q5.** What is the difference between a retry and an SLA?

- A) They are synonyms; both re-run the task.
- B) A retry re-runs the task after a failure; an SLA is a notification that the run is *late*, and it does **not** stop or re-run the task.
- C) An SLA re-runs the task on lateness; a retry only logs.
- D) A retry alerts a human; an SLA kills the task.

---

**Q6.** Why are retries dangerous on a `load` task that does an `INSERT` with no preceding `DELETE`?

- A) Retries are never dangerous; Airflow deduplicates automatically.
- B) If the task commits its rows and then the worker dies before reporting success, Airflow retries and the `INSERT` runs again — double-counting the window.
- C) `INSERT` is slower than `MERGE`, so retries time out.
- D) Retries change `data_interval_start`, loading the wrong window.

---

**Q7.** A backfill of 30 days runs, every task turns green, but the warehouse now shows double the expected rows for the days that were already loaded. The root cause is most likely:

- A) `max_active_runs` was set too low.
- B) The `load` is not idempotent — it appends instead of replacing its window, so re-loading an already-loaded day doubles it.
- C) The metadata DB ran out of disk.
- D) The sensor never fired.

---

**Q8.** "The task succeeded but lied" describes which situation?

- A) A task raised an exception and was marked failed.
- B) A task exited zero (marked success) while loading partial, wrong-window, or garbage data — because exit code reflects the process, not data correctness.
- C) A task was skipped due to `soft_fail`.
- D) A sensor timed out.

---

**Q9.** In Airflow's architecture, a task is stuck in `queued` and never starts. Which component should you check first?

- A) The webserver — it must be down.
- B) The triggerer — it runs all tasks.
- C) The scheduler — if it is dead (often OOM-killed on a memory-starved laptop), tasks are queued but never handed to the executor.
- D) The DAG file — `queued` means a syntax error.

---

**Q10.** In Dagster's software-defined-asset model, how is the dependency between `raw_sales` and `fact_sales` declared?

- A) With `raw_sales >> fact_sales`.
- B) By `fact_sales` taking a parameter named `raw_sales`; Dagster infers the dependency from the function signature.
- C) By listing both in a `DAG()` constructor.
- D) Dependencies cannot be expressed between Dagster assets.

---

## Answer key

<details>
<summary>Click to reveal answers</summary>

1. **C** — A run *owns* the data interval `[start, end)` and fires *after* `end` closes, because only then is the interval's data complete. The run for the 18th fires on the 19th. (A)/(B) misread "the run runs during its interval"; (D) is false — `@daily` schedules automatically once unpaused. See Lecture 1 §3.1.

2. **C** — `data_interval_start`/`data_interval_end` name the window the run owns, and they are correct under scheduled runs *and* backfills. Keying off `now()` (A) or `CURRENT_DATE` (B) loads *today's* data into a past window during a backfill — silent corruption. `start_date` (D) is the DAG's first interval, not the run's window. Lecture 1 §3.2.

3. **B** — `catchup=True` with a far-back `start_date` and no concurrency cap enumerates every interval and launches them at once, flooding the warehouse and the (also-Postgres) metadata DB. This is "the backfill that melts the cluster." Fix: `max_active_runs`. (C) describes `catchup=False`; (A)/(D) are false. Lecture 1 §4, Lecture 2 §4.2.

4. **B** — `reschedule` releases the worker slot between pokes, so a multi-hour wait does not occupy a slot the whole time and starve other tasks. `poke` (A) holds the slot for the full 3 hours. `soft_fail` (C) is about whether a timeout fails or skips, not the wait strategy. (D) is false — `poke` does hold a slot. Lecture 2 §1.3.

5. **B** — Retries re-run a *failed* task (with backoff); an SLA is a *lateness notification* (`sla` + `sla_miss_callback`) that fires while the task keeps running and never stops or re-runs it. They answer "should this run again?" vs "is this run late?" Lecture 2 §3.

6. **B** — The dangerous interleaving: the task commits its rows, then the worker dies before Airflow records success; Airflow retries; the `INSERT`-only load runs again and doubles the window. The fix is idempotency (delete-then-insert), not fewer retries. (A) is false — Airflow does not dedup. Lecture 2 §2.2, Lecture 3 §1.

7. **B** — A non-idempotent (append-only) `load` doubles any window that runs twice, which a backfill of already-loaded days does. The runs are green because nothing *failed*; the data is just wrong. (A) would slow the backfill, not double-count. Lecture 2 §4.2, Lecture 3 §1.1; this is exactly Challenge 1.

8. **B** — Exit code is a statement about the process, not the data. A task can exit zero having loaded partial/wrong/garbage data and turn green. The defense is a separate assertion task (row-count, volume, checksum) that fails loudly. Lecture 3 §2.

9. **C** — The scheduler decides what is runnable and hands queued tasks to the executor; if it is dead (commonly OOM-killed when Docker has < 4 GB), tasks sit in `queued` with no obvious error. The webserver (A) only renders; the triggerer (B) only runs deferrable waits, not all tasks; `queued` is not a syntax-error state (D). Lecture 1 §5.1, README "running on a laptop."

10. **B** — In Dagster, `fact_sales` depending on `raw_sales` is expressed by `fact_sales` taking a parameter named `raw_sales`; the dependency is inferred from the signature — there is no `>>`. That argument-driven inference *is* the asset model's lineage. Lecture 3 §3.1.

</details>

---

## Self-assessment

- **9–10 correct:** You have the orchestration model and the failure modes down. Start the mini-project — you are ready for the Phase I gate.
- **7–8 correct:** Solid. Re-read the sections behind your misses (the data-interval model and idempotency are the load-bearing ones) before the backfill.
- **5–6 correct:** Re-read Lecture 1 §3 (intervals) and Lecture 3 §1–2 (idempotency, the task that lied) carefully, then retake. These two ideas decide whether your backfill is safe.
- **Under 5:** Work back through all three lectures with the Airflow stack running beside you, doing each code block live. Orchestration clicks when you watch a backfill run, not when you read about one.

If you scored under 7, re-read the lectures, then retake before starting the [mini-project](./mini-project/README.md).
