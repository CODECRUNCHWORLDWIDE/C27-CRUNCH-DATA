# Challenge 2 — Rewrite the Loader as Dagster Software-Defined Assets

> **Time:** ~90 minutes.
> **Prerequisites:** Exercise 3 (the idempotent Airflow loader) working; Lecture 3 §3 (Dagster assets, partitions, the decision framework); Python 3.11+ on the host; the warehouse Postgres running.
> **Citations:** [Dagster software-defined assets](https://docs.dagster.io/concepts/assets/software-defined-assets), [Dagster partitions & backfills](https://docs.dagster.io/concepts/partitions-schedules-sensors/partitions), [Dagster schedules](https://docs.dagster.io/concepts/automation/schedules), [Dagster sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors), [Dagster GitHub](https://github.com/dagster-io/dagster).

## Premise

You have a working, idempotent, backfillable Airflow DAG from Exercise 3. This challenge asks you to express *the same pipeline* the other way — as Dagster **software-defined assets** — so the asset-oriented mental model stops being theory. You will not throw away your Airflow work; the point is to feel, with your hands, the difference between "run these tasks in this order" (Airflow) and "these data products exist; keep them fresh" (Dagster), and to leave with a defensible opinion about which fits a given team.

You should finish able to say, from experience and not from a blog post: *here is what the asset graph gave me that the task graph did not, and here is what I gave up.*

## Setup

Dagster runs on the host (no Docker required for the dev server). Pin the version:

```bash
python -m venv .venv && source .venv/bin/activate
pip install "dagster==1.7.*" "dagster-webserver==1.7.*" psycopg2-binary pandas
```

Keep the **same warehouse Postgres** from Exercise 3 (it can stay in Docker; Dagster connects to `localhost:5432`). Reuse the same `fact_sales` table and the same deterministic synthetic source so the per-day counts are directly comparable to your Airflow run.

Project layout:

```
dagster_loader/
├── __init__.py
├── definitions.py        # Definitions(assets=[...], schedules=[...], sensors=[...])
└── assets.py             # raw_sales, fact_sales, the asset_check
```

## Steps

1. **Define the daily partition.** Create a `DailyPartitionsDefinition(start_date="2026-05-01")`. This makes each partition a one-day window — Dagster's first-class version of the Airflow data interval ([partitions](https://docs.dagster.io/concepts/partitions-schedules-sensors/partitions)).

2. **Write the `raw_sales` asset.** A partitioned `@asset` whose `context.partition_key` is the window date. Read (or, for the exercise, deterministically synthesize) that day's rows and return a DataFrame. Attach output metadata (`context.add_output_metadata({"rows": len(df), "window": window})`) so the materialization shows row counts in the UI ([software-defined assets](https://docs.dagster.io/concepts/assets/software-defined-assets)).

3. **Write the `fact_sales` asset.** A partitioned `@asset` that takes `raw_sales` *as a parameter* (the dependency is inferred from the argument name — there is no `>>`). Its body is the **same idempotent delete-then-insert** as Exercise 3, keyed off `context.partition_key`:

   ```python
   @dg.asset(partitions_def=daily)
   def fact_sales(context, raw_sales: pd.DataFrame) -> None:
       window = context.partition_key
       with warehouse_conn() as conn, conn.cursor() as cur:
           cur.execute("DELETE FROM fact_sales WHERE sales_date = %s", (window,))
           # ... executemany INSERT for this window ...
           conn.commit()
       context.add_output_metadata({"window": window, "rows": len(raw_sales)})
   ```

4. **Add the assertion gate as an `@asset_check`.** Attach a check to `fact_sales` that queries the warehouse window count and returns `AssetCheckResult(passed=n > 0, ...)`. This is the Lecture 3 §2.2 gate as a first-class Dagster object instead of a downstream task.

5. **Wire schedule + sensor.** Build a `build_schedule_from_partitioned_job` for the daily cadence ([schedules](https://docs.dagster.io/concepts/automation/schedules)), and a `@dg.sensor` that issues a `RunRequest(partition_key=...)` when the day's file lands ([sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors)). Register everything in `Definitions(...)`.

6. **Run it and backfill.** Launch the dev UI:

   ```bash
   dagster dev -m dagster_loader.definitions
   ```

   In the UI (`http://localhost:3000`), materialize the `2026-06-18` partition and watch the asset graph and the check. Then **backfill** partitions `2026-05-20` → `2026-06-18` from the Backfills page. Re-run the same backfill and confirm — via your `SELECT sales_date, count(*) ...` proof query — that per-day counts are **identical** (idempotent by the same delete-then-insert discipline).

7. **Write the comparison.** In `notes/airflow-vs-dagster.md` (250–400 words), answer:
   - What did the **asset graph** show you that the Airflow grid did not? (Per-partition materialization state, lineage, the inferred dependency.)
   - Where did Dagster's model feel *more natural* than Airflow's, and where did Airflow feel more natural or more mature?
   - The dependency in Dagster was inferred from a function argument; in Airflow you wrote `>>`. Which do you prefer, and what is the cost of each?
   - Using Lecture 3 §4's framework, pick Airflow or Dagster for **two** concrete teams: (a) a 4-person greenfield startup data team, (b) a 60-person company already running 300 Airflow DAGs. Justify each pick.

## Acceptance criteria

- [ ] `dagster dev` launches and the asset graph (`raw_sales` → `fact_sales`) renders with the dependency inferred from the argument, not declared with `>>`.
- [ ] Both assets are partitioned by a `DailyPartitionsDefinition`; `context.partition_key` drives the window.
- [ ] `fact_sales` does the idempotent delete-then-insert per partition; a re-materialization of a partition does not change its row count.
- [ ] An `@asset_check` on `fact_sales` is present and visible (green/red) in the UI.
- [ ] A schedule and a sensor are registered in `Definitions`.
- [ ] A partitioned backfill of `2026-05-20`→`2026-06-18` run **twice** yields identical per-day counts, matching the Exercise 3 Airflow result.
- [ ] `notes/airflow-vs-dagster.md` (250–400 words) answers all four prompts, including a defensible pick for both example teams.

## Stretch goals

- **I/O managers.** Replace the hand-rolled Postgres writes with a Dagster I/O manager so the asset function returns data and the I/O manager handles persistence. Note how this separates compute from storage.
- **Freshness / auto-materialize.** Add a freshness policy (or an auto-materialize policy) so Dagster knows when `fact_sales` is stale and can keep it fresh automatically. Compare to wiring the same behavior in Airflow.
- **Asset checks as gates.** Make the `@asset_check` *blocking* so a downstream `daily_mart` asset refuses to materialize when the check fails — the Dagster equivalent of the Exercise 3 `assert_load >> publish` gate.
- **One repo, two orchestrators.** Keep both the Airflow DAG and the Dagster project in your portfolio `week-04/`, sharing the same idempotent SQL helper. This is a strong portfolio artifact: the same correct pipeline, two orchestration paradigms, one source of truth for the load logic.

## References

- Dagster — software-defined assets: <https://docs.dagster.io/concepts/assets/software-defined-assets>
- Dagster — partitions & backfills: <https://docs.dagster.io/concepts/partitions-schedules-sensors/partitions>
- Dagster — schedules: <https://docs.dagster.io/concepts/automation/schedules>
- Dagster — sensors: <https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors>
- Dagster source: <https://github.com/dagster-io/dagster>

*Submit the `dagster_loader/` project and `notes/airflow-vs-dagster.md` in your portfolio's `week-04/challenge-02/`. PRs to <https://github.com/CODE-CRUNCH-CLUB>. Licensed GPL-3.0.*
