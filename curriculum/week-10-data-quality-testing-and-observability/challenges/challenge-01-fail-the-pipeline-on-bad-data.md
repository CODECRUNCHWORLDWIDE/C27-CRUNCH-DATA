# Challenge 1 — Fail the Pipeline on Bad Data

**Time estimate:** ~90 minutes.

## Problem statement

Your nightly `orders` DAG (from Week 4) currently runs a Great Expectations checkpoint as a *monitor*: it validates the raw load, updates Data Docs, and then runs the load no matter what the validation said. A truncated file landed last week and nobody noticed until the dashboard was wrong. Your job is to turn that monitor into a **gate**: wire the GX checkpoint into the DAG so a malformed load **halts the DAG and alerts**, and then *prove it* by feeding the pipeline a deliberately corrupted file and watching it stop.

This is the headline skill of the week. A check that doesn't halt is theater (Lecture 1 §5). The difference between a junior who "added some validation" and an engineer who built a gate is exactly this: when the bad file arrives, does the pipeline stop, or does it run green and publish garbage?

## Why it matters

The most expensive data bug is the one that succeeds — runs green, lands bad data, refreshes the dashboard, and is discovered downstream by an executive (Lecture 1 §1). The only thing standing between you and that incident is a check with the *authority to halt*. In every data-engineering interview and every production on-call, "how does your pipeline stop a bad load?" is the question, and "we log a warning" is the wrong answer. After this challenge you have a working, demonstrable halting gate — the artifact that makes "our pipeline catches bad loads" a fact you can show, not a claim you make.

## Procedure

### Phase 1 — Baseline the broken (monitor-only) state

1. Take your Week 4 `orders` DAG and your Exercise 01/02 GX checkpoint (`orders_ingestion_checkpoint`).
2. Wire the checkpoint into the DAG as it *wrongly* is now: a task that runs `checkpoint.run(...)` and ignores the result. Confirm the failure mode: feed it a corrupted file (a short row count, or a null `order_id`) and watch the DAG go **green** and the bad data land. Capture this — it's your "before."

### Phase 2 — Build the corrupted fixture

3. Create a deliberately malformed input file `fixtures/orders_corrupt.csv` that violates *at least three* expectations from your suite: e.g. a null `order_id`, a `status` of `PLCAED`, a negative `total_cents`, and a row count of 8,000 (below your volume band). Keep a clean `fixtures/orders_clean.csv` alongside it.

### Phase 3 — Make the task a gate

4. Replace the monitor task with a gate. Two acceptable approaches:
   - **`GreatExpectationsOperator`** (from `airflow-provider-great-expectations`) pointed at your checkpoint, with `fail_task_on_validation_failure=True` (the default) — it raises on a failed checkpoint for you.
   - **`PythonOperator`** that runs the checkpoint and explicitly `raise`s on `not result.success`:
     ```python
     def gate_orders_ingestion(**ctx):
         result = checkpoint.run(batch_parameters={"dataframe": read_input(ctx)})
         if not result.success:
             failed = [r.expectation_config.type
                       for run in result.run_results.values()
                       for r in run.results if not r.success]
             raise AirflowException(f"orders_ingestion FAILED: {failed}")
     ```
5. Set the DAG dependencies so the gate runs **before** the load/staging tasks: `extract >> gate >> load >> stage`. A failed gate must leave `load` in `upstream_failed` / `skipped` state — the load never runs.

### Phase 4 — Alert

6. Wire an alert on failure. Either an `on_failure_callback` on the DAG that sends to a Slack/email/webhook, or a GX `Action` in the checkpoint, or both. The alert message must name the table, the run, and which expectations failed.

### Phase 5 — Prove it

7. Run the DAG against `orders_clean.csv` → all tasks **succeed**, data lands, Data Docs shows green.
8. Run the DAG against `orders_corrupt.csv` → the gate task goes **failed**, `load` and everything downstream is **not run**, the alert fires, and the bad data **never landed**. Capture the Airflow grid view showing the failed gate and the skipped downstream, the alert, and a query proving the target table did not receive the corrupt rows.

## Deliverable

A directory `challenge-01/` containing:

- [ ] The modified DAG file with the gate task and dependencies.
- [ ] `fixtures/orders_clean.csv` and `fixtures/orders_corrupt.csv`.
- [ ] The alert config (callback or GX action).
- [ ] `PROOF.md` with: the "before" (monitor-only green run on bad data), the "after" (gate fails on the same file), the Airflow grid screenshot showing the failed gate + skipped downstream, the alert that fired, and a SQL query proving the corrupt rows did not land.

## Pass criteria

- [ ] On the corrupt file, the gate task ends in **failed** state and the load/staging tasks are **not executed** (skipped/upstream_failed). The corrupt rows are **not** in the target table.
- [ ] On the clean file, the full DAG succeeds and the data lands.
- [ ] An **alert fires** on the failure, naming the table and the failed expectations.
- [ ] `PROOF.md` shows the before/after and the evidence above.
- [ ] The gate inspects `result.success` (or uses `fail_task_on_validation_failure=True`) — i.e. it *acts on* the result, not merely runs the check.

## Stretch

- **Graduated halt.** Make the gate `warn` on a soft volume dip (10–20% below baseline) but `fail` on a hard one (>50%), so a slow day alerts without halting and a truncation stops the pipeline.
- **Quarantine instead of drop.** On a failed gate, route the corrupt file to a `quarantine/` location and emit a row to `meta.load_metrics` with `status='gated'`, so the bad load is preserved for forensics rather than lost.
- **Retry then alert.** Configure the gate's upstream `extract` with one retry (in case the truncation was a transient upstream blip) and only halt+alert if the re-pulled file still fails the suite.

## References

- **Great Expectations — documentation** (checkpoints, actions, result objects): <https://docs.greatexpectations.io/docs/>
- **`airflow-provider-great-expectations`** (the `GreatExpectationsOperator`): <https://github.com/great-expectations/airflow-provider-great-expectations>
- **Apache Airflow — trigger rules & task dependencies** (how a failed task halts downstream): <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#trigger-rules>
- Lecture 1 §5 (the halting gate) and Lecture 2 §4 (checkpoint + the gate `raise`).
