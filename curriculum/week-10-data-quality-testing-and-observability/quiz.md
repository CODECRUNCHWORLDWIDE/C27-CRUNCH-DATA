# Week 10 — Quiz

Ten questions. Take it with your lecture notes closed — the concepts (the six dimensions, gate vs monitor, severity, source freshness, contracts, anomaly detection) should be reflexes by now, not lookups. Aim for 8/10 before you start Week 11. Answer key is at the bottom; don't peek.

---

**Q1.** Name the six data-quality dimensions and give a one-line failure example of each.

---

**Q2.** A data check fails. What is the difference between the check being a **gate** and being a **monitor**, and why does it matter for the incident in Lecture 1 §1 (the truncated load discovered by the VP)?

---

**Q3.** Your ingestion task runs `result = checkpoint.run(...)` and then returns. The Airflow task goes green even when the data is corrupt. What is wrong, and what one thing must you add?

---

**Q4.** A truncated upstream load delivers 16,000 perfectly valid rows when ~40,000 is normal. Which boundary catches it — ingestion (GX per-row checks) or mart (freshness + volume) — and why does the *other* boundary miss it?

---

**Q5.** When should a data-quality check use `severity: warn` rather than `severity: error`? Give one check that should almost always be `error` and one that should almost always be `warn`, with reasoning.

---

**Q6.** What three things do you configure to make `dbt source freshness` work, and what does a non-zero exit code from `dbt source freshness` let you do in an Airflow DAG?

---

**Q7.** Why is a *rolling baseline* a better volume check than a static `ExpectTableRowCountToBeBetween(30000, 50000)`? Give a concrete case where the static band fails and the rolling baseline does not.

---

**Q8.** You write `ExpectColumnValuesToNotBeNull(column="customer_id")` and it fails every night on a handful of legitimate guest-checkout rows, so the team disables it. What parameter would have kept the check *and* tolerated the legitimate nulls, and what's the lesson?

---

**Q9.** List the clauses a real data contract contains (Lecture 3 §1). For the **change policy** clause specifically: which kind of change is typically allowed without notice, and which requires notice + a version bump + sign-off?

---

**Q10.** You have GX, dbt tests, and `dbt source freshness` available. Match each to the boundary it owns, and explain why you wouldn't use one tool for all three boundaries.

---
---

## Answers

**A1.** **Completeness** — is all the data here? (a non-null column has nulls / a partition is missing). **Validity** — is each value well-formed/in range? (`total_cents = -1`, `status = "PLCAED"`). **Uniqueness** — no unintended duplicates? (two rows with the same `order_id`). **Freshness** — is it recent enough? (newest row 6h old vs a 2h SLA). **Volume** — is the *amount* right? (16,000 rows where 40,000 is normal). **Distribution** — is the *shape* right? (mean order value jumps 5×, null-rate goes 1%→40%, cardinality collapses). The first three are per-row/intrinsic; the last three are per-load/contextual. (Lecture 1 §2.)

**A2.** A **gate** can *halt* the pipeline — it fails the task, stops the downstream, and alerts. A **monitor** only logs/warns and lets the pipeline continue. It matters because only a gate prevents the bad load from landing: in §1, a monitor would have logged "volume 16,000, expected ~40,000," the load would have proceeded anyway, the dashboard would have updated, and the VP would still have called the meeting. The log was written; nobody read it in time. A log nobody reads in time is not a control. (Lecture 1 §5.)

**A3.** The task runs the check but never inspects the result, so it can never fail the task — it's a monitor, not a gate. You must add a `raise` on `not result.success` (or use `GreatExpectationsOperator(... fail_task_on_validation_failure=True)`). Running the check is not gating; *acting on the result* is gating. (Lecture 1 §5.1, Lecture 2 §4.)

**A4.** The **mart boundary** (volume check) catches it; the **ingestion boundary** misses it. Every one of the 16,000 rows is individually valid — not null, in range, in the value set, unique — so the per-row GX checks all pass. The problem is the *aggregate count*, which only a per-load volume check (this load vs the baseline) can see. This is exactly why you gate both boundaries: ingestion catches malformed rows, the mart catches untrustworthy results made of valid rows. (Lecture 1 §4, mini-project.)

**A5.** Use `warn` for *suspicious but ambiguous* signals where a false halt is costly and the failure might be legitimate change; use `error` for *unambiguous corruption* that must never reach downstream. Almost-always-`error`: a duplicate primary key or a null on a required key — never acceptable, halt. Almost-always-`warn`: distribution drift (a mean can legitimately move on a promotion) — flag a human, don't stop the trains. You can also graduate: `warn` at a 20% volume dip, `error` at 50%. (Lecture 1 §5.2–5.3.)

**A6.** You configure `loaded_at_field` (the column with the per-row load time), `warn_after` (the soft SLA), and `error_after` (the hard SLA). `dbt source freshness` exits **non-zero** on an `error`, so an Airflow `BashOperator` running it fails its task on that exit code — which halts the DAG. That makes source freshness the freshness gate at the source boundary. (Lecture 3 §2.1.)

**A7.** A static band drifts out of calibration because normal volume changes over the year: a band that fits a normal Tuesday will **false-halt on Black Friday** (a legitimate spike to ~95,000 trips `max_value=50000`) and may miss a slow decay. A rolling baseline compares today to the trailing mean, so it stretches with legitimate trends and still flags a genuine anomaly. Concrete case: Black Friday's 95,000 rows fail the static `[30k, 50k]` band but pass a "within 50–200% of the trailing mean" check once the baseline has risen; the truncated 16,000 against a 41,000 baseline fails the rolling check correctly. (Lecture 3 §3, Homework P3.)

**A8.** The `mostly` parameter: `ExpectColumnValuesToNotBeNull(column="customer_id", mostly=0.99)` keeps the check but tolerates up to 1% legitimate nulls. The lesson: a check that false-fails on normal data gets disabled, which is worse than no check — and a disabled gate is the §5.3 failure mode. Real data is rarely 100% clean; tune the tolerance so the gate survives. (Lecture 1 §3.1, Lecture 2 §3.)

**A9.** Schema (fields + types), grain + semantics (what one row is, what each field means), SLAs (freshness + volume), ownership (accountable team + contact), change policy, and PII flags. For the **change policy**: *additive* changes (adding a nullable column) are typically allowed without notice; *breaking* changes (dropping/renaming/narrowing a field, changing the grain) require notice (e.g. 14 days) + a major version bump + consumer sign-off. (Lecture 3 §1.)

**A10.** **GX → ingestion boundary** (raw data before it enters dbt: files, dataframes, non-dbt sources; rich Data Docs; distribution expectations out of the box). **dbt tests → transformation layer** (the models dbt builds; co-located with the models; `relationships` for clean referential integrity). **`dbt source freshness` → source boundary** (is the upstream recent enough?). You don't use one tool everywhere because each owns a different boundary and a different strength: GX can't do cross-table referential integrity as cleanly as dbt's `relationships`, dbt can't easily validate a raw file *before* it's loaded, and freshness is a source-level concept neither per-row tool expresses as directly as `dbt source freshness`. Pick the tool that owns the boundary. (Lecture 2 §7, Lecture 3 §2.)
