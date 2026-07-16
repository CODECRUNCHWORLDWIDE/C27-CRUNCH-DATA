# Exercise Solutions — Week 12

This week has no coding exercises — the work is finishing, demoing, and defending the capstone pipeline. Instead, this document is the *model answer set*: worked answers to the capstone-defense Q&A, a worked postmortem against the rubric, and model answers to the eight interview rounds. Read it after you have drafted your own answers — comparing yours to these is the point; copying these is not (a reviewer can tell a memorized answer from an understood one in one follow-up question).

---

## Part A — The capstone-defense Q&A, worked

### "What is the grain of your fact table, and why?"

**Model answer.** One row per order line. I chose that grain because the business questions reach down to the line — "what's the discount rate on SKU X," "what's the margin by line within an order" — and the rule is that the fact's grain must be the lowest grain any question touches; if I'd modeled at the order grain I could never answer a line-level question without a lossy split. The fact carries the foreign keys to the conformed `dim_customer`, `dim_product`, and `dim_date`, plus the additive measures (quantity, extended price, discount). Anything not additive at this grain — like an order-level shipping fee — I either allocate explicitly or keep in an order-grain fact, not smeared across lines. The grain is the first sentence of my architecture doc because everything downstream depends on it.

**What the reviewer is checking.** That you led with the grain, defended it against a concrete failure (the line-level question you couldn't answer at order grain), and know what "additive at this grain" means.

### "Show me exactly where a row could be lost between Kafka and the lakehouse."

**Model answer.** Walk the path. The producer acks on commit to the partition log, so once the producer has its ack the broker durably has the event — that's the first boundary, "produced" now means "in Kafka," not "in the lakehouse." Spark Structured Streaming consumes with a checkpoint; each micro-batch reads a range of offsets, computes the windowed aggregate, and commits the *offsets and the output atomically* — the offset advance and the Iceberg snapshot commit are one unit. So the loss windows are: (1) if the job dies after writing the snapshot but before committing offsets, on recovery it re-reads the same offsets and re-writes the same snapshot — at-least-once delivery, but the sink is idempotent (the snapshot overwrite is deterministic), so no duplicate lands; (2) if MinIO is unavailable, the snapshot commit fails, the batch doesn't advance, and the job retries from the last committed offset — lag, not loss. The genuine loss risk is Kafka retention: if the consumer is down longer than the topic's retention, the earliest offsets age out before they're read. That's the boundary I monitor — consumer lag against retention.

**What the reviewer is checking.** That you can trace the boundary precisely, name "at-least-once plus idempotent sink equals exactly-once *into the table*," and identify the *real* loss risk (retention) rather than hand-waving "it's exactly-once."

### "You ran the backfill twice. Why didn't the mart double-count?"

**Model answer.** Because the load is idempotent by construction, not by discipline. The incremental model is a dbt incremental with `unique_key` set to the natural key, which compiles to a `MERGE` — the second run upserts the same rows to the same values, so the row count and the measures don't change; a re-run is a no-op. If I'd used an `INSERT … SELECT`, the second run would have appended a duplicate set and doubled the measure. The backfill is bounded by the high-water mark from Week 3: I reprocess partitions at or after `max(loaded_at) − lateness_grace`, so a backfill reprocesses a bounded recent window idempotently rather than the whole history. Double-counting is the failure that loses an analyst's trust fastest, so it's the one I designed out rather than tested away.

**What the reviewer is checking.** That you named the *mechanism* (`MERGE` on a natural key, not `INSERT`) rather than asserting "it's idempotent," and that you tie it to the high-water-mark backfill.

### "Prove your stream is exactly-once and not just at-least-once."

**Model answer.** Exactly-once is at-least-once plus an idempotent sink — there's no magic, so I prove the two halves. At-least-once: the checkpoint guarantees every offset is eventually consumed; nothing is skipped. Idempotent sink: the Iceberg write per micro-batch is an atomic snapshot keyed deterministically on the window, so re-processing the same offsets after a failure overwrites the same snapshot rather than appending. The proof I show is a row-count reconciliation: I produced N events with known keys, killed the streaming job mid-batch to force a replay, let it recover, and compared the windowed aggregate against a ground-truth count of the produced events — equal, no duplicates, no gaps. That reconciliation is in my data-quality report; "it's exactly-once" is a claim, the reconciliation is the proof.

**What the reviewer is checking.** That you decompose exactly-once into its two real parts and offer a *reconciliation against ground truth* as the proof, not an assertion.

### "A number on the dashboard is wrong. Trace it back to the source."

**Model answer.** Lineage is the first thing I open in an incident, not the last. The dbt docs DAG (or Marquez, if I've wired OpenLineage) traces the dashboard metric back through the mart model, the intermediate model, the staging model, to the raw table and the source file. So instead of guessing which transformation is wrong, I follow the edge: is the mart's number wrong, or is it correctly aggregating a wrong staged value, or is the staged value correctly cleaning a wrong raw row, or did the raw load ingest a bad source file? Each hop narrows it. In practice the most common culprit is the source — a late or malformed file — which is why my freshness check and the ingestion gate exist; if those fired, the lineage just confirms where. The lineage map is in the architecture doc precisely so an on-call who isn't me can do this trace.

**What the reviewer is checking.** That you reach for lineage *first* and can describe walking the DAG hop by hop to localize the fault, rather than debugging blind.

---

## Part B — A worked postmortem (malformed-batch-load drill)

> **Summary.** During a planned chaos drill, a daily source file was dropped into the landing zone with one column renamed (`order_total` → `total`). The extract task loaded 0 rows instead of erroring; the incremental dbt mart treated the empty increment as a no-op and silently served the prior day's number. In a *pre-fix* run the dashboard went flat for 7 hours before an analyst noticed; in the *drilled* run the new ingestion gate caught it at extract time and halted the load. No bad data reached the mart; the mart's number was unchanged because the load was blocked before transformation. Detection in the drilled run was immediate (the Great Expectations checkpoint failed and alerted); recovery (quarantine, fix the file, re-run) took 11 minutes.

> **Timeline.**
> - 02:00 — daily file landed in the landing zone (renamed column).
> - 02:03 — Airflow DAG triggered on the landing sensor.
> - 02:04 — the Great Expectations checkpoint at the ingestion boundary ran `expect_table_columns_to_match_set` and failed (missing `order_total`, unexpected `total`).
> - 02:04 — the extract task went red; the `on_failure` alert fired to the on-call channel; the load halted before the staging write.
> - 02:05 — confirmed via the dbt source-freshness check that the mart's `max(loaded_at)` was unchanged — the bad file never reached staging or mart.
> - 02:10 — quarantined the bad file to the `_rejected/` prefix; obtained the corrected file from the source team's contract endpoint.
> - 02:14 — re-ran the DAG on the corrected file; the gate passed; staging and mart built; the dashboard metric updated.
> - 02:15 — fleet healthy; row-count reconciliation confirmed the expected row delta, no duplication.

> **Root cause (chain).** (1) Immediate: the dashboard *would have* gone flat (in the pre-fix design) because the renamed column produced an empty extract. (2) Why empty rather than an error: the extract selected by column name and silently produced 0 rows when the column was absent, instead of failing on an unexpected schema. (3) Why an empty load served yesterday's number: the mart is incremental, and an empty increment is a no-op, so the dashboard silently served the prior snapshot. (4) Why none of this was caught for hours (pre-fix): there was no quality gate at the ingestion boundary and no mart-freshness check. The root cause is *not* "the source team renamed a column" (the external trigger) but "the ingestion boundary had no schema gate and no fail-on-empty, and the mart had no freshness check, so a schema break degraded silently instead of halting loudly."

> **Detection.** In the drilled run: immediate, by the ingestion checkpoint at 02:04. In the pre-fix design it would have been an analyst at ~09:00 — a ~7-hour gap between when it *should* have been caught (02:04, at ingestion) and when it *would* have been (09:00, by a human). That gap is itself a finding and motivates both the ingestion gate and the freshness check.

> **Resolution.** Quarantined the bad file; retrieved the corrected file; re-ran the idempotent DAG; the gate passed; the mart built; a row-count reconciliation against the source confirmed the expected delta and no double-count. No manual mart surgery was required — because the gate halted the load *before* the mart, there was nothing downstream to clean up.

> **Lessons / action items (blameless).**
> 1. Gate the ingestion boundary with a Great Expectations checkpoint asserting the exact column set and types; fail the task on a mismatch. *Owner: me. Done when: the checkpoint halts a repeat of this drill before staging.* (Prevent recurrence.)
> 2. Make the extract fail on an unexpectedly empty load (0 rows when ≥1 is expected for a daily file). *Owner: me. Done when: an empty extract errors instead of producing a silent no-op.* (Prevent recurrence.)
> 3. Add a dbt source-freshness check that errors when the mart's `max(loaded_at)` is older than 6 hours. *Owner: me. Done when: the freshness check fails in a repeat of this drill if the gate is somehow bypassed.* (Detect faster.)
> 4. Establish a data contract with the source team for schema-change notice. *Owner: me + source team. Done when: the contract is written and a schema-change webhook exists.* (Address the external boundary.)

**Why this scores well.** Blameless (root cause is the missing gate and the silent-no-op behavior, not "the source team renamed a column"), factual (timestamped, neutral), actionable (four items, each with an owner and a done-when, split into prevent-recurrence vs detect-faster), it follows the chain past the symptom, and — critically for a data postmortem — it *proves no bad data reached the mart* via the freshness check and the row-count reconciliation.

---

## Part C — The eight interview rounds, model answers (abbreviated)

1. **Data-modeling whiteboard — "Model this retail domain from the business questions."** Start from the questions, not the tables: list what the business asks ("revenue by region by month," "discount by SKU"), find the lowest grain any question touches (order line), make that the fact grain, then the dimensions the questions group by (customer, product, date, store) — conformed so a "revenue by region" question and an "orders by region" question agree. SCD: `dim_customer` Type 2 because we report by the segment-as-of-the-order; `dim_product` Type 1 if we only ever want the current description. Surrogate keys on the dimensions, natural keys retained for lineage (Week 1).

2. **SQL tuning — "Why is this query slow, and how do you read the plan?"** Read the plan bottom-up: find the largest scan and check whether a filter pushed down (a partition/predicate pushdown turning a full-table scan into a few files). Look for the join strategy — a sort-merge join on a skewed key spills; a broadcast join needs the build side under the threshold. Check for an accidental cross join or a missing filter exploding row counts. The fix is usually: prune the scan (partition pushdown), pick the right join (broadcast the small side or salt the skewed key), or pre-aggregate before the join (Week 2).

3. **Idempotency & incrementality — "Design a load that survives a re-run."** Key the load on a natural key and write with a `MERGE`/upsert, never a bare `INSERT`, so a re-run upserts the same rows to the same values. Drive incrementality off a high-water mark (`max(loaded_at)`) and reprocess only `≥ hwm − lateness_grace` so late records inside the grace window are caught and the re-run is bounded. The test is: run it twice, assert the row count and the measures are unchanged (Week 3).

4. **Orchestration — "Design the DAG and its failure handling."** Tasks are idempotent and retryable; the DAG is backfillable (parameterized by the logical date, not "now"); a landing sensor or schedule triggers it; each layer boundary has a quality gate task whose failure halts the DAG and alerts (`on_failure_callback`). Backfill reprocesses a date range without double-counting because the underlying load is idempotent. The five most likely failures (late/missing source, schema drift, stream lag, bad data landing, runaway cost) each have a first-three-diagnostics entry in the runbook (Week 4).

5. **Storage internals — "What do Iceberg/Delta add over raw Parquet, and what's the small-files problem?"** Raw Parquet gives you columnar storage, row groups, and predicate pushdown but no transactions, no schema evolution, and no snapshot isolation — concurrent writes and partial files corrupt readers. Iceberg/Delta add atomic snapshot commits (a reader sees a consistent table or the prior one, never a half-write), schema evolution without rewriting data, time travel, and metadata that prunes files. The small-files problem: streaming writes one file per micro-batch, so the table accretes thousands of tiny files that kill scan performance; the fix is a scheduled `OPTIMIZE`/compaction that rewrites them into target-sized files (Weeks 6, 11).

6. **Distributed compute — "Diagnose and fix a skewed join in Spark."** A skewed join is one where a few keys hold most of the rows, so one task does most of the work and spills while the rest finish — visible in the Spark UI as one straggler stage with a huge shuffle-read and spill. Fixes: broadcast the small side if it fits the threshold (no shuffle at all); or salt the skewed key (append a random suffix to the hot key on both sides and explode the lookup) to spread it across tasks; or use Spark's adaptive query execution skew-join handling. I read the SQL tab's exchange and spill metrics to confirm before and after (Week 7).

7. **Streaming — "Event time vs processing time, and what does your watermark do?"** Event time is when the event happened; processing time is when the engine saw it — they diverge under lag and out-of-order delivery, and aggregations must use event time to be correct. The watermark bounds how long the engine waits for late events: a 10-minute watermark keeps a window's state open for 10 minutes past its event time, then finalizes and drops later stragglers — a bounded-state-for-correctness trade I make on purpose, with the late stragglers reconciled in the nightly batch. Exactly-once into the sink is at-least-once plus the idempotent snapshot write (Week 9).

8. **The incident story — "Tell me about a data incident."** The chaos drill: name the symptom (the dashboard went flat / a partition's lag exploded / a breaking schema change), the wrong first hypothesis, the diagnostic that disproved it (the lineage trace, the lag panel, the registry rejection), the actual root cause (the missing gate, the un-sized buffer, the absent freshness check), the fix, and the one thing you changed so it can't recur. 90 seconds, with the artifact (the quality-report page, the Spark UI, the rejected schema). This is your Week 12 postmortem told out loud.

---

## A general note on being defended-against well

The reviewer who pushes hardest is the one who thinks your platform is good enough to be worth probing. A defense with no hard questions is a defense the panel decided wasn't worth their attention. When the questions get sharp — "prove it isn't double-counting," "trace this number," "where exactly is the loss boundary" — that is the signal you built something real. Answer honestly, narrate your reasoning, reach for lineage and reconciliations rather than assurances, and when you hit the edge of what you know, say so and say how you'd find out. That single move — handled well, even once — does more for your evaluation than a flawless run through the easy questions.
