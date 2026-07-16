# Quiz — Week 12: The Final Review

Ten questions spanning all 12 weeks of C27. This is the integration exam in miniature — each question reaches across the course the way the capstone defense will. Closed-book on the concepts; open-book on syntax. ~45 minutes. Answer key at the bottom; do not peek before completing.

---

## Question 1 (Week 1 — modeling)

You are modeling a retail sales domain and a stakeholder asks, "what's the average discount per SKU per region per month?" What grain must your sales fact be at to answer this without lossy aggregation, and what is the danger of choosing a coarser grain?

## Question 2 (Week 1 — SCD)

Your `dim_customer` carries a `segment` column that changes over time, and the business reports revenue by the customer's segment *as it was at the time of the sale*. Is this dimension Type 1, Type 2, or Type 3, and what specifically goes wrong if you implement the other common choice?

## Question 3 (Week 3 — idempotency)

Two analysts both trigger a backfill for the same date range within an hour. Your fact table's row count and total revenue are unchanged after both runs complete. Name the load mechanism that makes this true, and name the mechanism that would have *doubled* the revenue instead.

## Question 4 (Weeks 6 — storage)

Streaming a windowed aggregate into an Iceberg/Delta table degrades the dashboard's query performance over a week, even though the row count is modest. Name the problem, its cause, and the operation that fixes it.

## Question 5 (Week 7 — distributed compute)

A Spark join on `customer_id` runs for forty minutes while one task does almost all the work and spills to disk; the rest finish in seconds. Name the phenomenon, where in the Spark UI you confirm it, and two distinct fixes.

## Question 6 (Weeks 8–9 — streaming semantics)

State precisely what "exactly-once" means for your Structured Streaming job writing into the lakehouse, decomposed into its two underlying guarantees, and describe the concrete check you would run to *prove* it rather than claim it.

## Question 7 (Week 9 — watermarks)

Your streaming windowed aggregate uses a 10-minute event-time watermark. A click event arrives 25 minutes after its event time. What happens to it, why is dropping it the correct default, and where do you recover it if you need it?

## Question 8 (Week 10 — quality gates)

A teammate says, "we have data quality — we log a warning when a value is out of range." Why is this not a quality gate, and what would make it one?

## Question 9 (Week 12 — postmortem)

A teammate writes a postmortem whose root cause is "the analyst ran the backfill twice and double-counted revenue; we reminded the team to be careful." What is wrong with this root cause, and what is the correct framing?

## Question 10 (Week 12 — the defense)

During your capstone defense, a reviewer asks how your stream behaves under a partition leader election in Kafka — something you have not measured. What is the correct response, and why is it better than the alternative?

---

## Answers

### Q1 (Week 1)

The fact must be at **one row per order line** (the line grain). The question filters and groups by SKU, region, and month — all of which a line-level fact carries (the SKU via `dim_product`, the region via `dim_customer`/`dim_store`, the month via `dim_date`). The rule is that the fact's grain must be at or below the lowest grain any question reaches; SKU-level discount lives on the line, so the line is the floor. The danger of a coarser grain (e.g., order grain): you would have to allocate or split an order's discount across its SKUs to answer the question, which is lossy and arbitrary, and any future line-level question becomes unanswerable. Grain is the first and most consequential modeling decision (Week 1).

### Q2 (Week 1)

**Type 2.** A Type-2 dimension keeps a new row (a new surrogate key, with effective/expiry dates) each time the tracked attribute changes, so a fact joined on the surrogate key in effect at the sale date sees the segment *as it was then*. If you implemented **Type 1** (overwrite in place), every segment change would retroactively re-segment all historical sales, and last quarter's revenue-by-segment would silently move every time a customer changed tier — the report would not reproduce. Type 3 (a single "previous value" column) only remembers one change and can't represent a full history. The "as it was at the time" requirement is the signature of Type 2 (Week 1).

### Q3 (Week 3)

The load is idempotent because it writes with a **`MERGE`/upsert keyed on the natural key** (a dbt incremental with `unique_key` set), so a second run upserts the same rows to the same values — a no-op on the row count and the measures. The mechanism that would have **doubled** revenue is a bare **`INSERT … SELECT`**, which appends a second copy of every row on the re-run. Backfill is further bounded by the high-water mark (`max(loaded_at) − lateness_grace`) so a re-run reprocesses a bounded recent window rather than the whole history. Double-counting is the trust-destroying failure, so it's designed out, not tested away (Week 3).

### Q4 (Week 6)

The **small-files problem**. Each streaming micro-batch commits its own small file (or set of files), so over a week the table accretes thousands of tiny files; query engines pay per-file open/metadata overhead, so scan performance degrades even at modest total row counts. The fix is a scheduled **`OPTIMIZE`/compaction** (Delta `OPTIMIZE`, Iceberg rewrite-data-files) that rewrites the small files into target-sized files (commonly ~128–512 MB) and lets you expire the superseded snapshots. This is part of the Week 11 cost discipline — the before/after file count and bytes-scanned belong in your cost section (Weeks 6, 11).

### Q5 (Week 7)

**Data skew** (a skewed join): a few values of `customer_id` hold most of the rows, so the task handling those keys does most of the work and spills while the others finish. You confirm it in the **Spark UI's SQL/Stages tab** — one straggler task with a far larger shuffle-read and a non-zero spill, while the stage's other tasks are tiny. Two distinct fixes: (1) **broadcast the small side** if it fits under the broadcast threshold, eliminating the shuffle entirely; (2) **salt the skewed key** — append a random suffix to the hot key on both sides and explode the lookup so the load spreads across tasks. (Spark's adaptive query execution skew-join handling is a third, automatic option.) (Week 7.)

### Q6 (Weeks 8–9)

"Exactly-once" into the lakehouse means **every input event is reflected in the output table exactly once, even across failures and replays**. It decomposes into: (1) **at-least-once delivery** — the checkpoint guarantees every offset is eventually consumed, nothing is skipped; and (2) an **idempotent sink** — the per-micro-batch write is an atomic Iceberg/Delta snapshot keyed deterministically, so re-processing the same offsets after a failure overwrites the same snapshot rather than appending. The **proof**: produce N events with known keys, kill the job mid-batch to force a replay, let it recover, and reconcile the windowed aggregate against a ground-truth count of the produced events — equal, with no duplicates and no gaps. The reconciliation is the proof; "it's exactly-once" is only a claim (Weeks 8, 9).

### Q7 (Week 9)

The 25-minute-late event is **dropped** from the streaming aggregate, because it arrived outside the 10-minute watermark — the window for its event time has already been finalized and its state evicted. Dropping it is the correct default because holding window state open indefinitely to wait for arbitrarily-late events would grow state without bound and never let a window close; the watermark is a deliberate **bounded-state-for-correctness trade**. You recover the dropped stragglers in the **nightly batch path**, which reprocesses the full source (including late arrivals) idempotently — so the near-real-time number is fast-and-slightly-incomplete and the batch number is complete-and-authoritative, by design (Week 9).

### Q8 (Week 10)

A warning that only **logs** is not a gate, because it does not *stop the bad data* — the load proceeds, the bad value lands in the mart, and the warning scrolls past unread in a log nobody watches. A **quality gate halts and alerts**: a Great Expectations checkpoint (or a dbt test with `severity: error`) at the layer boundary *fails the task*, which stops the pipeline before the bad data reaches the mart and fires an alert to a channel someone is on call for. The difference is enforcement: a gate that can only log is a smoke detector with no batteries (Week 10).

### Q9 (Week 12)

"The analyst ran it twice; be careful" is a **symptom dressed as a root cause**, and it is neither blameless nor actionable — you cannot ship "be careful" to every analyst who will ever trigger a backfill. The correct framing targets the **system property** that allowed the action to cause harm: "the load was not idempotent — it used an `INSERT` with no high-water mark, so a second run double-counted." The action items then target that: make the load a `MERGE` keyed on the natural key, add the high-water-mark guard, and add a row-count reconciliation that fails when a re-run changes the count — each with an owner and a "done when." Root cause is always a system property, never "human error" (Week 12, Lecture 2).

### Q10 (Week 12)

The correct response: **"I haven't measured that — here's the adjacent thing I do know — and here's exactly how I'd find out."** Concretely: "I haven't tested a leader election under load, but I know my consumer reads with a checkpoint and reconnects from the last committed offset, so the expected behavior is a brief stall and a replay from that offset — at-least-once into an idempotent sink, so no duplication. I'd confirm by killing the partition leader during a run and checking the consumer-lag panel and the row-count reconciliation." It is better than bluffing because a reviewer who runs data platforms detects a bluff in one follow-up, and a caught bluff makes every *other* answer suspect; the honest answer demonstrates the actual senior skill — knowing the limits of your knowledge and having a method to extend them (Week 12, Lecture 3 §3).

---

## Where to go from here

You have finished C27. The capstone is the artifact; the portfolio (the public repo with the README, architecture diagram, and data-quality report; the merged PR; the incident blog post; the landing page) is how the world sees it; the interview loop is how it becomes a job. Keep the merged-PR habit — the second contribution is far easier than the first, and a contribution history compounds across a career. Re-run the chaos drills on the next platform you build, and write the postmortems before anyone asks. And the next time a dashboard number is wrong — it will be — trace it through lineage, prove where it came from, and add it to the incident-story repertoire. That is the job you trained for. Go own a platform.

(End of quiz. End of C27.)
