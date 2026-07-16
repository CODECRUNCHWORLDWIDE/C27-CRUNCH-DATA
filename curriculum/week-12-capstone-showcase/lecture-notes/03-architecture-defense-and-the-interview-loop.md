# Lecture 3 — Architecture Defense and the Data-Engineering Interview Loop

> *The capstone defense and a senior data-engineering interview loop are the same exam wearing two hats. In both, someone who knows more than you about at least one layer sits across from you, looks at the platform you built or claim to understand, and pushes on it until they find the edge of your understanding — not to humiliate you, but because the edge of your understanding is the only interesting place in the conversation. The candidate who passes is not the one with no edges; everyone has edges. It is the one who, when pushed past the edge, says "I don't know, but here is exactly how I would find out," and means it. This lecture is about that exam: the taxonomy of questions a reviewer panel asks, the eight rounds of a real senior data-engineering loop and which part of your capstone answers each, and how to package the whole thing into a portfolio a hiring manager actually opens.*

## 1. The defense is an integration exam

The phase gates tested whether you could do a thing in isolation — land a batch pipeline (Week 4), stand up a lakehouse (Week 6), build a streaming aggregate (Week 9). The capstone defense tests something different: whether you can *own a data platform*, which is a breadth-first, integration skill. The 20-minute Q&A is designed so that no single week of C27 answers it; each question reaches across two or three. "Show me exactly where a row could be lost between Kafka and the lakehouse" is Week 8 (offsets and delivery semantics) + Week 9 (exactly-once and checkpointing) + Week 6 (the idempotent sink). "How does a re-run not double-count" is Week 3 (idempotency and the high-water mark) + Week 4 (the backfillable DAG) + Week 5 (the dbt incremental's `MERGE`). The defense rewards the candidate who has connected the weeks into a platform, not the one who memorized each in isolation.

## 2. The question taxonomy

Reviewer questions fall into six families. Anticipate one or two from each.

### 2.1 Modeling questions

"What is the grain of your fact table, and why?" "Is this dimension Type 1 or Type 2, and what breaks if you got it wrong?" "Where are your conformed dimensions?" These test whether your model was *designed* or *accreted*. The strong answer names the grain first — "one row per order line, because the business questions are about line-level discounts and the line is the lowest grain a question reaches" — and then defends the SCD choice against a concrete failure: "the customer dimension is Type 2 because we report revenue by the customer's segment *as it was at the time of the order*; if it were Type 1 we'd retroactively re-segment historical revenue every time a customer changed tier, and last quarter's numbers would silently move" (Week 1). "Grain" is the most important word in the answer, exactly as it was the most important word in the course.

### 2.2 Data-loss and data-duplication questions

"Where does a 'message produced' stop meaning 'row stored'?" "If MinIO is unavailable for an hour, what do you lose?" "Prove this isn't double-counting." These test whether you understand your own pipeline's guarantees. The strong answer traces the boundary precisely: "Kafka acks a produce on commit to the partition log, so the producer's 'sent' means 'the broker durably has it,' not 'it's in the lakehouse.' Spark Structured Streaming reads with a checkpoint that commits the consumed offsets *and* the output atomically per micro-batch; the sink is an Iceberg table whose write is a single atomic snapshot commit. So a failure mid-batch re-reads from the last committed offset and re-writes the same snapshot — at-least-once delivery plus an idempotent sink equals exactly-once *into the table*. If MinIO is down, the streaming job's checkpoint commit fails, the batch does not advance, and on recovery it replays from the last good offset — no loss, no duplication, just lag" (Weeks 8, 9, 6).

### 2.3 Idempotency and incrementality questions

"You ran the backfill twice. Walk me through why the mart didn't double." "How does your incremental load handle a late record that arrives after the watermark?" These are your Week 3 contract. The strong answer is the mechanism, not the assurance: "The load is keyed on a natural key and written with a `MERGE` (dbt incremental, `unique_key` set), not an `INSERT`, so a second run upserts the same rows to the same values — re-running is a no-op on the row count. The watermark is the high-water mark from Week 3: I only re-process partitions at or after `max(loaded_at) - lateness_grace`, so a re-run reprocesses a bounded recent window idempotently rather than the whole history" (Weeks 3, 5).

### 2.4 Streaming-semantics questions

"Prove your stream is exactly-once and not just at-least-once." "What does your watermark drop, and is that correct?" "What happens to a record that arrives two hours late?" These test whether you understand the trade you chose. The strong answer is honest about what is dropped on purpose: "Exactly-once is at-least-once plus an idempotent sink — I don't get magic, I get a checkpoint that commits offsets and output atomically into an Iceberg snapshot, so a replay overwrites rather than appends. The watermark is 10 minutes: a click that arrives within 10 minutes of its event time still lands in its window; one that arrives later is dropped, on purpose, because holding window state open indefinitely would grow unboundedly. That is a correctness-for-bounded-state trade, and the late-arriving stragglers go to a side output I reconcile in the nightly batch" (Week 9).

### 2.5 Storage and cost questions

"How many bytes does this dashboard query scan, and how would you cut it?" "Why is your table partitioned the way it is?" "What's the small-files problem and do you have it?" These test storage literacy. The strong answer names the mechanism: "I partition by event date because the dashboard's queries are date-ranged, so predicate pushdown prunes to a few days of files instead of scanning the table — I measured 14 GB scanned before partitioning, 380 MB after. Streaming into the lakehouse creates the small-files problem — one file per micro-batch — so I run a scheduled `OPTIMIZE`/compaction (Week 11) that rewrites them into target-sized files; the before/after file count and bytes-scanned is in my cost section" (Weeks 6, 11).

### 2.6 Governance questions

"A number on the dashboard is wrong. Trace it back to the source." "A user invokes their right to be deleted, but their data is in an immutable Parquet file. How?" These test whether you can operate under an incident and a regulation. The strong answer reaches for lineage first: "Lineage is the first thing I open in an incident — the dbt docs DAG (or Marquez) traces the dashboard metric back through the mart, the staging model, to the raw table and the source file, so I can find *which* load introduced the wrong number rather than guessing. For the deletion: the table format makes it tractable — Iceberg/Delta rewrite the affected data files on a `DELETE`, producing a new snapshot without the row, and I expire the old snapshots past the retention window so the data is genuinely gone, not just hidden" (Week 11).

## 3. The "I don't know" answer

Every defense reaches a question past your edge. The only wrong response is to bluff — a reviewer who runs data platforms can smell a bluff instantly, and a caught bluff costs you more than ten "I don't know"s, because it makes every *other* answer suspect. The correct senior answer has three parts: "I don't know — here is what I *do* know that's adjacent — and here is exactly how I would find out." Example: "I haven't measured the exact shuffle spill on that join, but I know it's a sort-merge join because the build side is above the broadcast threshold, and I'd confirm by reading the Spark UI's SQL tab for the exchange and the spill metrics, then decide whether to bump the broadcast threshold or salt the skewed key." That answer demonstrates more competence than a confident wrong number.

## 4. The eight rounds, mapped to your capstone

A senior data-engineering loop runs roughly eight rounds. Each has an answer in your capstone — which is the whole point of having built it.

| Round | What it probes | The capstone answer | Weeks |
|-------|----------------|---------------------|-------|
| **Data-modeling whiteboard** | grain, SCD types, conformed dimensions, from business questions backward | Your star schema and the Type-2 customer dimension; the grain you defended in scope review | 1 |
| **SQL tuning** | window functions, anti-joins, reading a query plan, "why is this slow" | Your analytical queries and the `EXPLAIN` plan you read to add the partition filter | 2 |
| **Idempotency & incrementality** | a load that survives a re-run; watermarking; late data | Your `MERGE`-based incremental loader and the high-water-mark backfill | 3 |
| **Orchestration** | DAG design, retries, backfills, idempotent tasks | Your Airflow DAG — idempotent, watermarked, backfillable — and the alert on a gate failure | 4 |
| **Storage internals** | Parquet row groups, predicate pushdown, what Iceberg/Delta add, small files | Your lakehouse layout, the partitioning, and the compaction job | 6 |
| **Distributed compute** | shuffles, skew, join strategy, reading the Spark UI | Your Spark job and the skewed join you fixed (salting / broadcast) | 7 |
| **Streaming** | event vs processing time, watermarks, exactly-once, the idempotent-sink pattern | Your Structured Streaming job, its watermark, and its exactly-once Iceberg sink | 9 |
| **The incident story** | a real incident, told on the spot, with detection and recovery | Your chaos-drill postmortem — the malformed file, the lag spike, or the schema break | 12 |

The preparation move: for each round, have the *specific artifact* ready — the model diagram, the query plan, the DAG, the Spark UI screenshot, the postmortem — and a 90-second story. "Tell me about an incident" should not make you reach; you have the chaos-drill postmortem, documented, with the row-count proof that no data was lost.

## 5. Thinking out loud

In both the defense and the interview, the reviewer is grading your *process*, not just your answer — which means a silent five minutes of correct thinking scores worse than a narrated five minutes of slightly-wrong-then-corrected thinking. Narrate: "Okay, the dashboard query filters on `order_date` between two dates and groups by region — so the first thing I want is partition pruning on `order_date`; if the table's partitioned by date this query touches a handful of files. Then the group-by is a shuffle on region — wait, region is low-cardinality, so that shuffle is cheap and probably fine; the cost is the scan, not the aggregate. So I'd check the plan for a partition filter being pushed down, and if it isn't — maybe the date column is a string, not a date, and pushdown failed — that's where I'd look first." The reviewer sees you reason, form a hypothesis, and self-correct — which is exactly the skill they are hiring for.

## 6. Portfolio packaging

The capstone becomes a credential only if a hiring manager opens it. Four pieces, in priority order:

1. **The public GPL-3.0 capstone repo with a real README, an architecture diagram, and the data-quality report.** The README is opened first; it must, in its first screen, say what the platform is, show the architecture diagram, link the 5-minute video, and link the data-quality report. The architecture doc is opened second; it is the ~8–10-page document with the data model, the storage layout, the streaming semantics, the quality gates, and the lineage map. A repo with a one-line README is invisible to a hiring manager.
2. **One merged PR into an open-source data project** (dbt, Airflow, Dagster, DuckDB, Iceberg, Delta, Great Expectations, or a Spark/Kafka client). This is Challenge 2 this week. A merged PR proves you can read an unfamiliar codebase, work to its conventions, pass its CI, and get a maintainer to accept your change — the single most predictive portfolio signal for "can this person contribute on day one."
3. **A short technical blog post explaining one bug** from your chaos drill — a late record that wasn't counted, a skewed join that spilled, a schema-evolution surprise. Specific, with the artifact (the lag panel, the Spark UI, the rejected schema), with the fix. One good incident post outperforms ten "intro to dbt" posts.
4. **A landing page that links all of the above** and does not contain the phrase "data-driven." It says, plainly, what you build and links to the proof. The SYLLABUS is explicit about the "data-driven" prohibition; it is a real signal — the phrase marks a portfolio written for a recruiter's keyword filter rather than for an engineer who will read the code.

## 7. The through-line: owning the platform

Everything in this lecture reduces to one claim the defense is built to test: a data engineer owns the artifact end to end. The architecture doc shows you designed it. The data-quality report shows you gated it. The lineage map shows you can trace any number to its source. The cost section shows you measured it. The postmortem shows you own its failures. The defense Q&A shows you understand every layer. The portfolio shows you can hand it to someone else. That is ownership, and it is the thing the title "senior" actually denotes — not years, not lines of SQL, but the demonstrated ability to take a platform from business question to deployed-gated-and-defended and stand behind every number.

## 8. Summary

The defense is an integration exam; questions reach across weeks. The taxonomy: modeling (name the grain), data-loss/duplication (trace the boundary), idempotency (name the mechanism, not the assurance), streaming semantics (be honest about what the watermark drops on purpose), storage and cost (name the bytes scanned and the fix), and governance (reach for lineage first). The only wrong answer past your edge is a bluff; the right one is "I don't know, here's what I do know, here's how I'd find out." The eight interview rounds each map to a specific capstone artifact — have the file, the plan, the screenshot, and the 90-second story ready for each. Think out loud; the process is graded, not just the answer. Package the portfolio in priority order: the public repo with a real README, architecture diagram, and data-quality report; one merged open-source PR; one technical incident post; and a "data-driven"-free landing page. The through-line is ownership — business question to deployed-and-defended, standing behind every number.

That is the end of the C27 lecture sequence. What remains is the work: finish the pipeline, run the drill, write the postmortem, rehearse the cold-start demo, defend it, and ship the portfolio. The mini-project brief is your checklist.

## References for this lecture

- SYLLABUS, "Career engineering pack" — the interview-prep topics, the runbook contents, and the portfolio recommendations.
- Joe Reis and Matt Housley, *Fundamentals of Data Engineering*, O'Reilly, 2022. ISBN 978-1-098-10830-4. — the lifecycle the rounds probe. <https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/>
- "Ace the Data Science Interview" (Nick Singh, Kevin Huo) — for the SQL and modeling round structure, adapted to data engineering.
- DataExpert.io / "Awesome Data Engineering" question banks — community-maintained, to broaden your prep beyond the SYLLABUS rounds.
- The "I don't know, here's how I'd find out" norm — Google SRE Book, on the culture of honesty under questioning. <https://sre.google/sre-book/table-of-contents/>
