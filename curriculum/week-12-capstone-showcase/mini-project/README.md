# Capstone Build-and-Defense Brief — End-to-End Lakehouse + Streaming Pipeline

> **Time:** ~14 hours across the week (the bulk of Week 12). **Prerequisites:** Weeks 1–11 complete; the pipeline built incrementally since Phase II; this week's Lectures 1–3 and both challenges; one chaos drill run on yourself before the demo. **Citations:** the C27 SYLLABUS capstone spec, deliverables, and grading axes; the Google SRE postmortem chapter at <https://sre.google/sre-book/postmortem-culture/>; the tool docs (Airflow, dbt, Spark, Kafka, Iceberg/Delta, MinIO, Great Expectations) you cite for each axis. **This is the C27 capstone.**

This is the full capstone build-and-defense brief — the most important document in C27. It tells you exactly what you bring to the defense, in what order you demo it, what each grading axis looks for, and how the defense session runs. **Read it first** this week, before the lectures. The capstone is 30% of your C27 grade, and per the SYLLABUS passing bar, **a pipeline that loses or double-counts data does not pass regardless of every other score** ("a weak quiz week is forgivable; a pipeline that loses or double-counts data is not").

## What the capstone is

**One substantial system — the SYLLABUS end-to-end lakehouse + streaming pipeline — not several disconnected demos.** It has been in flight since Phase II and operated since Week 9. Everything runs locally in Docker; no cloud account is required. The product domain is open (retail-sales analytics, a clickstream / product-analytics pipeline, an IoT-telemetry lakehouse, a financial-transactions warehouse, or any domain you can defend in scope review) — the technical bar is fixed. The architecture is the SYLLABUS diagram:

```text
   +------------------+        +---------------------+
   |  Batch source    |        |  Event source       |
   |  (daily file /   |        |  (Kafka clickstream |
   |   CDC extract)   |        |   producer)         |
   +---------+--------+        +----------+----------+
             |                            |
       Airflow DAG                  Kafka topic
       (idempotent,                 (keyed, partitioned,
        watermarked,                 schema-registered)
        backfillable)                      |
             |                             v
             |                 +-----------+-----------+
             |                 |  Spark Structured     |
             |                 |  Streaming job        |
             |                 |  (event-time          |
             |                 |   watermark, windows, |
             |                 |   exactly-once sink)  |
             v                 +-----------+-----------+
   +---------+--------------------------+  |
   |   Lakehouse on MinIO (object       |<-+
   |   storage): Iceberg / Delta tables |
   |   raw -> staged -> mart            |
   +---------+--------------------------+
             |
        dbt transformation
        (staging / intermediate / mart,
         tests, snapshots, docs, lineage)
             |
   +---------+--------+        +------------------------+
   | Great Expectations|       |  DuckDB / Spark query  |
   | + dbt quality     |       |  layer                 |
   | GATES at every    |       +-----------+------------+
   | boundary          |                   |
   +-------------------+                   v
                                 +---------+---------+
                                 |  Dashboard        |
                                 | (a trusted, tested|
                                 |  metric an analyst|
                                 |  would rely on)   |
                                 +-------------------+
```

Every box is a week of C27: the model (Week 1), the SQL layer (Week 2), the idempotent load (Week 3), the Airflow DAG (Week 4), dbt (Week 5), the lakehouse (Week 6), Spark (Week 7), Kafka and the schema registry (Week 8), the streaming aggregate (Week 9), the quality gates (Week 10), and lineage/cost/governance (Week 11). The capstone is the integration that proves you can do the whole platform, not just its parts. Week 12 builds nothing new — it **finishes, packages, demos, and defends** it.

## The nine required deliverables

All nine must be present and mutually consistent — the architecture doc must describe the system you actually demo:

1. **Architecture document (~8–10 pages).** Covers the data model (grain, dimensions, facts), the storage layout (partitioning, table format), the orchestration design, the streaming semantics (watermark, windowing, delivery guarantee), the quality gates, and the lineage map. It is the document a hiring manager opens second (after the README). It must reflect the system as built, including its limits (what the gates do *not* catch, where the lab-scale simplification diverges from production — Lecture 3, §2.4).
2. **A working batch path.** An idempotent, watermarked, backfillable Airflow DAG that lands a batch source into the lakehouse and transforms it with dbt into a tested dimensional mart (Weeks 3, 4, 5).
3. **A working streaming path.** A Spark Structured Streaming job consuming a Kafka topic with an event-time watermark and an exactly-once sink into the lakehouse (Weeks 8, 9).
4. **Data-quality gates.** Great Expectations suites and dbt tests at every layer boundary, wired so a bad load **halts and alerts** rather than landing silently (Week 10). A gate that only logs is not a gate.
5. **End-to-end lineage.** Source-to-dashboard lineage exposed (dbt docs DAG + lineage graph), with at least one PII column classified and masked (Week 11).
6. **A runnable system.** `docker compose up` brings up the whole platform (Postgres, MinIO, Airflow, Kafka, Spark) so a reviewer can run it during the demo. No cloud account required.
7. **A 5-minute demo video.** Voice-over required, no marketing edits, showing batch + stream → lakehouse → dbt → streaming aggregate → dashboard, and one quality gate firing on bad data (Lecture 1, §6).
8. **A data-quality report.** The generated Great Expectations Data Docs for the final run, plus row-count and freshness evidence (Week 10).
9. **A chaos-drill postmortem (~3–5 pages).** One drill you ran on yourself (Challenge 1 / Lecture 2): malformed batch load, stream partition lag spike, or schema-evolution event. Document the failure, detection, recovery, data impact, and action items.

## The six grading axes

| Axis | Weight | What it looks for |
|------|-------:|-------------------|
| **Data modeling** | 15% | Correct, defended grain; Type-1 vs Type-2 SCD chosen for a reason; conformed dimensions. The "did you design the model or accrete it" axis (Week 1). |
| **System correctness** | 25% | Does it actually work end to end — batch *and* stream — with **no data loss and no double-counting**. The largest single axis, and the gate on passing: a defect here fails the capstone (Weeks 3, 6, 9). |
| **Lakehouse & transformation quality** | 20% | Storage layout (partitioning, table format), dbt project structure (staging / intermediate / mart), tests and snapshots. The "could another engineer extend this" axis (Weeks 5, 6). |
| **Streaming semantics** | 15% | Event-time watermark correct, windowing chosen for the question, exactly-once *proven* not claimed (Week 9). |
| **Data quality, lineage & cost** | 15% | Gates that halt at every boundary, the lineage map, partition pruning and compaction (Weeks 10, 11). |
| **Communication** | 10% | The architecture doc, the 5-minute video, the postmortem. Can you explain it as well as you built it. |

Note the weighting: *system correctness (25%) + lakehouse & transformation quality (20%)* together are 45% — the capstone rewards a platform that works and can be extended far more than a clever-but-fragile one. And system correctness is a **gate**: per the SYLLABUS, no data-loss or double-counting defect may exist in the final run.

## The demo order (the live portion, ~10 minutes)

Follow the Lecture 1 four-part structure, pre-staging the slow parts:

1. **Architecture overview (60 s).** The diagram. Narrate the data flow end to end. Give the reviewer the map.
2. **Live happy-path (~4 min).** Batch: the daily file is in the landing zone; trigger the DAG (or show it at the last task) → the task succeeds → dbt builds the mart → the dashboard metric updates. Say the contract out loud: "this is idempotent — I can re-run it and the number does not change." Stream: the Kafka producer is emitting → the Structured Streaming windowed aggregate updates the dashboard's near-real-time panel. Point at the watermark and name what it drops on purpose.
3. **One deliberate failure + recovery (~3 min).** Drop a deliberately malformed file → the Great Expectations checkpoint fails → the Airflow task goes red → the alert fires → and critically, **the mart's number does not change** because the gate halted the load before it reached the mart. Then recover: quarantine the file, fix it, re-run, the gate passes, the mart updates. A bad load *caught and recovered* beats every panel green.
4. **The artifact for the unshowable (~2 min).** The data-quality report (open the page for the run where the gate fired), the before/after bytes-scanned from your compaction (Week 11), and the lineage graph tracing the dashboard number back to its source. These prove the parts you cannot show live — exactly-once, cost, lineage — are real.

Then the **20-minute Q&A** (Lecture 3): the panel probes modeling, data-loss boundaries, idempotency, exactly-once, cost, and lineage. Answer honestly; narrate your reasoning; when you hit the edge of what you know, say "I don't know, here's what I do know, here's how I'd find out."

## The defense session (~30 minutes total)

1. **Setup (before the clock).** The platform up from a *cold* `docker compose up` verified in the dress rehearsal; the lakehouse seeded with history; the Kafka topic warm and the consumer caught up; the dashboard reachable; the malformed file pre-tested against the checkpoint so you know it trips the gate. Do this *before* the panel is watching.
2. **Demo (~10 min).** The four-part structure above. Recorded-with-live-fallback for the flake-prone parts — the ten-minute load, the streaming warm-up (Lecture 1, §1, §4).
3. **Q&A (~20 min, may overlap).** The architecture defense. Have the SOLUTIONS model answers internalized (not memorized — understood; a memorized answer dies to the first follow-up).
4. **Wrap.** The reviewer notes which axes passed and where the soft spots were. You submit the portfolio (repo + architecture doc + video + data-quality report + postmortem) and any open items.

## Pass criteria

- All nine deliverables present and mutually consistent.
- The system works end to end — batch *and* stream — in the live demo (or the recorded demo with the live system demonstrably available).
- **No data-loss or double-counting defect in the final run** (the SYLLABUS gate — this fails the capstone regardless of other scores).
- The quality gate halts a bad load and alerts, demonstrated deliberately in the failure portion.
- Exactly-once is *proven* (offsets + idempotent sink + a row-count check against ground truth), not merely claimed.
- The chaos-drill postmortem is blameless, factual, and actionable (Lecture 2), and proves no data was lost or double-counted in the drill.
- The Q&A demonstrates integration across weeks and honesty at the edges (no caught bluffs).

## Common defense-day failures (and the pre-empt)

1. **A cold `docker compose up` raced a dependency.** The dbt job ran before MinIO was ready; the streaming job started before the topic existed. *Pre-empt:* `depends_on` + healthchecks; dress-rehearse a cold start at least once and fix every ordering bug it reveals (Lecture 1, §4).
2. **The full batch load created ten minutes of dead air.** *Pre-empt:* pre-stage the load; demo the last task and the mart update (the interesting 15 seconds), or narrate the architecture while it runs (Lecture 1, §1).
3. **The streaming consumer had not caught up / the dashboard cached the old number.** *Pre-empt:* warm the topic and let the consumer reach steady state before the panel arrives; know the dashboard hard-refresh, or query the mart directly to show the true value (Lecture 1, §5).
4. **The "malformed" file wasn't malformed enough and the gate passed.** *Pre-empt:* test the bad file against the checkpoint *before* the demo so you know it trips the gate (Lecture 1, §5).
5. **A question hit a soft spot and you bluffed.** Don't. "I don't know, here's how I'd find out" scores higher than a caught wrong-confident answer (Lecture 3, §3).
6. **The architecture doc described a pipeline you no longer have.** Feature-freeze at the start of the week and reconcile the doc to the demo. Inconsistency between the doc and the live system is the single most damaging defense finding.

## The week's schedule for finishing

- **Monday** — feature-freeze; finish the architecture doc; structure the demo; start Challenge 2 (review cycles are slow).
- **Tuesday** — run the chaos drill (Challenge 1); write the postmortem; generate the final data-quality report.
- **Wednesday** — map the capstone to the Q&A; rehearse answers against SOLUTIONS Part A.
- **Thursday** — the mock interview loop (homework); polish the portfolio; the merged-PR (Challenge 2).
- **Friday** — full dress rehearsal from a *cold* `docker compose up`, end to end; record the 5-minute video after the rehearsal shakes out the bugs.
- **Saturday** — the defense.
- **Sunday** — final review quiz; submit the portfolio; push the merged PR if not yet done.

## References

- SYLLABUS, "Capstone" — the deliverables, the architecture diagram, the chaos-drill menu, and the grading axes (this brief is the operational expansion of that section).
- All three Week 12 lecture notes.
- Weeks 9, 10, 11 — the streaming path, the quality gates, and the lineage/cost discipline that feed this capstone.
- Weeks 1, 3, 5, 6, 8 — the model, idempotency, dbt, the lakehouse, and Kafka that the deliverables draw on.
- Google SRE Book, "Postmortem Culture". <https://sre.google/sre-book/postmortem-culture/>
