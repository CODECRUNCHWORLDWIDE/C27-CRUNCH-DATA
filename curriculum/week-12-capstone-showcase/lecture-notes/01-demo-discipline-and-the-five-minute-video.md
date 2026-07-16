# Lecture 1 — Demo Discipline and the Five-Minute Video

> *A capstone defense is not a code review and it is not a slide deck. It is a demonstration, under time pressure, in front of people paid to find the soft spot, that a system you built actually works — and a data pipeline is the cruelest possible thing to demo live, because its slowest, flakiest moments are exactly the ones you most want to show. The full batch load takes minutes. The streaming consumer needs to warm up and catch up. The dbt build reads state you might have left dirty. The dashboard caches the number you just changed. This lecture is about the discipline that turns all of that from a liability into a fifteen-minute performance you control: what you pre-stage, what you show live, what you show from a recording, and the one moment — a quality gate firing on bad data — that is worth more than every green panel combined.*

## 1. Why demo discipline is a graded skill

The SYLLABUS makes the demo a deliverable in two forms: the live defense (Saturday) and the 5-minute video (a separate, tighter artifact, voice-over required, no marketing edits). Both are graded under the *communication* axis (10%), but they also gate the others — a reviewer cannot award the *system correctness* axis (25%) for a pipeline they could not see work. A capstone that runs perfectly but demos badly leaves points on the table that a slightly weaker pipeline, demoed with discipline, will take.

The deeper reason is that demoing is the job. A data engineer ships a pipeline and then has to convince an analyst the number is trustworthy, a manager the migration is safe, a reviewer the design is sound. The capstone defense is a rehearsal of that motion. The reviewer is not asking "is your code perfect" — they are asking "can you stand in front of a system you own and explain why I should trust it."

## 2. The three laws of demoing a data pipeline

### Law 1 — Never demo live what you can demo from a recording with a live fallback

A data pipeline's worst demo moments are its slowest ones: a ten-minute batch load, a streaming job warming up, a Spark stage that shuffles. None of that is interesting to watch, and all of it is where the demo dies (the load picks up yesterday's file; the consumer has not caught up; a container OOMs). The fix: **pre-stage the slow parts and show the interesting transition.** Run the full batch load *before* the panel is watching, leave the DAG at the point just before the final task, and demo the *last task succeeding and the mart updating* — the fifteen interesting seconds, not the ten dead minutes. Have a screen recording of the full run ready as the fallback if the live system stalls.

This is not cheating — it is the same thing every production demo does. The reviewer knows the load takes ten minutes; they do not want to watch it. What they want to see is that the transition works and that you understand what happened during the part you skipped.

### Law 2 — Every demo has a failure, so script the recovery

Something will go wrong. A container will be slow to start, a token will expire, a port will be taken, the dashboard will show a cached number. The amateur freezes; the professional has a scripted recovery for each likely failure. Before the demo, list the five things most likely to break (see §5) and write the one-line recovery for each — "if the dashboard shows the old number, it cached; here is the hard-refresh / here is the recording." A recovery you rehearsed looks like competence; the same failure unrehearsed looks like a broken project.

### Law 3 — A quality gate firing beats every panel green

This is the law specific to a *data* capstone and the most important one. The instinct is to show everything working: every dashboard green, every row present, every model built. But a pipeline of green panels proves only that the happy path works — it does not prove the system protects the number when the input is bad, which is the entire point of the quality layer you built in Week 10. The strongest single moment in your demo is a **malformed file landing, the Great Expectations checkpoint catching it, the Airflow task failing red, the alert firing, and the mart staying clean** — because that is the failure data engineers are paid to prevent, demonstrated being prevented. Build the demo around the system's *resilience*, not its perfection. A bad load that halts cleanly is a better result than a clean load that happens to pass.

## 3. The four-part demo structure

Follow this structure in the live defense (the ~10-minute live portion). It mirrors the SYLLABUS architecture diagram and gives the reviewer a map before the details.

### Part 1 — Architecture overview (60 seconds)

Put the architecture diagram on screen and narrate the data flow once, end to end: "A daily file and a Kafka clickstream both feed an Iceberg lakehouse on MinIO. Airflow orchestrates the batch path — idempotent, watermarked, backfillable. dbt transforms raw → staged → mart with tests and snapshots. A Spark Structured Streaming job consumes the Kafka topic with an event-time watermark and an exactly-once sink. Great Expectations and dbt tests gate every boundary. The mart feeds a dashboard." Sixty seconds, no detail — the map, so the reviewer knows where each later piece sits.

### Part 2 — Live happy-path (~4 minutes)

Two flows, pre-staged to the interesting transition:

- **Batch:** the new daily file is in the landing zone; trigger the DAG (or show it at the last task); watch the task succeed, dbt build the mart, and the dashboard's metric update. Say the contract out loud: "this is idempotent — I can re-run it and the number does not change."
- **Stream:** the Kafka producer is emitting the clickstream; show the Spark Structured Streaming job's windowed aggregate updating the dashboard's near-real-time panel. Point at the watermark: "late events inside the watermark are still counted; later ones are dropped, on purpose, and that is the trade I chose."

### Part 3 — One deliberate failure + recovery (~3 minutes)

This is Law 3 made concrete. Drop a deliberately malformed file (a column renamed, a value out of range) into the landing zone and trigger the load. Show the Great Expectations checkpoint failing, the Airflow task going red, the alert firing, and — critically — the mart's number *not changing*, because the gate halted the load before it reached the mart. Then show the recovery: quarantine the bad file, fix or replace it, re-run, the gate passes, the mart updates. A reviewer who sees a bad load *caught and recovered* has seen the whole quality discipline of the course in three minutes.

### Part 4 — The artifact for the unshowable (~2 minutes)

Some things cannot be shown live: the exactly-once guarantee (you cannot watch a row not-double-count), the bytes-scanned cost reduction, the end-to-end lineage. Show the artifacts instead: the data-quality report (the Great Expectations Data Docs, with the gate-that-fired page open), the before/after bytes-scanned numbers from your Week 11 compaction, and the lineage graph (dbt docs DAG or Marquez) tracing the dashboard number back to its source. These are the proof that the parts you cannot demo live are real.

Then the **20-minute Q&A** (Lecture 3): the panel probes modeling, data-loss boundaries, idempotency, exactly-once, cost, and lineage. Answer honestly; narrate your reasoning; when you hit the edge of what you know, say "I don't know — here's what I do know — here's how I'd find out."

## 4. Recorded-with-live-fallback

The strongest demo posture is a **recording of the full end-to-end run, with the live system available as proof.** You play the recording (which is tight, edited for time, and never flakes), and the live system sits beside it so a reviewer who says "show me that for real" can watch you do it. This gives you the reliability of a recording and the credibility of a live system. The 5-minute video (§6) is the polished version of this recording; the live defense is where you prove it was not faked.

Pre-stage everything the recording assumes: the lakehouse seeded with history, the Kafka topic warm, the dashboard reachable, the env vars set, the volumes mounted. The single most common defense-day failure is a cold `docker compose up` that races a dependency (the dbt job runs before MinIO is ready; the streaming job starts before the topic exists). The dress rehearsal (Friday) exists to shake exactly these out — run the whole thing from a cold start at least once before the defense, and fix every ordering bug `depends_on` / a healthcheck reveals.

## 5. Demo failure modes and the pre-empt for each

| Failure mode | Why it happens | The pre-empt |
|---|---|---|
| The batch load takes ten minutes of dead air | The full load is slow; the demo waits on it | Pre-stage the load; demo the last task + the mart update (Law 1) |
| The streaming consumer has not caught up | Cold start; the consumer is replaying from the earliest offset | Warm the topic and let the consumer catch up before the panel arrives; show steady-state |
| The dashboard shows a stale/cached number | The BI tool cached the pre-update value | Know the hard-refresh; or query the mart directly (`duckdb`/`psql`) to show the true value |
| `docker compose up` races a dependency | A service starts before MinIO/Kafka/Postgres is ready | `depends_on` + healthchecks; dress-rehearse a cold start (§4) |
| dbt picks up dirty state | A prior partial run left a stale `target/` or incremental state | Start from a clean checkout; `dbt clean`; rebuild from a known seed in the rehearsal |
| The quality-gate demo passes instead of failing | The "malformed" file was not actually malformed enough | Test the bad file against the checkpoint *before* the demo so you know it trips the gate |
| A port is taken / a container OOMs | Local environment drift; 8 GB RAM and the Spark week is tight | Free the ports; use the trimmed Compose profile; have the recording as fallback |

The pattern: list your likely failures, write the recovery for each, and rehearse the ones most likely to fire. A demo with a pre-empt for each failure mode does not have a "what if it breaks" — it has a "when it breaks, here's the recovery."

## 6. The 5-minute video

The SYLLABUS requires a separate 5-minute demo video: voice-over required, no marketing edits, showing batch + stream → lakehouse → dbt → streaming aggregate → dashboard, and one quality gate firing on bad data. It is a *tighter* artifact than the live defense — five minutes, no Q&A, no dead air — and it is the thing a hiring manager actually watches.

- **Voice-over, not music.** Narrate what is happening and why, in the language of trade-offs: "I partition by event date so the dashboard's date-range query prunes to a few files instead of scanning the table." A reviewer hearing you reason is worth more than any visual.
- **No marketing edits.** No logo splash, no transitions, no stock footage. The SYLLABUS is explicit. The video shows the system, not a brand.
- **Hit the five beats:** (1) the architecture in 30 seconds; (2) the batch path landing and the mart updating; (3) the streaming aggregate updating; (4) the dashboard with the trusted number; (5) a malformed file tripping the gate and the mart staying clean. That last beat is the one that makes the video memorable.
- **Edit for time, not for polish.** Cut the dead air (the ten-minute load), keep the transitions. Five minutes is short; every second earns its place.

Record the video *after* the dress rehearsal, because the rehearsal shakes out the bugs and the rehearsal's clean run is the one you narrate.

## 7. The data-quality report as a demo asset

You generated a data-quality report in Week 10 (the Great Expectations Data Docs plus row-count and freshness evidence). In the demo it does double duty: it is a required deliverable, and it is the artifact for Part 4 (the exactly-once / cost / lineage proofs you cannot show live live beside it). The most interesting page of the report is the one for the run where the gate *fired* — open it during Part 3, after the malformed-file demo, so the reviewer sees the same failure both live and in the report. A dated, generated quality report from your own pipeline is among the most credible items in a data portfolio; it is not fakeable and it proves you built the gate, not just talked about it.

## 8. Summary

Demoing a data pipeline is a graded skill and the cruelest thing to do live, because its slowest moments are the ones you most want to show. Three laws: pre-stage the slow parts and demo the interesting transition (recorded-with-live-fallback beats fully-live); script a recovery for every likely failure; and build the demo around resilience — a quality gate firing on bad data is a stronger result than every panel green. The four-part structure: a 60-second architecture overview, the live happy-path (batch + stream), one deliberate failure with a scripted recovery (the malformed-file gate), and the artifacts for the unshowable (the quality report, the bytes-scanned numbers, the lineage graph). The 5-minute video is the tighter, voice-over artifact a hiring manager actually watches — hit the five beats, cut the dead air, record it after the dress rehearsal. Pre-empt the seven failure modes; dress-rehearse a cold `docker compose up`. The demo is the moment the work becomes a credential.

Next lecture: writing the blameless, factual, actionable postmortem of one of the three chaos drills you run on your own pipeline.

## References for this lecture

- SYLLABUS, "Capstone" — the nine deliverables, the architecture diagram, the chaos-drill menu, the 5-minute-video requirement (this lecture operationalizes the communication axis).
- Joe Reis and Matt Housley, *Fundamentals of Data Engineering*, O'Reilly, 2022. ISBN 978-1-098-10830-4. — on the data-engineering lifecycle the demo walks. <https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/>
- The Heilmeier Catechism. <https://www.darpa.mil/about/heilmeier-catechism> — the question set ("What are you trying to do? Why is it hard? What's new?") that structures a clear architecture overview.
- Brett Victor, "Inventing on Principle". <https://www.youtube.com/watch?v=PUv66718DII> — a masterclass in demos that communicate rather than impress.
- Docker Compose `depends_on` / healthcheck reference. <https://docs.docker.com/reference/compose-file/services/#depends_on> — for a cold-start that does not race its dependencies.
- Great Expectations Data Docs. <https://docs.greatexpectations.io/docs/core/configure_project_settings/configure_data_docs/> — the data-quality report you show in Part 4.
