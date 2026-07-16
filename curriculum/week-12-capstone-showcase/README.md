# Week 12 — Capstone Showcase

> *Everything in C27 has been building to one Saturday afternoon: you, a reviewer panel, a laptop running `docker compose up`, and twenty-five minutes to prove that the platform you built actually ingests, lands, transforms, streams, and serves data without losing or double-counting a single row — and that you understand why every layer is the way it is. This week you do not build a new layer. You finish, package, demo, and defend the end-to-end lakehouse + streaming pipeline the SYLLABUS specified, run one chaos drill on it on purpose, and write the postmortem. The phase gates tested whether you could land a batch pipeline and stand up a lakehouse. The capstone tests whether you can own a data platform — model it, build it, gate it, operate it, explain it, and stand behind it when a senior reviewer asks "show me exactly where a row could be lost." That is the actual job of a data engineer, and this is the week you rehearse it.*

Welcome to Week 12 of C27, the final week. You are not learning a new engine, a new format, or a new framework this week. The capstone — the end-to-end lakehouse + streaming pipeline defined in the SYLLABUS — has been in flight since Week 9, and the last three weeks gave it its streaming path (Week 9), its quality gates (Week 10), and its lineage and cost discipline (Week 11). This week you assemble all of it into one runnable system, package the nine required deliverables, demo it, and defend it. The deliverables are fixed by the SYLLABUS; the skills this week adds are the ones that turn a working pipeline into a *credential*: demo discipline, postmortem writing, portfolio packaging, and the data-engineering interview loop.

The capstone is **one substantial system, not several disconnected demos**. The architecture is the SYLLABUS diagram: a batch source (a daily file drop or CDC extract) and an event source (a Kafka clickstream producer) both feeding an Iceberg/Delta lakehouse on MinIO; an idempotent, watermarked, backfillable Airflow DAG orchestrating the batch path; dbt transforming raw → staged → mart with tests, snapshots, and docs; a Spark Structured Streaming job consuming the Kafka topic with an event-time watermark and an exactly-once sink; Great Expectations and dbt tests gating every layer boundary so a bad load halts and alerts; end-to-end lineage exposed; and a dashboard an analyst would trust at 9 AM on a Monday. The nine required deliverables (architecture document, the working batch path, the working streaming path, the quality gates, end-to-end lineage, a `docker compose up`-runnable system, a 5-minute demo video, a data-quality report, and a chaos-drill postmortem) are graded on six axes (data modeling, system correctness, lakehouse & transformation quality, streaming semantics, data quality / lineage / cost, communication). The `mini-project/README.md` in this week's folder is the full capstone build-and-defense brief — it spells out exactly what you bring to the room, in what order you demo it, and what each grading axis looks for. **Read it first.**

**Demo discipline** is the skill that separates a capstone that defends well from one that falls over. The first law: never demo live what you can demo from a recording with a live fallback — and for a data pipeline, the slowest, flakiest part is always the full batch load and the streaming warm-up, so pre-stage them. The second: every demo has a failure, so script the recovery. The third: the most impressive thing you can show is not three green dashboards — it is a *quality gate firing on bad data and halting the pipeline*, because that proves the system protects the number an analyst relies on. A malformed file that lands silently is the failure data engineers are paid to prevent; a malformed file that trips a Great Expectations checkpoint, fails the Airflow task, and alerts — while the mart stays clean — is the single strongest moment in your demo. Lecture 1 walks the structure of a defensible demo and the 5-minute video.

**Postmortem writing** is the skill the SYLLABUS weights as its own capstone deliverable, and it is the one most engineers are worst at, because it requires writing honestly about a failure. A good postmortem is *blameless* (the failure is a system property, not a person's fault), *factual* (a timestamped timeline, not a narrative), and *actionable* (every lesson becomes an action item with an owner and a "done when"). You run one chaos drill on your own pipeline before the demo — a malformed batch load, a stream partition lag spike, or a schema-evolution event — and the postmortem of that drill is what you submit. Lecture 2 covers the structure, the three drills mapped to the failure classes they exercise, and the specific trap of the "human error" non-cause.

**The interview loop** is where the capstone pays off. Lecture 3 walks the actual structure of a senior data-engineering loop — the data-modeling whiteboard (grain, SCD, conformed dimensions), the SQL-tuning round (read this plan, make it fast), the idempotency-and-incrementality round, the storage-internals round (Parquet, predicate pushdown, what Iceberg/Delta add), the distributed-compute round (shuffles, skew, join strategy), the streaming round (event time, watermarks, exactly-once), the cost-and-governance round, and the behavioral "tell me about an incident" — and maps each onto the part of your capstone that answers it. The homework this week is a full **mock interview loop** you run with a peer; the quiz is the final review across all twelve weeks. By Saturday you will have a capstone you would happily send to a hiring manager and the practiced ability to defend every layer of it.

This is the end of C27. You started on a star schema in Postgres in Week 1; you finish operating a platform that ingests from two sources, lands in a lakehouse, transforms with tested code, streams an exactly-once aggregate, gates every boundary, traces every number, and serves a dashboard — and you finish able to defend it to someone paid to find the soft spot. That is the difference between someone who writes pipelines and someone who *owns* a data platform. Finish it owning.

---

## Learning objectives

By the end of this week, you will be able to:

- **Finish and package** the capstone to the SYLLABUS specification. All nine deliverables present and mutually consistent: the ~8–10-page architecture document (data model, storage layout, orchestration, streaming semantics, quality gates, lineage map); a working idempotent, watermarked, backfillable batch path (Airflow → lakehouse → dbt mart); a working Spark Structured Streaming path (Kafka → event-time watermark → exactly-once lakehouse sink); Great Expectations + dbt quality gates at every boundary wired to halt and alert; end-to-end lineage with at least one PII column classified and masked; a `docker compose up`-runnable system; a 5-minute voice-over demo video with no marketing edits; the generated data-quality report; and a 3–5-page chaos-drill postmortem.
- **Structure** a defensible live demo. The four-part structure: a 60-second architecture overview against the diagram; the live happy-path (a batch file lands → the DAG runs → dbt builds → the mart updates → the dashboard reflects it; the Kafka producer emits → the streaming aggregate updates the dashboard); one deliberate failure with a scripted recovery (a malformed file trips the GX gate and the pipeline halts and alerts while the mart stays clean); and the artifact that proves the part you cannot show live (the data-quality report, the bytes-scanned-before/after, the lineage graph). You will understand why a recorded demo with a live fallback beats a fully-live one, and why a quality gate firing is a stronger result than every panel staying green.
- **Write** a blameless, factual, actionable postmortem of one chaos drill (malformed batch load, stream partition lag spike, or schema-evolution event) you run on your own pipeline before the demo. The structure: summary (impact and duration), timeline (timestamped), root cause (the system property, never "human error", following the chain past the symptom), detection, resolution, and lessons as action items with owners and "done-when" criteria.
- **Run** a chaos drill on your own platform. Pick one of the three SYLLABUS drills, execute it against your live system, capture what happened (the failed DAG run, the lag panel, the rejected schema), prove no data was lost or double-counted, and recover. The drill is the source material for the postmortem and one of the grading inputs.
- **Defend** the architecture under a reviewer panel. You will field the 20-minute Q&A: "what is the grain of your fact table and why," "show me exactly where a row could be lost between Kafka and the lakehouse," "how does a re-run not double-count," "prove your stream is exactly-once and not just at-least-once," "trace this dashboard number back to its source," "what does this query cost and how would you cut it." Each question maps to a week of C27; the defense is the integration exam.
- **Map** your capstone onto the senior data-engineering interview loop. Each round has an answer in your capstone: the modeling whiteboard ↔ your star schema and Type-2 SCD (Week 1); SQL tuning ↔ your analytical queries and query plans (Week 2); idempotency ↔ your watermarked incremental loader (Week 3); storage internals ↔ your Parquet/Iceberg layout (Week 6); distributed compute ↔ your Spark job and the skew you fixed (Week 7); streaming ↔ your watermarked exactly-once sink (Week 9); quality ↔ your GX gates (Week 10); cost & governance ↔ your compaction, lineage, and PII handling (Week 11); the incident story ↔ your chaos-drill postmortem (this week).
- **Package** a portfolio a hiring manager will actually open: the public GPL-3.0 capstone repo with a real README, an architecture diagram, and the data-quality report; one merged PR into an open-source data project (dbt, Airflow, Dagster, DuckDB, Iceberg, Delta, Great Expectations, or a Spark/Kafka client); a short technical blog post explaining one bug from your chaos drill; and a landing page that links it all and does not contain the phrase "data-driven."
- **Conduct and survive** a mock senior data-engineering interview. As interviewee: answer the eight rounds under time pressure, think out loud, and recover gracefully from the question you do not know. As interviewer (you also run one for a peer): probe for depth, push past the rehearsed answer, and give specific, kind, actionable feedback.

---

## Prerequisites

You have a working pipeline from Weeks 9–11: the Spark Structured Streaming job (Week 9), the Great Expectations + dbt quality gates (Week 10), and the lineage, compaction, and PII handling (Week 11), all sitting on the Iceberg/Delta-on-MinIO lakehouse (Week 6), the dbt project (Week 5), and the Airflow DAG (Week 4) you built earlier. If any of these is not yet working, **this week is the deadline, not the start** — the defense is at the end of the week, and per the SYLLABUS, "a weak quiz week is forgivable; a pipeline that loses or double-counts data is not." A non-functional capstone does not pass regardless of every other score.

You have run at least one chaos drill on your pipeline, or you run it this week before the demo. The three options are the SYLLABUS set: a malformed batch load (a daily file arrives with a corrupted schema or out-of-range values), a stream partition lag spike (a consumer falls behind and a partition's lag explodes), or a schema-evolution event (a producer adds a field, then makes a breaking change). The postmortem deliverable is about whichever you choose.

You have your data-quality report generating from the final run (the Great Expectations Data Docs HTML plus row-count and freshness evidence — Week 10), and your end-to-end lineage exposed (dbt docs DAG + exposures, OpenLineage/Marquez — Week 11). These are deliverables, not nice-to-haves.

You have read the **SYLLABUS Capstone section** (the architecture diagram, the nine deliverables, the six grading axes, and the chaos-drill menu) and the **career engineering pack** (interview-prep topics, runbook contents, portfolio recommendations). This week operationalizes both; have the SYLLABUS open.

You have skimmed the **Google SRE "Postmortem Culture"** chapter (<https://sre.google/sre-book/postmortem-culture/>) — this week you write the real one against your capstone chaos drill.

---

## Topics covered

- **Demo discipline.** The four-part defensible-demo structure (architecture overview → live happy-path → deliberate failure + scripted recovery → artifact for the unshowable). Why recorded-with-live-fallback beats fully-live for a slow batch + warm-up-heavy streaming pipeline. Why a quality gate firing on bad data is a stronger demo than every dashboard panel green. The 5-minute video as a separate, tighter artifact than the live defense (voice-over required, no marketing edits — the SYLLABUS is explicit). Demo failure modes and how to pre-empt each (the batch load that takes ten minutes, the Kafka consumer that has not caught up, the dbt build that picks up a stale state, the dashboard that cached the old number).
- **Postmortem writing.** The blameless-factual-actionable triad. The structure: summary, timeline (timestamped, neutral), root cause (the system property), detection, resolution, lessons-as-action-items. The "human error" non-cause: "the analyst ran the backfill twice" is never a root cause — "the DAG was not idempotent, so a re-run double-counted" is. The single-root-cause trap (most incidents have a chain). Cite the Google SRE postmortem chapter and the SYLLABUS chaos-drill spec.
- **The three chaos drills.** Malformed batch load (Week 10's quality gate meets Week 3's idempotency: does the gate halt the load before it reaches the mart, and can you prove no bad data leaked downstream?). Stream partition lag spike (Week 8's consumer groups meets Week 9's exactly-once: what happens when a partition's lag explodes, does the rebalance or scaling response hold, and did exactly-once survive — no events lost or double-counted?). Schema-evolution event (Week 8's schema registry meets Week 6's table-format schema evolution: did the registry absorb the compatible change and reject the breaking one, and did downstream dbt models and the dashboard survive?).
- **Architecture defense.** The 20-minute Q&A as an integration exam. The question taxonomy: modeling (grain, SCD correctness, conformed dimensions), data-loss boundaries (where "produced" stops meaning "stored"), idempotency (how a re-run does not double-count), streaming semantics (proving exactly-once, not just claiming it), storage and cost (partition pruning, the bytes a query scans), and governance (lineage during an incident, PII deletion). Each question maps to a specific week; the defense rewards integration over recall.
- **The interview loop, round by round.** Data-modeling whiteboard (Week 1), SQL tuning (Week 2), idempotency & incrementality (Week 3), orchestration (Week 4), storage internals (Week 6), distributed compute (Week 7), streaming (Week 9), and the behavioral incident story (this week's chaos drill). Cite the SYLLABUS career pack.
- **Portfolio packaging.** The public GPL-3.0 capstone repo with a real README and architecture doc; the one merged open-source PR; the one technical blog post about one chaos-drill bug; the landing page that links everything and avoids "data-driven." Why a hiring manager opens the README first and the architecture doc second, and what each must do in the first 30 seconds.
- **The data-quality report as a credible artifact.** The Great Expectations Data Docs for the final run, plus the row-count and freshness evidence — why a generated, dated quality report from *your* pipeline is among the most credible items in a data portfolio, and how it ties to the postmortem (the gate that fired is the report's most interesting page).
- **Owning a platform end to end.** The through-line of the whole defense: a data engineer does not just write transformations, they own the artifact from ingestion to the trusted number. The architecture doc, the quality gates, the lineage, the cost discipline, and the postmortem are all facets of ownership, and the capstone defense is where you demonstrate you have it.

---

## Weekly schedule

| Day       | Focus                                                                            | Lectures | Exercises | Challenges | Quiz/Read | Homework | Mini-Project | Self-Study | Daily Total |
|-----------|----------------------------------------------------------------------------------|---------:|----------:|-----------:|----------:|---------:|-------------:|-----------:|------------:|
| Monday    | Demo discipline; the 5-minute video; failure pre-emption; feature-freeze         |   1.5h   |   0h      |    0h      |   0.5h    |   1h     |     2h       |    0.5h    |     5.5h    |
| Tuesday   | Postmortem writing; running your chaos drill; the data-quality report             |   1.5h   |   0h      |    0.5h    |   0.5h    |   1h     |     2h       |    0.5h    |     6h      |
| Wednesday | Architecture defense; the Q&A taxonomy; mapping capstone to interview rounds       |   1.5h   |   0h      |    0.5h    |   0.5h    |   1h     |     2h       |    0.5h    |     6h      |
| Thursday  | The mock interview loop (both sides); portfolio packaging; the merged PR           |   0h     |   0h      |    1h      |   0.5h    |   3h     |     1h       |    0.5h    |     6h      |
| Friday    | Dress rehearsal end-to-end on `docker compose up`; record the 5-minute video       |   0h     |   0h      |    1h      |   0.5h    |   0h     |     3h       |    0.5h    |     5h      |
| Saturday  | **Capstone defense** — live demo + 20-minute Q&A with the reviewer panel            |   0h     |   0h      |    0h      |   0h      |   0h     |     3h       |    0.5h    |     3.5h    |
| Sunday    | Final review quiz; submit the portfolio; push the merged PR if not yet done         |   0h     |   0h      |    0h      |   1h      |   0h     |     1h       |    0h      |     2h      |
| **Total** |                                                                                  | **6h**   | **0h**    |  **3.5h**  |  **4h**   |  **6h**  |   **14h**    |   **3h**   |   **34.5h** |

Self-paced cohorts schedule the defense whenever the artifacts are ready, but do not skip the dress rehearsal — every pipeline that has not been run end-to-end from a cold `docker compose up` fails in a new and surprising way (a missing volume, an unset env var, a service that races its dependency). The load-bearing items: the mini-project (finishing and packaging the capstone is the work), the chaos drill + postmortem (the deliverable most people under-invest in), the cold-start dress rehearsal (the single highest-leverage hour of the week), and the mock interview loop (the homework that turns the capstone into a job).

---

## How to navigate this week

| File | What's inside |
|------|---------------|
| [README.md](./README.md) | This overview |
| [resources.md](./resources.md) | SYLLABUS capstone spec, postmortem/demo-craft writing, senior data-engineering interview prep, portfolio guidance |
| [lecture-notes/01-demo-discipline-and-the-five-minute-video.md](./lecture-notes/01-demo-discipline-and-the-five-minute-video.md) | The defensible-demo structure, the 5-minute video, failure pre-emption for a batch + streaming pipeline |
| [lecture-notes/02-postmortem-writing-and-the-chaos-drills.md](./lecture-notes/02-postmortem-writing-and-the-chaos-drills.md) | Blameless-factual-actionable postmortems, the three chaos drills, the data-quality report |
| [lecture-notes/03-architecture-defense-and-the-interview-loop.md](./lecture-notes/03-architecture-defense-and-the-interview-loop.md) | The Q&A taxonomy, the eight interview rounds, mapping the capstone to each, portfolio packaging |
| [exercises/SOLUTIONS.md](./exercises/SOLUTIONS.md) | Model answers to the capstone-defense Q&A, a worked postmortem, and model interview-round answers |
| [challenges/challenge-01-run-a-chaos-drill.md](./challenges/challenge-01-run-a-chaos-drill.md) | Run one of the three SYLLABUS chaos drills on your pipeline; capture it; write the postmortem |
| [challenges/challenge-02-merge-an-open-source-pr.md](./challenges/challenge-02-merge-an-open-source-pr.md) | Land one PR in an open-source data project; document the contribution for your portfolio |
| [mini-project/README.md](./mini-project/README.md) | **The full capstone build-and-defense brief** — deliverables, demo order, grading axes, the defense session |
| [homework.md](./homework.md) | The mock senior data-engineering interview loop (both sides) |
| [quiz.md](./quiz.md) | The final review — 10 questions spanning all 12 weeks of C27 |

---

## Reading order if you are short on time

1. **mini-project/README.md** — the capstone build-and-defense brief; read it *first* this week; 25 minutes.
2. **README.md** — this overview; 20 minutes.
3. **lecture-notes/01-demo-discipline-and-the-five-minute-video.md** — demo craft; 35 minutes.
4. **lecture-notes/02-postmortem-writing-and-the-chaos-drills.md** — postmortems and drills; 40 minutes.
5. **lecture-notes/03-architecture-defense-and-the-interview-loop.md** — the defense and the loop; 45 minutes.
6. **exercises/SOLUTIONS.md** — the model Q&A and interview answers; read before your dress rehearsal; 40 minutes.
7. **challenges/challenge-01-run-a-chaos-drill.md** — read Monday, run Tuesday; 15 minutes.

The reading is ~3.5 hours; the work is finishing the pipeline, running the drill, rehearsing the cold-start demo, and defending — the bulk of a ~35-hour week.

---

## What is intentionally out of scope this week

- **New pipeline features.** The capstone is feature-frozen at the start of this week. A pipeline that is missing the streaming path is not getting a new dimension this week — it is getting the streaming path finished, then demoed in whatever state it is in. A smaller system that works end-to-end and is well-defended beats a larger one that does not run. Resist the urge to add scope in the final week; polish and harden, do not extend.
- **A real hiring interview.** The mock loop is with a peer, not a recruiter. The point is to practice the *motion* — thinking out loud, recovering from the unknown question, defending under push — not to actually be hired this week.
- **Cloud deployment.** The SYLLABUS is explicit: everything runs locally in Docker, `docker compose up` brings up the whole platform, and a reviewer runs it during the demo. You do not need a cloud account or a public URL. The skill is demonstrated by the system working in front of the reviewer, not by where it is hosted.
- **A perfect dashboard.** The capstone dashboard exists to prove the pipeline is trustworthy and queryable. It surfaces one tested metric an analyst would rely on. We do not grade visual design, color theory, or executive storytelling; we grade whether the number is correct, tested, and traceable.

---

## A note on owning the thing

For twelve weeks you have been told, repeatedly and on purpose, that the job is not writing SQL or wiring a DAG — it is *owning* a platform. You chose a fact-table grain in Week 1 and learned that "grain" is the most important word in the course. You made a load idempotent in Week 3 and learned that a re-run that double-counts is the failure that loses trust fastest. You proved a stream exactly-once in Week 9 and learned that exactly-once is at-least-once plus an idempotent sink, not magic. You wired a gate in Week 10 and learned that a gate that only logs is not a gate. You traced a number in Week 11 and learned that lineage is the first thing you reach for in an incident. This week you put all of it in one `docker compose` file and stand behind it.

The reviewer panel is not trying to fail you. They are doing exactly what a senior interviewer does: finding the one place your understanding is shallow, pushing on it, and watching whether you say "I don't know, but here is how I would find out" — the correct senior answer — or whether you bluff, which is the only wrong one. The capstone you defend on Saturday is the artifact you send to a hiring manager on Sunday. Make it true, make it yours, and be ready to say why every layer is the way it is. That is the job. Welcome to it.
