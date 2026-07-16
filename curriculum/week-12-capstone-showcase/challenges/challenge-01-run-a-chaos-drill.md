# Challenge 1 — Run a Chaos Drill on Your Own Pipeline

## Brief

Pick one of the three SYLLABUS chaos drills, run it against your live capstone pipeline, capture what happens (with artifacts), recover, **prove no data was lost or double-counted**, and write the postmortem. This challenge produces the capstone's postmortem deliverable and feeds the chaos-drill grading. The goal is not to survive the drill flawlessly — it is to learn something worth writing down and to prove you can operate your own platform under a failure you induced on purpose.

You should spend ~2 hours on this challenge (plus the writeup). The deliverable is `POSTMORTEM.md` and the captured artifacts.

## Why this challenge matters

A pipeline you have never broken is a pipeline you do not understand. Running a drill on your own platform — deliberately, with a hypothesis and a capture plan — is how you find the failure mode *before* a reviewer asks about it in the defense, and before an analyst finds a wrong number in production. The postmortem you write is, alongside the data-quality report, the most credible thing in your portfolio: it shows a hiring manager exactly how you reason about failure. Most capstone candidates under-invest here because writing honestly about a failure is uncomfortable; the ones who do it well stand out precisely because it is rare. It is also the source material for the behavioral "tell me about an incident" interview round (Lecture 3).

## Choose your drill

Pick **one**. Each exercises a different failure class and a different week's content (Lecture 2, §5).

### Option A — Malformed batch load (quality gate × idempotency)

A daily file arrives with a corrupted schema (a renamed or dropped column) or out-of-range values. Drop the bad file into the landing zone and trigger the DAG. Tests Week 10 + Week 3: does the Great Expectations checkpoint at the ingestion boundary catch the corruption and *halt* the load before it reaches the mart, or does the bad file land silently? When you re-run after fixing the file, does the load double-count, or is it idempotent? **Prove the bad data never reached the mart** — the row counts and the quality report are the proof.

### Option B — Stream partition lag spike (consumer groups × exactly-once)

A consumer falls behind and a partition's lag explodes mid-run. Throttle or pause the Spark Structured Streaming consumer (or flood the topic) so consumer-group lag on one partition climbs sharply, then resume. Tests Week 8 + Week 9: what happens as lag grows (back-pressure, rebalance, scaling)? When it catches up, **did exactly-once hold** — were any events lost, and were any double-counted when the job re-read from the last committed offset? Prove it with a windowed-aggregate-vs-ground-truth reconciliation.

### Option C — Schema-evolution event (schema registry × table-format evolution)

A producer adds a field (a compatible change) and later makes a breaking change (a type change or a dropped required field). Register the compatible schema, then attempt the incompatible one against the schema registry. Tests Week 8 + Week 6: did the registry absorb the compatible change and *reject* the breaking one? Did the lakehouse table format (Iceberg/Delta) evolve to add the new column without rewriting data? Did downstream dbt models and the dashboard survive the compatible change, and fail safely on the breaking one?

## Procedure

### Phase 1 — Hypothesis and capture plan

Before you break anything, write down:

1. **Your hypothesis** — what you *expect* to happen. ("I expect the ingestion checkpoint to fail on the renamed column and halt the DAG before staging, so the mart's number is unchanged.")
2. **Your capture plan** — what you will measure and how. For Option A, the failed Airflow task, the Great Expectations Data Docs page for the failed run, and the mart row count before/after. For Option B, the consumer-lag panel, the rebalance/scaling events, and the windowed-aggregate-vs-produced-events reconciliation. For Option C, the schema-registry rejection error, the table schema before/after the compatible change, and the downstream dbt build status.
3. **Your success criterion** — what "the system handled it" looks like (not "nothing broke" — "broke in the recoverable way I designed for, and no data was lost or double-counted").

### Phase 2 — Confirm a healthy baseline

Bring the pipeline to green: batch and stream both running, the mart fresh, the dashboard healthy, the quality report clean. Capture the baseline row counts and metric values. You cannot interpret the drill's results without a "before."

### Phase 3 — Run the drill

Induce the failure. Start a timer. Observe and capture everything per your plan. Do **not** intervene early — let the system attempt its own recovery; the whole point is to see what it does unattended. (The malformed-load and lag drills are safe to let run; for the schema drill, let the registry reject the breaking change rather than forcing it through — the rejection *is* the finding.)

### Phase 4 — Recover and confirm (and prove no data was lost or double-counted)

Restore the condition (quarantine and replace the bad file, resume the consumer, register a compatible schema). Confirm the pipeline returns to green. Then do the proof that defines a data postmortem: a **row-count and reconciliation check** showing the mart has exactly the rows it should — no bad rows leaked, no duplicates from the re-run, no events lost in the lag recovery. Note exactly what recovery required (self-recovery vs manual intervention — and if manual, that is a finding).

### Phase 5 — Write the postmortem

Write `POSTMORTEM.md` (3–5 pages) using the Lecture 2 structure: summary, timeline (timestamped), root cause (the chain, past the symptom), detection (and the gap between when it *was* and when it *should have been* caught), resolution (with the no-loss/no-double-count proof), and lessons-as-action-items (blameless, each with an owner and a "done when," split into prevent-recurrence vs detect-faster). Compare what happened to your Phase 1 hypothesis — where it differed is your most valuable finding.

## Deliverable

1. `POSTMORTEM.md` (3–5 pages) — the full postmortem.
2. The captured artifacts — the failed DAG run, the quality-report page, the lag panel, the registry rejection, the row-count reconciliation — annotated and referenced from the postmortem.
3. A one-paragraph "hypothesis vs reality" note: what you expected, what happened, and what the gap taught you.

Commit with a message like `week-12/challenge-01: <drill> chaos drill + postmortem`.

## Pass criteria

- The drill was actually run against the live pipeline (not described hypothetically) — the artifacts prove it.
- The postmortem follows the blameless-factual-actionable structure.
- The root cause follows the chain past the symptom (Lecture 2, §4) — not a single surface cause.
- The postmortem **proves no data was lost or double-counted** (the row-count reconciliation), which is the heart of a data postmortem and the SYLLABUS framing for every drill.
- Every lesson is an action item with an owner and a "done when."
- The "hypothesis vs reality" note is honest about where your expectation was wrong (and if it was exactly right, that is suspicious — re-examine).

## A note on the drill that goes "too well"

If your drill produced no surprises and no data impact and required no intervention — congratulations, but be suspicious. Either your design genuinely anticipated this failure (great — say so, and the postmortem documents a *validated* design decision), or your drill was too gentle to stress the real boundary. Push harder: a worse schema break, a longer lag that ages past Kafka retention, a breaking change the registry is configured too loosely to reject. The most useful drill is the one that finds the edge of your pipeline's resilience, and the postmortem that says "I pushed until it broke, here is where, here is the fix" is stronger than "everything was fine." Find the edge.

## References

- Lecture 2 — postmortem writing and the three chaos drills.
- SYLLABUS, "Capstone" — the chaos-drill menu and the data-quality-report requirement.
- Google SRE Book, "Postmortem Culture". <https://sre.google/sre-book/postmortem-culture/>
- Principles of Chaos Engineering. <https://principlesofchaos.org/>
