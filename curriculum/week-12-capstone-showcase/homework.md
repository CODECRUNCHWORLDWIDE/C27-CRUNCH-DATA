# Homework — Week 12: The Mock Senior Data-Engineering Interview Loop

This week's homework is a full mock senior data-engineering interview loop, run with a peer, both sides. You are the interviewee for your loop and the interviewer for theirs. The loop mirrors the eight rounds the SYLLABUS career pack names and that real senior data-engineering loops run. Estimated time: ~3 hours as interviewee, ~3 hours as interviewer, spread across Thursday.

Submit your artifacts as commits under `homework/week-12/`.

---

## Format

- **Pair up** with a peer (another C27 learner, or anyone competent enough to push back — a working data engineer is ideal).
- **Eight rounds**, ~12 minutes each: 8–9 minutes of questioning, 3 minutes of feedback.
- **Both sides.** You run all eight rounds as interviewee for your loop, then all eight as interviewer for theirs (or alternate round-by-round).
- **Record** at least two rounds (with permission) — watching yourself answer is the fastest way to improve, and a strong recorded round can be a portfolio artifact.
- **Think out loud** throughout. The process is graded, not just the answer (Lecture 3, §5).

---

## The eight rounds (interviewee prep)

For each round, prepare *the specific capstone artifact* that answers it (Lecture 3, §4) and a 90-second story. Do not memorize answers — a single follow-up exposes a memorized answer. Understand the topic well enough to be pushed.

### Round 1 — Data-modeling whiteboard

Sample questions: Model this retail domain from the business questions backward — what's the grain of your fact, and why? Type-1 vs Type-2 for this dimension, and what breaks if you get it wrong? Where are your conformed dimensions? Star vs snowflake here?
**Your artifact:** your star schema and the Type-2 customer dimension; the grain you defended in scope review (Week 1).

### Round 2 — SQL tuning

Sample: Read this query plan — why is it slow? Window function vs self-join for a running total. Anti-join vs `NOT IN` and the NULL trap. Where would predicate pushdown fail?
**Your artifact:** your analytical queries and the `EXPLAIN` plan you read to add a partition filter (Week 2).

### Round 3 — Idempotency & incrementality

Sample: Design a load that survives a re-run. How does your high-water mark handle a late record? `MERGE` vs `INSERT` and why it's the difference between a no-op and a double-count.
**Your artifact:** your `MERGE`-based incremental loader and the high-water-mark backfill (Week 3).

### Round 4 — Orchestration

Sample: Walk your DAG — what makes a task idempotent and retryable? How do you backfill a date range without double-counting? What halts the DAG when a gate fails, and who gets alerted?
**Your artifact:** your idempotent, watermarked, backfillable Airflow DAG and its `on_failure` alert (Week 4).

### Round 5 — Storage internals

Sample: What do Iceberg/Delta add over raw Parquet? What's the small-files problem and do you have it? How does predicate pushdown prune your dashboard query?
**Your artifact:** your lakehouse layout, your partitioning, and your compaction job (Weeks 6, 11).

### Round 6 — Distributed compute

Sample: Diagnose a skewed join. How do you read the Spark UI to find a spill? Broadcast vs sort-merge vs salting — when each? When is Spark the wrong tool and a single node right?
**Your artifact:** your Spark job and the skewed join you fixed (Week 7).

### Round 7 — Streaming

Sample: Event time vs processing time. What does your watermark drop, and is that correct? Prove your stream is exactly-once and not just at-least-once.
**Your artifact:** your Structured Streaming job, its watermark, and its exactly-once Iceberg sink with the reconciliation proof (Week 9).

### Round 8 — The incident story (behavioral)

Sample: "Tell me about a data incident you handled." Name the symptom, your wrong first hypothesis, the diagnostic that disproved it, the actual root cause, the fix, and what you changed so it can't recur.
**Your artifact:** your Week 12 chaos-drill postmortem — with the artifact (the quality-report page, the lag panel, the rejected schema) and the row-count proof.

---

## The interviewer's job (the other half)

Running the loop for your peer is half the homework, because interviewing well teaches you what interviewers look for. As interviewer:

1. **Probe past the rehearsed answer.** When they give a clean answer, ask "why" or "what if." The interesting information is at the edge of their understanding, not in the prepared opener. ("You said it's exactly-once — prove it. Show me the reconciliation.")
2. **Push on the bluff.** If you sense a confident-but-shaky answer, ask a specific follow-up. Note whether they recover honestly ("actually, let me reconsider") or dig in.
3. **Grade the process, not just the answer.** Did they think out loud? Did they catch their own error? Did they handle "I don't know" gracefully?
4. **Give specific, kind, actionable feedback.** Not "good job" — "your idempotency answer was solid but you said `INSERT` and then corrected to `MERGE`; lead with `MERGE` and name the `unique_key` next time." One specific fix per round beats five vague compliments.

---

## Deliverables

Submit under `homework/week-12/`:

1. `interview-prep.md` — your one-page prep sheet: for each of the eight rounds, the capstone artifact and the 90-second story you'll use.
2. `interviewee-feedback.md` — the feedback you *received* on your loop, per round, with your honest self-assessment of where your edges are and a plan to close the top two gaps.
3. `interviewer-notes.md` — your notes from interviewing your peer: per round, what you asked, how they did, and the one specific piece of feedback you gave.
4. (Optional, recommended) a link to one recorded round.

Commit with a message like `week-12 homework: mock senior data-engineering interview loop`.

---

## Submission

```bash
git add homework/week-12/
git commit -m "week 12 homework: mock senior data-engineering interview loop (both sides)"
git tag week-12-homework
git push origin main --tags
```

---

## The point of all this

You will not be hired by the mock loop. The point is the *motion* — answering under time pressure, thinking out loud, recovering from the question past your edge, and defending without bluffing. The first time you do this for real, with a recruiter, the stakes are high and the practice is expensive. The mock loop makes the practice cheap. Do it badly here so you do it well there. And the act of preparing the eight-round prep sheet forces you to confront which weeks of C27 you actually internalized versus merely completed — the gaps you find this Thursday are the gaps to close before your first real loop.

---

## References

- SYLLABUS, "Career engineering pack" — the interview-prep topics this loop is built from.
- Lecture 3 — the eight rounds mapped to capstone artifacts, and the "I don't know" answer.
- exercises/SOLUTIONS.md, Part C — model answers to each round (read after drafting your own).
- Chip Huyen, *Designing Machine Learning Systems* and the broader "Awesome Data Engineering" interview banks — for broadening the rounds beyond the SYLLABUS set.
