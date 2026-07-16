# Week 9 — Quiz

Ten multiple-choice questions on event vs processing time, watermarks, windows, output modes,
checkpointing, the exactly-once requirements, and Spark micro-batch vs Flink. Take it with the
lecture notes closed. Aim for 9/10 before the mini-project. Answer key at the bottom — do not
peek.

---

**Q1.** Windowing a clickstream by **processing time** instead of event time is undesirable for
analytics because:

- A) Processing time is harder to compute than event time.
- B) The same click can land in different windows on different runs depending on pipeline
  congestion, so the result is not reproducible.
- C) Spark does not support processing-time windows.
- D) Processing time is always earlier than event time.

---

**Q2.** A watermark in Structured Streaming, set with `withWatermark("event_ts","10 minutes")`,
is best described as:

- A) A fixed wall-clock deadline after which the job stops.
- B) A running lower bound on event time, computed as (max observed event time − 10 minutes),
  past which a window is considered closed.
- C) The Kafka offset the query has committed.
- D) The maximum number of late events allowed.

---

**Q3.** An event arrives whose `event_ts` is **older than the current watermark**. Spark:

- A) Raises an exception and stops the query.
- B) Reopens the closed window and recomputes it.
- C) Drops the event (counting it in `numRowsDroppedByWatermark`); its window was already
  closed and evicted.
- D) Writes it to a dead-letter topic automatically.

---

**Q4.** You want a **5-minute count where each event belongs to exactly one window**. The right
window type and call is:

- A) Sliding — `window(col, "5 minutes", "1 minute")`.
- B) Session — `session_window(col, "5 minutes")`.
- C) Tumbling — `window(col, "5 minutes")`.
- D) Global — `window(col)`.

---

**Q5.** Running an aggregation in **append** output mode **without** a watermark:

- A) Works fine and emits every batch.
- B) Raises an `AnalysisException`, because append needs a definition of "this row is final"
  and only the watermark provides it.
- C) Silently switches to complete mode.
- D) Emits results but uses unbounded state.

---

**Q6.** The exactly-once guarantee for a streaming write into a lakehouse table is the product
of:

- A) A fast network × a big cluster × a small batch.
- B) A replayable source (Kafka offsets) × a checkpoint × an idempotent (or transactional)
  sink.
- C) Append mode × complete mode × update mode.
- D) A watermark × a tumbling window × a session window.

---

**Q7.** Inside `foreachBatch`, your Delta `MERGE` on `(window, page)` uses
`event_count = t.event_count + s.event_count` (add). Under **update** output mode, on a batch
**replay after a crash**, this produces:

- A) The correct counts, because MERGE is always idempotent.
- B) Double-counted windows, because update mode re-emits a window's full running count and
  adding it to the existing value counts it twice.
- C) An exception, because you cannot add in a MERGE.
- D) Dropped rows, because the watermark expired.

---

**Q8.** You **delete the checkpoint directory** of an exactly-once Delta-`MERGE` job and re-run
it from `startingOffsets="earliest"`. The most accurate statement is:

- A) The job will refuse to start without a checkpoint.
- B) The whole topic is re-read and re-merged; because the `MERGE` overwrites the count it is
  idempotent and the table ends correct — but an append-mode sink would have double-written.
- C) The job resumes exactly where it left off.
- D) Nothing happens because the data is already in the table.

---

**Q9.** Spark Structured Streaming's latency floor is roughly the trigger/micro-batch interval,
while Flink can fire a window the instant its watermark passes, because:

- A) Flink runs on faster hardware.
- B) Spark is a micro-batch engine (a sequence of small batch jobs with per-batch planning and
  commit overhead), whereas Flink processes records through a standing dataflow graph
  one-at-a-time with no batch boundary.
- C) Spark does not support event time.
- D) Flink does not use watermarks.

---

**Q10.** Flink achieves exactly-once output via:

- A) Idempotent `MERGE` upserts only.
- B) Re-reading the whole topic on every checkpoint.
- C) Asynchronous barrier snapshots (a Chandy–Lamport variant) for consistent state recovery,
  plus two-phase-commit sinks for exactly-once output.
- D) Storing every record in memory forever.

---

## Answer key

<details>
<summary>Click to reveal answers</summary>

1. **B** — Processing-time windowing puts the same event in different windows depending on
   when the pipeline happened to read it, so the result depends on the run and is not
   reproducible. Event-time windowing is run-independent: the 09:03 click is always in the
   09:00–09:05 window. Lecture 1 §2.

2. **B** — The watermark is a running lower bound on event time, `max(event_time) − delay`,
   advancing at micro-batch boundaries. It closes windows, bounds state, and classifies late
   data — one concept, three jobs. It is the streaming analog of the Week-3 high-water mark.
   Lecture 1 §3.

3. **C** — Beyond the watermark, the window has been finalized and evicted, so the event is
   too late. Spark drops it silently (no error) and increments `numRowsDroppedByWatermark` in
   the progress JSON. To capture such events you lengthen the watermark or use Flink's side
   outputs. Lecture 1 §4.

4. **C** — Tumbling windows are fixed-size, non-overlapping, exactly-one-window-per-event:
   `window(col("event_ts"), "5 minutes")`. Sliding (A) overlaps so an event is in multiple
   windows; session (B) is gap-defined and variable-size. Lecture 1 §5.

5. **B** — Append on an aggregation requires a watermark; without one Spark raises
   `AnalysisException: Append output mode not supported when there are streaming aggregations
   ... without watermark`. Append emits a row once it is final, and the watermark is the
   definition of final. Lecture 2 §2.3, §2.4.

6. **B** — Exactly-once = replayable source × checkpoint × idempotent (or transactional) sink.
   Kafka offsets give replay, the checkpoint records what each batch covered and holds state,
   and a `MERGE` keyed on the window makes the write safe to repeat. Drop a factor and you get
   at-least-once or at-most-once. Lecture 2 §4.

7. **B** — Update mode re-emits a window's **full running count** each time it changes, not a
   +1 delta. Adding that running count to the existing value on a replay double-counts. The
   correct action is to **overwrite** (`set = s.event_count`), which makes a replay a no-op.
   Lecture 2 §5; SOLUTIONS Exercise 3.

8. **B** — The checkpoint is the query's memory of committed offsets; deleting it makes the
   query "fresh," re-reading from `earliest` and reprocessing everything. The idempotent
   `MERGE` (overwrite) survives this and the table ends correct, but an append-only sink would
   double-write every row — which is precisely why the idempotent sink is a required
   exactly-once factor. Lecture 2 §3, §4.

9. **B** — Spark is micro-batch: a streaming query is a sequence of small batch jobs, each with
   planning, scheduling, and commit overhead, so latency floors at ~the trigger interval.
   Flink deploys a standing dataflow graph once and processes each record as it arrives,
   firing a window the instant the watermark passes — no batch boundary, ms-scale latency.
   Lecture 1 §6, Lecture 3 §1, §4.

10. **C** — Flink uses asynchronous barrier snapshotting (a variant of Chandy–Lamport) to take
    a globally consistent state snapshot without stopping the pipeline, and two-phase-commit
    sinks (pre-commit during the checkpoint, commit once the whole checkpoint completes) for
    exactly-once output. Same guarantee as Spark's checkpoint + idempotent sink, different
    mechanism. Lecture 3 §5.

</details>

---

## Self-assessment scale

- **10/10** — You own the streaming model. Build the mini-project and benchmark Spark vs Flink.
- **8–9/10** — Solid. Re-skim the one or two sections you missed; you are ready for the
  challenges.
- **6–7/10** — Re-read Lecture 1 §3–§5 (watermarks + windows) and Lecture 2 §2–§4 (output
  modes + exactly-once) before the mini-project; those are the load-bearing sections.
- **< 6/10** — Re-read all three lectures and redo Exercises 2 and 3 hands-on. The concepts
  compound; do not move on until the watermark and the exactly-once multiplication are
  reflexes.
