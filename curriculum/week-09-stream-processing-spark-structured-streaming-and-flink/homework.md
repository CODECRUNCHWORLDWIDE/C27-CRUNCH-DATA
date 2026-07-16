# Week 9 — Homework

Six practice problems, ~45 minutes each. They reinforce the week's load-bearing ideas:
the batch↔stream mapping, watermarks, output-mode legality, the exactly-once multiplication,
the streaming-lakehouse upsert, trigger choice, and the Spark/Flink trade. Each problem names
a deliverable path and cites a real doc. Do them in order; later problems assume earlier ones.

Put your work under `homework/` in your week-9 repo.

---

## HW1 — Map every streaming concept back to its Week-3 batch analog (~45 min)

**Task.** Write a two-column table mapping each Week-9 streaming concept to its Week-3 batch
analog, with a one-sentence justification for each row: high-water mark ↔ watermark;
late-arriving record ↔ late event within allowed lateness; idempotent `MERGE` upsert ↔
idempotent streaming sink; nightly backfill ↔ allowed-lateness window; full table rebuild ↔
complete output mode; insert-only incremental load ↔ append output mode; control-table
`last_loaded_ts` ↔ checkpoint offset log. Then write one paragraph defending the claim
"streaming is batch's hard generalization," citing the Dataflow Model's framing that batch is
the special case where the window is "all of time."

**Deliverable.** `homework/hw1-batch-stream-mapping.md`.

**Cite.** Dataflow Model paper (Akidau et al., VLDB 2015):
<https://research.google/pubs/the-dataflow-model-a-practical-approach-to-balancing-correctness-latency-and-cost-in-massive-scale-unbounded-out-of-order-data-processing/>;
Structured Streaming basic concepts:
<https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#basic-concepts>

---

## HW2 — Reason about a watermark on real timestamps (~45 min)

**Task.** Given this sequence of `(event_ts, arrival order)` for a 5-minute tumbling window
aggregate with `withWatermark("event_ts","10 minutes")` — arrivals in order:
`09:04:00, 09:09:30, 09:03:10, 09:14:50, 09:01:00, 09:06:20` — compute, after **each**
arrival: the current max event time, the current watermark, and for each event whether it is
on-time, late-but-within-watermark, or too-late-and-dropped. Show your work in a table. Then
state which window each *accepted* event lands in, and the final count per window. Confirm
your hand analysis against a tiny PySpark `rate`-source or a static `createDataFrame`
reproduction that prints the windowed counts.

**Deliverable.** `homework/hw2-watermark-trace.md` + `homework/hw2_check.py`.

**Cite.** Handling late data & watermarking:
<https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking>

---

## HW3 — The output-mode legality matrix, demonstrated (~45 min)

**Task.** For a windowed `count` aggregate, write a short PySpark script that attempts each of
the three output modes (`append`, `update`, `complete`) **with** a watermark and **without**
one (six combinations). Capture which combinations Spark accepts and which raise, and paste
the exact exception message for the illegal ones (notably append-on-aggregate-without-watermark).
Write a one-paragraph explanation of *why* append requires a watermark on an aggregation —
tying "append emits a row once it is final" to "the watermark is the definition of final."

**Deliverable.** `homework/hw3_output_modes.py` + `homework/hw3-notes.md` (the matrix + the
captured exception text).

**Cite.** Output modes:
<https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes>

---

## HW4 — Make a sink idempotent and break it on purpose (~45 min)

**Task.** Take the Exercise-3 `foreachBatch` `MERGE`. (a) Run it normally and record the table.
(b) Change the merge action from `whenMatchedUpdate(set event_count = s.event_count)`
(overwrite) to `set event_count = t.event_count + s.event_count` (add), delete the checkpoint,
re-run from `earliest`, and show the counts are now **wrong** (doubled). (c) Restore the
overwrite, delete the checkpoint, re-run, and show the counts are correct again. Write one
paragraph explaining why overwrite is idempotent under update-mode re-emission and add is not,
and connect it to the Week-3 lesson that an upsert overwrites rather than accumulates.

**Deliverable.** `homework/hw4_idempotency.py` + `homework/hw4-notes.md` (the three table
states + the explanation).

**Cite.** Upsert from streaming queries using foreachBatch:
<https://docs.delta.io/latest/delta-update.html#upsert-from-streaming-queries-using-foreachbatch>;
Structured Streaming fault tolerance:
<https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#fault-tolerance-semantics>

---

## HW5 — Trigger choice and the 15-minute-micro-batch argument (~45 min)

**Task.** Run the Exercise-2 aggregate under three triggers: default (as-fast-as-possible),
`Trigger.ProcessingTime("15 seconds")`, and `Trigger.AvailableNow()`. For each, record the
number of micro-batches, the mean `processedRowsPerSecond`, and the wall-clock the query ran.
Then write a one-page memo arguing for a *specific* trigger choice for a clickstream dashboard
that refreshes every 15 minutes — making the "latency you do not need is cost you do pay" case
and naming the operational benefits of a 15-minute micro-batch (or `AvailableNow` on a 15-min
schedule) over an always-on true stream. Name one workload where the argument flips.

**Deliverable.** `homework/hw5-trigger-memo.md` (table of measurements + the memo).

**Cite.** Triggers:
<https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers>

---

## HW6 — Spark vs Flink, in your own words and one PyFlink run (~45 min)

**Task.** Stand up the minimal PyFlink job from Lecture 3 §3 (Kafka source with `WATERMARK
FOR`, `TUMBLE` TVF, `print` sink) and get it producing windowed counts off the Week-8 topic.
Then write a one-page comparison covering: processing model (micro-batch vs record-at-a-time),
event-time/window API differences, exactly-once mechanism (checkpointed offsets + idempotent
sink vs barrier snapshots + 2PC), and a decision rule for choosing one over the other. Back
the "Flink has lower latency" claim with the *architectural reason* (no micro-batch boundary),
not just an assertion.

**Deliverable.** `homework/hw6_flink_job.py` + `homework/hw6-spark-vs-flink.md`.

**Cite.** PyFlink Table API:
<https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/table/intro_to_table_api/>;
Flink fault tolerance:
<https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/fault_tolerance/>;
Flink Kafka connector:
<https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/kafka/>

---

When all six are done you have re-derived every load-bearing idea in the week by hand. Take the
[quiz](./quiz.md), then build the [mini-project](./mini-project/README.md).
