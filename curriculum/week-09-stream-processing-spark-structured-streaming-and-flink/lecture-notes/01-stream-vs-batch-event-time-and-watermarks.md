# Lecture 1 — Stream vs Batch, Event Time, and Watermarks

> **Duration:** ~3 hours of reading + a 30-minute PySpark sanity check.
> **Prerequisites:** Week 3 (batch ETL: high-water-mark incremental load, late records, idempotent upsert), Week 7 (PySpark DataFrames), Week 8 (the `clickstream` Kafka topic).
> **Citations:** Spark Structured Streaming Programming Guide (<https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html>); Structured Streaming + Kafka integration guide (<https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html>); Akidau et al., "Streaming 101" (<https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/>) and "Streaming 102" (<https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/>); the Dataflow Model paper (Akidau et al., VLDB 2015).
> **Outcome:** You can explain why streaming is batch's hard generalization, distinguish event from processing time, define a watermark as a running lower bound on event time, name the three window types, sketch the Structured Streaming micro-batch model, and write a `readStream` from the Week-8 Kafka topic with a `withWatermark`.

If you only remember one sentence from this week, remember this:

> **Streaming is batch's hard generalization. Every concept maps back to a Week-3 batch concept: the watermark is the high-water mark, late data is the late-arriving record, the idempotent sink is the idempotent upsert. What makes streaming hard is not new mechanisms — it is that the dataset is unbounded, so the boundaries you were *given* in batch you must now *infer* from the data, and the watermark is that inference.**

Week 8's Kafka topic is parked as a producer-side artifact. Week 9 reads it. The input is the same `clickstream` events; the output is a continuously-updated count-per-page-per-window table.

---

## 1. Bounded vs unbounded: what actually changes

A **batch** dataset is **bounded**: it has a first record and a last record, both known. The file on MinIO exists; the partition for `2026-06-18` is complete because the day is over; you can count the rows. Computation over a bounded dataset *terminates*: you read everything, compute, write, exit. Week 3's hourly Airflow DAG is the archetype — it ran, read everything newer than the stored watermark, upserted, and finished.

A **stream** is **unbounded**: it has a first record but no last record, by design. The `clickstream` topic keeps growing; there is no "the day is over"; you can never count the rows because more are coming. Computation over an unbounded dataset *does not terminate* — it runs forever, emitting results incrementally as data arrives. This is the entire difference, and everything else falls out of it.

The Dataflow Model paper (Akidau et al., VLDB 2015) makes the relationship precise and worth internalizing: **batch is the special case of streaming where the window is "all of time" and you wait until the end of all time before emitting.** A bounded dataset is just an unbounded one that someone already declared finished. So streaming is not a different paradigm bolted onto batch; it is the general case, and batch is the degenerate one where the unboundedness was resolved for you in advance. Spark leans into this directly: the Structured Streaming Programming Guide's "basic concepts" section frames a stream as an *unbounded table* to which rows are continuously appended, and a streaming query as the *same* query you would write over a static table, run incrementally. You write `groupBy(...).count()` exactly as in batch; Spark runs it forever.

Three things that were free in batch now cost you:

1. **You never know you are done.** In batch the file is complete. In streaming, "have I seen all the events for the 09:00–09:05 window?" is unanswerable with certainty — a straggler could always be in flight. You must *decide* when to stop waiting. (Watermark.)
2. **Records arrive out of order.** In batch you sort the file. In a stream, the event that happened at 09:03 may arrive *after* the event that happened at 09:04 because of network buffering, client offline mode, or partition lag. (Event time vs processing time.)
3. **State must be bounded.** In batch the working set is the file, then you exit and free it. In streaming the job runs forever, so any state you hold (partial aggregates) must be *bounded* or you run out of memory. (The watermark bounds it.)

These three are the load-bearing problems of stream processing, and the watermark is the answer to all three.

---

## 2. Three clocks: event time, processing time, ingestion time

Every record in a stream carries (at least implicitly) three timestamps. Getting these straight is half the week.

- **Event time** — *when the thing actually happened in the real world.* Your Week-8 producer stamped `event_ts` at the instant the simulated user clicked. This is intrinsic to the event and never changes, no matter how many times the event is replayed, buffered, or reprocessed. Event time is what analytics *means* — "how many checkouts happened between 09:00 and 09:05" is a question about event time.
- **Processing time** — *when your engine observed the record.* This is wall-clock time at the executor when a micro-batch read the record off Kafka. It depends on pipeline congestion, restarts, and replay, and it differs every run. Processing time is what's easy to compute (just look at the clock) and almost always wrong for analytics.
- **Ingestion time** — *when the record entered the system*, e.g. the timestamp Kafka stamped on the record when the broker received it. It sits between the other two: more stable than processing time (assigned once, at ingest) but still not the real event time (it includes producer-to-broker latency). Kafka exposes it as the record's `timestamp`, surfaced by the Spark Kafka source as the `timestamp` column.

The crucial fact: **these clocks drift apart, and the gap (the *skew*) is variable.** A mobile client buffers events while offline and flushes them ten minutes later — event time 09:03, processing time 09:13, skew 10 minutes. A Kafka partition lags under load — skew grows. A consumer restarts and replays an hour of history in thirty seconds — processing time races far ahead of event time. The "Streaming 102" article (Akidau) draws the canonical picture: a two-axis plot with event time on one axis and processing time on the other, and a wavy diagonal band showing real data is never on the 45° line — it is scattered around it, sometimes far.

Why this matters concretely: window the clickstream by **processing time** and a user's 09:03 click lands in whatever window happened to be open when the record arrived — 09:00–09:05 if the pipeline was healthy, 09:10–09:15 if it was congested. Re-run the job and you get a *different* answer, because processing time depends on the run. Window by **event time** and the 09:03 click *always* lands in 09:00–09:05, on every run, forever. Event-time windowing is reproducible; processing-time windowing is not. For analytics you almost always want event time — and event-time semantics are exactly what watermarks make possible.

> **Week-3 tie-back.** In Week 3 you had this same problem and called it "the event was created at `created_at` but loaded at `loaded_at`." You loaded incrementally by `created_at` (event time) and stored `loaded_at` for audit. Same two clocks. Streaming just makes the gap continuous and forces you to handle out-of-order arrival within the engine instead of in a nightly backfill.

---

## 3. The watermark: a running lower bound on event time

Here is the central mechanism. A **watermark** is the streaming engine's continuously-updated assertion:

> "I do not expect to see any more events with an event time earlier than *W*."

It is a *lower bound on the event times still to come*. Once the watermark passes time *W*, any window ending at or before *W* is declared **closed**: its result can be finalized and emitted, its state can be dropped from memory, and any event that arrives afterward claiming an event time before *W* is **too late** and is discarded (or routed to a side output, in engines that support it).

How does the engine compute the watermark? The simplest and most common policy — the one Spark uses — is:

```
watermark = (max event time observed so far) − (allowed lateness delay)
```

You set the delay with `withWatermark("event_ts", "10 minutes")`. Spark tracks the maximum `event_ts` it has seen across all data, subtracts ten minutes, and that is the current watermark. As newer events push the max event time forward, the watermark advances behind it, always trailing by the delay. The "handling late data and watermarking" section of the Programming Guide specifies this precisely, including the subtle rule that **the watermark only advances at micro-batch boundaries** and uses the max event time from the *previous* batch — so the watermark is always slightly conservative, which is the safe direction.

The delay is a **knob on the completeness/latency curve**:

- A **longer** delay (say 30 minutes) tolerates more lateness — events arriving up to 30 minutes after the fact still count — at the cost of *higher latency* (you wait 30 minutes before finalizing a window) and *more state* (you hold every open window for 30 minutes).
- A **shorter** delay (say 1 minute) finalizes windows fast and holds little state, but *drops* any event more than a minute late.

There is no correct universal value — it is a business decision about how much lateness your domain produces and how fresh your results must be. Set it from the data: measure your actual event-time skew distribution and pick a delay that captures, say, the 99th percentile of lateness.

> **Week-3 tie-back, stated exactly.** Week 3's high-water mark was a single value in a control table — `last_loaded_event_ts` — that you advanced by hand at the end of each run, and you ran a periodic backfill to catch records that arrived after their window's run. The streaming watermark *is that high-water mark*, with two differences: (1) the engine computes and advances it continuously from the data instead of you updating a row, and (2) "late record I backfill tomorrow" becomes "late event the watermark has not yet expired, folded in automatically." Same idea, made continuous. The backfill is no longer a separate job; it is the allowed-lateness window.

---

## 4. Late data and allowed lateness

A **late event** is one whose event time is earlier than the current maximum event time — it arrived out of order. There are two cases, and the watermark draws the line between them:

1. **Late but within the watermark.** The event's event time is later than the current watermark. Its window is still open. Structured Streaming *updates* the window's aggregate to include it. This is the happy case — late data is handled *correctly and automatically*, with no backfill, no special code. The injected late event in the lab is engineered to land here, and proving it folds into the right window is the lab's correctness test.
2. **Too late — beyond the watermark.** The event's event time is earlier than the current watermark. Its window has already been closed and dropped. Spark *discards* the event. It does not error; it silently drops it, and increments a metric you can see in `lastProgress` (the state operator's `numRowsDroppedByWatermark`). If you care about these, lengthen the watermark or route them elsewhere.

The key realization: **the watermark is what makes "late" a decidable predicate.** Without a watermark, the engine has no definition of "too late," so it would have to keep every window open forever (unbounded state) and could never emit a final result in append mode. The watermark is simultaneously (a) the completeness guarantee — "I've seen everything up to *W*," (b) the state-bounding mechanism — "drop everything older than *W*," and (c) the late-data classifier — "older than *W* is too late." One concept, three jobs.

---

## 5. Windows: tumbling, sliding, session

Aggregating a stream by event time means grouping events into **windows**. Three kinds:

### 5.1 Tumbling windows — fixed, non-overlapping

The event-time axis is chopped into fixed-size, contiguous, non-overlapping intervals. Every event belongs to **exactly one** window. A 5-minute tumbling window gives `[09:00,09:05)`, `[09:05,09:10)`, `[09:10,09:15)`, …. This is the lab's window. In PySpark:

```python
from pyspark.sql.functions import window, col, count

windowed = (events
    .withWatermark("event_ts", "10 minutes")
    .groupBy(window(col("event_ts"), "5 minutes"), col("page"))
    .agg(count("*").alias("event_count")))
```

```
event time:   09:00      09:05      09:10      09:15
              |----W1----|----W2----|----W3----|
events:        x  x x      x   x       x  x x
each event in exactly one window
```

Use tumbling for non-overlapping period counts: "events per page per 5 minutes."

### 5.2 Sliding windows — fixed size, overlapping

A window of fixed *size* that advances by a *slide interval* smaller than the size, so windows overlap and each event lands in multiple windows. A 10-minute window sliding every 5 minutes gives `[09:00,09:10)`, `[09:05,09:15)`, `[09:10,09:20)`, … and an event at 09:07 is in both `[09:00,09:10)` and `[09:05,09:15)`. In PySpark, add the slide argument:

```python
window(col("event_ts"), "10 minutes", "5 minutes")   # size, slide
```

```
event time:   09:00      09:05      09:10      09:15
              |------W1------|
                     |------W2------|
                            |------W3------|
event at 09:07 falls in W1 and W2
```

Use sliding for smoothed moving metrics: "10-minute rolling event count, updated every 5 minutes."

### 5.3 Session windows — gap-defined, variable size

No fixed size. Events are grouped into a session that stays open as long as events keep arriving within a *gap timeout*, and closes once there's a quiet period longer than the gap. A 15-minute session gap reconstructs browsing sessions: a user's clicks form one session until they go quiet for 15 minutes, then the next click starts a new session. Spark supports this with `session_window`:

```python
from pyspark.sql.functions import session_window
events.withWatermark("event_ts", "10 minutes") \
      .groupBy(session_window(col("event_ts"), "15 minutes"), col("user_id")) \
      .count()
```

```
user A clicks:  09:01  09:03  09:06 .................. 09:40  09:42
                |------session 1------|   (gap >15m)   |--session 2--|
session length is data-driven, not fixed
```

Use session windows for activity grouping: sessions, visits, bursts. The Programming Guide's "window operations on event time" and "types of time windows" sections specify all three with diagrams.

---

## 6. The Structured Streaming micro-batch model

Spark Structured Streaming is, by default, a **micro-batch** engine. It does *not* process records one at a time. Instead it runs a tight loop:

1. Wake up on the trigger (default: as soon as the previous batch finishes).
2. Ask each source what new data is available since the last committed offset. For Kafka, that is "what offsets are now available beyond what batch *N−1* committed."
3. Plan and run a **normal Spark batch job** over exactly that slice of data — the same Catalyst plan, the same executors, the same shuffle.
4. Update the **state store** with new partial aggregates; advance the watermark.
5. Write the output via the sink; **commit** the batch (record the offset range and state in the checkpoint).
6. Repeat.

So a streaming query is literally *a sequence of small batch jobs*, each over a slice of the stream, with state carried forward between them in the state store and progress recorded in the checkpoint. This is why your Week-7 batch knowledge transfers directly: each micro-batch *is* a Week-7 batch job. It is also why Spark's streaming latency floor is roughly the micro-batch interval — you cannot react to an event faster than the next batch boundary. (Spark also has a "Continuous Processing" experimental mode for sub-batch latency, but it is limited and rarely used; the micro-batch model is what production runs.) Contrast this with Flink in Lecture 3, which processes each record as it arrives with no batch boundary.

You observe the loop through `query.lastProgress` — a JSON blob per batch with `batchId`, `numInputRows`, `inputRowsPerSecond`, `processedRowsPerSecond`, the source offsets, the sink, and per-state-operator metrics including the current watermark and `numRowsDroppedByWatermark`. The Spark UI's Structured Streaming tab (`http://localhost:4040`) visualizes the same. Reading this JSON *is* how you debug a streaming job — there is no other window into what it's doing.

---

## 7. readStream from the Week-8 Kafka topic

Now the code that connects this week to last week. Reading the `clickstream` topic is `spark.readStream.format("kafka")` with the bootstrap servers and topic, per the Structured Streaming + Kafka integration guide. The source returns a fixed schema: `key`, `value` (both binary), `topic`, `partition`, `offset`, `timestamp` (the Kafka ingestion time), `timestampType`. Your event fields live inside the `value` bytes; you must deserialize them.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark = (SparkSession.builder
         .appName("week09-clickstream-reader")
         # spark-sql-kafka is provided via --packages in docker-compose; pinned to Spark 3.5.1
         .getOrCreate())

raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")   # Week-8 broker, Docker network name
       .option("subscribe", "clickstream")
       .option("startingOffsets", "latest")               # or "earliest" to replay from the start
       .load())
```

`startingOffsets` controls where a *fresh* query (no checkpoint) begins: `"latest"` to read only new events, `"earliest"` to replay the whole topic. **Once a checkpoint exists, `startingOffsets` is ignored** — the query resumes from the committed offset. That is the replayability that underpins exactly-once (Lecture 2), and forgetting it is the source of the "I deleted my checkpoint and got duplicates" footgun.

### 7.1 Deserializing the payload

The Week-8 producer wrote Avro with a Confluent Schema Registry envelope (a magic byte + 4-byte schema id, then the Avro body). Two paths:

**Avro via `from_avro`** (the production path) — Spark's `from_avro` decodes Confluent-framed Avro when you strip the 5-byte envelope and supply the schema, available through the `spark-avro` package:

```python
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import expr

# Confluent envelope = 1 magic byte + 4-byte schema id, then the Avro body.
# Strip the first 5 bytes before decoding with the registered schema string.
avro_schema = open("/opt/schemas/clickstream.avsc").read()
decoded = raw.select(
    from_avro(expr("substring(value, 6, length(value)-5)"), avro_schema).alias("e")
).select("e.*")
```

**JSON fallback** (simpler for the lab if your Week-8 producer also writes a JSON topic) — `from_json` with an explicit schema:

```python
schema = StructType([
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("page", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", LongType()),       # epoch millis
    StructField("processing_ts", LongType()),
])
decoded = (raw.select(from_json(col("value").cast("string"), schema).alias("e"))
              .select("e.*")
              .withColumn("event_ts", (col("event_ts") / 1000).cast("timestamp")))
```

Either way you end up with a DataFrame that has a proper `event_ts` **timestamp** column — that is the column you watermark and window on. Casting epoch millis to a `timestamp` is mandatory: `window()` and `withWatermark()` require a `TimestampType`, not a long.

### 7.2 Apply the watermark and a trivial sink

```python
events = decoded.withWatermark("event_ts", "10 minutes")

query = (events.writeStream
         .format("console")
         .option("truncate", "false")
         .outputMode("append")          # raw rows, no aggregation -> append is fine
         .start())
query.awaitTermination()
```

Run this and you will see batches of parsed events print to the console as they arrive. `awaitTermination()` blocks forever — that is correct; a stream does not end. Stop it with Ctrl-C or, in a notebook, `query.stop()`. Exercise 1 is exactly this pipeline; you fill in the watermark and the deserialization.

---

## 8. Summary

- **Streaming is batch's hard generalization.** Batch is the special case where the window is "all of time" and you wait until the end before emitting. The mechanisms are isomorphic; the hard part is that the dataset is unbounded, so boundaries must be inferred.
- **Three clocks.** Event time (when it happened, intrinsic, reproducible), processing time (when the engine saw it, run-dependent, wrong for analytics), ingestion time (when it entered the system). Window by event time for correctness.
- **The watermark** is a running lower bound on event time: `max(event_time) − delay`. It closes windows, bounds state, and classifies late data — one concept, three jobs. It *is* the Week-3 high-water mark, made continuous and computed by the engine.
- **Late data** within the watermark is folded in automatically; beyond the watermark it is dropped (and counted in `numRowsDroppedByWatermark`).
- **Windows:** tumbling (fixed, non-overlapping, one window per event), sliding (fixed size, overlapping, multiple windows per event), session (gap-defined, variable size, activity grouping).
- **Micro-batch model:** a streaming query is a sequence of small batch jobs with state carried in the state store and progress in the checkpoint; latency floor ≈ trigger interval. Observe it via `lastProgress` and the Spark UI.
- **`readStream.format("kafka")`** reads the Week-8 topic; deserialize the Avro/JSON `value`, cast `event_ts` to a timestamp, `withWatermark`, then sink. `startingOffsets` only applies to a fresh query — once a checkpoint exists it resumes from the committed offset.

Next: Lecture 2 turns these parsed, watermarked events into a stateful windowed aggregate, picks the right output mode, and writes it exactly-once into the lakehouse.
