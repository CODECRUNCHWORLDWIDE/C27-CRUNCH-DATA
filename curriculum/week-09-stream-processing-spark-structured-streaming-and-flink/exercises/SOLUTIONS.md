# Week 9 — Exercise Solutions

Worked solutions for the three streaming exercises. Each shows **what success looks like**
(the console / streaming-progress output you should see), the **full annotated solution**,
**what the watermark gating looks like**, and **common pitfalls**. Run the exercises
yourself first — the value is in watching the watermark gate emission and the late event
fold into the right window, which a transcript cannot give you.

All output below was produced against the lab `docker-compose.yml` (Spark 3.5.1, Delta 3.1.0,
Kafka 3.7, MinIO) with the Week-8 producer running at ~1,000 events/second across ten pages
(`/home`, `/search`, `/product`, `/cart`, `/checkout`, `/account`, `/help`, `/blog`, `/login`,
`/logout`) and a configured 0–8 minute event-time lateness injection.

---

## Exercise 1 — readStream from Kafka and watermark

### What success looks like

The console sink prints a batch of parsed events each micro-batch. The struct is flattened,
`event_ts` is a real timestamp (not a long), and rows keep arriving:

```
-------------------------------------------
Batch: 1
-------------------------------------------
+--------+-----------+---------+----------+-------------------+-------------------+
|user_id |session_id |page     |event_type|event_ts           |processing_ts      |
+--------+-----------+---------+----------+-------------------+-------------------+
|u-004182|s-0091abf3 |/product |view      |2026-06-19 09:03:11|2026-06-19 09:03:12|
|u-007710|s-00b2c1de |/search  |view      |2026-06-19 09:03:09|2026-06-19 09:03:12|
|u-001934|s-00f7a002 |/checkout|click     |2026-06-19 09:02:58|2026-06-19 09:03:13|
...
+--------+-----------+---------+----------+-------------------+-------------------+
only showing top 20 rows
```

A sanity check on `query.lastProgress` (from a second terminal or a notebook) shows the
source advancing and the rate:

```json
{
  "batchId": 4,
  "numInputRows": 4021,
  "inputRowsPerSecond": 1005.2,
  "processedRowsPerSecond": 8800.4,
  "sources": [{
    "description": "KafkaV2[Subscribe[clickstream]]",
    "startOffset": {"clickstream": {"0": 12010, "1": 11988, "2": 12002}},
    "endOffset":   {"clickstream": {"0": 13352, "1": 13330, "2": 13339}}
  }],
  "sink": {"description": "org.apache.spark.sql.execution.streaming.ConsoleTable$"}
}
```

The `startOffset`/`endOffset` per partition is the replayable-source machinery (Lecture 1 §7,
Lecture 2 §4) — each batch records exactly which Kafka offsets it covered.

### The completed solution

The exercise file is already runnable; the "Step N" blanks are filled inline. The two lines
that matter:

```python
# Step 3-4: parse JSON value, flatten, cast epoch-millis event_ts -> timestamp
events = (
    raw.select(from_json(col("value").cast("string"), event_schema()).alias("e"))
    .select("e.*")
    .withColumn("event_ts", (col("event_ts") / 1000).cast("timestamp"))
)

# Step 5: the event-time watermark — the streaming high-water mark
watermarked = events.withWatermark("event_ts", "10 minutes")
```

### The Avro variant (if your Week-8 topic is Confluent Avro)

If you have no JSON mirror topic, strip the 5-byte Confluent envelope and decode with
`from_avro`:

```python
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import expr

avro_schema = open("/opt/schemas/clickstream.avsc").read()   # the Week-8 registered schema
events = (
    raw.select(from_avro(expr("substring(value, 6, length(value)-5)"), avro_schema).alias("e"))
       .select("e.*")
       .withColumn("event_ts", (col("event_ts") / 1000).cast("timestamp"))
)
```
Add `org.apache.spark:spark-avro_2.12:3.5.1` to `--packages`. The `substring(value, 6, ...)`
drops the 1-byte magic + 4-byte schema id; from there it is identical to the JSON path.

### Common pitfalls

- **Forgetting to cast `event_ts`.** Leaving it a `long` makes `window()` and `withWatermark()`
  raise an analysis error — both require a `TimestampType`. Symptom: `cannot resolve ... due to
  data type mismatch`. Fix: divide millis by 1000 and `.cast("timestamp")`.
- **`startingOffsets` with no effect.** If a checkpoint already exists, `startingOffsets` is
  ignored and the query resumes from the committed offset. To truly replay from the start,
  use a fresh checkpoint directory.
- **Expecting the query to return.** `awaitTermination()` blocks forever — that is correct.
  Stop with Ctrl-C; in a notebook use `query.stop()`.

---

## Exercise 2 — windowed aggregate and output modes

### What success looks like — `update` mode

Each batch emits the `(window, page)` rows whose counts **changed**, and the same window
reprints with a higher count as more events land in it:

```
-------------------------------------------
Batch: 6
-------------------------------------------
+------------------------------------------+---------+-----------+
|window                                    |page     |event_count|
+------------------------------------------+---------+-----------+
|{2026-06-19 09:00:00, 2026-06-19 09:05:00}|/checkout|412        |
|{2026-06-19 09:00:00, 2026-06-19 09:05:00}|/product |1190       |
|{2026-06-19 09:05:00, 2026-06-19 09:10:00}|/checkout|58         |
+------------------------------------------+---------+-----------+
```

Run it again a batch later and `/checkout` `09:00–09:05` reprints as e.g. `431` — the running
count, re-emitted because it changed. Update mode emits **deltas**, so the sink must upsert
(overwrite), not append, or it accumulates stale duplicate rows.

### What success looks like — `append` mode

In append mode, **nothing prints for a window until the watermark passes its end.** With a
5-minute window and a 10-minute watermark, the `09:00–09:05` window's row appears only once
the watermark (max event_ts − 10 min) is past `09:05` — i.e. once event time reaches ~`09:15`.
Then it prints **once**, final:

```
-------------------------------------------
Batch: 19            <- much later than batch 6 above, in event time
-------------------------------------------
+------------------------------------------+---------+-----------+
|window                                    |page     |event_count|
+------------------------------------------+---------+-----------+
|{2026-06-19 09:00:00, 2026-06-19 09:05:00}|/checkout|447        |   <- the FINAL count, emitted once
|{2026-06-19 09:00:00, 2026-06-19 09:05:00}|/product |1268       |
+------------------------------------------+---------+-----------+
```

That window never prints again. This is the watermark **gating emission**: append waits for
"final," and the watermark is the definition of final.

### What the watermark gating looks like in the progress JSON

Watch the state operator across batches. The `watermark` advances behind the max event time,
and a window only emits in append once `watermark >= window.end`:

```json
{
  "batchId": 19,
  "eventTime": {
    "max": "2026-06-19T09:15:42.000Z",
    "watermark": "2026-06-19T09:05:42.000Z"        // max - 10 min; now past 09:05, so 09:00-09:05 finalizes
  },
  "stateOperators": [{
    "numRowsTotal": 18,                              // open windows still in state
    "numRowsUpdated": 2,
    "numRowsRemoved": 10,                            // 09:00-09:05 keys evicted after emitting
    "numRowsDroppedByWatermark": 0
  }]
}
```

`watermark` crossing `09:05:00` is exactly when `numRowsRemoved` jumps and the `09:00–09:05`
rows finalize and leave the state store.

### Common pitfalls

- **Append without a watermark = `AnalysisException`.** Remove the `withWatermark` and run
  with `append` and Spark refuses:
  `Append output mode not supported when there are streaming aggregations on streaming
  DataFrames/DataSets without watermark`. This *is* the rule (Lecture 2 §2.3) — append needs
  a definition of "final," and only the watermark provides it.
- **"My append output is empty."** It is not broken — you have not waited long enough in
  *event time*. With a 10-minute watermark, the first window finalizes 10 minutes (event time)
  after its end. Feed event-time faster (set the producer's timestamps to advance quickly) or
  shorten the watermark to see output sooner.
- **Update mode into an append-only sink.** Update re-emits a window's count repeatedly; an
  append-only sink then holds many rows for the same `(window, page)` with different counts.
  Update mode must pair with an upsert sink (Exercise 3's `MERGE`).
- **Unbounded state with no watermark.** A windowed aggregate without a watermark in
  `complete`/`update` mode never evicts old windows; `numRowsTotal` grows forever and the
  job eventually OOMs. The watermark is what bounds state.

---

## Exercise 3 — exactly-once into the lakehouse, with a late event

### What success looks like — the table after a clean load

After `trigger(availableNow=True)` drains the topic and the `MERGE` lands the aggregate, the
Delta table holds one row per `(window, page)`:

```
=== page_counts after exactly-once load ===
+-------------------+-------------------+---------+-----------+
|window_start       |window_end         |page     |event_count|
+-------------------+-------------------+---------+-----------+
|2026-06-19 09:00:00|2026-06-19 09:05:00|/checkout|447        |
|2026-06-19 09:00:00|2026-06-19 09:05:00|/product |1268       |
|2026-06-19 09:00:00|2026-06-19 09:05:00|/search  |903        |
|2026-06-19 09:05:00|2026-06-19 09:10:00|/checkout|461        |
...
+-------------------+-------------------+---------+-----------+
```

### Proving exactly-once on a re-run

Re-run the script **without deleting the checkpoint**. Because the checkpoint records the
committed offsets, the second run reads **no new data** (the topic was fully drained), the
`MERGE` runs zero or trivially-idempotent updates, and the table is **byte-for-byte the same**.
The `numInputRows` for the new run is 0. That is exactly-once at work: reprocessing changes
nothing.

If you instead **delete the checkpoint** and re-run with `startingOffsets="earliest"`, the
whole topic is re-read and re-merged. Because the sink is a `MERGE` that *overwrites* the count
(idempotent), the table still ends correct — but note that an *append*-mode native sink would
have **double-written** every row. This is the practical demonstration of why the idempotent
sink is a required factor: it is what makes a re-read survivable.

### What the late-event handling looks like

1. Initial load: `('/checkout', 09:00–09:05) = 447`.
2. Inject one `/checkout` event with `event_ts = 09:02:41` (inside the window) arriving now,
   with the current max event time around `09:16` so the watermark is `09:06` — and `09:02:41`
   is **older than the latest events but the window has not yet been evicted**... except in
   this drained-topic case the window was already finalized. To see the fold-in cleanly, run
   with `outputMode("update")` and a *live* trigger (`processingTime="10 seconds"`) while the
   producer is still emitting around `09:0x`, so the `09:00–09:05` window is still open. Then:

```
# progress JSON for the batch that read the injected late event
"eventTime": { "max": "2026-06-19T09:04:30Z", "watermark": "2026-06-19T08:54:30Z" }
# 09:02:41 > watermark 08:54:30  -> WITHIN the watermark -> folded in
"stateOperators": [{ "numRowsUpdated": 1, "numRowsDroppedByWatermark": 0 }]
```

   and the table updates:

```
('/checkout', 09:00-09:05)  447 -> 448      # the late event folded into the CORRECT event-time window
```

   The event landed in `09:00–09:05` because that is its **event time**, not in whatever window
   was open in processing time. That is the whole point of event-time windowing.

3. Now inject an event with `event_ts = 08:40:00` (older than the watermark `08:54:30`). Re-run:

```
"stateOperators": [{ "numRowsUpdated": 0, "numRowsDroppedByWatermark": 1 }]
```

   The count does **not** change and `numRowsDroppedByWatermark` increments. The event was too
   late; its window had been closed and evicted. Spark drops it silently (no error) and counts
   it. To capture such events you would lengthen the watermark or use Flink's side outputs
   (Lecture 3 §2).

### The annotated `MERGE` (why overwrite, not add)

```python
.whenMatchedUpdate(set={"event_count": "s.event_count"})   # OVERWRITE with the latest running count
```

Update mode re-emits a window's **full running count** every time it changes — not a delta of
+1. So the right `MERGE` action is to **overwrite** the target's count with the source's count.
Replaying the same batch overwrites with the same value: a no-op → idempotent → exactly-once.
If you wrote `event_count = t.event_count + s.event_count` instead, a replay would **add the
running count again**, double-counting. This is the single subtlest exactly-once bug in the
week, and it is the Week-3 "upsert overwrites, it does not accumulate" lesson restated.

### Common pitfalls

- **Deleting the checkpoint to "start fresh."** This discards the committed-offset memory.
  The query re-reads from `startingOffsets` and reprocesses everything. Idempotent `MERGE`
  survives it; an append sink double-writes. Treat the checkpoint as production state.
- **Sharing one checkpoint between two queries.** Each `writeStream` needs its **own**
  `checkpointLocation`. Sharing corrupts both queries' offset/commit logs.
- **`MERGE` that adds instead of overwrites.** Double-counts on replay (above).
- **s3a misconfiguration.** Missing `path.style.access=true` or the wrong endpoint makes the
  Delta write fail to reach MinIO. Symptom: `UnknownHostException` on a bucket-style hostname.
  Fix: path-style access + `http://minio:9000` endpoint (see `configure_s3a`).
- **Schema drift between the table and the batch.** If `flat` has a different column set than
  the existing Delta table, the `MERGE` fails. Keep the `selectExpr` projection stable, or
  enable schema evolution explicitly (`spark.databricks.delta.schema.autoMerge.enabled`).
- **Iceberg instead of Delta.** The same job works against Iceberg: swap the `MERGE` for an
  `spark.sql("MERGE INTO lakehouse.clickstream.page_counts t USING ... ")` inside
  `foreachBatch`, or use the Iceberg native streaming sink `.format("iceberg").toTable(...)`
  for the append-only case (Lecture 2 §5.1).

---

If all three exercises run and you have watched (a) the watermark gate append emission and
(b) the late event fold into the correct event-time window while a too-late event is dropped,
you have the core streaming model in your hands. Move on to the [challenges](../challenges/)
and then the [mini-project](../mini-project/README.md).
