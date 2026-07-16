# Lecture 2 — Windowed Aggregation, Output Modes, and Exactly-Once Sinks

> **Duration:** ~3 hours of reading + a 45-minute PySpark sanity check.
> **Prerequisites:** Lecture 1 (watermarks, windows, the micro-batch model, `readStream`), Week 3 (idempotent `MERGE` upsert), Week 6 (Iceberg/Delta on MinIO).
> **Citations:** Spark Structured Streaming Programming Guide — output modes (<https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes>), fault tolerance / checkpointing (<https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing>), triggers (<https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers>), `foreachBatch` (<https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch>); Delta Lake streaming (<https://docs.delta.io/latest/delta-streaming.html>) and `foreachBatch` upsert (<https://docs.delta.io/latest/delta-update.html#upsert-from-streaming-queries-using-foreachbatch>); Iceberg Spark Structured Streaming (<https://iceberg.apache.org/docs/latest/spark-structured-streaming/>).
> **Outcome:** You can build a stateful windowed aggregate, choose the legal output mode, explain what a checkpoint contains, state the exactly-once multiplication and implement it with `foreachBatch` + a `MERGE` into a lakehouse table, pick the right trigger, and argue when a 15-minute micro-batch beats a true stream.

Lecture 1 left us with parsed, watermarked `clickstream` events. This lecture turns them into a windowed count, gets that count *exactly once* into an Iceberg/Delta table, and explains every guarantee in terms of Week 3's batch upsert.

> **The one sentence to carry out of this lecture:**
> **Exactly-once = replayable source × checkpoint × idempotent sink.** Kafka offsets give replayability, the checkpoint records what each batch covered and holds the state, and a `MERGE` keyed on `(window, page)` makes the write safe to repeat. Drop any factor and you fall back to at-least-once or at-most-once. This is the same sentence Week 3 used about batch retries — a retried upsert on a natural key is a no-op.

---

## 1. Stateful aggregation and the state store

A windowed aggregate is **stateful**: to compute "count of events per page per 5-minute window" the engine must remember, *between micro-batches*, the running count for every `(window, page)` pair that is still open. Those running counts live in the **state store** — a keyed key/value store local to each executor, keyed by the grouping key (here `(window, page)`), holding the partial aggregate (here the count so far).

The micro-batch loop touches the state store every batch: read the current partial aggregate for each key in the new data, fold in the new rows, write back the updated aggregate, and emit output according to the output mode. The watermark is what bounds the store: once a window's end is older than the watermark, the engine *evicts* that window's keys from the state store and never touches them again. Without a watermark the store grows without bound, because the engine can never prove a window is finished. **This is why a windowed aggregate without a watermark leaks memory and eventually OOMs** — a classic Week-9 mistake.

The default state-store backend keeps state on the JVM heap (`HDFSBackedStateStoreProvider`); for large state Spark ships a RocksDB backend (`spark.sql.streaming.stateStore.providerClass=...RocksDBStateStoreProvider`) that spills to local disk so state can exceed heap. For the lab's tiny state (a few hundred keys) the default is fine; the knob matters at production scale and is worth knowing exists.

```python
from pyspark.sql.functions import window, col, count

agg = (events                                   # watermarked DataFrame from Lecture 1
       .groupBy(window(col("event_ts"), "5 minutes"), col("page"))
       .agg(count("*").alias("event_count")))
# agg schema: window (struct<start,end>), page (string), event_count (long)
```

> **Week-3 tie-back.** Week 3's incremental aggregate kept a running total in a Postgres summary table that you updated each run. The state store *is* that summary table — held in the engine, keyed by window, evicted by the watermark instead of by a manual cleanup job.

---

## 2. Output modes: append, update, complete

The output mode tells Spark *which rows of the result table to push to the sink each micro-batch*. There are three, and the most common first mistake is choosing the wrong one. The Programming Guide's "output modes" section is the authority; here is the working model.

### 2.1 Complete mode

Emit the **entire** result table every micro-batch — every window, every key, every batch, whether or not it changed.

- **No watermark required** — nothing is ever finalized or dropped, so there is no need for a "this is final" definition.
- **State is never evicted** — the engine must keep the full result to re-emit it, so state grows without bound. Only feasible for small aggregates (a handful of keys).
- **Sink must accept a full overwrite** each batch.
- **Week-3 analog:** a full table rebuild — `TRUNCATE` + `INSERT` the whole summary every run.

```python
agg.writeStream.outputMode("complete").format("console").start()
```

### 2.2 Update mode

Emit only the rows that **changed** since the previous micro-batch — if a window's count went from 3 to 4, emit `(window, page, 4)`.

- **Watermark optional but recommended** — with a watermark, closed windows are evicted and state stays bounded; without one, state grows.
- **Efficient** — only deltas flow to the sink.
- **Sink must support upsert** — the same `(window, page)` row is emitted repeatedly with a growing count, so the sink must overwrite, not append, or you get duplicates with stale counts.
- **Week-3 analog:** the idempotent upsert — `MERGE` the changed rows on the natural key.

```python
agg.writeStream.outputMode("update").foreachBatch(upsert_fn).option("checkpointLocation", CKPT).start()
```

This is the mode the lab uses for the lakehouse sink, because a `MERGE` on `(window, page)` is exactly the upsert update mode wants.

### 2.3 Append mode

Emit each result row **exactly once and never again**, only *after* the watermark guarantees the row will not change.

- **Watermark REQUIRED for an aggregation.** This is the rule to memorize: **append on an aggregate is only legal with a watermark**, because append needs a definition of "this row is final," and the watermark is that definition. A window's count is only appended once its end passes the watermark — until then the window is still open and its count could change, so nothing is emitted. Try append without a watermark on an aggregate and Spark throws `AnalysisException: Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark`.
- **Latency = watermark delay** — you wait the full watermark delay before a window's row appears, because that is when it is finalized.
- **Sink can be insert-only** — each row is emitted once, so an append-only sink (plain `INSERT`, an immutable file) is safe.
- **Week-3 analog:** insert-only incremental load — append the new finalized rows, never touch old ones.

```python
agg.writeStream.outputMode("append").format("parquet").option("path", OUT).option("checkpointLocation", CKPT).start()
```

### 2.4 The legality matrix, condensed

| Query type | append | update | complete |
|---|---|---|---|
| No aggregation (raw rows) | yes | yes (= append) | no |
| Aggregation **with** watermark | yes (emits once, finalized) | yes (emits deltas) | yes (re-emits all) |
| Aggregation **without** watermark | **no** (AnalysisException) | yes (state unbounded) | yes (state unbounded) |

The single most useful row: **aggregation + append + no watermark = error**. If Spark rejects your query with that message, you forgot the watermark.

---

## 3. Checkpointing: what's in the directory

A **checkpoint** is a directory (we put it on MinIO: `s3a://lakehouse/checkpoints/clickstream-agg/`) that holds everything Spark needs to resume a query *exactly* after a crash or restart. The "recovering from failures with checkpointing" section specifies it. The contents:

- **`offsets/`** — a write-ahead log: *before* running batch *N*, Spark writes the source offset range batch *N* will cover. This is what makes the source replayable — on restart Spark reads the last planned offsets and re-runs that batch over the same Kafka records.
- **`commits/`** — one file per *successfully committed* batch. A batch is durable only once its commit file exists. On restart, the highest commit tells Spark which batch finished; anything planned in `offsets/` but not in `commits/` is re-run.
- **`state/`** — periodic snapshots and deltas of the state store (the running aggregates), so a restart resumes with the partial counts intact rather than recomputing from offset zero.
- **`metadata`** — the query id and configuration.

Two iron rules about checkpoints, both load-bearing:

1. **The checkpoint is bound to one query and one query plan.** Change the aggregation, the output mode, or the schema in an incompatible way and the old checkpoint can no longer be resumed — you must start a new checkpoint. This is by design: the state layout is plan-specific.
2. **Never delete a checkpoint you want exactly-once from.** Delete it and the query is "fresh" again — it re-reads from `startingOffsets` and re-processes everything, double-counting into the sink. The checkpoint *is* the memory of "what I have already processed." This is the footgun the README flags; the exercises flag it again.

The checkpoint location is set per query: `.option("checkpointLocation", "s3a://lakehouse/checkpoints/clickstream-agg/")`. Each query needs its **own** location — sharing a checkpoint between two queries corrupts both.

---

## 4. Exactly-once = replayable source × checkpoint × idempotent sink

This is the heart of the week. "Exactly-once" does not mean each record is *physically* processed once — a crash forces reprocessing. It means each record's *effect on the output* appears exactly once. That guarantee is a **product of three factors**, and you need all three:

1. **Replayable source.** On restart you must be able to re-read the *same* records batch *N* was meant to process. Kafka is replayable: each record has a durable offset, and the checkpoint's `offsets/` log records which offsets batch *N* covers, so Spark rewinds and re-reads them precisely. (A source that cannot replay — e.g. a socket — can only ever give at-most-once.)
2. **Checkpoint.** The write-ahead offset log + commit log + state, as in §3. This is what lets the engine know exactly where it was and resume there.
3. **Idempotent (or transactional) sink.** The write of batch *N* must be safe to repeat, because a crash between "wrote to sink" and "committed batch *N*" forces Spark to re-run batch *N* and re-write. Two ways to make the write repeat-safe:
   - **Idempotent** — re-writing produces the same result. A `MERGE` keyed on `(window, page)` is idempotent: replaying it overwrites the same rows, no duplicates. (This is Week 3's idempotent upsert.)
   - **Transactional** — the write and the offset commit are atomic, so either both happen or neither. Delta/Iceberg streaming sinks with `txnVersion`/`txnAppId` deduplication give this for the native sink path.

> **The multiplication, made concrete:**
> - replayable ✗ → at-most-once (a crash loses the un-replayable records).
> - idempotent sink ✗ → at-least-once (a crash re-writes, duplicating).
> - all three ✓ → **exactly-once**.

> **Week-3 tie-back, exact.** In Week 3 a failed run was safe to retry *because* the load was idempotent: re-reading from the stored watermark (replayable source) and `MERGE`-ing on the natural key (idempotent sink) meant a retry double-counted nothing. Streaming exactly-once is the same property, with the engine's checkpoint playing the role of your control table. If you understood Week-3 retries, you understand streaming exactly-once.

---

## 5. foreachBatch and the idempotent lakehouse upsert

Most lakehouse sinks need an **upsert**, not a blind append (so a re-run overwrites rather than duplicates). The built-in `writeStream` sinks do not expose `MERGE`. The escape hatch is **`foreachBatch`**: it hands you each micro-batch as an ordinary *static* DataFrame plus its `batchId`, and you do whatever batch operation you like — including a Delta/Iceberg `MERGE`. The Delta docs' "upsert from streaming queries using foreachBatch" recipe is the canonical pattern.

```python
from delta.tables import DeltaTable

TARGET = "s3a://lakehouse/clickstream/page_counts"   # a Delta table on MinIO
CKPT   = "s3a://lakehouse/checkpoints/page_counts"

def upsert_to_delta(micro_batch_df, batch_id):
    """Idempotent MERGE of one micro-batch into the page_counts Delta table.
    Keyed on (window_start, window_end, page) so replaying batch_id is a no-op."""
    # Flatten the window struct so the merge keys are top-level columns.
    flat = (micro_batch_df
            .selectExpr("window.start as window_start",
                        "window.end   as window_end",
                        "page", "event_count"))
    spark = micro_batch_df.sparkSession
    if not DeltaTable.isDeltaTable(spark, TARGET):
        flat.write.format("delta").mode("overwrite").save(TARGET)
        return
    tgt = DeltaTable.forPath(spark, TARGET)
    (tgt.alias("t")
        .merge(flat.alias("s"),
               "t.window_start = s.window_start AND t.window_end = s.window_end AND t.page = s.page")
        .whenMatchedUpdate(set={"event_count": "s.event_count"})   # overwrite, not add -> idempotent
        .whenNotMatchedInsertAll()
        .execute())

query = (agg.writeStream
         .outputMode("update")                 # emit changed windows; MERGE folds them in
         .foreachBatch(upsert_to_delta)
         .option("checkpointLocation", CKPT)    # exactly-once: replay + checkpoint + idempotent MERGE
         .trigger(processingTime="30 seconds")
         .start())
```

The critical detail: the `MERGE` uses `whenMatchedUpdate(set event_count = s.event_count)` — it **overwrites** the count with the latest value, it does not *add*. Because update mode re-emits a window's full running count each time it changes, overwriting is correct and idempotent: replay batch *N* and the same final count is written again, a no-op. If you instead did `event_count = t.event_count + s.event_count` you would double-count on replay — a subtle exactly-once bug.

> **`foreachBatch` is your bridge from streaming back to batch.** Inside it you are holding a normal static DataFrame; everything you learned in Week 7 (and Week 3's `MERGE`) applies. This is the most important API in the week for landing a stream into the lakehouse.

### 5.1 Native streaming sinks (the simpler path)

If you do not need an upsert — append-only, finalized rows — both Delta and Iceberg offer **native streaming sinks** with built-in exactly-once, no `foreachBatch` needed:

```python
# Delta native streaming sink (append-only, exactly-once via txn dedup)
agg.writeStream.format("delta").outputMode("append") \
   .option("checkpointLocation", CKPT).start(TARGET)

# Iceberg native streaming sink
agg.writeStream.format("iceberg").outputMode("append") \
   .option("checkpointLocation", CKPT).toTable("lakehouse.clickstream.page_counts")
```

Use the native append sink when each window's finalized row should be written once (append mode + watermark). Use `foreachBatch` + `MERGE` (update mode) when you want the table to reflect the latest running count of each window, including late updates within the watermark — which is what the lab's dashboard wants, so the lab uses the `MERGE` path.

---

## 6. The streaming-lakehouse pattern

Put §1–§5 together and you have the **streaming-lakehouse pattern**: a stream lands continuously into a table that is *simultaneously* a normal, queryable, ACID analytics table.

```
Kafka clickstream  ──readStream──▶  watermark + window + aggregate
                                          │
                                          ▼  foreachBatch MERGE / native sink (exactly-once)
                                 ┌─────────────────────────┐
                                 │  Iceberg/Delta table     │   ◀── DuckDB queries it
                                 │  page_counts (ACID)      │   ◀── the dashboard SELECTs it
                                 └─────────────────────────┘   ◀── a Week-7 batch job reads it
```

The stream is the *writer*; the lakehouse table is the *shared truth*; everything downstream is a *reader* — and readers see ACID-consistent snapshots, time travel, and schema evolution because Iceberg/Delta provide them. This dissolves the old **Lambda architecture** (a fast "speed layer" for fresh-but-approximate results plus a slow "batch layer" for correct results, with the pain of maintaining two code paths that must agree) into a single continuously-updated table that is both fresh and correct. Delta's and Iceberg's streaming docs both describe a table that is read and written by streaming *and* batch interchangeably; that interchangeability is the whole value. Week 6 built this table; Week 9 makes a stream the writer.

---

## 7. Trigger modes: how often the micro-batch fires

The **trigger** controls when the next micro-batch runs. The "triggers" section lists them:

- **Default (no trigger set)** — fire the next batch as soon as the previous one finishes. Lowest latency Spark offers, highest resource use (always busy), most frequent (tiny) writes to the sink.
- **`Trigger.ProcessingTime("30 seconds")`** — fire a batch every fixed interval; if a batch overruns the interval, the next fires immediately after. Bounds write frequency and resource use; latency ≈ the interval. This is the production default for most jobs.
- **`Trigger.AvailableNow()`** — drain *all* currently-available data across as many internal batches as needed, then **stop**. This is the "streaming as batch" trick: the same query you run continuously, run once on a schedule to catch up and exit. Brilliant for the lab and for Airflow-orchestrated incremental loads — you get streaming's exactly-once and state handling with batch's terminate-and-exit lifecycle.

```python
.trigger(availableNow=True)                # drain everything available, then stop (great for the lab)
.trigger(processingTime="30 seconds")      # fixed micro-batch every 30s
# (no .trigger)                            # as fast as possible
```

`AvailableNow` is how you test a streaming job *without* it running forever: point it at the topic, drain, exit, inspect the output table. The exercises lean on it.

---

## 8. When a 15-minute micro-batch beats a true stream

The heresy that closes the lecture, stated plainly: **most "real-time" requirements are not.** Before reaching for sub-second streaming, ask what consumes the result. If the dashboard refreshes every 15 minutes, a person looks at it twice an hour, and the business decision it feeds happens daily, then sub-second latency is **cost you pay for freshness nobody consumes.**

In that common case, a Structured Streaming job triggered with `Trigger.ProcessingTime("15 minutes")` — or even an Airflow DAG running a plain Week-7 batch job every 15 minutes with a Week-3 watermarked incremental load — is *simpler to operate, cheaper to run, and easier to reason about* than a standing true-streaming system: no always-on cluster, no continuous state to babysit, failures are isolated to one run, and the code is ordinary batch code your whole team already understands.

Choose **true streaming** (Spark default trigger, or Flink) when low latency is a **product requirement**: fraud blocking that must fire before the transaction completes, an alerting pipeline measured in seconds, a feature store feeding online inference. Choose a **15-minute micro-batch** (or hourly batch) when the consumer's cadence is minutes-to-hours, which is most analytics. The right answer is set by the *latency the consumer actually needs*, never by the latency the engine can achieve. Latency you do not need is cost you do pay — and operational complexity you will pay for at 3 a.m.

---

## 9. Summary

- **Stateful aggregation** keeps running partial aggregates in the **state store**, keyed by `(window, page)`; the **watermark evicts** closed windows, bounding state. No watermark on an aggregate ⇒ unbounded state ⇒ OOM.
- **Output modes:** complete (re-emit all, no watermark needed, unbounded state), update (emit deltas, upsert sink, Week-3 upsert analog), append (emit once when finalized, **watermark required** for aggregates, insert-only sink, Week-3 insert analog). Aggregation + append + no watermark = `AnalysisException`.
- **Checkpoint** = offset write-ahead log + commit log + state snapshots + metadata, on MinIO. Bound to one query and plan; never delete one you want exactly-once from, or you re-read and double-count.
- **Exactly-once = replayable source × checkpoint × idempotent sink.** Kafka offsets + checkpoint + a `MERGE` on `(window, page)`. Drop a factor ⇒ at-least-once or at-most-once. Same property as Week-3 idempotent retries.
- **`foreachBatch`** hands you each micro-batch as a static DataFrame; do a Delta/Iceberg `MERGE` keyed on the window — overwrite the count (idempotent), never add (double-counts on replay). Native Delta/Iceberg append sinks exist for the insert-only case.
- **Streaming-lakehouse pattern:** stream writes one Iceberg/Delta table that is also a queryable ACID analytics table; dissolves Lambda's speed/batch split into one continuously-updated table.
- **Triggers:** default (ASAP), `ProcessingTime` (fixed interval), `AvailableNow` (drain-and-stop, streaming-as-batch — the lab's testing trick).
- **A 15-minute micro-batch beats a true stream** whenever the consumer's cadence is minutes-to-hours. Choose true streaming only when low latency is a product requirement.

Next: Lecture 3 builds the *same* windowed count in Apache Flink, a true record-at-a-time engine, and develops a defensible answer to "Spark or Flink?"
