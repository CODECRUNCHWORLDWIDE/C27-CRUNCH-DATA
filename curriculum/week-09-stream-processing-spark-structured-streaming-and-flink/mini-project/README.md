# Mini-Project — Streaming Clickstream Aggregate to the Lakehouse, Spark and Flink

> **Estimated time:** ~11 hours across the week (Wed–Sun in the schedule).
> **Prerequisites:** Exercises 1–3 and both challenges; Week-8 Kafka `clickstream` topic; Week-6 MinIO + Iceberg/Delta; PyFlink 1.18.1.
> **Citations:** Spark Structured Streaming Programming Guide (<https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html>); Kafka integration (<https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html>); Delta streaming (<https://docs.delta.io/latest/delta-streaming.html>); Iceberg Spark streaming (<https://iceberg.apache.org/docs/latest/spark-structured-streaming/>); PyFlink Table API (<https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/table/intro_to_table_api/>); Flink fault tolerance (<https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/fault_tolerance/>).

## The capstone

Ship one repository that does the whole week, end to end, twice:

1. A **Spark Structured Streaming** job consuming the Week-8 `clickstream` topic, applying an
   event-time watermark, computing a windowed aggregate, **handling an injected late event**,
   and writing **exactly once** into an Iceberg/Delta lakehouse table the dashboard can query.
2. The **same aggregate in PyFlink**, with checkpointing, producing the same counts.
3. A **comparison write-up** (`PERF.md`) backed by measured latency, throughput, and
   recovery-time-after-kill numbers, plus a defended engine recommendation.

This is the streaming-lakehouse pattern, built and benchmarked. It is the closer of the
streaming arc; Week 10 will wrap a data-quality gate around the table you produce here.

## Topology

```
                          Week-8 producer (~1,000 ev/s, 0-8 min injected lateness)
                                          │
                                   Kafka topic: clickstream
                                   (3 partitions, keyed by user_id, Avro/JSON)
                                          │
                  ┌───────────────────────┴───────────────────────┐
                  ▼                                                 ▼
   ┌──────────────────────────────┐                 ┌──────────────────────────────┐
   │ SPARK Structured Streaming    │                 │ FLINK (PyFlink Table API)     │
   │ readStream(kafka)             │                 │ CREATE TABLE ... WATERMARK FOR │
   │ withWatermark("event_ts",10m) │                 │ TUMBLE(..., 5 min)            │
   │ window(event_ts, 5 min) count │                 │ GROUP BY window_start,end,page │
   │ foreachBatch -> Delta MERGE   │                 │ checkpointing on (barriers+2PC)│
   │ checkpointLocation (MinIO)    │                 │                               │
   └──────────────┬────────────────┘                 └──────────────┬────────────────┘
                  ▼                                                 ▼
        Iceberg/Delta table on MinIO                       print / Iceberg sink
        lakehouse.clickstream.page_counts                  (counts must MATCH Spark)
        one row per (window, page)
                  │
                  ▼
        DuckDB / dashboard SELECTs it (the shared truth)
```

## Functional requirements

- **F1 — Source.** Read `clickstream` via `spark.readStream.format("kafka")` with
  `subscribe` and a fixed `startingOffsets` for reproducibility; deserialize the payload
  (Avro via `from_avro` after stripping the Confluent envelope, or JSON via `from_json`);
  cast `event_ts` (epoch millis) to a `TimestampType`.
- **F2 — Watermark.** Apply `withWatermark("event_ts", "10 minutes")`, justified against the
  producer's configured lateness. Document the completeness/latency trade the choice encodes.
- **F3 — Windowed aggregate.** A 5-minute tumbling count of events per page over event time
  (`groupBy(window(col("event_ts"),"5 minutes"), col("page")).count()`), column `event_count`.
- **F4 — Exactly-once lakehouse sink.** Write via `outputMode("update")` + `foreachBatch`
  doing a Delta `MERGE` (or Iceberg `MERGE INTO`) keyed on `(window_start, window_end, page)`,
  **overwriting** the count, with a `checkpointLocation` on MinIO. The table must be queryable
  by a second engine (DuckDB or a fresh Spark session).
- **F5 — Late-event handling, proven.** Inject one event whose `event_ts` falls in an open
  window and is newer than the watermark; show (progress JSON + before/after query) that it
  folds into the correct event-time window. Inject one event older than the watermark; show
  it is dropped (`numRowsDroppedByWatermark` increments, count unchanged).
- **F6 — Exactly-once recovery.** Kill the job mid-run and restart against the same checkpoint;
  prove the table has no duplicated or lost windows.
- **F7 — Flink equivalent.** Implement the same aggregate in PyFlink (Table API, `WATERMARK
  FOR` + `TUMBLE`) with checkpointing enabled; its counts must match the Spark table on the
  same replayed data.
- **F8 — Comparison.** Produce `PERF.md` with measured numbers and a defended recommendation.

## Deliverables

```
mini-project-submission/
├── README.md                  # how to run, the watermark + window choices, the topology
├── docker-compose.yml         # Kafka (wk8) + Schema Registry + MinIO (wk6) + Spark + Flink
├── spark/
│   └── streaming_job.py       # F1-F6: the full Spark streaming-lakehouse job
├── flink/
│   └── flink_job.py           # F7: the PyFlink equivalent
├── jars/                      # Flink Kafka/Avro connector jars (gitignored if large)
├── evidence/
│   ├── progress_within_watermark.json   # F5: late event folded in (numRowsDroppedByWatermark=0)
│   ├── progress_too_late.json           # F5: too-late event dropped (numRowsDroppedByWatermark=1)
│   ├── table_before.txt / table_after.txt   # before/after the late event
│   └── recovery.txt                     # F6: kill + restart, table consistent
└── PERF.md                    # F8: latency, throughput, recovery time, engine recommendation
```

Commit the code, compose file, evidence, and PERF.md. **Gitignore** the checkpoint
directories, the MinIO data volume, the connector jars if bulky, and any local Kafka logs.

## PERF.md — what to measure and report

`PERF.md` is the heart of the comparison; fill it with **numbers you measured**, not adjectives:

- **Latency.** For both engines, the wall-clock gap between an event's `event_ts` and the
  moment its window's count is emitted at the sink. Report **median and p95**. Expect Flink
  markedly lower (true streaming vs micro-batch, Lecture 3 §4).
- **Throughput.** Sustained `processedRowsPerSecond` (Spark `lastProgress`) and the Flink
  equivalent at the lab's ~1,000 ev/s, plus the max throughput each sustains if you crank the
  producer.
- **Recovery time after kill.** Kill each job mid-run; measure restart-to-caught-up and
  confirm exactly-once (no double-count). State the mechanism each used: Spark = checkpointed
  offsets + idempotent `MERGE`; Flink = asynchronous barrier snapshots (Chandy–Lamport) + 2PC.
- **State and checkpoint size.** The state-store size (bounded by the watermark — should be
  tiny) and the checkpoint directory size for each engine.
- **Engine recommendation.** One paragraph: which engine you would run for *this* dashboard
  workload (latency need is minutes; one-engine simplicity; `AvailableNow`/`foreachBatch`
  reuse), and which for a hypothetical sub-second fraud-blocking workload — each justified by
  a measured number, per Lecture 3 §6.

## Grading rubric (100 points)

| Criterion | Points |
|---|---|
| F1–F3 Spark source + watermark + windowed aggregate run correctly | 20 |
| F4 exactly-once Delta/Iceberg sink (MERGE keyed on window, overwrite, checkpoint) | 20 |
| F5 late-event handling proven both ways (folded-in and dropped) with evidence | 15 |
| F6 kill + restart recovery proven exactly-once | 10 |
| F7 PyFlink equivalent runs and produces matching counts | 15 |
| F8 PERF.md with real measured latency/throughput/recovery + defended recommendation | 15 |
| README + reproducible `docker compose up` + clean gitignore | 5 |

## Tips

- Develop with `trigger(availableNow=True)` for fast drain-and-stop iteration; switch to a
  live `processingTime` trigger only when proving the within-watermark late-event fold-in,
  which needs an open window.
- The `MERGE` **overwrites** the count; it does not add. Adding double-counts on replay — the
  subtlest exactly-once bug in the week (SOLUTIONS, Exercise 3).
- Never delete a checkpoint you want exactly-once from; it is the query's memory of committed
  offsets. Use a fresh checkpoint dir to genuinely re-run from scratch.
- Make the Spark and Flink counts comparable by replaying the **same** topic data from
  `earliest` into both, with identical watermark (10 min) and window (5 min) settings.
