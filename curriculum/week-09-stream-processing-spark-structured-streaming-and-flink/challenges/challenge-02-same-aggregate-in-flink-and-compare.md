# Challenge 2 — The Same Aggregate in Flink, and a Comparison

> **Estimated time:** 2–3 hours.
> **Prerequisites:** Challenge 1 done (you have a working Spark version + numbers); Week-8 Kafka topic live; PyFlink 1.18.1 installed.
> **Citations:** PyFlink Table API intro (<https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/table/intro_to_table_api/>); Flink Kafka SQL connector (<https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/kafka/>); windowing TVFs (<https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/window-tvf/>); Flink fault tolerance / checkpointing (<https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/fault_tolerance/>); time & watermarks (<https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/>).

## Premise

Build the **exact same** windowed count — events per page per 5-minute tumbling window over
event time, 10-minute watermark, reading the same Week-8 `clickstream` topic — in **PyFlink**,
and then write a grounded comparison of Spark Structured Streaming vs Flink on the dimensions
that actually decide engine choice: **latency, semantics, exactly-once mechanism, and
operational complexity.** The deliverable is a working Flink job *plus* a comparison backed by
measured numbers, not adjectives.

## Setup

PyFlink runs from Python with the Kafka SQL connector jar on the pipeline classpath. Add a
Flink service to the Challenge-1 compose, or run PyFlink in a container with the connector:

```yaml
  flink:
    image: apache/flink:1.18.1-scala_2.12-java11
    depends_on: [kafka, schema-registry]
    volumes: ["./:/work"]
    working_dir: /work
    command: ["bash", "-lc", "pip install apache-flink==1.18.1 && tail -f /dev/null"]
```

Download the matching connector jars into `./jars/` (versions must match Flink 1.18.x):
`flink-sql-connector-kafka-3.1.0-1.18.jar` and, for Confluent Avro,
`flink-sql-avro-confluent-registry-1.18.1.jar`. Run:

```bash
docker compose exec flink python /work/challenge02_flink_job.py
```

## Tasks

1. **Build the Flink job (Table API).** Create a streaming `TableEnvironment`
   (`EnvironmentSettings.in_streaming_mode()`), register the Kafka source table with a
   `WATERMARK FOR event_ts AS event_ts - INTERVAL '10' MINUTE`, and run the windowed count
   with a `TUMBLE(TABLE clickstream, DESCRIPTOR(event_ts), INTERVAL '5' MINUTE)` TVF grouped
   by `window_start, window_end, page`. Reference: Lecture 3 §3. Add the connector jars via
   `t_env.get_config().set("pipeline.jars", "file:///work/jars/flink-sql-connector-kafka-...jar")`.
2. **Sink.** For the comparison run, a `print` connector is fine. For the streaming-lakehouse
   parity, sink to Iceberg via the Flink Iceberg connector, or write the result to a second
   Kafka topic that a Spark job lands — document whichever you choose.
3. **Enable checkpointing.** Turn on Flink checkpointing
   (`t_env.get_config().set("execution.checkpointing.interval", "10 s")` and a MinIO/S3
   state backend) so you exercise the exactly-once machinery (barrier snapshots, Lecture 3 §5).
4. **Verify equivalence.** Run the Flink job and the Challenge-1 Spark job over the **same**
   topic data (replay from `earliest`) and confirm they produce the **same counts** per
   `(window, page)`. They must agree — they are computing the same event-time aggregate.
5. **Measure latency.** For both engines, measure the wall-clock gap between an event's
   `event_ts` and the moment its window's count is emitted at the sink (instrument the sink
   with the emit timestamp). Report median and p95 for each. Expect Flink markedly lower.
6. **Measure recovery.** Kill each job mid-run and restart against its checkpoint; measure the
   time from restart to "caught up" and confirm correctness (no double-count). Note the
   mechanism each used (Spark: offset WAL + idempotent sink; Flink: barrier snapshot + 2PC).
7. **Write `COMPARISON.md`.** Fill the table below with *your measured numbers* and a
   paragraph defending which engine you would choose for (a) this dashboard workload and
   (b) a hypothetical sub-second fraud-blocking workload.

## The comparison table to complete

| Dimension | Spark Structured Streaming | Apache Flink |
|---|---|---|
| Processing model | micro-batch (re-planned per trigger) | true record-at-a-time (standing dataflow) |
| Event-time API | `withWatermark("event_ts","10 minutes")` | `WATERMARK FOR event_ts AS event_ts - INTERVAL '10' MINUTE` |
| Window API | `window(col,"5 minutes")` | `TUMBLE(TABLE ..., DESCRIPTOR(event_ts), INTERVAL '5' MINUTE)` |
| Measured median emit latency | _your number_ | _your number_ |
| Measured p95 emit latency | _your number_ | _your number_ |
| Throughput at ~1k ev/s | _your number_ | _your number_ |
| Exactly-once mechanism | checkpointed offsets + idempotent `MERGE` sink | async barrier snapshots (Chandy–Lamport) + 2PC sink |
| Recovery time after kill | _your number_ | _your number_ |
| Operational complexity | reuses the Spark batch stack; one engine | a second runtime to deploy/tune/monitor |
| Best fit | seconds-to-minutes analytics; lakehouse `MERGE`; `AvailableNow` batch reuse | sub-second latency, CEP, large keyed state |

## Acceptance criteria

- A runnable PyFlink job computes the identical 5-minute tumbling event count over the same
  topic, with a `WATERMARK FOR` clause and a `TUMBLE` TVF, and checkpointing enabled.
- The Flink and Spark counts **match** per `(window, page)` on the same replayed data.
- `COMPARISON.md` reports **measured** median/p95 latency, throughput, and recovery time for
  both engines (real numbers, not placeholders), and correctly states each engine's
  exactly-once mechanism.
- A defended recommendation: Spark for the dashboard workload (latency need is minutes,
  one-engine simplicity, `AvailableNow`/`foreachBatch` reuse); Flink for the sub-second
  fraud-blocking workload — each justified by a number from your measurements, per the
  decision framework in Lecture 3 §6.

## Stretch

- Implement the same aggregate in the **PyFlink DataStream API** with a
  `WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(10))` and a
  `TumblingEventTimeWindows`, and add a **side output** that captures events too late even
  for `allowedLateness` — something Spark cannot do (it only drops + counts). Report how many
  late events the side output catches.
- Tune Flink's checkpoint interval and measure its effect on latency and recovery time; find
  the interval that minimizes recovery time without hurting steady-state latency.
