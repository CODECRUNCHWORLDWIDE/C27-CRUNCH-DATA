# Challenge 1 — Windowed Clickstream Aggregate to the Lakehouse

> **Estimated time:** 2–3 hours.
> **Prerequisites:** Exercises 1–3 done; Week-8 Kafka `clickstream` topic live; Week-6 MinIO + Iceberg/Delta catalog up.
> **Citations:** Structured Streaming + Kafka integration (<https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html>); handling late data & watermarking (<https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking>); checkpointing (<https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing>); Delta `foreachBatch` upsert (<https://docs.delta.io/latest/delta-update.html#upsert-from-streaming-queries-using-foreachbatch>); Iceberg Spark streaming (<https://iceberg.apache.org/docs/latest/spark-structured-streaming/>).

## Premise

You will assemble the three exercises into **one production-shaped Structured Streaming job**:
read the Week-8 `clickstream` topic, apply a 10-minute event-time watermark, compute a
5-minute tumbling count of events per page, and write it **exactly once** into an Iceberg (or
Delta) table on MinIO that the Week-6 dashboard can `SELECT` from live. Then you will inject a
deliberately late event and **prove** — with the streaming-progress JSON and a before/after
query — that it folds into the correct event-time window when within the watermark and is
dropped when beyond it.

This is the canonical streaming-lakehouse job (Lecture 2 §6). Everything you build here you
reuse and extend in the mini-project.

## Setup

Bring up the dependencies you already have from prior weeks. A minimal `docker-compose.yml`
that references the Week-8 Kafka stack and the Week-6 MinIO/lakehouse:

```yaml
services:
  kafka:                    # from Week 8
    image: bitnami/kafka:3.7
    environment:
      - KAFKA_CFG_NODE_ID=1
      - KAFKA_CFG_PROCESS_ROLES=broker,controller
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@kafka:9093
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
    ports: ["9092:9092"]

  schema-registry:          # from Week 8 (only needed for the Avro path)
    image: confluentinc/cp-schema-registry:7.6.0
    depends_on: [kafka]
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: PLAINTEXT://kafka:9092
    ports: ["8081:8081"]

  minio:                    # from Week 6
    image: minio/minio:RELEASE.2024-01-16T16-07-38Z
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports: ["9000:9000", "9001:9001"]
    volumes: ["minio-data:/data"]

  spark:                    # from Week 7
    image: apache/spark:3.5.1
    depends_on: [kafka, minio]
    volumes: ["./:/work"]
    working_dir: /work
    entrypoint: ["tail", "-f", "/dev/null"]   # keep alive; spark-submit into it

volumes:
  minio-data:
```

```bash
docker compose up -d
# create the lakehouse bucket once (mc is the MinIO client; or use the console at :9001)
docker run --rm --network <net> minio/mc \
  sh -c "mc alias set local http://minio:9000 minioadmin minioadmin && mc mb -p local/lakehouse"
# start the Week-8 producer writing to `clickstream`
docker compose exec spark python /work/week08/producer.py --rate 1000
```

Submit the job into the Spark container:

```bash
docker compose exec spark /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /work/challenge01_job.py
```

(For the Iceberg sink, swap the Delta packages/extensions for the Iceberg runtime
`org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0` and configure a catalog per the
Iceberg Spark streaming docs.)

## Tasks

1. **Source.** Read `clickstream` with `spark.readStream.format("kafka")`, `subscribe` the
   topic, `startingOffsets` `earliest` for a reproducible run. Deserialize the payload
   (JSON via `from_json`, or Confluent Avro via `from_avro` after stripping the 5-byte
   envelope). Cast `event_ts` (epoch millis) to a `TimestampType`.
2. **Watermark.** Apply `withWatermark("event_ts", "10 minutes")`. Justify the 10-minute
   choice against the producer's configured lateness in a one-line comment.
3. **Windowed aggregate.** `groupBy(window(col("event_ts"), "5 minutes"), col("page"))` and
   `count`. Name the count column `event_count`.
4. **Exactly-once sink.** Write with `outputMode("update")`, a `foreachBatch` that does a
   Delta `MERGE` (or Iceberg `MERGE INTO`) keyed on `(window_start, window_end, page)`,
   **overwriting** `event_count` (not adding), and a `checkpointLocation` on MinIO. Use
   `trigger(availableNow=True)` for a drain-and-stop run, or `processingTime("15 seconds")`
   for a live run.
5. **Verify the table.** From a *separate* engine (DuckDB or a second Spark session) `SELECT`
   the table and confirm one row per `(window, page)` with plausible counts. This proves the
   streaming-lakehouse pattern: the stream wrote it, another reader queries it.
6. **Inject a late event, within the watermark.** While the `09:00–09:05` window is still open
   (live trigger, producer still around `09:0x`), inject one `/checkout` event with
   `event_ts` inside that window. Capture the progress JSON showing the event time, the
   watermark, and `numRowsDroppedByWatermark = 0`, and show the table count for that window
   increasing by exactly one.
7. **Inject a too-late event.** Inject one event with `event_ts` older than the current
   watermark. Capture the progress JSON showing `numRowsDroppedByWatermark = 1` and show the
   table count **unchanged**.
8. **Kill and recover.** Kill the job mid-run (Ctrl-C / `docker kill`), restart it against the
   **same checkpoint**, and confirm the table is consistent — no duplicated or lost windows.
   This exercises the exactly-once recovery path.

## Acceptance criteria

- The job runs end to end: Kafka → watermark → 5-minute tumbling count → exactly-once
  Delta/Iceberg table on MinIO, with a `checkpointLocation`.
- A second engine can `SELECT` the table and gets one row per `(window, page)`; the counts
  are stable across reruns (idempotent).
- The within-watermark late event folds into the **correct event-time window** (count +1),
  evidenced by the before/after query and the progress JSON (`numRowsDroppedByWatermark = 0`).
- The too-late event is **dropped**, evidenced by `numRowsDroppedByWatermark = 1` and an
  unchanged count.
- After a kill + restart against the same checkpoint, the table has no duplicates or gaps,
  demonstrating exactly-once recovery (`replayable source × checkpoint × idempotent sink`).
- A short `NOTES.md` records: the watermark choice and its justification, the offset ranges
  the recovery batch reprocessed, the `MERGE` action used (overwrite, not add) with one
  sentence on why, and a measured `processedRowsPerSecond`.

## Stretch

- Add a second, **sliding** 10-minute/5-minute window aggregate writing to a second table and
  reason about why the same event now contributes to two windows.
- Replace the Delta `MERGE` with the Iceberg native streaming sink (append mode + watermark)
  and compare the table semantics (insert-only finalized rows vs upserted running counts).
- Switch the state backend to RocksDB
  (`spark.sql.streaming.stateStore.providerClass=...RocksDBStateStoreProvider`) and confirm
  the job still passes; note where you would need it (large keyed state).
