# Week 9 — Resources

Every resource on this page is **free** and **publicly accessible** (the one book is paid, and
flagged as such — but its source articles, "Streaming 101/102," are free and cover the same
material). Where we name a version (Spark 3.5.1, Delta 3.1.0, Iceberg 1.5.0, Flink 1.18.1,
PyFlink 1.18.1, Kafka 3.7), use that exact version when running locally — it pins your
reproducibility. If a link breaks, please open an issue.

## Required reading (work it into your week)

- **Spark Structured Streaming Programming Guide** — the primary reference for the whole Spark
  half of the week: basic concepts, event-time windowing, watermarks, output modes,
  checkpointing, triggers, `foreachBatch`. Read it front to back once.
  <https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html>
- **Structured Streaming + Kafka Integration Guide** — exactly how `readStream.format("kafka")`
  works: the source schema, `subscribe`/`assign`, `startingOffsets`, offset management, and
  the exactly-once semantics of the Kafka source.
  <https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html>
- **Akidau, "Streaming 101: The world beyond batch"** — the free article that defines event vs
  processing time, the time-domain skew, and why event-time windowing matters. The conceptual
  bedrock of the week.
  <https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/>
- **Akidau, "Streaming 102: The world beyond batch"** — the sequel: windows (tumbling/sliding/
  session), watermarks, triggers, and accumulation modes, with the canonical event-time/
  processing-time diagrams.
  <https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/>

## The Dataflow Model (the theory underneath everything)

- **Akidau, Bradshaw, Chambers, Chernyak, Fernández-Moctezuma, Lax, McVeety, Mills, Perry,
  Schmidt, Whittle (2015)** — "The Dataflow Model: A Practical Approach to Balancing
  Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing."
  *Proceedings of the VLDB Endowment* 8(12):1792. The paper that unified batch and streaming
  and framed batch as the special case where the window is "all of time." Free PDF:
  <https://research.google/pubs/the-dataflow-model-a-practical-approach-to-balancing-correctness-latency-and-cost-in-massive-scale-unbounded-out-of-order-data-processing/>
  Direct VLDB PDF: <https://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf>

## The book (paid, but the canonical deep dive)

- **Akidau, Chernyak, Lax (2018)** — *Streaming Systems: The What, Where, When, and How of
  Large-Scale Data Processing.* O'Reilly Media. ISBN 9781491983874. The book-length expansion
  of the Streaming 101/102 articles; the definitive treatment of watermarks, windows, triggers,
  accumulation, and exactly-once. If you read one thing beyond the docs, read this. Publisher
  page:
  <https://www.oreilly.com/library/view/streaming-systems/9781491983867/>
  (The free Streaming 101/102 articles above cover the same core ideas at lower depth.)

## Spark Structured Streaming — specific sections to bookmark

- **Handling late data and watermarking** —
  <https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking>
- **Window operations on event time** (tumbling/sliding/session) —
  <https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time>
- **Output modes** —
  <https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes>
- **Recovering from failures with checkpointing** —
  <https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing>
- **Fault-tolerance semantics** (exactly-once requirements) —
  <https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#fault-tolerance-semantics>
- **Triggers** (default, `ProcessingTime`, `AvailableNow`) —
  <https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers>
- **Using `foreach` and `foreachBatch`** —
  <https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch>

## The streaming-lakehouse sink — Delta and Iceberg

- **Delta Lake — Table streaming reads and writes** — Delta as a streaming source and sink,
  exactly-once semantics, `txnAppId`/`txnVersion` idempotent dedup.
  <https://docs.delta.io/latest/delta-streaming.html>
- **Delta Lake — Upsert from streaming queries using `foreachBatch`** — the canonical
  `MERGE`-inside-`foreachBatch` recipe used in Exercise 3 and the challenge.
  <https://docs.delta.io/latest/delta-update.html#upsert-from-streaming-queries-using-foreachbatch>
- **Apache Iceberg — Spark Structured Streaming** — Iceberg as a streaming sink (`writeStream`
  `.format("iceberg")` / `.toTable(...)`) and reading an Iceberg table as a stream.
  <https://iceberg.apache.org/docs/latest/spark-structured-streaming/>
- **Apache Iceberg — Spark writes (`MERGE INTO`)** — the SQL `MERGE` you call from inside
  `foreachBatch` for the Iceberg upsert path.
  <https://iceberg.apache.org/docs/latest/spark-writes/>

## Apache Flink and PyFlink

- **Apache Flink documentation (stable)** — the docs root.
  <https://nightlies.apache.org/flink/flink-docs-stable/>
- **Flink architecture** (JobManager/TaskManager, dataflow, task slots) —
  <https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink-architecture/>
- **Flink — Timely stream processing** (event time, processing time, watermarks) —
  <https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/>
- **Flink — Fault tolerance via state snapshots** (checkpointing, barriers, exactly-once) —
  <https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/fault_tolerance/>
- **Flink — Checkpointing** (configuration and mechanics) —
  <https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/checkpointing/>
- **PyFlink — Intro to the Table API** —
  <https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/table/intro_to_table_api/>
- **PyFlink — DataStream API tutorial** —
  <https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/datastream_tutorial/>
- **Flink — Kafka SQL connector** (the `WITH ('connector'='kafka', ...)` DDL) —
  <https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/kafka/>
- **Flink — Windowing table-valued functions** (`TUMBLE`, `HOP`, `CUMULATE`) —
  <https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/window-tvf/>

## Background / context (from prior weeks, restated here)

- **Apache Kafka documentation** — the source you consume this week; offsets, partitions,
  consumer groups (Week 8).
  <https://kafka.apache.org/documentation/>
- **Confluent Schema Registry — wire format** — the magic byte + 4-byte schema id envelope you
  strip before `from_avro` (Week 8's serialization).
  <https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format>
- **PySpark `from_avro` / `to_avro`** — decoding the Avro `value` payload.
  <https://spark.apache.org/docs/latest/sql-data-sources-avro.html>
- **MinIO documentation** — the S3-compatible object store hosting the checkpoint and the
  lakehouse table (Week 6).
  <https://min.io/docs/minio/container/index.html>

## How to use these this week

- **Monday/Tuesday:** Streaming 101 + the Programming Guide's basic-concepts, event-time, and
  watermark sections; the Kafka integration guide for the source.
- **Wednesday/Thursday:** the output-modes, checkpointing, fault-tolerance, and `foreachBatch`
  sections; the Delta/Iceberg streaming docs for the sink.
- **Friday:** Streaming 102 for the unifying model; the Flink architecture, time, and
  fault-tolerance docs; the PyFlink Table API + Kafka connector + windowing-TVF docs for the
  comparison job.
- **The Dataflow Model paper** is the through-line — read it once, ideally Friday, to see why
  every Spark and Flink mechanism is one realization of a single model.
