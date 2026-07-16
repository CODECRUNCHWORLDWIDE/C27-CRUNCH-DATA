#!/usr/bin/env python3
"""
Exercise 3 — exactly-once windowed aggregate into the lakehouse, with a late event
====================================================================================

Estimated time: 45-60 minutes.

GOAL
----
Write the Exercise-2 windowed count EXACTLY ONCE into a Delta table on MinIO, using the
three exactly-once ingredients from Lecture 2:

    exactly-once = replayable source (Kafka offsets)
                 x checkpoint (checkpointLocation on MinIO)
                 x idempotent sink (foreachBatch MERGE on (window, page))

Then prove correct LATE-EVENT handling: with a 10-minute watermark, an out-of-order event
whose event_ts is older than the latest events but NEWER than the watermark must fold into
the RIGHT window (updating its count), while an event older than the watermark is dropped.

WHAT YOU ARE PRACTISING
-----------------------
- foreachBatch handing you a static DataFrame + batchId (Lecture 2 §5)
- a Delta MERGE keyed on the window -- the Week-3 idempotent upsert, now streaming (§4,§5)
- checkpointLocation as the query's memory; why you must NOT delete it (§3)
- watermark-gated late-event handling: folded in if within the watermark, dropped if not

RUN
---
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        exercises/exercise-03-exactly-once-into-the-lakehouse.py

MinIO (Week 6) must be up and the s3a credentials configured (see configure_s3a below).
Use trigger(availableNow=True) so the job DRAINS the topic once and STOPS -- the
"streaming as batch" trick (Lecture 2 §7) that lets you inspect the table without a job
that runs forever.

PROVING THE LATE EVENT (the deliverable)
----------------------------------------
1. Run once to populate page_counts; note the count of some (window, page), e.g.
   ('/checkout', 09:00-09:05) = 41.
2. From your Week-8 producer, inject ONE event with page='/checkout' and an event_ts inside
   the 09:00-09:05 window but stamped to arrive now (so it is "late"). Keep it NEWER than
   (current max event_ts - 10 min) so it is within the watermark.
3. Run again. The MERGE updates that window's count to 42. The late event folded into the
   correct EVENT-TIME window, not whatever window is open in processing time. That is the
   proof.
4. (Optional) Inject an event older than the watermark; rerun; confirm the count does NOT
   change and numRowsDroppedByWatermark increments in lastProgress.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count
from pyspark.sql.types import StructType, StructField, StringType, LongType
from delta.tables import DeltaTable

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "clickstream"
WATERMARK_DELAY = "10 minutes"
WINDOW_SIZE = "5 minutes"

TARGET = "s3a://lakehouse/clickstream/page_counts"          # the Delta table the dashboard reads
CHECKPOINT = "s3a://lakehouse/checkpoints/page_counts"      # the query's exactly-once memory


def configure_s3a(builder):
    """Step 1: point s3a at the Week-6 MinIO. Endpoint + creds + path-style access.

    These match the lab's MinIO container; change only if your Week-6 setup differs.
    """
    return (
        builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )


def event_schema() -> StructType:
    return StructType(
        [
            StructField("user_id", StringType()),
            StructField("session_id", StringType()),
            StructField("page", StringType()),
            StructField("event_type", StringType()),
            StructField("event_ts", LongType()),
            StructField("processing_ts", LongType()),
        ]
    )


def upsert_to_delta(micro_batch_df, batch_id: int) -> None:
    """Step 3: idempotent MERGE of one micro-batch into the page_counts Delta table.

    Keyed on (window_start, window_end, page). This is the Week-3 upsert, invoked from inside
    a streaming foreachBatch. The MERGE OVERWRITES event_count with the latest running value
    (whenMatchedUpdate set = s.event_count) -- it does NOT add. Overwriting is what makes a
    replay of batch_id a no-op, hence exactly-once. Adding would double-count on replay.
    """
    # Flatten the window struct so merge keys are top-level columns.
    flat = micro_batch_df.selectExpr(
        "window.start as window_start",
        "window.end   as window_end",
        "page",
        "event_count",
    )
    spark = micro_batch_df.sparkSession

    # First batch: the table may not exist yet -> create it by writing the batch.
    if not DeltaTable.isDeltaTable(spark, TARGET):
        flat.write.format("delta").mode("overwrite").save(TARGET)
        return

    # Step 4: the MERGE. Match on the full window key + page; update count; insert new windows.
    tgt = DeltaTable.forPath(spark, TARGET)
    (
        tgt.alias("t")
        .merge(
            flat.alias("s"),
            "t.window_start = s.window_start AND t.window_end = s.window_end AND t.page = s.page",
        )
        .whenMatchedUpdate(set={"event_count": "s.event_count"})  # overwrite -> idempotent
        .whenNotMatchedInsertAll()
        .execute()
    )


def main() -> None:
    builder = SparkSession.builder.appName("week09-ex03-exactly-once")
    builder = configure_s3a(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Step 2: source -> parse -> cast -> watermark -> windowed count (reuse Ex 1 + Ex 2).
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")  # replay the whole topic for a reproducible count
        .load()
    )
    events = (
        raw.select(from_json(col("value").cast("string"), event_schema()).alias("e"))
        .select("e.*")
        .withColumn("event_ts", (col("event_ts") / 1000).cast("timestamp"))
        .withWatermark("event_ts", WATERMARK_DELAY)
    )
    agg = events.groupBy(window(col("event_ts"), WINDOW_SIZE), col("page")).agg(
        count("*").alias("event_count")
    )

    # Step 5: the exactly-once write.
    #   - outputMode("update"): emit each window's running count when it changes; the MERGE
    #     folds late-but-within-watermark events into the correct window's count.
    #   - foreachBatch(upsert_to_delta): the idempotent sink.
    #   - checkpointLocation: the replay/commit memory. DO NOT DELETE between runs, or you
    #     re-read from earliest and the table is rebuilt (still correct here because MERGE is
    #     idempotent -- but in append mode deleting the checkpoint double-writes).
    #   - trigger(availableNow=True): drain the topic once, then stop, so you can SELECT the table.
    query = (
        agg.writeStream.outputMode("update")
        .foreachBatch(upsert_to_delta)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()  # with availableNow this returns once the topic is drained

    # Step 6: read the table back and print it -- this is what the dashboard will query.
    print("=== page_counts after exactly-once load ===")
    spark.read.format("delta").load(TARGET).orderBy("window_start", "page").show(
        100, truncate=False
    )
    print(
        "Now inject a late event from the Week-8 producer (see the module docstring),"
        " rerun this script, and watch the affected window's count change by exactly one."
    )
    spark.stop()


if __name__ == "__main__":
    main()
