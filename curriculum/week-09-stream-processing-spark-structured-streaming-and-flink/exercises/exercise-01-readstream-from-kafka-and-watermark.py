#!/usr/bin/env python3
"""
Exercise 1 — readStream from the Week-8 Kafka clickstream, parse, watermark, console sink
============================================================================================

Estimated time: 45 minutes (15 min env check, 15 min coding, 15 min reading the output).

GOAL
----
Build the front half of every streaming job in this week: read the Week-8 `clickstream`
Kafka topic with Structured Streaming, deserialize the JSON payload into typed columns,
turn the epoch-millis `event_ts` into a real timestamp, attach a 10-minute event-time
watermark, and write the parsed events to the console sink so you can watch them arrive.

You produce no table this exercise — just a running query that prints parsed events. You
reuse this exact source-and-watermark front end in Exercises 2 and 3 and in the challenges.

WHAT YOU ARE PRACTISING
-----------------------
- spark.readStream.format("kafka") with bootstrap servers + subscribe (Lecture 1 §7)
- from_json on the Kafka `value` bytes with an explicit schema (Lecture 1 §7.1)
- casting epoch millis -> TimestampType so window()/withWatermark() are legal (Lecture 1 §7)
- withWatermark("event_ts", "10 minutes") as the streaming high-water mark (Lecture 1 §3)
- the console sink + query.lastProgress as your only window into a running stream (§6)

RUN
---
This file is launched with spark-submit inside the lab's Spark container, with the Kafka
package on the classpath (pinned to Spark 3.5.1):

    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        exercises/exercise-01-readstream-from-kafka-and-watermark.py

The Week-8 broker must be up (docker compose up -d kafka schema-registry) and the producer
must be writing to the `clickstream` topic. The query blocks forever; stop it with Ctrl-C.

NOTE ON THE PAYLOAD
-------------------
The canonical Week-8 producer writes Confluent-framed Avro. For this first exercise we read
the JSON mirror topic (or a JSON-serialized `value`) so you focus on the streaming mechanics,
not Avro framing; the SOLUTIONS file shows the from_avro path. If your Week-8 setup only has
the Avro topic, point `TOPIC` at the JSON mirror your Week-8 producer also writes, or follow
the from_avro variant in SOLUTIONS.md.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType

# --- Config: the Week-8 broker + topic. Hostnames are Docker-network names. ---
KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "clickstream"
WATERMARK_DELAY = "10 minutes"   # allowed lateness; matches the lecture and the Flink job


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("week09-ex01-readstream")
        # quieter logs so the console sink output is readable
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def event_schema() -> StructType:
    """Schema of the JSON `value` payload your Week-8 producer wrote.

    Step 1: confirm these field names and types match your Week-8 producer's record.
    `event_ts` and `processing_ts` are epoch MILLISECONDS (a long), not strings.
    """
    return StructType(
        [
            StructField("user_id", StringType()),
            StructField("session_id", StringType()),
            StructField("page", StringType()),
            StructField("event_type", StringType()),
            StructField("event_ts", LongType()),       # epoch millis
            StructField("processing_ts", LongType()),  # epoch millis
        ]
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Step 2: open the streaming source on the Kafka topic.
    #   - format("kafka")
    #   - option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    #   - option("subscribe", TOPIC)
    #   - option("startingOffsets", "latest")   # "earliest" to replay the whole topic
    # The returned DataFrame has columns: key, value, topic, partition, offset, timestamp, ...
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Step 3: deserialize the JSON `value` bytes into typed columns.
    #   - cast value to string
    #   - from_json(<that string>, event_schema()) into a struct column, e.g. alias "e"
    #   - select e.* to flatten
    # Leave this assignment for you to complete; the shape is:
    #   parsed = raw.select(from_json(col("value").cast("string"), event_schema()).alias("e")).select("e.*")
    parsed = (
        raw.select(from_json(col("value").cast("string"), event_schema()).alias("e"))
        .select("e.*")
    )

    # Step 4: turn epoch-millis `event_ts` into a TimestampType column.
    # window() and withWatermark() REQUIRE a timestamp, not a long. Divide millis by 1000
    # and cast to "timestamp". Replace the `event_ts` column in place with the cast value.
    # Hint: .withColumn("event_ts", (col("event_ts") / 1000).cast("timestamp"))
    events = parsed.withColumn("event_ts", (col("event_ts") / 1000).cast("timestamp"))

    # Step 5: attach the event-time watermark.
    # This is the streaming high-water mark (Lecture 1 §3): the engine will treat the max
    # observed event_ts minus WATERMARK_DELAY as the boundary for "too late".
    # Hint: .withWatermark("event_ts", WATERMARK_DELAY)
    watermarked = events.withWatermark("event_ts", WATERMARK_DELAY)

    # Step 6: write the parsed, watermarked rows to the console sink in append mode.
    # No aggregation here, so append is fine and no watermark is strictly required for the
    # sink to be legal -- but we attach it anyway because Exercise 2 reuses this DataFrame.
    #   .writeStream.format("console").option("truncate","false").outputMode("append").start()
    query = (
        watermarked.writeStream.format("console")
        .option("truncate", "false")
        .option("numRows", "20")
        .outputMode("append")
        .start()
    )

    # Step 7: print the streaming progress so you can read what the engine is doing.
    # In a second terminal (or after a few batches) inspect query.lastProgress -- it is the
    # only window into a running stream (Lecture 1 §6). awaitTermination() blocks forever;
    # that is correct for a stream. Ctrl-C to stop.
    print("Streaming query started. Watch parsed events print below. Ctrl-C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
