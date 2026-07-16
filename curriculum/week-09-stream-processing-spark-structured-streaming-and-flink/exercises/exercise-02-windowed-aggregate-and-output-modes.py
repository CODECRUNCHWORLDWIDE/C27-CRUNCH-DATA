#!/usr/bin/env python3
"""
Exercise 2 — windowed event count, append vs update output modes, watermark gating
====================================================================================

Estimated time: 45 minutes.

GOAL
----
Turn the parsed, watermarked clickstream from Exercise 1 into a STATEFUL windowed
aggregate: count events per page per 5-minute tumbling window over EVENT TIME. Then run
the SAME aggregate under two output modes and watch the difference:

  * update mode  -> emits a window's running count every time it CHANGES (early + often)
  * append mode  -> emits a window's count exactly ONCE, only after the watermark passes
                    the window end (late but final). REQUIRES a watermark on the aggregate.

You will SEE the watermark gate emission: in append mode, a window's row does not appear
until ~10 minutes (the watermark delay) after the window's end in event time. In update
mode you see the count climb 1, 2, 3, ... as events arrive.

WHAT YOU ARE PRACTISING
-----------------------
- groupBy(window(col, "5 minutes"), col("page")).count()  -- stateful aggregation (L2 §1)
- the state store + watermark eviction (L2 §1)
- update vs append output modes and the watermark-required rule (L2 §2)
- reading the per-batch progress to observe the watermark advancing (L1 §6)

RUN
---
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        exercises/exercise-02-windowed-aggregate-and-output-modes.py update

Pass "update" or "append" as the single CLI arg to choose the output mode. Run it twice,
once each way, and compare what prints and WHEN.

EXPECTED OBSERVATION (the learning)
-----------------------------------
- update: counts appear quickly and grow; the same (window,page) row reprints with a
  higher count as more events land in that window.
- append: nothing prints for a window until the watermark (max event_ts - 10 min) is past
  the window end; then each (window,page) prints once, final, and never again. If you
  forget the watermark, append THROWS an AnalysisException -- that is the rule in action.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count
from pyspark.sql.types import StructType, StructField, StringType, LongType

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "clickstream"
WATERMARK_DELAY = "10 minutes"
WINDOW_SIZE = "5 minutes"


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


def read_watermarked_events(spark: SparkSession):
    """Step 1: rebuild the Exercise-1 front end: readStream -> parse -> cast -> watermark.

    Reuse exactly what you wrote in Exercise 1. The output is a watermarked DataFrame with a
    proper TimestampType `event_ts` column.
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    events = (
        raw.select(from_json(col("value").cast("string"), event_schema()).alias("e"))
        .select("e.*")
        .withColumn("event_ts", (col("event_ts") / 1000).cast("timestamp"))
    )
    # Step 2: attach the 10-minute watermark BEFORE the groupBy. The watermark must be set
    # on the event-time column upstream of the aggregation, or append will be illegal and the
    # state store will never evict closed windows.
    return events.withWatermark("event_ts", WATERMARK_DELAY)


def windowed_count(watermarked):
    """Step 3: the stateful windowed aggregate.

    Count events per page per 5-minute tumbling window over event time.
      groupBy(window(col("event_ts"), WINDOW_SIZE), col("page")).agg(count("*").alias("event_count"))
    The result schema is: window: struct<start,end>, page: string, event_count: long.
    """
    return (
        watermarked.groupBy(window(col("event_ts"), WINDOW_SIZE), col("page"))
        .agg(count("*").alias("event_count"))
    )


def main() -> None:
    # Step 4: read the output mode from argv. Default to "update" if none given.
    mode = sys.argv[1] if len(sys.argv) > 1 else "update"
    if mode not in ("update", "append", "complete"):
        raise SystemExit("usage: exercise-02 [update|append|complete]")

    spark = SparkSession.builder.appName(f"week09-ex02-{mode}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    watermarked = read_watermarked_events(spark)
    agg = windowed_count(watermarked)

    # Step 5: write the aggregate to the console in the chosen output mode.
    # Try BOTH:
    #   - "update": emits changed (window,page) rows each batch -- watch counts grow.
    #   - "append": emits each (window,page) ONCE after the watermark closes its window.
    #               Requires the watermark you set in Step 2; remove it and append throws.
    #   - "complete": re-emits the whole result table each batch (small here, fine).
    # Note: append + a 5-minute window + 10-minute watermark means a window's row appears
    # ~15 minutes of event time after the window starts. Be patient, or feed faster event-time.
    query = (
        agg.writeStream.format("console")
        .option("truncate", "false")
        .outputMode(mode)
        .start()
    )

    # Step 6: while it runs, observe the watermark advancing. The per-batch progress JSON
    # carries the state operator's "watermark" and "numRowsDroppedByWatermark". Compare the
    # watermark value across batches to the window ends to understand WHY append gates output.
    print(f"Running windowed count in '{mode}' output mode. Ctrl-C to stop.")
    print("Tip: inspect query.lastProgress for the 'eventTime'/'watermark' fields per batch.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
