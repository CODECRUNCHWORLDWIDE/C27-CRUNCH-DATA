"""
Exercise 3 — Diagnose and fix data skew
=======================================

Estimated time: 45 minutes.

Goal
----
Build a join that is deliberately skewed on one hot key, watch it stall in the
Spark UI (one straggler task), confirm the skew from the task-duration
distribution, and then FIX it two ways: by broadcasting the small side, and by
salting the hot key for the large-large case. Measure before vs after.

The NYC taxi data is genuinely skewed on VendorID (vendor 2 is ~60% of rows), so
this is not a contrived example. We make it worse-on-purpose by joining the fact
table to a per-vendor "large" table so a broadcast is not trivially available,
forcing you to also practice salting.

This is a *starter*. Fill in the "Step N:" blanks, then check
exercises/SOLUTIONS.md.

Run it with AQE controllable:
    spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4 \
        exercise-03-diagnose-and-fix-skew.py

While it runs, open http://localhost:4040 -> Stages tab and watch the
task-duration distribution (Min / Median / Max).

References
----------
- Web UI:        https://spark.apache.org/docs/latest/web-ui.html
- Skew join:     https://spark.apache.org/docs/latest/sql-performance-tuning.html#optimizing-skew-join
- AQE:           https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution
"""

from __future__ import annotations

import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

TAXI_PARQUET = "data/yellow_tripdata_2023-*.parquet"


def build_spark(aqe: bool) -> SparkSession:
    return (
        SparkSession.builder
        .appName("c27-week07-ex03")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.sql.adaptive.enabled", str(aqe).lower())
        # Forbid auto-broadcast so the skewed sort-merge actually happens and you
        # can SEE the skew before you fix it.
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .getOrCreate()
    )


def main() -> None:
    # Start with AQE OFF so the skew is visible (AQE would silently split it).
    spark = build_spark(aqe=False)
    spark.sparkContext.setLogLevel("WARN")

    trips = spark.read.parquet(TAXI_PARQUET)

    # ------------------------------------------------------------------
    # Step 1: Confirm the skew is real. Show the row count per VendorID:
    #           trips.groupBy("VendorID").count().orderBy(F.desc("count")).show()
    #         You should see one VendorID dominating (~60% of rows). Write the
    #         exact percentage down; that is the hot key.
    # ------------------------------------------------------------------
    # Step 1: show VendorID distribution

    # ------------------------------------------------------------------
    # Step 2: Build a "large-ish" right side keyed by VendorID so a broadcast is
    #         not trivially available. Aggregate trips to per-VendorID stats and
    #         CROSS-multiply it so it is not tiny (simulate a fat dimension):
    #           vendor_stats = (trips.groupBy("VendorID")
    #                                .agg(F.avg("total_amount").alias("avg_amt")))
    #         (For the skew to bite, the join key must be the skewed VendorID.)
    # ------------------------------------------------------------------
    vendor_stats = ...  # Step 2

    # ------------------------------------------------------------------
    # Step 3: SKEWED JOIN (the "before"). With AQE off and broadcast forbidden,
    #         join trips to vendor_stats on VendorID and force the whole thing to
    #         run with an action:
    #           skewed = trips.join(vendor_stats, "VendorID")
    #           t0 = time.perf_counter()
    #           skewed.groupBy("VendorID").agg(F.count("*").alias("n")).collect()
    #           print("skewed join:", time.perf_counter() - t0, "s")
    #         NOW LOOK AT THE SPARK UI -> Stages tab for the join stage. Read the
    #         Duration row: Min / Median / Max. The Max should be many times the
    #         Median — that is the straggler holding the hot VendorID. Read the
    #         Shuffle Read Size row too: one task pulled far more than the rest.
    #         Record Min / Median / Max.
    # ------------------------------------------------------------------
    # Step 3: skewed join + time it + read the Stages tab

    # ------------------------------------------------------------------
    # Step 4: FIX A — broadcast. vendor_stats is actually small (2 rows), so the
    #         honest first fix is to broadcast it, removing the fact-side shuffle
    #         entirely:
    #           fixed_b = trips.join(broadcast(vendor_stats), "VendorID")
    #           fixed_b.explain(mode="formatted")   # confirm BroadcastHashJoin,
    #                                               # and NO Exchange over trips
    #           t0 = time.perf_counter()
    #           fixed_b.groupBy("VendorID").agg(F.count("*").alias("n")).collect()
    #           print("broadcast fix:", time.perf_counter() - t0, "s")
    #         The stage's Max should now ~= Median (no skewed partition).
    # ------------------------------------------------------------------
    # Step 4: broadcast fix + time + confirm Max ~= Median

    # ------------------------------------------------------------------
    # Step 5: FIX B — salting (the technique for when you CANNOT broadcast,
    #         i.e. both sides are large). Pretend vendor_stats is huge. Salt:
    #           N = 16
    #           left  = trips.withColumn("salt", (F.rand() * N).cast("int"))
    #           salts = spark.range(N).withColumnRenamed("id", "salt")
    #           right = vendor_stats.crossJoin(salts)
    #           salted = left.join(right, on=["VendorID", "salt"])
    #         Time the same downstream aggregate. The hot VendorID's rows are now
    #         spread across N salt buckets, so no single task gets all of them.
    #         Read the Stages tab again: Max should be much closer to Median than
    #         in Step 3. Record the numbers.
    # ------------------------------------------------------------------
    # Step 5: salting fix + time + re-read the Stages tab

    # ------------------------------------------------------------------
    # Step 6: FIX C — let AQE do it. Stop this session and rebuild with AQE ON
    #         and skewJoin enabled:
    #           spark.stop()
    #           spark = build_spark(aqe=True)
    #           spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    #         Re-run the Step 3 skewed join (re-read trips/vendor_stats on the new
    #         session). Time it. In the SQL tab the final plan should annotate the
    #         join as skew-split. Compare this wall-clock to your manual fixes.
    # ------------------------------------------------------------------
    # Step 6: AQE skew-join fix + time + compare

    # ------------------------------------------------------------------
    # Step 7: In a comment, write a 3-row table: technique | wall-clock | when to
    #         use it. State which fix you would ship for THIS join and why.
    # ------------------------------------------------------------------
    # Step 7: technique | wall-clock | when to use

    input("Inspect http://localhost:4040, then press Enter to stop Spark...")
    spark.stop()


if __name__ == "__main__":
    main()
