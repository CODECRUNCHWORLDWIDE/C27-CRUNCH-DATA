"""
Exercise 2 — Shuffles and join strategies
==========================================

Estimated time: 45 minutes.

Goal
----
Make a shuffle happen on purpose, then join the taxi facts to a tiny dimension
two different ways — once letting Spark sort-merge it, once forcing a broadcast —
and read both physical plans. By the end you should be able to look at an
explain() output and say "this is a SortMergeJoin with two Exchange nodes" or
"this is a BroadcastHashJoin with no shuffle of the fact table", and predict
which is faster and why.

This is a *starter*. SparkSession, imports, and a helper that builds a small
payment-type dimension are given. Fill in the "Step N:" blanks, then check
exercises/SOLUTIONS.md.

Run it:
    spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4 \
        exercise-02-shuffles-and-join-strategies.py

References
----------
- Join hints:            https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html
- Performance tuning:    https://spark.apache.org/docs/latest/sql-performance-tuning.html
- broadcast():           https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.broadcast.html
"""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

TAXI_PARQUET = "data/yellow_tripdata_2023-*.parquet"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("c27-week07-ex02")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "64")
        # Turn AQE OFF for this exercise so the plan you read is the one Catalyst
        # chose, not the one AQE rewrote at runtime. You will turn it back on in
        # Exercise 3. (Otherwise AQE may auto-switch your sort-merge to broadcast
        # and hide the very thing you are trying to observe.)
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )


def payment_type_dim(spark: SparkSession) -> DataFrame:
    """A tiny dimension: the 6 official NYC TLC payment_type codes. ~6 rows."""
    rows = [
        (1, "Credit card"),
        (2, "Cash"),
        (3, "No charge"),
        (4, "Dispute"),
        (5, "Unknown"),
        (6, "Voided trip"),
    ]
    return spark.createDataFrame(rows, ["payment_type", "payment_type_name"])


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    trips = spark.read.parquet(TAXI_PARQUET)
    pay = payment_type_dim(spark)

    # ------------------------------------------------------------------
    # Step 1: Trigger a pure shuffle with no join. Build:
    #           by_pay = trips.groupBy("payment_type").agg(F.count("*").alias("n"))
    #         Call by_pay.explain(mode="formatted"). Find the single
    #         Exchange hashpartitioning(payment_type, 64) node — that is the
    #         shuffle the groupBy forced. Then by_pay.show() and watch the Jobs
    #         tab: two stages (map-side partial agg, reduce-side final agg).
    # ------------------------------------------------------------------
    by_pay = ...  # Step 1

    # ------------------------------------------------------------------
    # Step 2: SORT-MERGE JOIN (the default for large-large). Temporarily DISABLE
    #         auto-broadcast so Spark cannot quietly broadcast the tiny dim:
    #             spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    #         Then:
    #             smj = trips.join(pay, "payment_type", "left")
    #             smj.explain(mode="formatted")
    #         Read the plan. You should see a SortMergeJoin with TWO Exchange
    #         hashpartitioning nodes (one per side) and two Sort nodes. Both
    #         sides — including the big fact table — got shuffled. Note that.
    # ------------------------------------------------------------------
    # Step 2: forbid broadcast, build sort-merge join, explain

    # ------------------------------------------------------------------
    # Step 3: BROADCAST HASH JOIN. Re-enable auto-broadcast (set the threshold
    #         back to the 10MB default, 10485760), OR force it explicitly with
    #         the broadcast() hint, which is the robust way:
    #             bhj = trips.join(broadcast(pay), "payment_type", "left")
    #             bhj.explain(mode="formatted")
    #         Read the plan. You should now see a BroadcastHashJoin over a
    #         BroadcastExchange of `pay`, and NO Exchange hashpartitioning over
    #         `trips`. The fact table did not shuffle at all. That absent shuffle
    #         is the whole win.
    # ------------------------------------------------------------------
    # Step 3: broadcast join, explain

    # ------------------------------------------------------------------
    # Step 4: Measure it. Run an action that forces each join to materialize a
    #         small aggregate (so .show() of a tiny result still drives the full
    #         join). For each of smj and bhj:
    #             out = j.groupBy("payment_type_name").agg(F.count("*").alias("n"))
    #             t0 = time.perf_counter(); out.collect(); dt = time.perf_counter() - t0
    #         Print both wall-clock times. The broadcast version should be faster
    #         because it avoids shuffling the big fact table. Record the numbers.
    # ------------------------------------------------------------------
    # Step 4: time sort-merge vs broadcast

    # ------------------------------------------------------------------
    # Step 5: In one sentence each, in a comment below, answer:
    #           (a) Why did the dimension join NOT need to shuffle the fact table
    #               once broadcast?
    #           (b) When would a broadcast be the WRONG choice (hint: how big is
    #               the "small" side allowed to get before it hurts the driver
    #               and every executor's memory)?
    # ------------------------------------------------------------------
    # Step 5: (a) ...
    #         (b) ...

    input("Inspect http://localhost:4040, then press Enter to stop Spark...")
    spark.stop()


if __name__ == "__main__":
    main()
