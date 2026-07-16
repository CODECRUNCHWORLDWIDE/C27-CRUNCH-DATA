"""
Exercise 1 — Your first Spark DataFrame job and the DAG
=======================================================

Estimated time: 45 minutes.

Goal
----
Stand up a local SparkSession, read the NYC yellow-taxi Parquet you prepared in
Week 6, run a small chain of transformations, fire an action, and then READ the
physical plan and the partition layout. By the end you should be able to point at
the line in the explain() output that is the shuffle, and say how many stages the
job has and why.

This is a *starter*. The structure, imports, and the SparkSession are given. The
learning work is in the steps marked "Step N:" — fill in the DataFrame calls and
the print statements yourself, then check exercises/SOLUTIONS.md.

Run it:
    spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4 \
        exercise-01-first-spark-job-and-the-dag.py

(Or `python exercise-01-first-spark-job-and-the-dag.py` if PySpark + the jars are
already on your classpath, e.g. inside the Week 7 docker-compose Spark image.)

While it runs, open the Spark UI at http://localhost:4040 and keep it open.

References
----------
- Cluster overview:        https://spark.apache.org/docs/latest/cluster-overview.html
- SQL/DataFrame guide:     https://spark.apache.org/docs/latest/sql-programming-guide.html
- EXPLAIN:                 https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html
- NYC TLC trip data:       https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Path to the taxi Parquet. Either a local glob over the monthly files you
# downloaded in Week 6, or the Iceberg table on MinIO. Use the local glob for
# this exercise so it runs without the lakehouse stack up.
TAXI_PARQUET = "data/yellow_tripdata_2023-*.parquet"


def build_spark() -> SparkSession:
    """A minimal local SparkSession. local[*] = all laptop cores, one JVM."""
    spark = (
        SparkSession.builder
        .appName("c27-week07-ex01")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    spark = build_spark()

    # ------------------------------------------------------------------
    # Step 1: Read the taxi Parquet into a DataFrame called `trips`.
    #         Use spark.read.parquet(TAXI_PARQUET).
    #         NOTE: this is a transformation-ish read; it computes nothing
    #         heavy yet beyond schema inference.
    # ------------------------------------------------------------------
    trips = ...  # Step 1

    # ------------------------------------------------------------------
    # Step 2: Print the schema with trips.printSchema(), and print the number
    #         of partitions the read produced with trips.rdd.getNumPartitions().
    #         Write down that number — it is the parallelism of the first stage.
    # ------------------------------------------------------------------
    # Step 2: printSchema + getNumPartitions

    # ------------------------------------------------------------------
    # Step 3: Build a chain of NARROW transformations (no shuffle):
    #         - keep only rows where trip_distance > 0 and total_amount > 0
    #         - add a "day" column = to_date(tpep_pickup_datetime)
    #         Call the result `clean`. Remember: this computes nothing yet.
    # ------------------------------------------------------------------
    clean = ...  # Step 3

    # ------------------------------------------------------------------
    # Step 4: Add ONE WIDE transformation: groupBy("VendorID", "day") and
    #         agg count("*") as "trips" and round(sum("total_amount"), 2) as
    #         "revenue". Call the result `daily`. Still no compute.
    # ------------------------------------------------------------------
    daily = ...  # Step 4

    # ------------------------------------------------------------------
    # Step 5: Print the PHYSICAL PLAN with daily.explain(mode="formatted").
    #         Read it bottom-up. Find:
    #           - the BatchScan / FileScan leaf (which columns did it read?)
    #           - the Filter node (was the predicate pushed near the scan?)
    #           - the Exchange hashpartitioning(...) node  <-- THE SHUFFLE
    #           - the two HashAggregate nodes (partial + final)
    #         How many Exchange nodes are there? That is the number of shuffles,
    #         and (shuffles + 1) is the number of stages.
    # ------------------------------------------------------------------
    # Step 5: daily.explain(mode="formatted")

    # ------------------------------------------------------------------
    # Step 6: Fire the ACTION. Call daily.orderBy(F.desc("revenue")).show(10).
    #         THIS is the line that actually triggers the job. Watch the Spark UI
    #         Jobs tab: a new job appears. Click into it: how many stages? Match
    #         that against your prediction from Step 5. (Note: the orderBy adds a
    #         second shuffle — so this action has more stages than the groupBy
    #         alone. Explain why.)
    # ------------------------------------------------------------------
    # Step 6: action + observe the UI

    # ------------------------------------------------------------------
    # Step 7: Demonstrate laziness. Time how long Step 4 (building `daily`) took
    #         vs how long Step 6 (the show) took, using time.perf_counter()
    #         around each. Confirm that the transformation is ~instant and the
    #         action carries all the cost.
    # ------------------------------------------------------------------
    # Step 7: time the transformation vs the action

    # Keep the UI alive for a moment so you can inspect it before the app exits.
    input("Inspect http://localhost:4040, then press Enter to stop Spark...")
    spark.stop()


if __name__ == "__main__":
    main()
