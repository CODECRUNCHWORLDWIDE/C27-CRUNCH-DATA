# Lecture 3 — Data Skew, the Spark UI, and Spark vs DuckDB

> **Duration:** ~1.5 hours of reading + running a skewed job and reading its Spark UI live.
> **Prerequisites:** Lectures 1 and 2 (execution model; shuffles and join strategies).
> **Citations:** Spark Web UI <https://spark.apache.org/docs/latest/web-ui.html>; SQL performance tuning / skew join <https://spark.apache.org/docs/latest/sql-performance-tuning.html#optimizing-skew-join>; AQE <https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution>; PySpark functions <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html>; DuckDB performance guide <https://duckdb.org/docs/stable/guides/performance/overview.html>.
> **Outcome:** You can spot a skewed stage in the Spark UI, name the hot key, fix the skew three ways with a measured before/after, read a physical plan end to end, and argue from a benchmark whether a job belongs on Spark at all.

If you only remember one thing from this lecture, remember this:

> **Spark splits work into tasks of equal row *count*, not equal *cost*. When one key holds a huge share of the rows, the one task that gets it becomes a straggler that runs for minutes while 199 idle tasks wait. You find it in the Spark UI's Stages tab — the moment the max task duration is many times the median, you have skew. You fix it by broadcasting the small side (skip the shuffle entirely), salting the hot key (spread it across many tasks), or letting AQE split the skewed partition. And sometimes the right fix is to not use Spark at all: if the data fits on one big machine, DuckDB has no shuffle to skew.**

Lecture 2's join strategies are the input. This lecture is what you do when the strategy is right but one key ruins it anyway.

---

## 1. What data skew is

Spark's parallelism rests on an assumption: that partitioning by a key spreads rows roughly evenly across partitions, so every task does about the same amount of work. **Data skew is when that assumption fails** — when the distribution of a key is so lopsided that one (or a few) partitions get far more rows than the rest.

The NYC taxi data is genuinely skewed in several columns:

- **`VendorID`** has two real values (1 and 2), and vendor 2 is roughly 60% of all trips. A `groupBy("VendorID")` or a join on `VendorID` puts 60% of the data into one of two groups.
- **`PULocationID`** (pickup zone) is dominated by a handful of Manhattan zones — a single airport or midtown zone can be a large fraction of all pickups, while most of the 265 zones are sparse.
- **`payment_type`** is dominated by credit card and cash; the rest are rounding error.

When you shuffle on a skewed key, the partition that receives the hot key gets enormous. The task assigned to that partition reads more data, holds more in memory (risking spill or OOM), and runs far longer than its siblings. Because a stage cannot finish until its *last* task finishes, that one **straggler** sets the wall-clock time of the entire stage. The other tasks finished in seconds; the cluster is mostly idle; the job is "slow." Adding cores does nothing — the bottleneck is one task that cannot be parallelized further.

---

## 2. The Spark UI, tab by tab

The Spark UI is your instrument for seeing all of this. In `local[*]` mode it serves on **`http://localhost:4040`** while the application runs (the next app uses 4041, etc.). In standalone mode the history is on the master UI. Full reference: <https://spark.apache.org/docs/latest/web-ui.html>.

### 2.1 Jobs tab

One row per **job** (one action). Shows the job's duration, the stages it spawned, and a **timeline** (event timeline) of when jobs ran. Use it to find *which* of your actions is the slow one. A `groupBy().show()` is one job; a `write` is one job. If your script ran for three minutes and the Jobs tab shows one job took 2m50s, you know which action to drill into.

### 2.2 Stages tab — where skew screams

Click a slow job, then its slow **stage**. This is the most important screen for diagnosis. For the stage's tasks it shows a **summary-metrics table** with distribution columns — **Min, 25th percentile, Median, 75th percentile, Max** — for:

- **Duration** — how long each task ran.
- **Shuffle Read Size / Records** — how much shuffle data each task pulled.
- **Spill (memory / disk)** — whether tasks ran out of memory and spilled.

**This is the skew detector.** In a *healthy* stage, Max ≈ Median: every task did about the same work. In a *skewed* stage, you see something like:

```
Metric              Min    25th   Median   75th    Max
Duration            3 s    3 s    4 s      4 s     92 s     <- Max is 23x the median!
Shuffle Read Size   28 MB  29 MB  30 MB    31 MB   1.8 GB   <- one task read 60x the rest
```

When **Max is many times Median**, you have skew. The single 92-second task is the straggler; it pulled 1.8 GB of shuffle (the hot key's worth) while everyone else pulled ~30 MB. The stage took 92 seconds because of that one task. The **event timeline** at the top of the stage page visualizes it too: a forest of short green bars and one bar stretching far to the right.

### 2.3 SQL / DataFrame tab

For DataFrame and SQL queries, this tab shows the **executed plan as a diagram**, with **per-node metrics**: rows output, time spent, data size at each operator. You can see exactly how many rows flowed into and out of the join, which `Exchange` was the expensive one, and whether the final plan was the adaptive one (`AdaptiveSparkPlan`). This is the plan from Lecture 2 §5, but annotated with real numbers from the run — invaluable for confirming a filter pushed down or a broadcast happened.

### 2.4 Executors tab

One row per executor. Shows **cores**, **memory used / available**, **shuffle read/write totals**, **task counts**, and crucially **GC time** (time spent in garbage collection). High GC time or high spill on an executor means memory pressure — often the downstream symptom of a skewed partition that didn't fit. In `local[*]` there is one executor (the driver); in standalone mode you see each worker, and you can tell if work is unevenly distributed across them.

### 2.5 The diagnosis workflow

1. **Jobs tab:** which action is slow?
2. **Stages tab:** within that job, which stage is slow, and is its task-duration Max ≫ Median? → skew.
3. **Stages tab, shuffle-read distribution:** which task pulled the giant slice? That is the hot key's partition.
4. **SQL tab:** confirm the plan — is this a `SortMergeJoin` that should have been a broadcast? Is there a shuffle you could remove?
5. **Executors tab:** any executor pinned on GC or spilling? → memory pressure from the skewed partition.

---

## 3. Fix 1 — broadcast the small side (skip the shuffle entirely)

The cleanest fix, when applicable, is to **not shuffle at all.** If the join that skewed was a fact-to-dimension join — taxi trips joined to a small lookup — broadcasting the dimension turns the `SortMergeJoin` into a `BroadcastHashJoin` (Lecture 2 §6.1). No shuffle of the fact table means **no skewed partition to begin with**: each executor streams its own trips through the broadcast hash table locally.

```python
from pyspark.sql.functions import broadcast
fixed = trips.join(broadcast(dim_payment), "payment_type", "left")
```

Before: a `SortMergeJoin` with one task reading 1.8 GB, stage = 92 s. After: a `BroadcastHashJoin`, no `Exchange` over `trips`, stage = 7 s. Confirm in the plan (no `Exchange hashpartitioning` over the fact) and in the Stages tab (Max ≈ Median again). **This is the first fix to reach for whenever one side is small enough to broadcast.**

---

## 4. Fix 2 — salting the hot key (for large–large joins)

When both sides are large and you cannot broadcast, broadcasting is off the table and a sort-merge join is forced. If that join skews, the technique is **salting**: artificially spread the hot key across many partitions by appending a random suffix, so no single reduce partition gets the whole hot key.

The idea, for a join `large_left ⋈ large_right` on `key` where one key value is hot:

1. **Salt the left side:** add a random salt `0..N-1` to every row, making a compound key `(key, salt)`.
2. **Explode the right side:** replicate every right row `N` times, once per salt value, so it can match any salt.
3. **Join on the compound key** `(key, salt)`. The hot key is now spread across `N` partitions instead of one.

```python
from pyspark.sql import functions as F

N = 16  # number of salt buckets; pick to spread the hot key across tasks

# Left (fact): one random salt per row.
left_salted = large_left.withColumn("salt", (F.rand() * N).cast("int"))

# Right (other large side): replicate each row across all salt values.
salts = spark.range(N).withColumnRenamed("id", "salt")
right_exploded = large_right.crossJoin(salts)

# Join on the compound key. The hot key's rows now land in N partitions.
joined = left_salted.join(
    right_exploded,
    on=[left_salted.key == right_exploded.key,
        left_salted.salt == right_exploded.salt],
    how="inner",
)
```

The cost is the `N`-fold replication of the right side, so keep `N` modest (8–32 is typical) — large enough to break up the hot key, small enough not to blow up the other side. Salting is a manual, surgical fix; reach for it only when broadcast is impossible and AQE's automatic handling (Fix 3) is not enough. A common refinement is to salt **only the hot keys** (identified from the data) and leave the rest unsalted, which avoids replicating the whole right side.

---

## 5. Fix 3 — let AQE split the skewed partition

**Adaptive Query Execution** can handle skew automatically for sort-merge joins (SQL performance tuning, optimizing skew join, <https://spark.apache.org/docs/latest/sql-performance-tuning.html#optimizing-skew-join>). With `spark.sql.adaptive.enabled` and `spark.sql.adaptive.skewJoin.enabled` both true (defaults in 3.2+), Spark watches the actual post-shuffle partition sizes and, when one is far larger than the median, **splits it into several sub-partitions** that run as separate tasks. The hot key is divided across tasks without any code change.

```python
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
# A partition is "skewed" if it is BOTH larger than the median by this factor...
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 5)
# ...AND larger than this absolute threshold.
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

In the SQL tab the final adaptive plan will annotate the join with a skew-split note, and the Stages tab Max/Median ratio drops. AQE is the lowest-effort fix and should be on by default; salting is what you do when AQE alone is not enough (e.g. extreme skew, or skew in a `groupBy` rather than a join). Broadcast beats both when the small side allows it.

---

## 6. Reading a physical plan end to end

Tie Lecture 2 together with a full read of a skewed-then-fixed join plan. The **before** (sort-merge, skewed):

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   *(5) HashAggregate(keys=[...], functions=[sum(total_amount)])      <- reduce-side aggregate
   +- AQEShuffleRead coalesced                                        <- AQE coalesced partitions
      +- ShuffleQueryStage 2
         +- Exchange hashpartitioning(VendorID, day, 64)              <- shuffle for the groupBy
            +- *(4) HashAggregate(... partial_sum ...)                <- map-side partial aggregate
               +- *(4) SortMergeJoin [VendorID], [VendorID], Inner    <- the SKEWED join
                  :- Sort [VendorID ASC]
                  :  +- AQEShuffleRead                                <- shuffle read, left
                  :     +- ShuffleQueryStage 0
                  :        +- Exchange hashpartitioning(VendorID, 64) <- shuffle of left (fact)
                  :           +- BatchScan lake.nyc.yellow_tripdata
                  +- Sort [VendorID ASC]
                     +- AQEShuffleRead                                <- shuffle read, right
                        +- ShuffleQueryStage 1
                           +- Exchange hashpartitioning(VendorID, 64) <- shuffle of right
                              +- BatchScan lake.nyc.vendor_daily
```

Bottom-up: two `BatchScan` leaves → each shuffled by `VendorID` (two `Exchange` nodes) → sorted → `SortMergeJoin` → partial aggregate → another `Exchange` for the `groupBy` → final aggregate. **Three `Exchange` nodes = three shuffles.** The skew is on the join's `Exchange hashpartitioning(VendorID, 64)`: vendor 2's rows all land in one partition. `*(4)` and `*(5)` are whole-stage codegen group markers. `AdaptiveSparkPlan isFinalPlan=true` and the `AQEShuffleRead coalesced` / `ShuffleQueryStage` nodes tell you AQE re-planned this at runtime.

The **after** (broadcast the small side): the two join `Exchange` nodes and the `Sort`/`SortMergeJoin` collapse into a single `BroadcastHashJoin` over a `BroadcastExchange` of the small side, leaving only the `groupBy`'s one `Exchange`. Three shuffles become one. That collapse, visible in the plan, is the whole point of the diagnosis.

---

## 7. Spark vs DuckDB — when distribution is worth it

Now the honest part. Everything above is machinery for **distributing** a computation. Distribution costs a coordination tax — JVM serialization, shuffle-to-disk, network transfer, task scheduling, the driver/executor split. **DuckDB** (the in-process engine from Week 6) pays *none* of it. It runs in your Python process, reads Parquet directly, vectorizes execution across cores, and never shuffles to disk or serializes across a network boundary (DuckDB performance guide, <https://duckdb.org/docs/stable/guides/performance/overview.html>).

So on data that fits on one machine, DuckDB usually **wins** — often by a wide margin — on exactly the kind of mart-building scan-filter-join-aggregate query this week is about:

```python
import duckdb
con = duckdb.connect()
con.sql("""
  SELECT VendorID, date_trunc('day', tpep_pickup_datetime) AS day,
         count(*) AS trips, round(sum(total_amount), 2) AS revenue
  FROM read_parquet('data/yellow_tripdata_2023-*.parquet')
  WHERE trip_distance > 0 AND total_amount > 0
  GROUP BY 1, 2
""").show()
```

This runs the year of taxi data on a laptop in **seconds**, with no cluster, no JVM warmup, no shuffle, and no skew (there is no shuffle to skew). Spark, on the same laptop and same data, pays seconds of JVM/session startup before it even reads a row, and then a real network-free-but-still-serialized shuffle for the `groupBy`.

**The decision rule you should be able to defend:**

| Use DuckDB when… | Use Spark when… |
|---|---|
| The working set fits in one machine's memory (or comfortably streams from disk on one machine). | The data does not fit on one machine, or the compute genuinely exceeds one machine. |
| You want the lowest latency on an interactive analytical query. | You need to scale *out* across a real cluster (10s–100s of TB). |
| The whole job is one process and you value simplicity. | You need Spark-only features: Structured Streaming, a huge shuffle-heavy join, a managed cluster, ML on distributed data. |
| Laptop-scale exploration, dashboards, single-node marts. | Production pipelines where the same code must run unchanged from 1 GB to 1 PB. |

The number that settles it is a **measured wall-clock**, not a habit. This week's mini-project makes you put the two times side by side in a `PERF.md`. Often, at laptop scale, DuckDB wins and the right engineering decision is "this does not need Spark." That conclusion — backed by a benchmark — is a *correct* answer, and being able to reach it is a skill the lab grades.

The flip side: the *same* PySpark DataFrame job that loses to DuckDB on one month of data, run unchanged on a 500-node cluster over a petabyte, is the only one of the two that finishes at all. Spark's value is not speed on small data; it is **the same code scaling to data that has no single-machine answer.** Choose the engine for the data, not for fashion.

---

## 8. Summary

- **Data skew** is uneven key distribution: one key holds a huge share of rows, so its shuffle partition is huge, its task is a **straggler**, and that one task sets the stage's wall-clock. Adding cores does not help. The taxi data is really skewed on `VendorID`, `PULocationID`, and `payment_type`.
- The **Spark UI** (`localhost:4040`): **Jobs** (find the slow action), **Stages** (Max-task-duration ≫ Median = skew; the shuffle-read distribution names the hot partition), **SQL/DataFrame** (the plan annotated with real row counts and times), **Executors** (GC time and spill = memory pressure).
- **Fix 1 — broadcast** the small side: turns a sort-merge into a broadcast hash join, removes the fact-side shuffle, so there is no skewed partition. First choice when a side is small.
- **Fix 2 — salting:** for large–large joins, append a random salt to spread the hot key across `N` partitions (and replicate the other side `N×`). Surgical; keep `N` modest; ideally salt only the hot keys.
- **Fix 3 — AQE skew join** (`spark.sql.adaptive.skewJoin.enabled`, default on): Spark splits skewed partitions at runtime, no code change. Lowest effort; on by default.
- **Reading a plan end to end:** bottom-up; count `Exchange` nodes for shuffles; spot `SortMergeJoin` vs `BroadcastHashJoin`; `AQEShuffleRead`/`ShuffleQueryStage`/`isFinalPlan=true` mark AQE's runtime re-planning.
- **Spark vs DuckDB:** distribution costs a coordination tax DuckDB never pays. On data that fits one machine, DuckDB usually wins; use Spark when the data or compute exceeds one machine, or you need Spark-only features. Decide from a **measured benchmark**, not habit — and "this doesn't need Spark" is a correct, defensible answer.

That is the week: the model (Lecture 1), the shuffle and the joins (Lecture 2), and the diagnosis and the judgment call (Lecture 3). The exercises and the mini-project make all three muscle memory over the real NYC taxi data on your Week 6 lakehouse.
