# Exercise Solutions — Week 7

> Reference implementations, the console output and explain plans you should
> reproduce, and the common pitfalls for Exercises 1–3. Read these only after
> you have written your own answers. The point of an exercise is the friction of
> doing it the first time; the solution exists to clarify after the fact.
>
> Wall-clock numbers below are from a 2023 8-core laptop over the full year of
> NYC yellow-taxi Parquet (~38 M rows, ~600 MB). Your absolute numbers will
> differ; the *ratios* and the *shapes of the plans* are what matter.

---

## Exercise 1 — Your first Spark DataFrame job and the DAG

### Reference solution

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TAXI_PARQUET = "data/yellow_tripdata_2023-*.parquet"

spark = (
    SparkSession.builder.appName("c27-week07-ex01").master("local[*]")
    .config("spark.sql.shuffle.partitions", "64")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Step 1
trips = spark.read.parquet(TAXI_PARQUET)

# Step 2
trips.printSchema()
print("read partitions:", trips.rdd.getNumPartitions())     # e.g. 12 (one per monthly file)

# Step 3 — narrow only, no shuffle
clean = (trips
         .filter((F.col("trip_distance") > 0) & (F.col("total_amount") > 0))
         .withColumn("day", F.to_date("tpep_pickup_datetime")))

# Step 4 — one wide op (groupBy)
daily = (clean.groupBy("VendorID", "day")
              .agg(F.count("*").alias("trips"),
                   F.round(F.sum("total_amount"), 2).alias("revenue")))

# Step 5
daily.explain(mode="formatted")

# Step 6 / Step 7 — the action carries the cost
t0 = time.perf_counter()
daily.orderBy(F.desc("revenue")).show(10)
print("action wall-clock:", round(time.perf_counter() - t0, 2), "s")

spark.stop()
```

### What success looks like (console)

```
read partitions: 12

+--------+----------+-----+--------+
|VendorID|       day|trips| revenue|
+--------+----------+-----+--------+
|       2|2023-03-17|98412|2841190.55|
|       2|2023-03-10|97233|2799004.10|
| ...
+--------+----------+-----+--------+
only showing top 10 rows

action wall-clock: 6.41 s
```

### The explain plan you should reproduce

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[VendorID, day], functions=[count(1), sum(total_amount)])
   +- Exchange hashpartitioning(VendorID, day, 64)          <- THE SHUFFLE (groupBy)
      +- HashAggregate(keys=[VendorID, day], functions=[partial_count(1), partial_sum(total_amount)])
         +- Project [VendorID, to_date(tpep_pickup_datetime) AS day, total_amount]
            +- Filter ((trip_distance > 0.0) AND (total_amount > 0.0))
               +- FileScan parquet [VendorID, tpep_pickup_datetime, trip_distance, total_amount]
                    PushedFilters: [GreaterThan(trip_distance,0.0), GreaterThan(total_amount,0.0)]
```

Read off it:

- **One `Exchange`** → one shuffle → `groupBy` produced it. The job for `daily`
  alone has **two stages**. The Step-6 action adds `orderBy`, which is a *second*
  shuffle (a global sort), so that action's job has **three stages** — this is
  the "explain why" in Step 6.
- **`FileScan parquet [...4 columns...]`** with **`PushedFilters`** → Catalyst
  pruned to four columns and pushed both predicates into the Parquet read. Free,
  from the columnar layout you built in Week 6.
- **Two `HashAggregate`** nodes (`partial_*` below the Exchange, full above) →
  map-side partial aggregation shrinks the data *before* it shuffles.

### Common pitfalls

- **Expecting the `groupBy` line to be slow.** It is instant — it only records
  lineage. The `.show()` is where the time goes (Step 7 proves it). This is lazy
  evaluation; internalize it.
- **`.collect()` on the un-aggregated DataFrame.** Pulling 38 M rows to the
  driver will OOM it. Always aggregate or `.limit()`/`.show()` first.
- **Counting stages wrong.** `shuffles + 1 = stages`. The groupBy job = 2
  stages; add `orderBy` = 3.

---

## Exercise 2 — Shuffles and join strategies

### Reference solution

```python
import time
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
# spark, trips, pay built as in the starter (AQE OFF)

# Step 1 — a pure shuffle
by_pay = trips.groupBy("payment_type").agg(F.count("*").alias("n"))
by_pay.explain(mode="formatted")     # one Exchange hashpartitioning(payment_type, 64)
by_pay.show()

# Step 2 — sort-merge join (broadcast forbidden)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
smj = trips.join(pay, "payment_type", "left")
smj.explain(mode="formatted")        # SortMergeJoin + TWO Exchange nodes

# Step 3 — broadcast hash join (forced)
bhj = trips.join(broadcast(pay), "payment_type", "left")
bhj.explain(mode="formatted")        # BroadcastHashJoin, NO Exchange over trips

# Step 4 — measure
def timeit(df, label):
    out = df.groupBy("payment_type_name").agg(F.count("*").alias("n"))
    t0 = time.perf_counter(); out.collect()
    print(f"{label}: {round(time.perf_counter() - t0, 2)} s")

timeit(smj, "sort-merge")            # e.g. 9.8 s
timeit(bhj, "broadcast")             # e.g. 3.1 s
```

### What success looks like (console)

```
sort-merge: 9.83 s
broadcast:  3.07 s
```

The broadcast join is ~3× faster here because it does **not shuffle the 38 M-row
fact table** — the only thing that moves is one copy of the 6-row dimension to
each executor.

### The two explain plans you should reproduce

**Sort-merge (Step 2)** — note the **two** `Exchange` nodes and the `Sort` nodes:

```
SortMergeJoin [payment_type], [payment_type], LeftOuter
:- Sort [payment_type ASC NULLS FIRST], false, 0
:  +- Exchange hashpartitioning(payment_type, 64)        <- shuffle of the FACT table (expensive)
:     +- FileScan parquet [...]
+- Sort [payment_type ASC NULLS FIRST], false, 0
   +- Exchange hashpartitioning(payment_type, 64)        <- shuffle of the dimension
      +- LocalTableScan [payment_type, payment_type_name]
```

**Broadcast (Step 3)** — note: **no** `Exchange` over the fact table:

```
BroadcastHashJoin [payment_type], [payment_type], LeftOuter, BuildRight
:- FileScan parquet [...]                                <- fact table stays put, NO shuffle
+- BroadcastExchange HashedRelationBroadcastMode(...)    <- only the tiny dim is broadcast
   +- LocalTableScan [payment_type, payment_type_name]
```

### Step 5 answers

- **(a)** Once `pay` is broadcast, a full copy lives in every executor's memory as
  a hash table. Each executor probes that local hash table with its *own*
  partition of `trips` — the fact table never has to move to co-locate matching
  keys, so there is no shuffle of it.
- **(b)** Broadcast is wrong when the "small" side is not small. The driver first
  `collect()`s the broadcast side, then ships a full copy to *every* executor and
  holds it in each one's memory. Past tens of MB this OOMs the driver or the
  executors. The 10 MB `autoBroadcastJoinThreshold` default is deliberately
  conservative for this reason.

### Common pitfalls

- **Forgetting to disable auto-broadcast in Step 2.** With AQE off the threshold
  default (10 MB) still applies, and the 6-row dim will broadcast *anyway* —
  you'll never see a sort-merge join. Set the threshold to `-1` for that step.
- **Leaving AQE on.** AQE can switch a planned sort-merge to a broadcast at
  runtime, hiding the contrast. This exercise runs with AQE off on purpose.
- **Reading the plan top-down.** Read bottom-up: scans at the leaves, join in the
  middle, output at the root.

---

## Exercise 3 — Diagnose and fix data skew

### Reference solution

```python
import time
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
# spark built with aqe=False, autoBroadcastJoinThreshold=-1; trips loaded

# Step 1 — confirm the skew
trips.groupBy("VendorID").count().orderBy(F.desc("count")).show()

# Step 2 — right side keyed by the skewed VendorID
vendor_stats = trips.groupBy("VendorID").agg(F.avg("total_amount").alias("avg_amt"))

def timeit(df, label):
    t0 = time.perf_counter()
    df.groupBy("VendorID").agg(F.count("*").alias("n")).collect()
    print(f"{label}: {round(time.perf_counter() - t0, 2)} s")

# Step 3 — skewed sort-merge join (the "before")
skewed = trips.join(vendor_stats, "VendorID")
timeit(skewed, "skewed sort-merge")           # e.g. 41.2 s; ONE task ~38 s

# Step 4 — Fix A: broadcast
fixed_b = trips.join(broadcast(vendor_stats), "VendorID")
fixed_b.explain(mode="formatted")             # BroadcastHashJoin, no Exchange over trips
timeit(fixed_b, "broadcast fix")              # e.g. 5.9 s

# Step 5 — Fix B: salting (pretend vendor_stats is large)
N = 16
left = trips.withColumn("salt", (F.rand() * N).cast("int"))
salts = spark.range(N).withColumnRenamed("id", "salt")
right = vendor_stats.crossJoin(salts)
salted = left.join(right, on=["VendorID", "salt"])
timeit(salted, "salted join")                 # e.g. 12.4 s

# Step 6 — Fix C: AQE (rebuild session with AQE on)
spark.stop()
spark = build_spark(aqe=True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
trips = spark.read.parquet(TAXI_PARQUET)
vendor_stats = trips.groupBy("VendorID").agg(F.avg("total_amount").alias("avg_amt"))
aqe_join = trips.join(vendor_stats, "VendorID")
timeit(aqe_join, "AQE skew-split")            # e.g. 9.1 s
```

### What success looks like

Step 1 confirms the skew is real:

```
+--------+--------+
|VendorID|   count|
+--------+--------+
|       2|22914003|     <- ~60% of all rows: the hot key
|       1|15102887|
|       6|   41122|
+--------+--------+
```

The Spark UI **Stages tab** for the Step-3 join stage shows the skew unambiguously:

```
Metric              Min    25th   Median   75th    Max
Duration            0.8 s  0.9 s  1.0 s    1.1 s   38 s      <- Max is ~38x Median
Shuffle Read Size   9 MB   9 MB   10 MB    11 MB   612 MB    <- one task pulled vendor 2's whole share
```

The before/after wall-clock table you should produce in Step 7:

```
technique          wall-clock   when to use it
-----------------  ----------   ------------------------------------------------
skewed sort-merge  41.2 s       never on purpose — the baseline to beat
broadcast          5.9 s        small side fits in memory (BEST here)
salting (N=16)     12.4 s       both sides large; broadcast impossible
AQE skew-split     9.1 s        on by default; lowest effort; first thing to try
```

For **this** join, **broadcast wins** because `vendor_stats` is tiny (2 rows) —
ship it everywhere and the fact table never shuffles, so there is no skewed
partition to stall on. Salting is the answer only when the right side is genuinely
too large to broadcast.

### The plans you should reproduce

**Before (skewed sort-merge):** a `SortMergeJoin` over two `Exchange
hashpartitioning(VendorID, 64)` nodes; vendor 2's rows all hash to one partition.

**After (broadcast):**

```
BroadcastHashJoin [VendorID], [VendorID], Inner, BuildRight
:- FileScan parquet [...]                          <- fact table NOT shuffled -> no skew
+- BroadcastExchange ...
   +- HashAggregate(keys=[VendorID], ...)
```

**After (AQE):** the final plan shows `AdaptiveSparkPlan isFinalPlan=true`, an
`AQEShuffleRead` with a skew annotation, and the join's hot partition split into
several sub-tasks.

### Common pitfalls

- **Leaving AQE on for Step 3.** AQE's skew-join handling will *silently* split
  the partition and you will never see the straggler you are supposed to diagnose.
  Steps 1–5 run with `aqe=False` on purpose; only Step 6 turns it on.
- **Salting both sides symmetrically with a random salt.** The *fact* side gets a
  random salt; the *other* side must be **replicated across all salt values**
  (the `crossJoin(salts)`), or matching keys land in different buckets and you
  drop rows. Get this backwards and your join silently returns too few rows.
- **Choosing salt N too large.** Replicating the right side 256× to fix a 2-value
  skew wastes more than it saves. N = 8–32 is the usual range; tune to the number
  of cores and the hotness of the key.
- **Concluding "Spark is slow."** The skewed 41 s is not Spark being slow — it is
  one task doing 38 s of single-threaded work because of the data shape. The cluster
  was ~idle. The fix is the data layout, not more cores.
