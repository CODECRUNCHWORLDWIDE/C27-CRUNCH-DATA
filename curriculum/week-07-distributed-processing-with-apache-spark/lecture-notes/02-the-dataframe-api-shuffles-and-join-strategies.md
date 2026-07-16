# Lecture 2 — The DataFrame API, Shuffles, and Join Strategies

> **Duration:** ~1.5 hours of reading + running every code block and reading the plans it prints.
> **Prerequisites:** Lecture 1 (execution model, narrow vs wide, the SparkSession).
> **Citations:** SQL/DataFrame guide <https://spark.apache.org/docs/latest/sql-programming-guide.html>; PySpark DataFrame API <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html>; SQL performance tuning <https://spark.apache.org/docs/latest/sql-performance-tuning.html>; join hints <https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html>; EXPLAIN <https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html>.
> **Outcome:** You can write a non-trivial DataFrame job, predict where it shuffles, read its physical plan, name the join strategy Spark chose and why, force a broadcast when you want one, and explain what AQE changed at runtime.

If you only remember one thing from this lecture, remember this:

> **A join either shuffles or it does not. If one side fits under the broadcast threshold, Spark ships that side to every executor and joins with NO shuffle — the broadcast hash join, the cheapest join there is. If both sides are large, Spark shuffles and sorts both — the sort-merge join, correct but expensive. The whole game of join performance is getting a broadcast when you can, and a well-balanced sort-merge when you can't. You read which one you got in the physical plan: `BroadcastHashJoin` vs `SortMergeJoin`, with `Exchange` marking every shuffle.**

Lecture 1's `SparkSession` over the lakehouse is the input. This lecture turns DataFrame calls into plans you can read.

---

## 1. The DataFrame API: structured, lazy, optimized

A Spark **DataFrame** is a distributed table with a schema: named, typed columns over partitioned rows. It is deliberately pandas-shaped so the API feels familiar, but two things differ profoundly:

1. **It is lazy** (Lecture 1 §5) — operations build a plan and compute nothing until an action.
2. **It is optimized** — every DataFrame query passes through the **Catalyst** optimizer (Armbrust et al. 2015, *SIGMOD*, <https://dl.acm.org/doi/10.1145/2723372.2742797>), which rewrites it (predicate pushdown, column pruning, constant folding, join reordering) and then through **Tungsten** code generation, which compiles the plan to tight JVM bytecode. You write declarative DataFrame calls; Spark decides *how* to execute them.

The core verbs (PySpark DataFrame API, <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html>):

```python
from pyspark.sql import functions as F

trips = spark.table("lake.nyc.yellow_tripdata")

(trips
 .select("VendorID", "tpep_pickup_datetime", "trip_distance",
         "passenger_count", "total_amount", "payment_type", "PULocationID")  # column prune
 .filter((F.col("trip_distance") > 0) & (F.col("total_amount") > 0))         # row prune
 .withColumn("day", F.to_date("tpep_pickup_datetime"))                       # derive
 .withColumn("tip_share",
             F.col("total_amount") / F.greatest(F.col("trip_distance"), F.lit(0.1)))
 .groupBy("VendorID", "day")                                                 # WIDE
 .agg(F.count("*").alias("trips"),
      F.sum("total_amount").alias("revenue"),
      F.avg("trip_distance").alias("avg_dist"))
 .orderBy(F.desc("revenue")))                                                # WIDE (global sort)
```

`select`, `filter`, `withColumn` are **narrow**. `groupBy().agg()` and `orderBy()` are **wide** — each forces a shuffle and a stage boundary. Reading a pipeline, you can count the wide verbs to count the stages.

---

## 2. Transformations vs actions, and why it matters for shuffles

Recall: transformations are lazy, actions are eager. The reason this matters *here* is that a shuffle only happens when an action drives a wide transformation to execute, and Spark may **reuse** shuffle output across actions if you cache. Two pitfalls:

- **Re-shuffling on every action.** If you call `.count()` then `.show()` on the same un-cached DataFrame that contains a `groupBy`, Spark runs the whole pipeline — including the shuffle — **twice**. Cache the post-shuffle result with `.cache()` (and an action to materialize it) if you will consume it more than once.
- **The slow line is the action.** When `.show()` takes 90 seconds, the cost is the shuffle that `groupBy` recorded three lines earlier, not the `show` itself.

```python
agg = trips.groupBy("VendorID").agg(F.count("*").alias("n")).cache()
agg.count()      # materializes the shuffle once, caches the result
agg.show()       # reuses the cached result, no second shuffle
```

---

## 3. The shuffle, mechanically

A **shuffle** is how Spark gets rows that share a key onto the same partition. It is the most expensive thing Spark does, and understanding the mechanics tells you why (RDD guide, shuffle operations, <https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations>).

```
MAP SIDE (end of stage N)                 REDUCE SIDE (start of stage N+1)

Task on partition P0                      Task on shuffle-partition S0
  for each row:                             reads its slice from EVERY
    compute hash(key) % numShufflePartitions   map task's output files
    write row to local file for that          (a network pull from each
    shuffle-partition bucket                   executor that ran a map task)
  -> spill/sort buckets to disk            -> merge + final aggregate/join
        │                                          ▲
        └──────── all-to-all over network ─────────┘
```

Step by step:

1. **Map side.** Each task in the upstream stage takes its rows and, for each row, computes `hash(key) % spark.sql.shuffle.partitions` to decide which downstream partition it belongs to. It writes rows into per-target-partition buckets and **spills them to local disk** (`spark.local.dir`), sorting and serializing as it goes.
2. **The exchange.** This is the `Exchange` node in the plan. Map output is now sitting as files on every executor's local disk, organized by target partition.
3. **Reduce side.** Each task in the downstream stage is responsible for one shuffle partition. It must **fetch its slice from every map task's output** — an all-to-all network transfer. It merges the fetched pieces and does the final aggregation or join.

Why this hurts: it touches **disk** (spill + read), the **network** (all-to-all transfer), the **CPU** (serialize/deserialize/sort), and it is a **barrier** — the reduce stage cannot start until every map task has finished writing. A single slow map task delays the whole shuffle. And if one reduce partition gets far more rows than the others, that is **skew** (Lecture 3).

The lesson that names the lecture: **avoid shuffles you don't need, shrink the ones you do.** Filter early so fewer rows shuffle. Project away columns you don't need so each shuffled row is smaller. Pre-aggregate where possible (Spark already does map-side partial aggregation for `groupBy`). And, above all, **avoid shuffling a join when you can broadcast it instead.**

---

## 4. `spark.sql.shuffle.partitions` — the most important knob

After any shuffle, the data lands in `spark.sql.shuffle.partitions` partitions. **Default: 200** (SQL performance tuning, other configuration options, <https://spark.apache.org/docs/latest/sql-performance-tuning.html#other-configuration-options>). This default is almost always wrong:

- **Too many for small data.** Aggregating 3 million rows into 200 partitions means each reduce task handles ~15k rows — 200 tasks of scheduling overhead for trivial work. The job is dominated by task launch, not computation.
- **Too few for huge data.** Shuffling 2 billion rows into 200 partitions means each partition is ~10 GB — far more than an executor can hold, causing heavy spill or OOM.

The heuristic: target partitions of **~128 MB each after the shuffle**, and a count that is a small multiple of total executor cores. For the year of taxi data on an 8-core laptop, 64–128 is right; 200 wastes time.

```python
spark.conf.set("spark.sql.shuffle.partitions", 64)
```

**Adaptive Query Execution makes this less critical** (Section 7): with AQE on, Spark *coalesces* over-provisioned shuffle partitions at runtime using the actual sizes it observed, so setting a high-ish value and letting AQE coalesce down is a reasonable strategy. But you should still understand the knob, because AQE cannot fix a value set absurdly low.

---

## 5. Reading a physical plan with `explain`

`df.explain(mode="formatted")` prints the **physical plan**: the tree of operators Spark will actually run (EXPLAIN, <https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html>). Read it **bottom-up** — the leaves are the scans, the root is the final output.

```python
agg = (trips
       .filter(F.col("trip_distance") > 0)
       .groupBy("VendorID")
       .agg(F.count("*").alias("n")))
agg.explain(mode="formatted")
```

A representative output (trimmed):

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[VendorID], functions=[count(1)])           <- final aggregate (reduce side)
   +- Exchange hashpartitioning(VendorID, 64)                     <- THE SHUFFLE
      +- HashAggregate(keys=[VendorID], functions=[partial_count(1)])  <- partial aggregate (map side)
         +- Project [VendorID]                                    <- column pruning
            +- Filter (trip_distance > 0.0)                       <- predicate pushed near the scan
               +- BatchScan lake.nyc.yellow_tripdata[VendorID, trip_distance]  <- Iceberg scan
```

What to read off it:

- **`BatchScan ... [VendorID, trip_distance]`** — Spark only reads two columns, not the whole row. That is **column pruning**, free from the columnar Parquet/Iceberg layout (Week 6).
- **`Filter (trip_distance > 0.0)`** sitting just above the scan — **predicate pushdown**. Rows are dropped as early as possible.
- **Two `HashAggregate` nodes** with `partial_count` below the exchange and `count` above — Spark does a **map-side partial aggregation** *before* the shuffle, so fewer rows cross the network. This is automatic for `groupBy`.
- **`Exchange hashpartitioning(VendorID, 64)`** — the shuffle, repartitioning by `VendorID` into 64 partitions. Every `Exchange` is a shuffle and a stage boundary. Count the `Exchange` nodes to count the shuffles.
- **`AdaptiveSparkPlan isFinalPlan=false`** at the root — AQE is on; this is the plan *before* runtime re-optimization. After execution it becomes `isFinalPlan=true` and may differ (coalesced partitions, switched joins).

`explain(mode="formatted")` is the verbose, readable form. There is also `explain(mode="extended")` (parsed/analyzed/optimized/physical), `explain(mode="cost")` (with size estimates), and the bare `explain()` (terse). Use `"formatted"` for learning.

---

## 6. Join strategies — the heart of the lecture

When Spark joins two DataFrames, it picks one of several **physical join strategies**. Which one it picks dominates performance.

### 6.1 Broadcast hash join — no shuffle

If one side is **small enough to fit in each executor's memory**, Spark **broadcasts** it: serializes the whole small table, ships a copy to every executor, builds a hash table from it on each executor, and then **streams the large side through locally**, probing the hash table per row. **No shuffle of the large side at all.** This is the cheapest join and the single biggest lever you have.

Spark does this automatically when it estimates one side is under **`spark.sql.autoBroadcastJoinThreshold`**, default **10 MB** (10485760 bytes) (SQL performance tuning, <https://spark.apache.org/docs/latest/sql-performance-tuning.html#other-configuration-options>). Dimension tables — payment types (6 rows), vendors (a handful), taxi zones (265 rows), a date dimension — are all far under 10 MB and should always broadcast.

```python
zones = spark.table("lake.nyc.dim_zone")        # 265 rows, tiny
joined = trips.join(zones, trips.PULocationID == zones.LocationID, "left")
joined.explain(mode="formatted")
# look for:  +- BroadcastHashJoin ...
#            :  +- BroadcastExchange HashedRelationBroadcastMode...
```

If Spark does **not** broadcast automatically (it underestimated the size, or the side is just over threshold), force it with the **`broadcast()` hint** from `pyspark.sql.functions`:

```python
from pyspark.sql.functions import broadcast
joined = trips.join(broadcast(zones), trips.PULocationID == zones.LocationID, "left")
```

The plan will now show `BroadcastHashJoin` with a `BroadcastExchange` over the small side — and crucially, **no `Exchange hashpartitioning` over the large side.** That absent shuffle is the win.

You can also raise (or lower) the threshold globally, or disable auto-broadcast with `-1`:

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)  # 50 MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)                # never auto-broadcast
```

**Caution:** broadcasting something too large blows up the driver (it collects the small side first) and every executor's memory. Broadcast is for *small* tables — single-digit to low-tens of MB.

### 6.2 Sort-merge join — shuffle both sides

When **both** sides are large, neither can be broadcast, so Spark falls back to the **sort-merge join** (the default for large–large joins, `spark.sql.join.preferSortMergeJoin=true`):

1. **Shuffle** both sides by the join key (two `Exchange` nodes).
2. **Sort** each partition of each side by the key.
3. **Merge** the two sorted streams partition by partition, matching equal keys.

Correct and scalable to any size, but it pays for **two shuffles and two sorts**. The plan:

```
SortMergeJoin [PULocationID], [LocationID], LeftOuter
:- Sort [PULocationID ASC], false, 0
:  +- Exchange hashpartitioning(PULocationID, 64)      <- shuffle of left side
+- Sort [LocationID ASC], false, 0
   +- Exchange hashpartitioning(LocationID, 64)        <- shuffle of right side
```

Two `Exchange` nodes = two shuffles. If you see a `SortMergeJoin` where one side is actually small, that is a bug — broadcast it.

### 6.3 Shuffle hash join — shuffle both, hash the smaller

A middle option: shuffle both sides by key (like sort-merge) but **build a hash table** from the smaller shuffled side instead of sorting. Avoids the sort but needs the smaller side's partition to fit in memory. Spark uses it in narrower circumstances (`spark.sql.join.preferSortMergeJoin=false` or when AQE decides); you will see it less often than the other two. Recognize it in the plan as `ShuffledHashJoin`.

### 6.4 The decision table

| Situation | Strategy | Shuffle? | Plan node |
|---|---|---|---|
| One side < ~10 MB (a dimension) | Broadcast hash join | No (large side stays put) | `BroadcastHashJoin` |
| Both sides large | Sort-merge join | Yes, both sides + sort | `SortMergeJoin` |
| Both large, one fits in memory after shuffle, no sort wanted | Shuffle hash join | Yes, both sides | `ShuffledHashJoin` |

The instinct to build: **fact joined to dimension → broadcast the dimension.** Fact joined to fact → sort-merge, and watch for skew (Lecture 3). When in doubt, `explain` and look at which node you got.

---

## 7. Adaptive Query Execution (AQE)

Catalyst optimizes the plan *before* execution using **estimated** sizes — which are often wrong, especially after filters and joins. **Adaptive Query Execution** re-optimizes the plan *at runtime* using the **actual** statistics observed after each shuffle. AQE is **on by default since Spark 3.2** (`spark.sql.adaptive.enabled=true`; SQL performance tuning, AQE section, <https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution>). It does three things:

1. **Coalescing shuffle partitions.** If you set 200 shuffle partitions but a stage only produced 30 MB of shuffle output, AQE merges the tiny partitions into a few right-sized ones (`spark.sql.adaptive.coalescePartitions.enabled`). This is why over-provisioning `shuffle.partitions` is safe with AQE on.
2. **Dynamically switching join strategy.** If Catalyst planned a sort-merge join but at runtime one side turns out to be under the broadcast threshold (e.g. a filter shrank it), AQE switches it to a broadcast hash join — saving a shuffle.
3. **Optimizing skew joins.** AQE detects partitions that are far larger than the median and **splits** them into sub-partitions so the work spreads across more tasks (`spark.sql.adaptive.skewJoin.enabled`, default true). This is Lecture 3's automatic skew fix.

You can confirm AQE ran by re-`explain`-ing *after* an action, or by reading the SQL tab in the UI: the final plan shows `AdaptiveSparkPlan isFinalPlan=true` and may carry annotations like `Coalesced partitions` or `Skewed partition split`.

```python
spark.conf.set("spark.sql.adaptive.enabled", True)             # on by default in 3.2+
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
```

AQE is powerful but not magic: it cannot create a broadcast that the data doesn't support, and its skew handling helps sort-merge joins but is no substitute for broadcasting a small dimension. Understand the manual fixes (Lecture 3) so you know when AQE has and hasn't saved you.

---

## 8. A worked plan: mart rebuild with a dimension join

Put it together. A slice of the Week 7 mart — daily revenue by vendor and payment type, with the payment-type *name* joined in from a tiny dimension:

```python
from pyspark.sql.functions import broadcast

trips = spark.table("lake.nyc.yellow_tripdata")
pay   = spark.table("lake.nyc.dim_payment_type")   # 6 rows

mart = (
    trips
    .filter((F.col("trip_distance") > 0) & (F.col("total_amount") > 0))     # narrow
    .withColumn("day", F.to_date("tpep_pickup_datetime"))                   # narrow
    .join(broadcast(pay), "payment_type", "left")                           # broadcast join, NO shuffle
    .groupBy("VendorID", "day", "payment_type_name")                        # WIDE -> shuffle
    .agg(F.count("*").alias("trips"),
         F.round(F.sum("total_amount"), 2).alias("revenue"))
)
mart.explain(mode="formatted")
```

Read the plan and confirm:

- `BroadcastHashJoin` with a `BroadcastExchange` over `pay` — and **no** `Exchange hashpartitioning` over `trips` for the join. The dimension join cost nothing in network.
- **Exactly one** `Exchange hashpartitioning(VendorID, day, payment_type_name, 64)` — the single shuffle, from the `groupBy`. One shuffle, one stage boundary.
- Filters and projections pushed down to the `BatchScan`.

One shuffle for the whole mart, because the only wide operation is the final `groupBy` and the dimension join was broadcast. That is what a well-written mart job looks like in plan form.

---

## 9. Summary

- A **DataFrame** is a lazy, schema-carrying, distributed table; Catalyst optimizes it and Tungsten compiles it. You write *what*, Spark decides *how*.
- **Transformations** build lineage (no compute); **actions** trigger jobs. Cache a post-shuffle result you'll reuse, or pay for the shuffle on every action.
- A **shuffle** redistributes rows by key across the network — map-side spill, all-to-all transfer, reduce-side merge. It touches disk, network, and CPU and is a barrier. Avoid the ones you don't need; shrink the rest with early filters and projections.
- **`spark.sql.shuffle.partitions`** (default 200) sets post-shuffle partition count; target ~128 MB per partition. AQE can coalesce an over-provisioned value at runtime.
- **Read physical plans bottom-up** with `explain(mode="formatted")`: `BatchScan` (with pushed-down filters and pruned columns), `Exchange` (= a shuffle), `HashAggregate` (partial + final), and the join node.
- **Join strategies:** **broadcast hash join** (small side shipped everywhere, no shuffle — force with `broadcast()`); **sort-merge join** (both sides shuffled and sorted — for large–large); **shuffle hash join** (both shuffled, smaller hashed). Threshold: `spark.sql.autoBroadcastJoinThreshold` (10 MB). Fact-to-dimension → broadcast the dimension.
- **AQE** (`spark.sql.adaptive.enabled`, on by default 3.2+) re-optimizes at runtime: coalesces shuffle partitions, switches join strategies, and splits skewed partitions. Confirm with `AdaptiveSparkPlan isFinalPlan=true`.

Next: **Lecture 3 — data skew, the Spark UI tab by tab, the salting and broadcast fixes, and the honest comparison of Spark against DuckDB-on-one-big-machine.**
