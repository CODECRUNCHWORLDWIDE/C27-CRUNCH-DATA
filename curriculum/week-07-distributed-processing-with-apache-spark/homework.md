# Week 7 Homework

Six practice problems that revisit the week's topics. Each is scoped to about
**45 minutes**, for ~6 hours total. Work in your
`crunch-data-portfolio-<yourhandle>/week-07/homework/` directory so each problem
produces at least one commit you can point to later.

Each problem includes a **problem statement**, **acceptance criteria** (so you
know when you are done), a **hint**, an **estimated time**, and **citations**.

---

## Problem 1 — Predict the stage count, then verify it

**Problem statement.** For each of the three DataFrame pipelines below, *predict
on paper* how many stages the triggering action will run, then verify by reading
`explain(mode="formatted")` (count the `Exchange` nodes) and the Spark UI Jobs tab.

```python
# A
trips.filter(F.col("trip_distance") > 0).select("VendorID", "total_amount").show()
# B
trips.groupBy("VendorID").agg(F.sum("total_amount")).show()
# C
(trips.groupBy("VendorID", "PULocationID").agg(F.count("*").alias("n"))
      .orderBy(F.desc("n")).show())
```

**Acceptance criteria.**

- `homework/p1_stages.md` has a 3-row table: pipeline | predicted stages | actual stages | number of `Exchange` nodes.
- For each pipeline you state which transformation produced each shuffle.
- Your prediction matches reality (A: 1 stage, no shuffle; B: 2 stages, 1 shuffle; C: 3 stages, 2 shuffles).

**Hint.** `stages = shuffles + 1`. Narrow ops (`filter`, `select`) add no shuffle; `groupBy` and `orderBy` each add one. Citation: <https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations>.

**Estimated time.** 45 minutes.

---

## Problem 2 — Force every join strategy and read the plans

**Problem statement.** Take the taxi fact table and the 265-row `dim_zone`. Produce
the join three ways and save each physical plan: (a) default (let Spark choose),
(b) `broadcast(dim_zone)` forced, (c) auto-broadcast disabled
(`spark.sql.autoBroadcastJoinThreshold=-1`) so it becomes a sort-merge join. For
each, name the join node and count the `Exchange` nodes over the fact table.

**Acceptance criteria.**

- `homework/p2_joins/{default,broadcast,sortmerge}.txt` each contain the saved plan.
- `homework/p2_joins/notes.md` states, for each: the join node (`BroadcastHashJoin` / `SortMergeJoin`), and how many shuffles touched the fact table (0 for broadcast, 1 for sort-merge).
- You explain in two sentences why (a) and (b) produce the same plan (the dim is under 10 MB, so Spark broadcasts it anyway).

**Hint.** The broadcast plan has `BroadcastExchange` over the *dim* and **no** `Exchange hashpartitioning` over the fact. Citations: join hints <https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html>; tuning <https://spark.apache.org/docs/latest/sql-performance-tuning.html#other-configuration-options>.

**Estimated time.** 45 minutes.

---

## Problem 3 — Tune `spark.sql.shuffle.partitions` and chart the U-shape

**Problem statement.** Run the same `groupBy("VendorID", "PULocationID").agg(...)`
mart aggregation over the year of taxi data at five settings of
`spark.sql.shuffle.partitions`: 4, 16, 64, 200, 800 (with AQE **off**, so the
setting is honored verbatim). Record the wall-clock for each. You should see a
U-shape: too few partitions overload each task, too many drown the job in
scheduling overhead.

**Acceptance criteria.**

- `homework/p3_shuffle_partitions.md` has a table: partitions | wall-clock | post-shuffle partition count (`result.rdd.getNumPartitions()`).
- A 2-3 sentence explanation of the U-shape and where the sweet spot fell on your machine.
- A note on how AQE's `coalescePartitions` would change the picture (you may re-run with AQE on to show it).

**Hint.** Set `spark.conf.set("spark.sql.shuffle.partitions", n)` before the action and turn AQE off so it does not coalesce. Citation: <https://spark.apache.org/docs/latest/sql-performance-tuning.html#other-configuration-options>.

**Estimated time.** 45 minutes.

---

## Problem 4 — Quantify the skew, then fix it three ways

**Problem statement.** Build the skewed `trips ⋈ vendor_stats on VendorID` join from
Lecture 3 (AQE off, broadcast off). Measure the baseline wall-clock and the
Stages-tab Max/Median task duration. Then apply each fix — broadcast, salting
(N=16), AQE skew-join — and record the wall-clock and Max/Median for each.

**Acceptance criteria.**

- `homework/p4_skew.md` has a 4-row table: technique | wall-clock | Max/Median duration | one-line "when to use".
- You state which fix you would ship for this join and why (broadcast — the side is tiny).
- A screenshot or a copied Stages-tab metrics row for the *before* showing Max ≫ Median.

**Hint.** AQE off makes the skew visible; AQE on (Fix C) hides it by splitting the partition. Citation: <https://spark.apache.org/docs/latest/sql-performance-tuning.html#optimizing-skew-join>.

**Estimated time.** 45 minutes.

---

## Problem 5 — Spark vs DuckDB at three data sizes

**Problem statement.** Run the same mart aggregation (clean → broadcast-join dims →
`groupBy` day/zone/vendor → revenue) in both Spark (`local[*]`) and DuckDB at three
input sizes: **1 month** (~50 MB), **1 quarter** (~150 MB), **1 year** (~600 MB).
Record wall-clock for both engines at each size and find the crossover (if any) on
your machine.

**Acceptance criteria.**

- `homework/p5_spark_vs_duckdb.md` has a table: data size | rows | Spark wall-clock | DuckDB wall-clock | winner.
- A 3-4 sentence verdict: at what size (if any) Spark overtakes DuckDB on your machine, and why DuckDB wins at the small end (no coordination tax).
- The two jobs are confirmed to produce the same row count and total revenue at each size.

**Hint.** DuckDB reads Parquet with `read_parquet('...*.parquet')` and one `GROUP BY`. Spark's per-run JVM/session startup is a fixed cost that dominates at small sizes. Citation: <https://duckdb.org/docs/stable/guides/performance/overview.html>.

**Estimated time.** 45 minutes.

---

## Problem 6 — Read a plan you did not write

**Problem statement.** Take the `explain(mode="formatted")` output below (a join +
aggregation) and annotate it: for each non-leaf node, say what it does and which
SQL/DataFrame operation produced it; mark every shuffle; say how many stages the
query has; and state whether AQE was on.

```
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[zone_name], functions=[sum(revenue)])
   +- Exchange hashpartitioning(zone_name, 64)
      +- HashAggregate(keys=[zone_name], functions=[partial_sum(revenue)])
         +- Project [zone_name, total_amount AS revenue]
            +- BroadcastHashJoin [PULocationID], [LocationID], Inner, BuildRight
               :- Filter (trip_distance > 0.0)
               :  +- FileScan parquet [PULocationID, trip_distance, total_amount]
               +- BroadcastExchange HashedRelationBroadcastMode(...)
                  +- FileScan csv [LocationID, zone_name]
```

**Acceptance criteria.**

- `homework/p6_plan_reading.md` annotates every node, marks the one `Exchange` (the only shuffle), states **2 stages**, names the `BroadcastHashJoin` as a broadcast (no fact-side shuffle), and notes AQE is on (`AdaptiveSparkPlan isFinalPlan=false` = pre-runtime plan).
- You identify the predicate pushdown (`Filter` near the scan) and the column pruning (the `FileScan` reads only three columns).

**Hint.** Read bottom-up. One `Exchange` = one shuffle = two stages. `BroadcastHashJoin` + `BroadcastExchange` over the *dim* means the fact never shuffled for the join. Citation: EXPLAIN <https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html>.

**Estimated time.** 45 minutes.

---

When all six are committed, take the [quiz](./quiz.md) closed-book, then start the
[mini-project](./mini-project/README.md) if you have not already.
