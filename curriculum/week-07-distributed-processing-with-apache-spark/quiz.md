# Week 7 — Quiz

Ten multiple-choice questions on the Spark execution model, shuffles, join
strategies, data skew, the Spark UI, and Spark vs DuckDB. Take it with the lecture
notes closed. Aim for 9/10 before the mini-project. Answer key at the bottom — do
not peek.

---

**Q1.** In a Spark application, the process that runs your PySpark script, holds
the `SparkSession`, builds the DAG, and schedules tasks is the:

- A) Executor.
- B) Cluster manager.
- C) Driver.
- D) Worker node.

---

**Q2.** A Spark job is split into stages at:

- A) Every transformation.
- B) Every shuffle boundary (wide dependency).
- C) Every action.
- D) Every 200 rows.

---

**Q3.** Which of these is a **narrow** transformation (no shuffle)?

- A) `groupBy("VendorID").count()`
- B) `join(other, "key")` (large–large)
- C) `withColumn("day", to_date("ts"))`
- D) `orderBy("revenue")`

---

**Q4.** You write `df2 = df.groupBy("k").count()` and the line returns instantly,
but `df2.show()` three lines later takes 40 seconds. This is because:

- A) `show()` is buggy.
- B) Spark is lazy: `groupBy` only records lineage; the shuffle and aggregation actually run when the action (`show`) fires.
- C) The schema is being inferred on every call.
- D) `groupBy` ran twice.

---

**Q5.** `spark.sql.shuffle.partitions` defaults to:

- A) 8.
- B) 64.
- C) 200.
- D) The number of input files.

---

**Q6.** You join a 38-million-row fact table to a 265-row dimension. The right
default strategy, and the one that shuffles the *least*, is:

- A) Sort-merge join (shuffle and sort both sides).
- B) Broadcast hash join (ship the dimension to every executor; do not shuffle the fact).
- C) Cross join.
- D) Shuffle hash join on both sides.

---

**Q7.** `spark.sql.autoBroadcastJoinThreshold` defaults to **10 MB**. What does it
control?

- A) The maximum size of any DataFrame.
- B) The estimated size below which Spark will automatically broadcast a join side instead of shuffling it.
- C) The shuffle spill threshold.
- D) The size of each Parquet row group.

---

**Q8.** In the Spark UI Stages tab, the task **Duration** distribution reads
Min 1 s, Median 1 s, Max 90 s. The most likely diagnosis is:

- A) The cluster is out of cores.
- B) Data skew: one task got a hot key (a partition far larger than the others) and became a straggler.
- C) The network is down.
- D) AQE is misconfigured for partition coalescing.

---

**Q9.** For a large–large join skewed on one hot key where neither side fits the
broadcast threshold, the appropriate manual fix is:

- A) Broadcast the larger side.
- B) Salt the hot key (random salt on one side, replicate the other side across all salt values) so the hot key spreads across many partitions.
- C) Increase `spark.sql.shuffle.partitions` to 10,000.
- D) Switch to a cross join.

---

**Q10.** You benchmark a mart aggregation over 600 MB of taxi Parquet on your
laptop: DuckDB finishes in 4 s, Spark `local[*]` in 14 s. The correct engineering
conclusion is:

- A) Spark is broken.
- B) At this data size DuckDB wins because it pays none of Spark's coordination tax (no JVM startup, no shuffle-to-disk, no serialization); Spark earns its keep only when the data or compute exceeds one machine.
- C) Always use Spark; the benchmark is wrong.
- D) DuckDB cannot produce the same result as Spark.

---

## Answer key

<details>
<summary>Click to reveal answers</summary>

1. **C** — The **driver** runs your program, holds the `SparkSession`/`SparkContext`, builds the DAG, and schedules stages and tasks onto executors. Executors do the data work; the cluster manager grants resources. Lecture 1 §2. <https://spark.apache.org/docs/latest/cluster-overview.html>

2. **B** — Stages are cut at **shuffle boundaries** (wide dependencies). All narrow transformations between two shuffles fuse into one stage. `stages = shuffles + 1`. Actions trigger *jobs*, not stages. Lecture 1 §3, §6. <https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations>

3. **C** — `withColumn` is **narrow**: each output partition depends on exactly one input partition, no data crosses the network. `groupBy`, large–large `join`, and `orderBy` are all wide and force a shuffle. Lecture 1 §6.

4. **B** — **Lazy evaluation.** Transformations (`groupBy`) only extend the lineage and compute nothing; the work runs when an **action** (`show`) fires. The slow line is always the action, not the transformation that "looks" expensive. Lecture 1 §5, Lecture 2 §2.

5. **C** — **200.** This is the default post-shuffle partition count and is usually wrong: too many tiny partitions for small data, too few huge ones for large data. AQE can coalesce an over-provisioned value at runtime. Lecture 1 §7, Lecture 2 §4. <https://spark.apache.org/docs/latest/sql-performance-tuning.html#other-configuration-options>

6. **B** — **Broadcast hash join.** A 265-row dimension is far under the 10 MB threshold, so Spark ships it to every executor and joins locally, **without shuffling the 38 M-row fact table**. That absent fact-side shuffle is the win. Lecture 2 §6.1. <https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html>

7. **B** — It is the estimated-size cutoff below which Spark **auto-broadcasts** a join side rather than shuffling it for a sort-merge join. Default 10 MB (10485760 bytes); set to `-1` to disable. Lecture 2 §6.1. <https://spark.apache.org/docs/latest/sql-performance-tuning.html#other-configuration-options>

8. **B** — **Data skew.** When Max task duration is many times the Median, one task got a partition far larger than its siblings (a hot key) and became a straggler. The stage cannot finish until that one task does; adding cores does not help. Read the shuffle-read-size distribution to confirm one task pulled far more. Lecture 3 §1, §2.2. <https://spark.apache.org/docs/latest/web-ui.html#stages-tab>

9. **B** — **Salting.** Append a random salt (0..N-1) to the hot side and replicate the other side across all salt values, then join on the compound key — the hot key now spreads across N partitions instead of one. Broadcasting the *larger* side (A) is nonsense; bumping shuffle partitions to 10,000 (C) does not split a single hot key's rows, which all still hash to one bucket. Lecture 3 §4. <https://spark.apache.org/docs/latest/sql-performance-tuning.html#optimizing-skew-join>

10. **B** — DuckDB wins at this scale because it pays **none** of Spark's coordination tax — no driver/executor split, no JVM warmup, no shuffle-to-disk, no cross-network serialization. Spark earns its keep when the data or compute exceeds one machine (or you need Spark-only features). The correct, defensible conclusion is "this mart does not need Spark at 600 MB," reached from a measured benchmark — not from habit. Lecture 3 §7. <https://duckdb.org/docs/stable/guides/performance/overview.html>

</details>

---

If you scored under 7, re-read Lecture 1 §2–6 (the execution model and narrow vs
wide) and Lecture 2 §3–6 (shuffles and join strategies). If you scored 9 or 10, you
are ready to start the [mini-project](./mini-project/README.md).

## Self-assessment scale

Rate yourself 1–5 on each; aim for 4+ before moving on:

- **The model:** I can trace one action from my Python line to the tasks on executors, and name driver/executor/stage/task without looking it up.
- **Shuffles:** I can read a chain of DataFrame calls and predict exactly where each shuffle (stage boundary) falls.
- **Joins:** I can name the join strategy from an `explain` plan and justify broadcast vs sort-merge from the input sizes.
- **Skew:** I can spot a straggler in the Spark UI Stages tab and pick the right fix (broadcast / salt / AQE) for the situation.
- **Judgment:** I can argue from a benchmark whether a given job belongs on Spark or on DuckDB-on-one-machine.
