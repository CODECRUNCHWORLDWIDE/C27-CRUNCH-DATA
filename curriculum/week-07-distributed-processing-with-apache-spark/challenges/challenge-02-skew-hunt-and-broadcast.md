# Challenge 2 — Skew hunt and broadcast/salting fix

> **Estimated time:** 2 hours.
> **Prerequisites:** Lecture 3 (skew, the Spark UI, salting/broadcast/AQE fixes); Challenge 1's mart job and lakehouse setup.
> **Citations:** Web UI <https://spark.apache.org/docs/latest/web-ui.html>; skew join <https://spark.apache.org/docs/latest/sql-performance-tuning.html#optimizing-skew-join>; AQE <https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution>; join hints <https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html>.
> **Goal:** Deliberately trigger a skewed join over the NYC taxi data, find the straggler task in the Spark UI, name the hot key, and fix the skew three ways (broadcast, salting, AQE) with a measured before/after wall-clock for each. Capture the Spark UI showing the straggler.

This challenge makes the lecture's "the shuffle is the enemy" concrete on real,
genuinely-skewed data. You will produce a slow job, prove from the UI *why* it is
slow (not "Spark is slow" — *one task* is slow, for a *named reason*), and show
three fixes with numbers.

---

## Premise

The NYC taxi data is lopsided: `VendorID` 2 is ~60% of all rows, and a handful of
Manhattan pickup zones (`PULocationID`) dominate the rest. Join the fact table to
a table keyed by one of these hot columns **with auto-broadcast disabled and AQE
off**, and you force a sort-merge join whose shuffle dumps the hot key into one
partition. One task does most of the work; the stage waits on it. That is the
straggler you are going to hunt.

---

## Setup

Reuse Challenge 1's `docker-compose.yml` (Spark + MinIO + Iceberg). All the work
here is in one PySpark script, `skew_hunt.py`, submitted the same way:

```bash
docker compose exec spark spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  skew_hunt.py
```

Keep the Spark UI open at `http://localhost:4040` the whole time. Because the app
exits when the script ends, either add an `input(...)` pause before `spark.stop()`
so the live UI stays up, or enable the Spark **history server** so you can inspect
after the fact.

---

## Tasks

### Task 1 — Prove the skew exists

Read the fact table and show the per-`VendorID` row distribution
(`groupBy("VendorID").count().orderBy(desc("count"))`). Record the exact share of
the dominant vendor. Do the same for the top 10 `PULocationID` values. Save the
two distributions to `evidence/key_distributions.txt`. These are your hot keys.

### Task 2 — Trigger the skewed join (the "before")

With **AQE off** (`spark.sql.adaptive.enabled=false`) and **auto-broadcast off**
(`spark.sql.autoBroadcastJoinThreshold=-1`), build a join on the hot key. A clean
way: aggregate the fact to per-`VendorID` stats and join it back to the fact on
`VendorID`, then aggregate downstream so a single action drives the whole join:

```python
vendor_stats = trips.groupBy("VendorID").agg(F.avg("total_amount").alias("avg_amt"))
skewed = trips.join(vendor_stats, "VendorID")
t0 = time.perf_counter()
skewed.groupBy("VendorID").agg(F.count("*").alias("n")).collect()
print("skewed:", round(time.perf_counter() - t0, 2), "s")
```

Record the wall-clock.

### Task 3 — Find the straggler in the Spark UI

In the UI: **Jobs** → the slow job → its slow **Stage**. In the stage's
summary-metrics table read the **Duration** and **Shuffle Read Size** rows
(Min / 25th / Median / 75th / **Max**). Confirm **Max ≫ Median** (the straggler).
Note which task index is the straggler and how much shuffle it read. Take a
**screenshot** of the Stages tab showing the skewed distribution (and the event
timeline with the one long bar) and save it to
`evidence/skew_stages_tab.png`. Write a 3-sentence diagnosis in
`evidence/diagnosis.md`: which key, which task, how many times the median.

### Task 4 — Fix A: broadcast

`vendor_stats` is tiny, so the honest first fix is broadcasting it:

```python
fixed = trips.join(broadcast(vendor_stats), "VendorID")
fixed.explain(mode="formatted")   # confirm BroadcastHashJoin, no Exchange over trips
```

Re-time the same downstream aggregate. Re-open the Stages tab and confirm
**Max ≈ Median** now (no skewed partition because the fact never shuffled).
Screenshot it to `evidence/fixed_broadcast_stages_tab.png`.

### Task 5 — Fix B: salting (the large–large technique)

Pretend the right side is too large to broadcast and salt the hot key
(`N = 16`): random salt on the fact, `crossJoin` the right side across all salt
values, join on `(VendorID, salt)`. Re-time. Confirm the Stages tab Max dropped
relative to Task 2. Note the cost of replicating the right side `N×`.

### Task 6 — Fix C: AQE skew join

Rebuild the session with `spark.sql.adaptive.enabled=true` and
`spark.sql.adaptive.skewJoin.enabled=true`. Re-run the Task-2 skewed join
unchanged. Re-time. In the **SQL/DataFrame** tab, confirm the final plan shows
`AdaptiveSparkPlan isFinalPlan=true` with a skew-split annotation on the join.

### Task 7 — The before/after table

Produce `RESULTS.md`:

```markdown
# Skew hunt — before and after

Hot key: VendorID = 2 (__% of rows). Join: trips ⋈ vendor_stats on VendorID.
Machine: <...>. Spark 3.5.3, shuffle.partitions=64.

| Technique          | Wall-clock | Max/Median task duration | When to use                  |
|--------------------|-----------:|-------------------------:|------------------------------|
| skewed sort-merge  |      __ s  |          __x             | never on purpose (baseline)  |
| broadcast          |      __ s  |          ~1x             | small side fits in memory    |
| salting (N=16)     |      __ s  |          __x             | both sides large             |
| AQE skew-split     |      __ s  |          __x             | on by default; first to try  |

## Verdict
<which fix you would ship for THIS join and why>
```

---

## Acceptance criteria

- `evidence/key_distributions.txt` records the hot-key shares for `VendorID` and top `PULocationID`.
- `evidence/skew_stages_tab.png` clearly shows Max-task-duration ≫ Median (the straggler) in the *before* run; `evidence/fixed_broadcast_stages_tab.png` shows Max ≈ Median *after*.
- `evidence/diagnosis.md` names the hot key, the straggler task, and the Max/Median ratio.
- The `explain` for the broadcast fix shows `BroadcastHashJoin` and **no** `Exchange hashpartitioning` over the fact table.
- `RESULTS.md` reports wall-clock and Max/Median for all four techniques (skewed baseline + three fixes) and a verdict naming the fix you would ship and why.
- Commit message in the style of `c27-w07-ch2: vendor-2 skew straggler 38x median, broadcast fix 7x faster`.

## Stretch

- Repeat the whole hunt with `PULocationID` as the hot key instead of `VendorID` (it skews differently — many sparse zones, a few dominant ones). Does salting or AQE do relatively better there?
- Turn AQE on but skew-join **off** (`spark.sql.adaptive.skewJoin.enabled=false`) and confirm the straggler returns — proving it was AQE's skew handling, not AQE's partition coalescing, that fixed it.
- Tune `spark.sql.adaptive.skewJoin.skewedPartitionFactor` and `...skewedPartitionThresholdInBytes` and observe when AQE decides a partition is "skewed enough" to split.
