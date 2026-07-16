# Challenge 01 — Cut the Scan Cost

> Take one representative query against your lakehouse, optimize the table under it, and prove a large, measured reduction in bytes scanned. No guessing, no "it feels faster" — a before/after number with screenshots.

## Why it matters

On the laptop the query is free; on a metered engine the bytes it scans are the bill. The single most valuable habit you will leave this course with is the reflex to measure bytes-scanned and drive it down before a query ships into a refresh loop. This challenge is that reflex, performed once with evidence so it becomes muscle memory. A senior engineer can look at a slow, expensive query and name which of compaction or pruning will fix it, in what order, and roughly by how much — this is where you earn that.

## The task

Pick **one representative query** — a query a dashboard or a downstream model actually runs, ideally a date-filtered aggregate against your `events`-style table from Weeks 6/9. Starting from the unoptimized lakehouse table, apply compaction and partition pruning and prove the bytes-scanned reduction.

**Target: a ≥10× reduction in bytes scanned** on the representative query, with the before/after evidence to back it.

## Procedure

### Phase 0 — Establish the baseline (≈30 min)

1. Pick and freeze the representative query. Write it down; you will run *exactly this query* before and after.
2. Capture the baseline table state: file count, total size, average file size (Iceberg `.files` metadata table or Delta `DESCRIBE DETAIL`).
3. Run the query and capture the scan metrics from the Spark UI SQL tab (`http://localhost:4040`): **size of files read**, **number of files read**, **partitions pruned**. Screenshot it. Cross-check with DuckDB `EXPLAIN ANALYZE` if you want a second number.

### Phase 1 — Compact (≈30 min)

4. Compact the small files (`rewrite_data_files` binpack, or Delta `OPTIMIZE`). Record `rewritten` vs `added` file counts.
5. Re-run the frozen query, re-capture the scan metrics. Note that file count and wall-clock drop sharply but bytes-scanned likely does **not** — and be ready to explain why.

### Phase 2 — Partition for pruning (≈45 min)

6. Re-create the table with a partition layout aligned to the query's predicate (Iceberg hidden partitioning `days(...)` / `bucket(...)`, or Delta partition / liquid clustering).
7. Re-run the frozen query, re-capture the scan metrics. This is where bytes-scanned collapses. Confirm **partitions pruned > 0**.

### Phase 3 — Prove and write up (≈30 min)

8. Demonstrate you understand *why* pruning fired: run the query once with the predicate function-wrapped (e.g. `date_trunc`) and show pruning silently turns off, then fix it. Include this in the writeup — it proves the reduction is from layout, not luck.
9. Assemble the before/after table and the screenshots.

## Deliverable

A short report (`challenge-01-scan-cost.md`) containing:

1. The frozen representative query.
2. A before/after table with at minimum: `file_count`, `avg_file_size`, `size_of_files_read`, `partitions_pruned`, and `wall_clock`, across three states — baseline, after-compaction, after-partitioning.
3. The two Spark UI scan-node screenshots (baseline and final).
4. Two to three sentences separating the contribution of compaction (latency / overhead) from partitioning (bytes scanned), and one sentence on the predicate-pushdown footgun you demonstrated.

## Pass criteria

- [ ] The same frozen query is used for every measurement.
- [ ] Bytes scanned (size of files read) is reduced by **≥10×** end to end, with screenshots.
- [ ] `partitions pruned > 0` is shown on the final query.
- [ ] The writeup correctly attributes the latency win to compaction and the bytes win to partitioning — not conflated.
- [ ] The function-wrapped-predicate footgun is demonstrated and explained.

## References

- [`../lecture-notes/01-cost-where-it-hides-and-how-to-kill-it.md`](../lecture-notes/01-cost-where-it-hides-and-how-to-kill-it.md) — the cost model, compaction, pruning, measuring bytes scanned.
- [`../exercises/exercise-01-compact-and-measure.sql`](../exercises/exercise-01-compact-and-measure.sql) and [`exercise-02-partition-for-pruning.sql`](../exercises/exercise-02-partition-for-pruning.sql) — the mechanics.
- Apache Iceberg — maintenance / compaction and partitioning. <https://iceberg.apache.org/docs/latest/>
- Delta Lake — `OPTIMIZE`, Z-ORDER, liquid clustering. <https://docs.delta.io/latest/optimizations-oss.html>
- DuckDB — `EXPLAIN ANALYZE`. <https://duckdb.org/docs/sql/statements/explain.html>
