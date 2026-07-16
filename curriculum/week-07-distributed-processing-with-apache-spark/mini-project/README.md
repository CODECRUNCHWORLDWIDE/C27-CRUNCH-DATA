# Mini-Project — Spark mart on the lakehouse, with skew injected and fixed

> Build a reproducible Spark job that rebuilds the dimensional mart over a year of NYC taxi data on the Iceberg lakehouse, deliberately inject a skewed join and fix it, benchmark the whole thing against the DuckDB single-node version, and deliver a Spark-UI screenshot that proves you diagnosed the skew. End with: a working `build_mart.py`, an Iceberg mart table, a `PERF.md` with the Spark-vs-DuckDB wall-clock, a `SKEW.md` with before/after numbers, and a Spark-UI screenshot of the straggler.

This is the C27 Week-7 capstone. It is the union of Challenge 1 (rebuild the mart
on Spark, benchmark vs DuckDB) and Challenge 2 (hunt and fix skew), assembled into
one portfolio-grade deliverable you can point a recruiter at: a real distributed
job over a real lakehouse, with a measured argument about *why* it is built the
way it is and *whether Spark was even the right call*.

**Estimated time:** 10 hours (split across Wednesday → Saturday in the suggested schedule).

---

## What you will produce

In your portfolio repo (`crunch-data-portfolio-<yourhandle>`), add a
`week-07/mini-project/` directory:

```
crunch-data-portfolio-<yourhandle>/
├── README.md                       (updated, with a Week 7 section)
└── week-07/
    └── mini-project/
        ├── README.md               this spec / your write-up (~600-800 words)
        ├── docker-compose.yml      Spark + MinIO + Iceberg (extends Week 6)
        ├── run.sh                  one-command reproduction (compose up -> submit -> down)
        ├── requirements.txt        pyspark==3.5.3, duckdb, pyarrow (pinned)
        ├── jobs/
        │   ├── build_mart.py       the Spark mart job (with the skew fix)
        │   ├── build_mart_skewed.py the deliberately-skewed "before" variant
        │   └── build_mart_duckdb.py the single-node DuckDB baseline
        ├── dims/
        │   ├── dim_payment_type.csv  6 rows
        │   └── dim_zone.csv          265 rows (NYC TLC taxi-zone lookup)
        ├── plans/
        │   ├── mart_before.txt      explain() of the skewed join
        │   └── mart_after.txt       explain() after the fix
        ├── evidence/
        │   ├── skew_stages_tab.png  Spark UI: the straggler (Max >> Median)
        │   └── fixed_stages_tab.png Spark UI: after the fix (Max ~= Median)
        ├── results/
        │   ├── mart_spark.parquet   the mart from Spark (also written to Iceberg)
        │   └── mart_duckdb.parquet  the mart from DuckDB
        ├── PERF.md                  Spark vs DuckDB wall-clock benchmark
        └── SKEW.md                  before/after skew fix, with the screenshots
```

---

## Topology

```
                         ┌──────────────────────────────────────────┐
                         │            Docker network                  │
                         │                                            │
   year of yellow-taxi   │   ┌──────────┐         ┌───────────────┐  │
   Parquet (~38M rows) ──┼──>│  MinIO   │<───────>│     Spark      │  │
   written as Iceberg    │   │  (S3 API)│  s3a:// │  driver +      │  │
   in Week 6             │   │  :9000   │         │  executors     │  │
                         │   └──────────┘         │  local[*]      │  │
                         │        ▲               │  UI :4040      │  │
                         │        │               └───────┬───────┘  │
                         │        │ Iceberg catalog 'lake' │           │
                         │        │ (snapshots, schema)    │           │
                         └────────┼────────────────────────┼──────────┘
                                  │                         │
                                  │                  build_mart.py:
                                  │                  read trips (Iceberg)
                                  │                  broadcast dims
                                  │                  groupBy day,zone,vendor,pay
                                  │                  fix the VendorID skew
                                  │                  write fct_daily_zone_revenue
                                  ▼                         │
                         lake.nyc.fct_daily_zone_revenue <──┘   (Iceberg table on MinIO)

   Baseline (no cluster):  build_mart_duckdb.py  --  in-process, reads Parquet directly,
                           one GROUP BY, no shuffle  -->  results/mart_duckdb.parquet
```

The Spark job and the DuckDB job produce the **same mart** by different means; the
mini-project's argument is *which engine to use and why*, backed by a benchmark.

---

## Functional requirements

**F1 — Spark mart job over Iceberg.** `jobs/build_mart.py` creates a `SparkSession`
with the Iceberg `lake` catalog and S3A→MinIO config (Lecture 1 §8), reads
`lake.nyc.yellow_tripdata` (a full year), cleans it (`trip_distance > 0`,
`total_amount > 0`, `passenger_count >= 1`, `day = to_date(tpep_pickup_datetime)`),
**broadcast-joins** `dim_payment_type` and `dim_zone`, aggregates to the grain
`(day, PULocationID, VendorID, payment_type_name)` with trip count, revenue,
avg distance, avg tip, and writes the result back as the Iceberg table
`lake.nyc.fct_daily_zone_revenue`. Uses `spark.sql.shuffle.partitions=64`, AQE on.

**F2 — Deliberate skew, then fixed.** `jobs/build_mart_skewed.py` introduces a
join on the hot `VendorID` key with AQE off and auto-broadcast off so a
sort-merge join produces a straggler. `build_mart.py` then fixes it (broadcast the
small side; or salt the hot key if you frame the joined table as large). Save the
`explain()` of both to `plans/mart_before.txt` and `plans/mart_after.txt`.

**F3 — Spark-UI evidence.** Capture the Spark UI **Stages tab** for the skewed run
showing Max task duration ≫ Median (`evidence/skew_stages_tab.png`) and for the
fixed run showing Max ≈ Median (`evidence/fixed_stages_tab.png`). These two
screenshots are the proof you diagnosed the skew, not just guessed.

**F4 — DuckDB baseline.** `jobs/build_mart_duckdb.py` builds the identical mart in
DuckDB from the year of Parquet, one process, no shuffle, writing
`results/mart_duckdb.parquet`.

**F5 — Equivalence check.** The Spark mart and the DuckDB mart must agree: same row
count, and total revenue equal to within rounding. The check runs in `run.sh` and
its result is recorded in `PERF.md`. A mismatch means your two jobs' filters or
join keys diverged — reconcile them.

**F6 — Benchmark (`PERF.md`).** Median wall-clock over three runs each of the Spark
job and the DuckDB job, with machine spec, input spec (row count, byte size), output
row count, and a **verdict** naming the winner at this scale, by how much, *why*,
and the data size at which Spark would overtake DuckDB on this machine.

**F7 — Reproducibility (`run.sh`).** A fresh checkout plus `./run.sh` brings up the
compose stack, submits the Spark job, runs the DuckDB job, runs the equivalence
check, and tears down. Versions pinned in `requirements.txt`
(`pyspark==3.5.3`, Iceberg runtime `1.6.1`, `duckdb` 1.1.x).

---

## Deliverables

1. **`jobs/build_mart.py`** — the Spark mart job with the skew fixed (F1, F2).
2. **`jobs/build_mart_skewed.py`** — the deliberately-skewed "before" variant (F2).
3. **`jobs/build_mart_duckdb.py`** — the DuckDB baseline (F4).
4. **`plans/mart_before.txt` and `plans/mart_after.txt`** — the physical plans, before and after the fix (F2).
5. **`evidence/skew_stages_tab.png` and `evidence/fixed_stages_tab.png`** — the Spark UI screenshots (F3).
6. **`PERF.md`** — the Spark-vs-DuckDB benchmark and verdict (F6, with the F5 equivalence result).
7. **`SKEW.md`** — the before/after skew narrative: the hot key, the straggler (Max/Median), each fix's wall-clock, and which fix you shipped and why. Embed the two screenshots.
8. **`README.md`** (your write-up) — 600–800 words connecting it all: what the mart is, why the dimension joins broadcast, where the one shuffle is, how you found the skew, how you fixed it, and the honest Spark-vs-DuckDB verdict.
9. **`run.sh`** — one-command reproduction (F7).

### On `PERF.md`

`PERF.md` is the document that separates this from a toy. It must contain a table
with **numbers and units**, not adjectives:

```markdown
# Mart rebuild — Spark vs DuckDB

Input: NYC yellow-taxi 2023, 12 monthly Parquet, ~38.0 M rows, ~600 MB on MinIO.
Machine: <cpu / cores / RAM / disk>. Spark 3.5.3 local[*], shuffle.partitions=64, AQE on.
Output: lake.nyc.fct_daily_zone_revenue, grain (day, PULocationID, VendorID, payment_type_name), ~XXX,XXX rows.
Equivalence: Spark and DuckDB row counts match; total revenue diff = $0.0X (within rounding).

| Engine | Run 1 | Run 2 | Run 3 | Median | Notes                                  |
|--------|------:|------:|------:|-------:|----------------------------------------|
| Spark  |  __ s |  __ s |  __ s |   __ s | JVM startup ~__ s; one groupBy shuffle |
| DuckDB |  __ s |  __ s |  __ s |   __ s | in-process, vectorized, no shuffle     |

## Verdict
<2-3 sentences. Which won at 600 MB and by how much. WHY (the coordination tax).
At what input size does Spark overtake DuckDB on this machine? Would you ship this
mart on Spark or DuckDB in production, and what would change your answer?>
```

A `PERF.md` whose verdict is "DuckDB is faster at this scale, so this mart does not
need Spark — but the same code scales unchanged if the data grows past one machine"
is a **correct and high-scoring** answer. The skill being graded is the judgment,
backed by the measurement.

---

## Acceptance criteria

- `./run.sh` on a fresh checkout brings up the stack, builds both marts, runs the equivalence check, and tears down — no manual steps.
- `lake.nyc.fct_daily_zone_revenue` exists as an Iceberg table after the Spark run.
- `plans/mart_after.txt` shows **two broadcast joins and exactly one shuffle** (`Exchange`); `plans/mart_before.txt` shows the skewed sort-merge.
- `evidence/skew_stages_tab.png` shows Max ≫ Median; `evidence/fixed_stages_tab.png` shows Max ≈ Median.
- `PERF.md` has the three-run median table for both engines, the equivalence result, and a verdict with the crossover size.
- `SKEW.md` names the hot key, the Max/Median ratio, every fix's wall-clock, and the shipped fix with justification.
- `README.md` write-up is 600–800 words and defends every parameter and the engine choice.
- Commit message in the style of `c27-w07-mp: spark mart on iceberg, vendor-2 skew fixed by broadcast, duckdb wins at 600MB`.

## Grading rubric (self-check)

| Dimension | What "excellent" looks like |
|---|---|
| Correctness | Spark and DuckDB marts agree exactly; the equivalence check passes. |
| Plan literacy | You can point at each node in `mart_after.txt` and name the code that made it; exactly one shuffle. |
| Skew diagnosis | The screenshots prove the straggler; `SKEW.md` names the hot key and the Max/Median ratio with numbers. |
| Fix quality | The shipped fix is the *right* one for this join (broadcast, since the side is tiny) and you say why; salting and AQE are measured as alternatives. |
| Judgment | `PERF.md` reaches a defensible Spark-vs-DuckDB verdict from the benchmark, including the crossover size — not from habit. |
| Reproducibility | `./run.sh` reproduces everything; versions pinned. |

## Up next

Week 8 (Apache Kafka) leaves the bounded batch behind for an unbounded stream. The
lakehouse you queried with both DuckDB and Spark this week becomes one of the sinks
a stream lands in; Spark Structured Streaming is the bridge between the
distributed-processing instincts you built here and the event-time world of Week 8.
