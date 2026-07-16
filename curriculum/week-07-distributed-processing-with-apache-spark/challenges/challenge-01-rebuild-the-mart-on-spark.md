# Challenge 1 — Rebuild the mart on Spark, benchmark against DuckDB

> **Estimated time:** 2 hours.
> **Prerequisites:** Lectures 1–2; Week 6 lakehouse (MinIO + Iceberg) running; the dimensional mart you designed earlier in the course.
> **Citations:** Iceberg-Spark <https://iceberg.apache.org/docs/latest/spark-getting-started/>; SQL/DataFrame guide <https://spark.apache.org/docs/latest/sql-programming-guide.html>; DuckDB performance <https://duckdb.org/docs/stable/guides/performance/overview.html>; NYC TLC data <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>.
> **Goal:** Rebuild your dimensional mart as a PySpark DataFrame job that reads the NYC taxi data from the Iceberg lakehouse, joins conformed dimensions, aggregates to a daily/zone/vendor grain, and writes the result back as an Iceberg table — then build the *same* mart with DuckDB on one machine and put the two wall-clock times side by side in a `PERF.md`.

This is the bridge between "I ran a Spark job in `local[*]` over one Parquet file"
(the exercises) and "I have a mart-build job that reads and writes the lakehouse
and I can defend, with numbers, whether it should run on Spark at all." The output
is a star schema living as Iceberg tables on MinIO, a Spark job that produces it,
a DuckDB job that produces the same thing, and a one-page benchmark.

---

## Premise

In Week 6 you wrote the NYC yellow-taxi trips into an Iceberg table on MinIO. In
Weeks 1–3 you designed a dimensional model: a **fact** table of trips plus
**conformed dimensions** for date, vendor, payment type, and pickup/dropoff zone.
This challenge rebuilds the *aggregated mart* — `fct_daily_zone_revenue`, one row
per (day, pickup-zone, vendor, payment-type) with trip count, total revenue,
average distance, average tip — as a distributed Spark job, and then proves
(or disproves) that Spark was the right engine for it.

Scale the input up so distribution has something to distribute: use a **full year**
of monthly yellow-taxi Parquet (~38 M rows, ~600 MB), not one month.

---

## Setup — Spark + MinIO + Iceberg in Docker

Extend your Week 6 `docker-compose.yml`. The relevant new service is `spark`; the
`minio` service is the one you already run. Iceberg and S3A jars are pulled by
`--packages` at submit time, or baked into the image.

```yaml
# docker-compose.yml (excerpt — minio is unchanged from Week 6)
services:
  minio:
    image: minio/minio:RELEASE.2024-09-13T20-26-02Z
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports: ["9000:9000", "9001:9001"]
    volumes: ["minio-data:/data"]

  spark:
    image: apache/spark:3.5.3-python3        # official Spark image with PySpark
    depends_on: [minio]
    environment:
      AWS_REGION: us-east-1
    ports:
      - "4040:4040"      # Spark UI (the application UI while a job runs)
      - "8080:8080"      # Spark master UI (standalone mode, optional)
    volumes:
      - ./jobs:/opt/jobs               # your PySpark scripts
      - ./data:/opt/data               # local taxi Parquet, if not on MinIO
      - spark-warehouse:/opt/warehouse
    working_dir: /opt/jobs
    # Keep it alive so you can `docker compose exec spark spark-submit ...`
    command: ["tail", "-f", "/dev/null"]

volumes:
  minio-data:
  spark-warehouse:
```

Submit a job (the `--packages` line wires Iceberg 1.6.1 and hadoop-aws onto the
classpath):

```bash
docker compose exec spark spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  build_mart_spark.py
```

The `SparkSession` inside `build_mart_spark.py` registers the Iceberg catalog
exactly as in Lecture 1 §8 (catalog `lake`, type `hadoop`, warehouse
`s3a://lakehouse/warehouse`).

---

## Tasks

### Task 1 — The Spark mart job (`build_mart_spark.py`)

Write a PySpark DataFrame job that:

1. Reads the fact table from Iceberg: `trips = spark.table("lake.nyc.yellow_tripdata")`.
2. Cleans it: keep `trip_distance > 0`, `total_amount > 0`, `passenger_count >= 1`; derive `day = to_date(tpep_pickup_datetime)`.
3. Joins the **conformed dimensions** — `dim_payment_type` (~6 rows) and `dim_zone` (~265 rows) — each **broadcast** (they are tiny; use the `broadcast()` hint so you never accidentally sort-merge a dimension).
4. Aggregates to the mart grain `groupBy("day", "PULocationID", "VendorID", "payment_type_name")` with `count(*) AS trips`, `round(sum(total_amount),2) AS revenue`, `round(avg(trip_distance),3) AS avg_dist`, `round(avg(tip_amount),3) AS avg_tip`.
5. Writes the result back as an Iceberg table: `mart.writeTo("lake.nyc.fct_daily_zone_revenue").createOrReplace()`.

Set `spark.sql.shuffle.partitions=64` and AQE on. Before the write, call
`mart.explain(mode="formatted")` and save the plan to `plans/mart_spark.txt`.

### Task 2 — Verify the plan has exactly the shuffles you expect

Read `plans/mart_spark.txt`. Confirm:

- **Two `BroadcastHashJoin`** nodes (the two dimension joins) with `BroadcastExchange` over the *dimensions* and **no `Exchange hashpartitioning` over the fact** for those joins.
- **Exactly one `Exchange hashpartitioning`** — the final `groupBy`. One shuffle for the whole mart.
- The `BatchScan lake.nyc.yellow_tripdata` reads only the columns the mart uses, with the cleaning predicates pushed down.

Write a 3–4 sentence note in `plans/mart_spark_notes.md` naming each `Exchange`/join
node and the line of your code that produced it.

### Task 3 — The DuckDB mart job (`build_mart_duckdb.py`)

Write the *same* mart in DuckDB, reading the year of Parquet directly (locally, or
from MinIO via the `httpfs`/`s3` extension), joining the same dimensions, aggregating
to the same grain, and writing `results/mart_duckdb.parquet`. One `SELECT ... GROUP BY`
does it; DuckDB has no shuffle to think about.

### Task 4 — Benchmark and write `PERF.md`

Time both jobs end to end (wall-clock from process start to mart written), three
runs each, report the median. Produce `PERF.md`:

```markdown
# Mart rebuild — Spark vs DuckDB

Input: NYC yellow-taxi 2023, 12 monthly Parquet files, ~38.0 M rows, ~600 MB.
Machine: <cpu / cores / RAM>. Spark 3.5.3 local[*], shuffle.partitions=64, AQE on.
Output grain: (day, PULocationID, VendorID, payment_type_name). Output rows: ~XXX,XXX.

| Engine  | Median wall-clock | Notes                                            |
|---------|-------------------|--------------------------------------------------|
| Spark   | __ s              | JVM/session startup ~__ s; one shuffle for groupBy |
| DuckDB  | __ s              | in-process, no shuffle, vectorized               |

## Verdict
<2-3 sentences: which won at this scale, by how much, and WHY. Then: at what
data size would Spark overtake DuckDB on this machine, and why distribution's
overhead is/ isn't worth it here.>
```

### Task 5 — Confirm the two marts agree

Both engines should produce the same numbers. Load both results and assert that
the row counts match and the total `revenue` agrees to within rounding (e.g.
`abs(spark_total - duckdb_total) < 1.0`). Record the check in `PERF.md`. If they
disagree, the cleaning filters or the join keys differ between your two jobs —
reconcile them.

---

## Acceptance criteria

- `jobs/build_mart_spark.py` runs end to end and writes the Iceberg table `lake.nyc.fct_daily_zone_revenue`.
- `plans/mart_spark.txt` contains the saved physical plan; `plans/mart_spark_notes.md` names every `Exchange`/join node and the producing code line. The plan shows **two broadcast joins and exactly one shuffle**.
- `jobs/build_mart_duckdb.py` runs end to end and writes `results/mart_duckdb.parquet` with the same grain.
- `PERF.md` reports median wall-clock for both engines over three runs each, with machine and input specs, and a verdict that names the winner *and the reason* and states the crossover data size.
- The two marts agree on row count and total revenue (the Task-5 check passes and is recorded).
- Commit message in the style of `c27-w07-ch1: spark mart over iceberg, duckdb wins at 600MB by 4x`.

## Stretch

- Run the Spark job in **standalone mode** (driver + one worker container) and compare the *Executors* tab to `local[*]`. The job code does not change.
- Add a third `PERF.md` row: DuckDB reading the Parquet *from MinIO* over `s3://` via `httpfs`, to make the comparison apples-to-apples on storage.
- Re-run the Spark job with `spark.sql.shuffle.partitions=200` (the default) and again with `8`, and add the wall-clocks. Explain the U-shape: too many tiny partitions wastes scheduling; too few overloads each task.
