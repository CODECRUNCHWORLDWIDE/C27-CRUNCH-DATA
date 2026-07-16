# Week 7 — Resources

Every resource on this page is **free** and **publicly accessible** (the two
O'Reilly books are linked at their publisher pages; "Learning Spark, 2nd Edition"
is additionally available as a no-cost PDF from Databricks, linked below). Where we
name a version (Spark 3.5.3, PySpark 3.5.3, Iceberg 1.6.1, DuckDB 1.1.x), use that
exact version when running locally — it pins your reproducibility. The Spark docs
deliberately link to `/docs/latest/`; if you need the exact 3.5.x docs, swap
`latest` for `3.5.3` in any URL. If a link breaks, please open an issue.

## Apache Spark — official documentation

- **Spark documentation home** — the index for every page below:
  <https://spark.apache.org/docs/latest/>
- **Cluster mode overview** — the driver / cluster-manager / executor model, jobs/stages/tasks. Read this for Lecture 1:
  <https://spark.apache.org/docs/latest/cluster-overview.html>
- **RDD Programming Guide** — transformations vs actions, narrow vs wide dependencies, and the shuffle-operations section that explains why a shuffle is expensive:
  <https://spark.apache.org/docs/latest/rdd-programming-guide.html>
  Shuffle operations specifically:
  <https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations>
- **Spark SQL, DataFrames and Datasets Guide** — the DataFrame API surface, the `SparkSession` entry point, reading/writing data:
  <https://spark.apache.org/docs/latest/sql-programming-guide.html>
- **PySpark documentation home** — the Python API landing page:
  <https://spark.apache.org/docs/latest/api/python/index.html>
- **PySpark DataFrame API reference** — every DataFrame method (`select`, `filter`, `join`, `groupBy`, `agg`, `write`, `explain`, `repartition`, `coalesce`):
  <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html>
- **PySpark functions reference** — `col`, `broadcast`, `to_date`, `count`, `sum`, `avg`, `rand`, and the rest of `pyspark.sql.functions`:
  <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html>

## Performance, tuning, AQE, and the Spark UI

- **Tuning Spark** — memory, serialization, parallelism, and data-locality guidance (the general tuning page):
  <https://spark.apache.org/docs/latest/tuning.html>
- **Performance Tuning (Spark SQL)** — the page that holds the levers this week is about. Anchors worth bookmarking:
  <https://spark.apache.org/docs/latest/sql-performance-tuning.html>
  - Automatically broadcasting joins (`spark.sql.autoBroadcastJoinThreshold`): <https://spark.apache.org/docs/latest/sql-performance-tuning.html#automatically-broadcasting-joins>
  - Join Strategy Hints (`BROADCAST` / `MERGE` / `SHUFFLE_HASH`): <https://spark.apache.org/docs/latest/sql-performance-tuning.html#join-strategy-hints>
  - Adaptive Query Execution (`spark.sql.adaptive.enabled`): <https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution>
  - Coalescing post-shuffle partitions: <https://spark.apache.org/docs/latest/sql-performance-tuning.html#coalescing-post-shuffle-partitions>
  - Splitting / optimizing skewed shuffle partitions and skew joins: <https://spark.apache.org/docs/latest/sql-performance-tuning.html#optimizing-skew-join>
  - Converting sort-merge join to broadcast join (AQE): <https://spark.apache.org/docs/latest/sql-performance-tuning.html#converting-sort-merge-join-to-broadcast-join>
- **Web UI** — the Jobs, Stages, SQL/DataFrame, and Executors tabs; the task-duration and shuffle-read distributions where skew shows. Read this for Lecture 3:
  <https://spark.apache.org/docs/latest/web-ui.html>
  Stages tab specifically:
  <https://spark.apache.org/docs/latest/web-ui.html#stages-tab>
- **EXPLAIN** — the SQL reference for reading physical plans (`explain(mode="formatted")` and friends):
  <https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html>
- **Join hints (SQL reference)** — the `/*+ BROADCAST(t) */` syntax and the DataFrame `broadcast()` equivalent:
  <https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html>
- **Submitting applications** — `spark-submit`, `--packages`, `--master`, deploy modes:
  <https://spark.apache.org/docs/latest/submitting-applications.html>
- **Configuration** — the full list of `spark.*` properties referenced this week (`spark.sql.shuffle.partitions`, `spark.local.dir`, `spark.sql.adaptive.*`):
  <https://spark.apache.org/docs/latest/configuration.html>

## Books

- **Chambers, Zaharia (2018) — *Spark: The Definitive Guide*** (O'Reilly). The comprehensive reference; the DataFrame API, the optimizer, tuning, and the execution model in depth. Publisher page:
  <https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/>
- **Damji, Wenig, Das, Lee (2020) — *Learning Spark, 2nd Edition: Lightning-Fast Data Analytics*** (O'Reilly). The accessible modern intro built around Spark 3.x and the DataFrame/Dataset API. Publisher page:
  <https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/>
  Free PDF (compliments of Databricks):
  <https://pages.databricks.com/202003-US-EB-Learning-Spark-2nd-Edition_01_Downloadpage.html>

## Papers (the why behind the engine)

- **Zaharia, Xin, Wendell, Das, Armbrust, et al. (2016)** — "Apache Spark: A Unified Engine for Big Data Processing." *Communications of the ACM* 59:56. The RDD lineage model and the case for a unified engine:
  <https://dl.acm.org/doi/10.1145/2934664>
- **Armbrust, Xin, Lian, Huai, Liu, et al. (2015)** — "Spark SQL: Relational Data Processing in Spark." *SIGMOD 2015*. The Catalyst optimizer that turns your DataFrame calls into the physical plan you read:
  <https://dl.acm.org/doi/10.1145/2723372.2742797>

## Apache Iceberg + Spark (the lakehouse from Week 6)

- **Iceberg — Spark Getting Started** — the `spark-sql` extensions, the catalog config, and reading/writing Iceberg tables from Spark:
  <https://iceberg.apache.org/docs/latest/spark-getting-started/>
- **Iceberg — Spark configuration** — catalog types (`hadoop`, `hive`, `rest`), the SQL extensions class, S3 / object-store wiring:
  <https://iceberg.apache.org/docs/latest/spark-configuration/>
- **Iceberg — Spark writes** — `writeTo(...).createOrReplace()`, append, overwrite, and the table-maintenance procedures used when rebuilding the mart:
  <https://iceberg.apache.org/docs/latest/spark-writes/>
- **Iceberg documentation home**:
  <https://iceberg.apache.org/docs/latest/>

## DuckDB (the single-node comparison)

- **DuckDB documentation home**:
  <https://duckdb.org/docs/>
- **DuckDB — Performance Guide** — why an in-process vectorized engine beats a distributed one on data that fits one machine. The basis for the Spark-vs-DuckDB verdict in Lecture 3 and the mini-project `PERF.md`:
  <https://duckdb.org/docs/stable/guides/performance/overview.html>
- **DuckDB — reading Parquet** — `read_parquet('...*.parquet')` over the monthly taxi files:
  <https://duckdb.org/docs/stable/data/parquet/overview>
- **DuckDB — the `httpfs` / S3 extension** — reading Parquet directly from MinIO over `s3://` for the apples-to-apples storage comparison:
  <https://duckdb.org/docs/stable/extensions/httpfs/overview>

## The dataset

- **NYC Taxi & Limousine Commission — TLC Trip Record Data** — the canonical source. Monthly yellow-taxi Parquet files, plus the data dictionary and the taxi-zone lookup used as a dimension:
  <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>
- **NYC TLC — Trip Record User Guide (PDF)** — the column definitions (`VendorID`, `tpep_pickup_datetime`, `trip_distance`, `payment_type`, `PULocationID`, `total_amount`, etc.):
  <https://www.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf>

## Docker images

- **Apache Spark official Docker image** (`apache/spark:3.5.3-python3`) — PySpark-ready, the base for the lab compose file:
  <https://hub.docker.com/r/apache/spark>
- **MinIO** (S3-compatible object store, the Week 6 lakehouse storage):
  <https://min.io/docs/minio/container/index.html>

## How to use this list

- For **Lecture 1** read the cluster-mode overview and the RDD guide's transformations/shuffle sections.
- For **Lecture 2** read the SQL/DataFrame guide, the EXPLAIN reference, and the join-strategy-hints + automatically-broadcasting-joins anchors on the performance-tuning page.
- For **Lecture 3** read the Web UI page (Stages tab), the AQE and optimizing-skew-join anchors, and the DuckDB performance guide.
- For the **challenges and mini-project** keep the Iceberg-Spark getting-started page and the Spark configuration page open; they hold the catalog and S3A wiring you will copy.
