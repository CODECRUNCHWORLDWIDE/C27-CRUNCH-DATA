# C27 · Crunch Data — Syllabus

> Data Engineering & Streaming
> 12 weeks · ~432 hours full-time (~144 hours self-paced) · 1 capstone · GPL-3.0
> Crunch Labs tier · sub-brand **Data** (`#0EA5E9`)

Twelve weeks. Three phases. Four weeks per phase. One capstone. The detail below is the contract: every week has a title, a topic list, a lecture spine, a named hands-on lab, and three concrete skills the cohort should be able to demonstrate before the next week begins.

Storage comes before compute, batch comes before streaming, and data quality comes after the cohort has felt the pain of a pipeline without it. Everything runs locally in Docker. For design rationale, see [`CHARTER.md`](./CHARTER.md). For audience and outcomes, see [`README.md`](./README.md).

The week titles below are canonical. They are the slugs the curriculum folders will take: `Week N — Title`.

---

## Phases

| Phase | Weeks | Title | Theme |
| --- | --- | --- | --- |
| **I — Modeling & Batch Foundations** | 1–4 | Dimensional Modeling, Analytical SQL, Python ETL, Orchestration | The layer every data engineer owns, on a laptop, in Postgres |
| **II — The Lakehouse & Distributed Compute** | 5–8 | dbt, Parquet & Table Formats, DuckDB & Spark, Kafka | Storage before compute; the modern lakehouse stack |
| **III — Streaming, Quality & Capstone** | 9–12 | Stream Processing, Data Quality, Governance & Cost, Capstone | The hard parts, and connecting every layer end-to-end |

---

## Phase I — Modeling & Batch Foundations (Weeks 1–4)

The goal of Phase I is the layer no data engineer can skip, taught entirely on a laptop with Postgres and Python in Docker: how to model data, how to query it well, how to load it without fear, and how to orchestrate the loading.

### Week 1 — The Data-Engineering Landscape & Dimensional Modeling

- **Topics:** What a data engineer owns vs an analyst, data scientist, and backend engineer; OLTP vs OLAP; the warehouse / lake / lakehouse lineage; Kimball dimensional modeling — fact-table grain, dimensions, star vs snowflake schemas; slowly-changing dimensions (Type 1 / 2 / 3); surrogate keys; conformed dimensions.
- **Lecture:** A field tour of the modern data platform — where the bytes flow, who is paged when each layer breaks, and why "grain" is the most important word in the course. Model a retail sales domain on the whiteboard from business questions backward to a star schema.
- **Hands-on:** **Lab 01 — Star schema in Postgres.** Spin up Postgres in Docker, model a star schema for a retail dataset, load dimensions and a fact table, and implement a **Type-2 slowly-changing dimension** with effective-dated rows you can audit with a single query.
- **Skills earned:**
  - Choose the correct grain for a fact table and defend it.
  - Implement a Type-2 SCD with surrogate keys and effective dating.
  - Explain OLTP vs OLAP and where the lakehouse fits.

### Week 2 — Advanced SQL & Analytical Queries

- **Topics:** Window functions (`ROW_NUMBER`, `RANK`, `LAG`/`LEAD`, running aggregates, frames); CTEs and recursive CTEs; `GROUPING SETS` / `ROLLUP` / `CUBE`; `QUALIFY`; anti-joins and semi-joins; query plans (`EXPLAIN ANALYZE`); indexes vs full scans; the cost of a join.
- **Lecture:** "Read the plan before you guess." How to read a Postgres and a DuckDB query plan, spot the sequential scan that should be an index seek, and recognize the join that exploded. The analytical-SQL patterns that appear in every real pipeline.
- **Hands-on:** **Lab 02 — Analytical query gauntlet.** Against the modeled warehouse, answer ten realistic business questions using window functions, `GROUPING SETS`, and anti-joins; for two of them, read `EXPLAIN ANALYZE`, identify the bottleneck, and make the query measurably faster.
- **Skills earned:**
  - Write window functions, recursive CTEs, and grouping-set aggregations fluently.
  - Read a query plan and locate the bottleneck before tuning.
  - Turn a vague business question into correct analytical SQL.

### Week 3 — Batch Ingestion & Python ETL

- **Topics:** Extract / transform / load vs extract / load / transform (ETL vs ELT); full vs incremental loads; watermarking and high-water marks; idempotency and restartability; late-arriving and out-of-order records; upserts / merge; bulk loading; connection and batch-size discipline; structured logging for pipelines.
- **Lecture:** "The idempotent pipeline." Why a re-run must never double-count, how a high-water mark makes a load incremental, and what to do with the record that arrives three days late. The shape of a Python ETL job you can re-run at 3 AM without thinking.
- **Hands-on:** **Lab 03 — Idempotent incremental loader.** Build a Python ETL job that incrementally loads a daily-growing source into the Postgres warehouse using a watermark, performs an idempotent upsert, handles a deliberately injected late record correctly, and produces the same result whether run once or five times.
- **Skills earned:**
  - Build an incremental load with watermarking and upserts.
  - Write an ETL job that is idempotent and restartable by construction.
  - Handle late-arriving and out-of-order records deliberately.

### Week 4 — Workflow Orchestration (Airflow & Dagster)

- **Topics:** DAGs, tasks, dependencies; schedules and intervals; sensors; retries, SLAs, and alerting; backfills and catchup; idempotent tasks and the "task succeeded but lied" failure; Airflow architecture (scheduler, executor, metadata DB); Dagster's asset-oriented model as the modern alternative.
- **Lecture:** "Orchestration is where pipelines actually break." Anatomy of a backfill that melts the cluster, the difference between a task that failed loudly and one that succeeded silently with wrong data, and why your DAG must be idempotent for catchup to be safe. Airflow vs Dagster — what each gets right.
- **Hands-on:** **Lab 04 — Orchestrate the batch pipeline.** Wrap Lab 03's loader in an Airflow DAG running in Docker; add a sensor that waits for the daily file, retries with backoff, an SLA alert, and a safe **backfill of 30 days** of history that does not double-count. Then read the same pipeline expressed as Dagster assets.
- **Skills earned:**
  - Author an Airflow DAG with sensors, retries, and SLAs.
  - Run a safe, idempotent backfill over a date range.
  - Compare Airflow and Dagster and pick one for a given team.

**Phase I gate (end of Week 4):** Demo an orchestrated, idempotent, incremental batch pipeline that loads a modeled star schema in Postgres, survives a re-run with no double-counting, and backfills 30 days cleanly.

---

## Phase II — The Lakehouse & Distributed Compute (Weeks 5–8)

Storage before compute. The cohort moves off the single Postgres box and onto the modern lakehouse: transformation as code, columnar storage on object storage, the ACID table formats, and the distributed engines that read them.

### Week 5 — Transformation with dbt

- **Topics:** Analytics-as-code; dbt models, sources, refs; the staging / intermediate / mart layering; materializations (view, table, incremental, ephemeral); tests (generic and singular); snapshots (dbt's SCD-2); documentation and the DAG; seeds; macros; the `dbt-core` CLI vs dbt Cloud.
- **Lecture:** "Why analytics-as-code wins." Hand-run SQL is unversioned, untested, and undocumented; dbt makes transformation a reviewed, tested, lineage-aware artifact. The staging→mart layering pattern and why every serious warehouse uses it.
- **Hands-on:** **Lab 05 — dbt transformation layer.** Re-express the Phase I warehouse as a dbt project against DuckDB: staging models over the raw sources, intermediate models, a dimensional mart, generic tests (`unique`, `not_null`, `relationships`), a `dbt snapshot` for the SCD, and generated docs with a lineage graph.
- **Skills earned:**
  - Structure a dbt project with staging / intermediate / mart layering.
  - Add tests, snapshots, and documentation to a transformation.
  - Read a dbt lineage DAG and reason about model dependencies.

### Week 6 — File Formats, Columnar Storage & the Lakehouse

- **Topics:** Row vs columnar storage; CSV/JSON vs Parquet; Parquet internals (row groups, pages, column encodings, dictionary encoding, statistics, predicate pushdown); partitioning and file sizing; the small-files problem; **Apache Iceberg** and **Delta Lake** — ACID, time travel, schema evolution, hidden partitioning; object storage with MinIO.
- **Lecture:** "The table format is the contract, the engine is interchangeable." Why a columnar scan with predicate pushdown reads a fraction of the bytes, what an ACID table format adds on top of plain Parquet, and how time travel and schema evolution actually work.
- **Hands-on:** **Lab 06 — Build a lakehouse on MinIO.** Land the warehouse data as partitioned Parquet on MinIO; create an **Iceberg** table over it; query it with DuckDB using predicate pushdown and prove (via file statistics) how few bytes were scanned; perform a schema-evolution add-column and a time-travel query to a prior snapshot. Read the same table as Delta and note the differences.
- **Skills earned:**
  - Explain Parquet internals and predicate pushdown from the bytes up.
  - Create and query an Iceberg (and Delta) table on object storage.
  - Use time travel and schema evolution on a lakehouse table.

### Week 7 — Distributed Processing with Apache Spark

- **Topics:** Why distributed compute; the Spark execution model (driver, executors, stages, tasks); the DataFrame API; partitions and parallelism; narrow vs wide transformations; shuffles and why they hurt; join strategies (broadcast vs sort-merge); data skew; the Spark UI; reading a physical plan.
- **Lecture:** "The shuffle is the enemy." How a wide transformation forces data across the network, why one skewed key can stall a whole job, and how to read the Spark UI to find the straggler. When Spark is the right answer and when DuckDB on one big machine is.
- **Hands-on:** **Lab 07 — Spark on the lakehouse.** Run Spark in Docker against the Iceberg lakehouse; rebuild the dimensional mart as a Spark job over a large dataset (NYC taxi); deliberately trigger a skewed join, diagnose it in the Spark UI, and fix it with a broadcast join or salting. Compare runtime against the DuckDB version.
- **Skills earned:**
  - Write a Spark DataFrame job over lakehouse tables.
  - Read the Spark UI to find shuffles, skew, and stragglers.
  - Choose a join strategy and justify Spark vs single-node compute.

### Week 8 — Event Streaming with Apache Kafka

- **Topics:** The log abstraction; topics, partitions, offsets; producers and consumers; consumer groups and rebalancing; delivery semantics (at-most-once, at-least-once, exactly-once); keys and per-partition ordering; retention and compaction; the schema registry and Avro/Protobuf; Redpanda as a drop-in.
- **Lecture:** "Ordering is only ever per-partition." Why Kafka is a distributed append-only log, not a queue; how consumer-group rebalancing redistributes partitions; what each delivery guarantee actually costs; and why the schema registry is what keeps a stream from rotting.
- **Hands-on:** **Lab 08 — Produce and consume a stream.** Stand up Kafka in Docker; write a Python producer that emits a synthetic clickstream to a keyed, partitioned topic; write a consumer group with two members and observe a rebalance; register an Avro schema in the schema registry and prove a backward-compatible schema change works while an incompatible one is rejected.
- **Skills earned:**
  - Produce and consume keyed, partitioned Kafka topics.
  - Reason about consumer groups, offsets, and delivery semantics.
  - Enforce schema compatibility through a schema registry.

**Phase II gate (end of Week 8):** Demo a dbt-transformed dimensional mart living as an Iceberg lakehouse on MinIO, queried by both DuckDB and Spark, with a working Kafka topic producing and consuming a stream.

---

## Phase III — Streaming, Quality & Capstone (Weeks 9–12)

The hard parts and the integration. Stream processing with real event-time semantics, the data-quality and observability layer that gates every boundary, the governance / lineage / cost discipline that separates senior from junior, and one end-to-end build.

### Week 9 — Stream Processing (Spark Structured Streaming & Flink)

- **Topics:** Stream processing vs batch; event time vs processing time; watermarks and late data; tumbling / sliding / session windows; stateful aggregation; output modes (append / update / complete); checkpointing and exactly-once sinks; the streaming-lakehouse pattern (streaming into Iceberg/Delta); Apache Flink as the dedicated alternative.
- **Lecture:** "Streaming is batch's hard generalization." Every concept maps back to the batch version from week 3 — watermarks are high-water marks, late data is the late record, idempotent sinks are the idempotent upsert. Why exactly-once requires checkpointing plus an idempotent sink, and where a 15-minute micro-batch beats a true stream.
- **Hands-on:** **Lab 09 — Streaming aggregate from Kafka to lakehouse.** Build a **Spark Structured Streaming** job that reads the Lab 08 clickstream, applies an event-time watermark, computes a windowed aggregate, handles a deliberately injected late event, and writes exactly-once into an Iceberg/Delta table that the dashboard can query. Implement the same windowed count in Flink and compare latency and semantics.
- **Skills earned:**
  - Build a watermarked, windowed Structured Streaming job.
  - Achieve exactly-once delivery into a lakehouse sink.
  - Compare Spark Structured Streaming and Flink for a workload.

### Week 10 — Data Quality, Testing & Observability

- **Topics:** Data-quality dimensions (completeness, validity, uniqueness, freshness, volume, distribution); **Great Expectations** suites and checkpoints; dbt tests revisited; data contracts; freshness and volume anomaly detection; quality gates that fail a pipeline vs warn; data-quality reports as artifacts; pipeline observability (run metadata, row counts, latency).
- **Lecture:** "Catch the bad load before the executive does." The taxonomy of data-quality failures and the right check for each; why a quality gate must be able to *halt* a pipeline, not just log; what a data contract between a producing and consuming team actually contains.
- **Hands-on:** **Lab 10 — Quality-gated pipeline.** Add a **Great Expectations** suite at the ingestion boundary (schema, null thresholds, ranges, referential integrity) and a freshness + volume check at the mart boundary; wire both into the Airflow DAG so a malformed load **fails the pipeline and alerts** instead of landing silently. Produce the human-readable data-quality report.
- **Skills earned:**
  - Author Great Expectations suites and wire them as pipeline gates.
  - Detect freshness, volume, and distribution anomalies.
  - Write a data contract two teams can hold each other to.

### Week 11 — Governance, Lineage & Cost

- **Topics:** Column- and table-level lineage; data catalogs (OpenMetadata / DataHub, dbt docs); PII classification and masking; access control and row/column security; partition pruning and file compaction; the cost model of a scan / shuffle / storage tier; query and storage optimization; the small-files problem at scale; GDPR-style deletion in an immutable lakehouse.
- **Lecture:** "The senior engineer reasons about the bill even when no bill arrives." Where cost hides in a data platform — full scans, oversized files, unpruned partitions, runaway shuffles — and the optimizations that fix each. Why lineage is the first thing you reach for during an incident. How to delete a user's data from an append-only table.
- **Hands-on:** **Lab 11 — Optimize, prune, and trace.** Compact the small files in the lakehouse and re-partition for pruning; measure the bytes-scanned improvement on a representative query; expose end-to-end lineage from source to dashboard (dbt docs + lineage graph); classify and mask a PII column; and document a GDPR-style hard-delete on the Iceberg/Delta table.
- **Skills earned:**
  - Optimize a lakehouse for scan cost via compaction and partition pruning.
  - Expose end-to-end column-level lineage from source to dashboard.
  - Handle PII classification, masking, and compliant deletion.

### Week 12 — Capstone Showcase

- **Topics:** Pipeline integration; the architecture document; the chaos drill; the data-quality report as a deliverable; the production runbook; the data-engineering interview loop; demo discipline and the honest postmortem.
- **Lecture:** "What hiring managers actually ask in a data-engineering loop" — modeling, SQL tuning, idempotency, exactly-once, partitioning, cost, and a real incident story — and how the capstone covers each axis. Final architecture sign-off.
- **Hands-on:** **Capstone defense.** Live demo of the end-to-end pipeline (batch + stream → lakehouse → dbt → streaming aggregate → dashboard, with quality gates and lineage). 20-minute Q&A from a reviewer panel. Public postmortem of one chaos drill.
- **Skills earned:**
  - Demo an end-to-end data platform without it falling over.
  - Defend modeling, storage, and streaming decisions to senior reviewers.
  - Ship a capstone artifact you would happily send to a hiring manager.

**Phase III gate (end of Week 12):** The capstone runs end-to-end, the chaos-drill postmortem is signed off, the data-quality report is clean, the portfolio is published, and the cohort completes the data-engineering mock interview.

---

## Assessment matrix

| Component | Weight | Cadence | Format |
| --- | --- | --- | --- |
| Weekly quiz | 10% | Weeks 1–11 | Auto-graded, ~30 min, formats-and-failure-modes heavy |
| Weekly lab | 30% | Weeks 1–11 | Reviewed by peers + a TA, with a DAG / query-plan / Spark-UI / DQ-report artifact |
| Phase gates (×2) | 15% | End of Weeks 4 and 8 | Live demo + code review against the rubric |
| Capstone | 30% | Weeks 9–12 | Live deploy, 5-min video, data-quality report, postmortem |
| Chaos drill | 5% | Week 12 | 60-min timed incident on a deliberately broken pipeline |
| Mock interview | 10% | Week 12 | 60-min senior data-engineering loop with an external reviewer |

Passing bar: **70% overall, AND a passing capstone, AND a passing chaos drill.** A weak quiz week is forgivable; a pipeline that loses or double-counts data is not.

---

## Capstone

### Specification — "End-to-End Lakehouse + Streaming Pipeline"

The capstone is **one** substantial system, not several disconnected demos. Everything runs locally in Docker; no cloud account is required. You will build:

```text
   +------------------+        +---------------------+
   |  Batch source    |        |  Event source       |
   |  (daily file /    |        |  (Kafka clickstream |
   |   CDC extract)   |        |   producer)         |
   +---------+--------+        +----------+----------+
             |                            |
       Airflow DAG                 Kafka topic
       (idempotent,                (keyed, partitioned,
        watermarked,                schema-registered)
        backfillable)                     |
             |                            v
             |                +-----------+-----------+
             |                |  Spark Structured     |
             |                |  Streaming job        |
             |                |  (event-time          |
             |                |   watermark, windows, |
             |                |   exactly-once sink)  |
             v                +-----------+-----------+
   +---------+--------------------------+ |
   |   Lakehouse on MinIO (object       |<+
   |   storage): Iceberg / Delta tables |
   |   raw -> staged -> mart            |
   +---------+--------------------------+
             |
        dbt transformation
        (staging / intermediate / mart,
         tests, snapshots, docs, lineage)
             |
   +---------+---------+        +------------------------+
   |  Great Expectations|       |  DuckDB / Spark query  |
   |  + dbt quality     |       |  layer                 |
   |  GATES at every    |       +-----------+------------+
   |  boundary          |                   |
   +--------------------+                   v
                                  +---------+---------+
                                  |  Dashboard         |
                                  |  (a trusted, tested|
                                  |   metric an analyst|
                                  |   would rely on)   |
                                  +-------------------+
```

The product domain is intentionally open — a retail sales analytics platform, a clickstream / product-analytics pipeline, an IoT-telemetry lakehouse, a financial-transactions warehouse, or any domain you can defend in scope review. The technical bar is fixed.

### Required deliverables

1. **Architecture document** (~8–10 pages) covering the data model (grain, dimensions, facts), the storage layout (partitioning, table format), the orchestration design, the streaming semantics (watermark, windowing, delivery guarantee), the quality gates, and the lineage map.
2. **A working batch path** — an idempotent, watermarked, backfillable Airflow DAG that lands a batch source into the lakehouse and transforms it with dbt into a tested dimensional mart.
3. **A working streaming path** — a Spark Structured Streaming job consuming a Kafka topic with an event-time watermark and an exactly-once sink into the lakehouse.
4. **Data-quality gates** — Great Expectations suites and dbt tests at every layer boundary, wired so a bad load **halts and alerts** rather than landing silently.
5. **End-to-end lineage** — source-to-dashboard lineage exposed (dbt docs + lineage graph), with at least one PII column classified and masked.
6. **A runnable system** — `docker compose up` brings up the whole platform (Postgres, MinIO, Airflow, Kafka, Spark) so a reviewer can run it during the demo.
7. **A 5-minute demo video** — voice-over required, no marketing edits, showing batch + stream → lakehouse → dbt → streaming aggregate → dashboard, and one quality gate firing on bad data.
8. **A data-quality report** — the generated Great Expectations report for the final run, plus row-count and freshness evidence.
9. **A chaos-drill postmortem (~3–5 pages)** — pick **one** drill from the menu below; document the failure, detection, recovery, data impact, and action items.

### Chaos-drill menu (pick one)

1. **Malformed batch load.** A daily file arrives with a corrupted schema / out-of-range values mid-pipeline. Prove the quality gate halted it before it reached the mart, document the alert path, the recovery, and how you proved no bad data leaked downstream.
2. **Stream partition lag spike.** A consumer falls behind and a partition's lag explodes mid-run. Document detection, the rebalance / scaling response, whether exactly-once held, and how you proved no events were lost or double-counted.
3. **Schema-evolution event.** A producer adds a field and later a breaking change. Document how the schema registry and the lakehouse table format absorbed the compatible change, rejected the breaking one, and how downstream dbt models and the dashboard survived.

### Capstone grading axes

| Axis | Weight |
| --- | --- |
| Data modeling (grain, dimensions, SCD correctness) | 15% |
| System correctness (batch + stream actually work end-to-end, no data loss / double-count) | 25% |
| Lakehouse & transformation quality (storage layout, dbt structure, tests) | 20% |
| Streaming semantics (watermark, windowing, exactly-once) | 15% |
| Data quality, lineage & cost discipline (gates, lineage map, partitioning) | 15% |
| Communication (architecture doc + video + postmortem) | 10% |

Minimum to pass: **70%**, AND no data-loss or double-counting defect in the final run.

---

## Career engineering pack

Delivered alongside the capstone, archived to the `interview-prep/` and `portfolio/` directories of the track.

### Interview prep topics (covered in Week 12)

- **Data modeling:** grain selection, star vs snowflake, SCD types, conformed dimensions — explained on a whiteboard from business questions backward.
- **SQL & tuning:** window functions, anti-joins, reading a query plan, and the "why is this query slow" walkthrough.
- **Idempotency & incrementality:** designing a load that survives a re-run; watermarking; handling late data.
- **Storage internals:** Parquet row groups and predicate pushdown; what Iceberg/Delta add; partitioning and the small-files problem.
- **Distributed compute:** Spark shuffles, skew, join strategies, reading the Spark UI; when Spark vs single-node.
- **Streaming:** event vs processing time, watermarks, exactly-once, the at-least-once-plus-idempotent-sink pattern.
- **Cost & governance:** estimating scan cost from a query, partition pruning, lineage during an incident, PII deletion in an immutable table.
- **A real incident story** you can tell on the spot — from your own chaos drill.

### Production runbook contents (template provided)

- Build, configure, and `docker compose up` the whole platform (every command, no hand-waving).
- The on-call surface: the DAG, the dbt build, the Spark job, the Kafka topic, the lakehouse, the dashboard.
- The five most likely failures (late/missing source, schema drift, stream lag, bad data landing, runaway cost) and the first three diagnostics for each.
- The backfill procedure and how to do it without double-counting.
- The quality-gate-fired procedure: how to triage, quarantine, and replay.
- The lineage-lookup procedure for "where did this wrong number come from."
- Who to call at 3 AM.

### Portfolio recommendations

- The capstone repo, public, GPL-3.0, with a real README, an architecture diagram, and the data-quality report.
- One PR merged into an open-source data project (dbt, Airflow, Dagster, DuckDB, Iceberg, Delta, Great Expectations, or a Spark/Kafka client library).
- A short technical blog post explaining one bug from your chaos drill — a late record, a skewed join, or a schema-evolution surprise.
- A LinkedIn / website page that links to all of the above and does not contain the phrase "data-driven."

---

## Licensing

This syllabus and all curriculum materials in `C27-CRUNCH-DATA/` are licensed under **GPL-3.0**. See [`LICENSE`](./LICENSE). Fork, teach, remix; PR improvements back to <https://github.com/CODE-CRUNCH-CLUB>.

Course identity, accent colour, and brand position are governed by the Crunch Labs Charter. The track's design rationale lives in [`CHARTER.md`](./CHARTER.md); audience and outcomes in [`README.md`](./README.md).
