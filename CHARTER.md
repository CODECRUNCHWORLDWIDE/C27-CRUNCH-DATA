# C27 · Crunch Data — Charter

> The design rationale for the track. Why data engineering is its own discipline, why 12 weeks, why we teach storage before compute and batch before streaming, why our open-source defaults are what they are, and how this track relates to C5, C15, and C16.
> Crunch Labs tier · sub-brand **Data** (`#0EA5E9`) · GPL-3.0

This document is the source of truth for *why* C27 is shaped the way it is. The `SYLLABUS.md` is the *what* and *when*; the `README.md` is the *who* and the *outcomes*. When this charter and the syllabus disagree, this charter wins — and the syllabus is the one we change.

---

## Why data engineering as its own course

For a decade, "data engineering" was a thing people did on the side of another job. The analyst hand-ran SQL. The backend engineer cron-jobbed a dump. The data scientist wrote a notebook that "worked on my machine" and never ran again. That era is over. Data engineering is now a distinct senior discipline with its own body of knowledge — storage formats, table formats, distributed query, orchestration, streaming semantics, data contracts, lineage, and a cost model that no other role internalizes — and it does not fit inside any of our existing tracks without being starved.

We could have bolted three more weeks onto C5 (AI & Data Science) and called it done. We refused, for the same reason a real org does not ask its data scientists to also own the platform: **the failure modes are different, the mental model is different, and the on-call surface is different.** A data scientist optimizes for a correct model on the data they have. A data engineer optimizes for correct, timely, tested, versioned data arriving every day forever, and is paged when it does not. Those are not the same job, and a curriculum that pretends they are produces engineers who are good at neither.

The honest reason C27 exists is that the platform layer is where data products actually break, and where the senior money and senior responsibility sit. A graduate who can model a star schema, orchestrate an idempotent backfill, reason about exactly-once delivery, and write the data-quality test that catches a bad load before it reaches a dashboard is hireable into a role that did not exist as a named title fifteen years ago and is now on every data org chart.

---

## Why 12 weeks

Data engineering is broad, but unlike embedded systems or iOS it does not require half a year to reach a defensible floor — because the learner arrives already knowing Python and SQL. We are not teaching a language from scratch; we are teaching a platform discipline on top of skills the learner already has. Twelve weeks (~432 hours full-time, ~144 self-paced) is the smallest honest window in which a cohort can touch every layer of a modern data platform *and* connect them into one working pipeline.

Twelve weeks buys exactly three phases of four weeks each:

1. **Modeling & batch foundations** (weeks 1–4) — the layer every data engineer must own no matter what comes above it: dimensional modeling, analytical SQL, idempotent Python ETL, and orchestration.
2. **The lakehouse & distributed compute** (weeks 5–8) — the storage and compute layer that defines the modern stack: dbt, Parquet, the ACID table formats, DuckDB, Spark, and the first taste of Kafka.
3. **Streaming, quality & capstone** (weeks 9–12) — the hard parts and the integration: stream processing, data quality and observability, governance and cost, and one end-to-end build.

Compressing below 12 would force one of three sacrifices we will not make: dropping streaming entirely (which would make the track a "modern warehouse" course, not a data-engineering course), dropping distributed compute (which would leave the graduate unable to reason about anything past a single machine), or dropping the capstone (which is the only artifact that proves the layers connect). None are negotiable.

Extending to 24, the Crunch Labs flagship length, would require padding. The learner already knows the language. The discipline is real but it is not a six-month subject for someone who arrives with Python and SQL in hand. We would rather ship a tight, dense 12 than a bloated 24.

---

## Topic ordering — why storage before compute, batch before streaming

The phase order is a dependency graph, not a preference:

```text
Dimensional model  ->  Analytical SQL  ->  Python ETL  ->  Orchestration
        |                                                         |
        v                                                         v
      dbt  ->  File/Table formats  ->  DuckDB / Spark  ->  Kafka intro
        |                                                         |
        v                                                         v
 Stream processing  ->  Data quality  ->  Governance & cost  ->  Capstone
```

A few choices deserve their own defense.

### Why dimensional modeling first

If you teach tools first, every problem looks like a tool problem. A learner who learns Spark before they learn what a fact-table grain is will build a beautifully parallelized pipeline that produces a wrong number. Dimensional modeling (Kimball's grain / dimension / fact discipline) is the conceptual spine of the whole track: it is what the SQL queries, what dbt materializes, what Spark computes, and what the dashboard reads. We teach it in week 1, on a laptop, in Postgres, before any distributed machinery exists to hide the modeling mistakes.

### Why storage formats before query engines

The single most common gap in a self-taught data engineer is that they treat Parquet, Delta, and Iceberg as magic and the query engine as the thing that matters. It is exactly backwards. The engine is interchangeable; the table format is the contract. We teach the bytes — row groups, column encodings, predicate pushdown, then the ACID / time-travel / schema-evolution guarantees the table formats add — in week 6, **before** we put Spark on top in week 7. A learner who understands why a columnar scan is fast can predict the engine's behavior instead of being surprised by it.

### Why batch before streaming

Streaming is the topic most likely to be over-marketed and the one that punishes premature adoption hardest. Every concept in streaming — idempotency, watermarks, late data, exactly-once — is *easier to understand as a generalization of the batch version of the same problem*. We teach idempotent, watermarked, incremental batch ETL in week 3, build orchestration intuition in week 4, and only reach stream processing in week 9, after the cohort has internalized the batch analogues. A learner who learned watermarking in a batch backfill understands event-time watermarking in a stream in an afternoon. The reverse does not hold.

### Why data quality near the end, not the start

It is tempting to teach testing first as a virtue. We deliberately place data quality, testing, and observability in week 11, after the cohort has *felt the pain* of an untested pipeline across the prior weeks. A test you write because you were burned is a test you keep. A test you write because the syllabus told you to is a test you delete. We let the burn happen first.

---

## How C27 differs from C5, C15, and C16

C27 has three close neighbors in the catalog. The boundaries are deliberate and enforced.

| Track | Owns | Does not own |
| --- | --- | --- |
| **C27 — Crunch Data (this)** | Ingestion; the lakehouse; transformation (dbt); distributed compute (Spark); streaming (Kafka, Structured Streaming); data quality, lineage, and cost | Model training; service operations at fleet scale; the transactional app |
| **C5 — AI & Data Science** | Analysis; classical ML; PyTorch; evaluation; shipping a model behind an API | Ingestion pipelines; the lakehouse; orchestration; streaming; data-platform on-call |
| **C15 — DevOps / SRE** | Docker, Kubernetes, CI/CD, observability and on-call for **services** | Data modeling; pipelines; the lakehouse; streaming semantics; data quality |
| **C16 — Web Backend** | The transactional Django/FastAPI service that **produces** operational data | The analytical platform that consumes it; distributed query; streaming |

The seams, stated explicitly:

- **C27 ↔ C5.** They meet at the **feature table**. C27 produces a clean, tested, versioned, on-time table; C5 trains a model on it. The single worst anti-pattern in industry is a data scientist hand-cleaning inputs in a notebook because no one owns the pipeline. C27 owns the pipeline. C5 owns the model. A capstone team may cross the seam, but each learner's deliverables stay inside their own track.
- **C27 ↔ C15.** They overlap on Docker, CI, and the discipline of a 3 AM runbook. They diverge on the object of that discipline: C15 keeps a *service* up; C27 keeps *data correct and on time*. A "the pod is healthy but the data is three hours stale and nobody noticed" incident is squarely a C27 problem, not a C15 one. A graduate who wants to run the C27 platform on Kubernetes at scale takes C15 next.
- **C27 ↔ C16.** C16 writes the row; C27 turns a billion rows into a trusted table. The handoff is **change-data-capture** and batch extraction. C16 graduates think in transactions and request latency; C27 graduates think in batches, partitions, and freshness. The same engineer often does both over a career, but the disciplines are distinct.

There is intentional overlap at every seam. There is no ambiguity about who owns what.

---

## Open-source-first, vendor-aware

C27 commits to an open-source-first stance, and unlike the cloud or mobile tracks, the data-engineering ecosystem makes this *easy* — every layer of a serious data platform has a first-class open-source implementation that runs on a laptop. The cloud products are, almost without exception, managed packagings of open-source projects (or close clones of their ideas). We teach the open core; the managed version is a one-week ramp.

| Layer | Open-source primary | Cloud / vendor secondary | Rationale |
| --- | --- | --- | --- |
| Relational / OLTP source | **Postgres** | RDS, Cloud SQL | The universal source-of-truth database; CDC source for the streaming weeks. |
| Embedded analytical engine | **DuckDB** | — | The single best teaching tool in the modern stack: runs a real columnar engine on a laptop, reads Parquet/Iceberg directly. |
| Object storage | **MinIO** (S3-compatible) | S3, GCS, ADLS | The lakehouse needs object storage; MinIO is the local stand-in and the API is identical. |
| File / table formats | **Parquet, Apache Iceberg, Delta Lake** | Snowflake / BigQuery native tables | Open table formats are the contract of the lakehouse. We teach both Iceberg and Delta and name the trade. |
| Transformation | **dbt-core** | dbt Cloud | The CLI is fully open. dbt Cloud is the orchestration-and-IDE layer, named but never required. |
| Orchestration | **Apache Airflow** (Dagster shown) | MWAA, Cloud Composer, Astronomer | The industry default. Dagster is taught as the modern asset-oriented alternative the cohort should be able to read. |
| Distributed compute | **Apache Spark** | Databricks, EMR, Dataproc | Spark is the open engine the managed products run. We teach Spark; Databricks is shown as a managed packaging. |
| Streaming transport | **Apache Kafka** | Confluent Cloud, MSK, Redpanda | The de-facto log. We run Kafka in Docker; Redpanda named as a drop-in. |
| Stream processing | **Spark Structured Streaming** (Flink shown) | Databricks DLT, Kinesis Data Analytics | We teach Structured Streaming for continuity with the Spark weeks; Flink is shown as the dedicated-streaming alternative. |
| Data quality | **Great Expectations**, **dbt tests** | Monte Carlo, Soda Cloud | Open assertion frameworks first; commercial observability platforms named as the scale-up. |
| Catalog / lineage | **OpenMetadata / DataHub** (shown), dbt docs | Unity Catalog, Collibra | We teach lineage as a property of the pipeline; open catalogs first. |

The point is not purity. The point is **portability of the learner's own work**. A C27 graduate who lands at a Snowflake shop ramps in a week because they understand columnar storage, micro-partitions, and time travel from the open equivalents. A graduate who only knew Snowflake could not do the reverse without retraining — and would not understand *why* their warehouse behaves the way it does.

---

## Why these specific tools

A short defense of choices a reviewer might question:

- **DuckDB as the teaching analytical engine.** It is the most pedagogically valuable tool to arrive in the data stack in years: a real vectorized columnar engine that runs in-process on a laptop, reads Parquet and Iceberg directly, and lets a learner *feel* predicate pushdown and columnar scans without standing up a cluster. We use it throughout, not just once.
- **Postgres as the source database.** Universal, free, the most common operational source in the world, and a clean change-data-capture source for the streaming weeks. The learner already half-knows it.
- **MinIO for object storage.** The lakehouse is defined by separating storage (object store) from compute (engine). MinIO gives the learner a real S3 API locally so the lakehouse weeks are not faked against a local filesystem.
- **Airflow as the primary orchestrator, Dagster shown.** Airflow is what the learner will most likely operate in their first job; it is the lingua franca. Dagster's asset-oriented model is the clearest articulation of where the field is heading, so the learner must be able to read it and argue the trade.
- **Iceberg *and* Delta, not one.** These are the two serious open table formats and the learner will meet both. Teaching only one would leave a blind spot. We build the lakehouse lab in one and read the other.
- **Spark Structured Streaming for the streaming weeks.** Choosing Structured Streaming over Flink for the core labs is deliberate: it reuses the Spark DataFrame mental model from the prior weeks, so the cohort spends its energy on streaming *semantics* (event time, watermarks, exactly-once) rather than a second engine's API. Flink is taught as the dedicated, lower-latency alternative and the cohort implements one comparison.
- **Great Expectations for data quality.** Open, declarative, and it produces a human-readable data-quality report — exactly the artifact a senior reviewer wants to see gating a pipeline boundary.
- **Everything in Docker.** No cloud account is a hard requirement of this track. A learner in a country where a cloud bill is a month's rent does the entire course, capstone included, on a laptop. That is a deliberate access decision, not a convenience.

---

## A note on cost discipline

We teach cost as a first-class engineering concern, not an afterthought, even though the labs run for free on a laptop. The reason is that the *shape* of cost in a cloud data platform — scan cost, storage cost, compute cost, the price of a full-table shuffle, the savings from partition pruning and file compaction — is learnable locally, and it is the single skill most often missing in junior data engineers. A pipeline that produces the right answer at ten times the necessary cost is a junior pipeline. Week 11 makes the cohort reason about the bill even though no bill arrives, because the habit is the point.

---

## What happens as the ecosystem moves

The data-engineering stack revs faster than most. Table formats, orchestrators, and streaming engines all ship meaningful releases yearly or faster, and new entrants (the cohort will hear about whatever is hot the season they enroll) appear constantly. The syllabus is therefore a *living document* with a stated revision policy:

1. The curriculum council reviews every week against the ecosystem before each cohort.
2. Where a new tool has genuinely displaced an incumbent (not merely hyped), the lab moves and the charter is amended with a recorded rationale.
3. The previous version is archived under `OLD/SYLLABUS-YYYY.md` and remains available to running cohorts.
4. The concepts — modeling, storage, distributed compute, streaming semantics, quality, lineage, cost — are stable even as the tools that implement them churn. We teach the concepts so the cohort outlives the tools.

---

## Status

This charter is the first edition of the C27 track, drafted under the Crunch Labs Charter. Changes that affect more than wording — week count, phase shape, capstone definition, the open-source posture, the boundaries with C5 / C15 / C16 — require a charter revision and a PR review, not a silent edit.

Signed by the Code Crunch Club curriculum council — Crunch Labs working group. Open an issue on the master curriculum repository to propose amendments.

C27 is licensed under **GPL-3.0**. See [`LICENSE`](./LICENSE). Fork, teach, remix; PR improvements back to <https://github.com/CODE-CRUNCH-CLUB>.
