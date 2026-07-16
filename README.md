# C27 · Crunch Data — Data Engineering & Streaming

> Code Crunch Club · Crunch Labs tier · sub-brand **Data** (`#0EA5E9`)
> 12 weeks · ~432 hours · GPL-3.0
> Track home: `C27-CRUNCH-DATA/`

Twelve weeks to walk from "I can write a SQL query and a Python script" to "I own the pipeline." We start where any working engineer can start — a dimensional model on a laptop, a `SELECT` that actually uses the index, a Python ETL job you can re-run without fear — and we end with an end-to-end lakehouse: data lands from a batch source and a Kafka topic, gets orchestrated by Airflow, transformed and tested by dbt, stored as Parquet in an Iceberg table, queried with DuckDB and Spark, validated by Great Expectations, and surfaced on a dashboard that an analyst trusts at 9 AM on a Monday. Along the way you will model a slowly-changing dimension, tune an analytical query against a columnar engine, backfill a year of history without melting the cluster, reason about exactly-once semantics, and write the data-quality test that catches the bad load before the executive does.

This is not a "drag boxes in a managed cloud console" course. It is the curriculum we wish existed for engineers who want to take data engineering seriously as a discipline — including the unglamorous parts about idempotency, late-arriving data, schema evolution, lineage, and what a pipeline costs when nobody is watching the bill. Everything runs on a laptop with Docker; nothing here requires a corporate cloud account or a vendor's free tier.

The center of gravity is the **lakehouse + the stream**. A modern data platform is no longer a warehouse you load overnight; it is a set of tables that batch jobs and streaming jobs write into continuously, with contracts, tests, and lineage between them. C27 teaches that platform from the storage layer up — file formats and table formats before query engines, query engines before orchestration, orchestration before streaming — so that you understand why each layer exists before you are asked to operate it.

---

## Who this is for

Four personas, all welcome, all stretched:

1. **The analyst or analytics engineer going upstream.** You write SQL and dbt models against tables someone else fills. You want to own the layer below — ingestion, orchestration, the lakehouse storage, the streaming source — so you stop filing tickets against the data platform and start building it. We start you on dimensional modeling and advanced SQL so you move fast before the distributed parts arrive.
2. **The backend or Python engineer pivoting to data.** You ship FastAPI, Django, or services every day (C16-style). You can write Python and you understand systems, but you have never reasoned about a 200 GB join, a columnar scan, or a Kafka consumer group rebalance. This track gives you the data-specific muscle: storage formats, partitioning, parallelism, and the failure modes that only show up at volume.
3. **The data scientist who is tired of dirty inputs.** You finished C5 (AI & Data Science). You can model, but your features arrive late, malformed, or silently wrong, and you spend half your week cleaning instead of modeling. C27 is the other half of your job: the pipeline that delivers clean, tested, versioned, on-time data so your models train on something real.
4. **The platform / DevOps engineer adding data to the stack.** You finished C15 (DevOps) and you operate services well. Now your org has a data platform and you are on call for it. You need to understand Airflow DAGs, dbt builds, Spark jobs, and Kafka topics well enough to debug them at 3 AM and reason about what they cost.

If you can write a non-trivial SQL query and a Python script that reads a file, you are ready. If you cannot, take C1 (Convos) and the SQL portion of C5 or C16 first.

---

## What you will be able to do at the end

Twelve concrete capabilities you should have on day 84:

1. Design a **dimensional model** (star schema, conformed dimensions, fact-table grain) for a real business domain, and implement a Type-2 slowly-changing dimension that you can audit.
2. Write **advanced analytical SQL** — window functions, common table expressions, `GROUPING SETS`, `QUALIFY`, anti-joins — and read a query plan well enough to know why it is slow before you guess.
3. Build a **batch ingestion + Python ETL** job that is idempotent, restartable, and incremental, with watermarking and a sane handling of late-arriving records.
4. Author and operate **workflow orchestration** in Airflow (and read a Dagster DAG) — schedules, sensors, backfills, retries, SLAs, and the difference between a task that failed and a task that lied.
5. Build a **transformation layer in dbt** — staging / intermediate / mart models, tests, documentation, sources, snapshots, and incremental materializations — and explain why analytics-as-code beats hand-run SQL.
6. Reason about **file and table formats** from the bytes up — row vs columnar, Parquet internals (row groups, encodings, predicate pushdown), and what Delta Lake / Apache Iceberg add (ACID, time travel, schema evolution) on top of object storage.
7. Run **distributed processing with Apache Spark** — the DataFrame API, partitioning, shuffles, joins, the cost of a wide transformation, and how to read a Spark UI to find the skewed task.
8. Stand up and operate **event streaming with Apache Kafka** — topics, partitions, consumer groups, offsets, delivery semantics, the schema registry, and why ordering is only ever per-partition.
9. Build a **stream-processing job** (Spark Structured Streaming, with Flink shown as the alternative) — event time vs processing time, watermarks, windowing, stateful aggregation, and exactly-once sinks.
10. Implement **data quality, testing, and observability** — Great Expectations suites, dbt tests, freshness and volume checks, and a pipeline that alerts you to bad data instead of letting it land silently.
11. Apply **governance, lineage, and cost discipline** — column-level lineage, a data catalog, partition pruning and file compaction for cost, PII handling, and a data contract that two teams can actually hold each other to.
12. Ship an **end-to-end lakehouse + streaming pipeline** — ingestion to lakehouse to transformation to streaming to dashboard, with data-quality gates at every boundary — and defend every layer of it to a senior reviewer.

---

## Prerequisites

| Required | Helpful | Not required |
| --- | --- | --- |
| **C1 — Code Crunch Convos** (or equivalent Python fluency) | **C5 — AI & Data Science** or **C16 — Web Backend** (SQL + data exposure) | A four-year CS degree |
| Comfort writing a non-trivial SQL query | **C15 — DevOps** (Docker, containers, CI) | A prior data-engineering job |
| Comfort reading and writing a Python script | Some exposure to pandas or a warehouse | A distributed-systems background |
| A laptop that can run Docker (16 GB RAM realistic, 8 GB workable) | A second terminal and the patience to read a log | A cloud account |

**Hardware reality.** Everything in C27 runs locally on a laptop with **Docker** and **Docker Compose** — no cloud account, no credit card, no vendor free tier. We use Postgres, DuckDB, MinIO (S3-compatible object storage), Airflow, dbt, Spark, and Kafka, all in containers, all open-source. **16 GB of RAM** is the realistic target; the Spark and Kafka weeks are tighter at 8 GB but we provide trimmed Compose profiles for smaller machines. A handful of datasets are large (the NYC taxi corpus, a synthetic clickstream); we ship a downloader and a `--sample` mode so you are never blocked on disk or bandwidth.

**No managed-cloud lock-in.** Where a cloud product is the industry reference (Snowflake, BigQuery, Databricks, Kafka-as-a-service, AWS Glue, Confluent Cloud), we name it, describe what it does, and explain the trade — but every lab has an open-source path that runs on your machine. The skills transfer to any cloud; the bill does not follow you home.

---

## Program at a glance — three phases

| Phase | Weeks | Title | Focus | Capstone milestone |
| --- | --- | --- | --- | --- |
| I | 1–4 | Modeling & Batch Foundations | Dimensional modeling, analytical SQL, Python ETL, orchestration | Orchestrated, idempotent batch pipeline into a modeled warehouse |
| II | 5–8 | The Lakehouse & Distributed Compute | dbt, Parquet, Delta/Iceberg, DuckDB, Spark, Kafka intro | A dbt-transformed lakehouse on object storage, queried by Spark |
| III | 9–12 | Streaming, Quality & Capstone | Stream processing, data quality, governance & cost, capstone | End-to-end lakehouse + streaming pipeline with quality gates |

Week-by-week detail lives in [`SYLLABUS.md`](./SYLLABUS.md). Design rationale (why data engineering is its own track, why storage before compute, why open-source-first) lives in [`CHARTER.md`](./CHARTER.md).

---

## Weekly cadence

The track runs at **36 hours per week** for full-time cohorts and compresses to **12 hours per week** for self-paced cohorts. Each week ships one lab, one quiz, and one logged pipeline-run-and-inspect entry (a screenshot of the DAG, the Spark UI, the query plan, or the data-quality report — proof you looked, not just ran).

| Day | Block | Typical content |
| --- | --- | --- |
| Mon | Lecture (2h) | Topic intro, reference reading, a real query plan or DAG walkthrough |
| Mon | Lab (3h) | Guided exercise — build the pipeline, run it, inspect what it did |
| Wed | Lecture (2h) | Deeper dive, code review of last week's lab, an architecture decision record |
| Wed | Lab (3h) | Open-ended mini-project sprint |
| Fri | Studio (4h) | Pipeline-debugging clinic, query-tuning office hours, cost-and-lineage review |
| Sun | Quiz (~30m) + reading | Auto-graded; covers the week's docs, formats, and failure modes |

The remaining hours are unstructured project time — building, breaking, backfilling, and reading logs.

---

## Recommended pre/post tracks

```text
C1 (Code Crunch Convos · Python)
        |
        v
C5 (Crunch AI / Data Science)  -- OR --  C16 (Crunch Pro — Web Backend)
        |                                        |
        +--------------------+-------------------+
                             v
        *** C27 (Crunch Data — Data Engineering & Streaming) ***
                             |
        +--------------------+--------------------+
        |                    |                    |
        v                    v                    v
   C15 (DevOps)         C18 / C19           C5 (back to ML)
   to operate the    (GCP / AWS) to run    with a real, tested
   platform on K8s   the lakehouse at      feature pipeline
                     cloud scale           feeding the model
```

- **C27 vs C5.** C5 owns the model — analysis, classical ML, PyTorch, evaluation, shipping a model behind an API. C27 owns the **data that feeds the model** — ingestion, the lakehouse, transformation, streaming, and quality. They meet at the feature table: C5 consumes it, C27 produces it. Take C27 if your problem is "the data is late, dirty, or untrusted"; take C5 if your problem is "I have clean data and need a model."
- **C27 vs C15.** C15 owns the operations platform — Docker, Kubernetes, CI/CD, observability for **services**. C27 owns the **data platform** — pipelines, the lakehouse, streaming, observability for **data**. They overlap on Docker, CI, and on-call discipline; they diverge on everything data-shaped. A graduate who wants to run the C27 capstone at fleet scale takes C15 next.
- **C27 vs C16.** C16 owns the **transactional service** — the Django/FastAPI app that produces operational data. C27 owns the **analytical platform** that consumes it via change-data-capture and batch extracts. C16 writes the row; C27 turns a billion rows into a trusted table.

---

## What this course will NOT do

Honest expectations, set up front:

- **It will not make you a data scientist.** We deliver clean, tested, on-time data; we do not teach model training, feature selection, or experiment tracking. That is C5. We respect the boundary in both directions.
- **It will not certify you on Snowflake, Databricks, or BigQuery.** We teach the concepts those products implement — columnar storage, ACID table formats, distributed query, time travel — on open-source equivalents that run on your laptop. A graduate ramps onto any of them in a week. We name them, describe them, and refuse to lock you into any.
- **It will not pretend "real-time" is free.** Streaming is powerful and expensive in complexity. We teach exactly-once semantics, watermarks, and stateful processing honestly — including the cases where a 15-minute batch is the right answer and a stream is over-engineering.
- **It will not turn you into a distributed-systems researcher.** We teach you to operate Spark and Kafka, read their UIs, and reason about partitions, shuffles, and consumer groups. We do not derive consensus protocols or teach you to write a query optimizer. We teach the user's depth, then point at the literature.
- **It will not make you a dashboard designer.** The capstone dashboard exists to prove the pipeline is trustworthy and queryable. We teach you to surface a tested metric; we do not teach BI visual design, color theory, or executive storytelling.
- **It will not lock you into a vendor.** Postgres, DuckDB, MinIO, Airflow, dbt-core, Spark, Kafka, Iceberg, Delta Lake, and Great Expectations are all open-source and all run on your machine. Cloud products appear as named comparisons, never as the only path.

---

## Capstone preview

The Phase III capstone is **one substantial end-to-end platform**, not a parade of disconnected demos:

> **End-to-End Lakehouse + Streaming Pipeline.** A pipeline that ingests from a batch source (a daily file drop / CDC extract) **and** a Kafka event stream, lands raw data into an Iceberg or Delta lakehouse on MinIO object storage, orchestrates the batch path with Airflow, transforms with dbt into a tested dimensional model, runs a Spark Structured Streaming job that updates a near-real-time aggregate, gates every layer boundary with Great Expectations and dbt tests, tracks lineage end-to-end, and surfaces the result on a dashboard an analyst would trust. Survives a documented chaos drill: a malformed batch load, a stream partition lag spike, or a schema-evolution event.

Full specification in [`SYLLABUS.md` § Capstone](./SYLLABUS.md#capstone). Deliverables include an architecture diagram, a five-minute walkthrough video, a chaos-drill postmortem, a data-quality report, and a production runbook.

---

## License & maintainers

Licensed **GPL-3.0**. See [`LICENSE`](./LICENSE).

You may fork, adapt, teach, and remix. If you improve it, please PR back to <https://github.com/CODE-CRUNCH-CLUB>.

Maintained by the Code Crunch Club curriculum council — Crunch Labs working group. Track lead: Data (`#0EA5E9`). Issues, errata, and PRs at the GitHub org.

This is a living document. The data-engineering ecosystem moves quickly — table formats, orchestrators, and streaming engines all rev often. We review the syllabus each cohort and freeze it before enrolment.
