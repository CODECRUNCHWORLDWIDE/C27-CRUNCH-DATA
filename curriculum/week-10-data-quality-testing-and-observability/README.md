# Week 10 — Data Quality, Testing and Observability

> This is the week your pipeline stops trusting its inputs. For nine weeks you built things that move data — a star schema, incremental ETL, Airflow DAGs, a dbt project, a lakehouse, Spark jobs, a streaming consumer. All of it assumed the data was *good*. This week you assume it is not, and you build the gates that catch the bad load **before the executive does** — at the ingestion boundary, where a malformed file should never have landed, and at the mart boundary, where a stale or short rollup should never have been published.

The most expensive data bug is not the one that crashes the pipeline. A crash is loud; someone gets paged; it gets fixed. The expensive bug is the one that *succeeds*: the load that runs green, lands 40% of yesterday's rows because an upstream API silently truncated, rolls cleanly through staging and into the mart, refreshes the executive dashboard, and is presented in a board meeting as a real 60% drop in revenue. Nobody was paged. The pipeline did exactly what it was told. The data was wrong, and the system had no opinion about it. This week is about giving the system an opinion.

That opinion has a name in the trade: a **quality gate**. A gate is a check with the authority to **halt** the pipeline — not log a warning into a file nobody reads, not color a cell red on a dashboard nobody opens, but *fail the task, stop the downstream, and alert a human*. The single most important idea this week, the one that separates a data engineer from someone who writes scripts that move data, is that **a check you only log is not a gate.** If the malformed load still lands when the check fails, you have built a smoke detector with no battery. The lecture title is the whole philosophy: catch the bad load before the executive does, and to do that the gate must be able to stop the load.

We approach quality the way a discipline approaches it: through a **taxonomy**. There are six dimensions of data quality — completeness, validity, uniqueness, freshness, volume, distribution — and each failure class has a *right* check. A null in a non-null column is a completeness failure; a `total_cents` of `-1` is a validity failure; two rows with the same `order_id` is a uniqueness failure; a `loaded_at` that is six hours stale is a freshness failure; a load of 400 rows where you expected 40,000 is a volume failure; a sudden swing in the mean order value or the null-rate of a column is a distribution failure. By Friday you will look at any data incident and name its dimension, then reach for the right check without thinking — a Great Expectations expectation, a dbt test, a source-freshness rule, a volume-delta query.

The tools are the ones you already half-know, used properly. **Great Expectations** (GX Core 1.x) is the validation framework you wire at the ingestion boundary: a Data Context, a Datasource, a Batch Definition, an Expectation Suite of declarative checks (`expect_column_values_to_not_be_null`, `expect_column_values_to_be_between`, `expect_compound_columns_to_be_unique`), a Validation Definition, and a **Checkpoint** that runs the suite and — this is the part that matters — *returns a result you can act on*, so your Airflow task can `raise` and halt the DAG when the suite fails. **dbt tests** are the gates you already met in Week 5, now revisited in depth: generic tests (`unique`, `not_null`, `accepted_values`, `relationships`), singular tests (arbitrary SQL in `tests/`), the `dbt_utils` and `dbt_expectations` packages, and the `severity: warn` vs `severity: error` distinction that decides whether a failure halts the build or merely complains. And **`dbt source freshness`** is the freshness gate: a `loaded_at_field`, a `warn_after`, an `error_after`, run as `dbt source freshness`, that fails the pipeline when the upstream table has gone stale.

Beyond the gates is the contract. A **data contract** is the agreement a producing team and a consuming team hold each other to: the schema and its grain, the semantics of each field, the SLAs for freshness and volume, who owns it, what change policy applies (can a column be dropped without notice? renamed? widened?), and which fields are PII. It is the artifact that turns "the upstream team broke us again" from a Slack fight into a contract-test failure that the *producer's* CI catches *before* they ship the breaking change. You will author one in real YAML and wire it to automated checks, so that a dropped column or a blown SLA is caught mechanically rather than discovered downstream.

And around all of it is **observability**: the run metadata every pipeline run should emit — start and end time, rows read and rows written, the latency of each stage, the freshness of the result — so that when something does slip a gate, you have the telemetry to find out *what* and *when*. A pipeline that runs but emits no metrics is a black box; the first question in any incident is "how many rows did last night's load write, and how does that compare to the night before?" and you should be able to answer it from a metadata table, not by re-running the job. Anomaly detection — freshness against an SLA, volume against a rolling baseline, distribution drift against history — is what turns that metadata from a forensic record into an early warning.

By Sunday you will have built the headline lab: a **quality-gated pipeline** with a GX suite at the ingestion boundary and a freshness + volume check at the mart boundary, both wired into an Airflow DAG so that a deliberately corrupted file *fails the pipeline and alerts* instead of landing silently — plus the human-readable data-quality report the pipeline emits as an artifact on every run. This is the week the pipeline grows a conscience.

## Learning objectives

By the end of this week, you will be able to:

- **Name the six data-quality dimensions** — completeness, validity, uniqueness, freshness, volume, distribution — and, given any data incident, classify it and reach for the right check for that dimension.
- **Distinguish a gate from a log.** Explain why a quality check must be able to *halt* a pipeline (fail the task, stop the downstream, alert a human) to be worth anything, and explain the difference between `warn` and `error` (fail) severities and when each is correct.
- **Author a Great Expectations suite** in GX Core 1.x — Data Context, Datasource, Data Asset, Batch Definition, Expectation Suite, Validation Definition, Checkpoint, Actions, Data Docs — and write real expectations for schema, null thresholds, ranges, value sets, uniqueness, and referential integrity.
- **Wire a GX Checkpoint as a pipeline gate** in an Airflow DAG so that a failing validation `raise`s and halts the DAG rather than logging and continuing.
- **Revisit dbt tests in depth** — generic tests, singular tests, `dbt_utils` and `dbt_expectations`, `severity`/`error_if`/`warn_if`/`store_failures` — and pick the right test type and severity for a given quality rule.
- **Configure `dbt source freshness`** with `loaded_at_field`, `warn_after`, and `error_after`, and detect freshness anomalies at a source boundary.
- **Detect volume and distribution anomalies** — row-count deltas against a rolling baseline, mean/null-rate/cardinality drift against history — with SQL and singular tests.
- **Write a data contract** two teams can hold each other to (schema, grain, semantics, freshness/volume SLAs, ownership, change policy, PII flags) and enforce its clauses as automated checks.
- **Emit pipeline observability** — run metadata, row counts, stage latency, result freshness — and produce a human-readable data-quality report as a pipeline artifact.

## Prerequisites

This week assumes you have completed **C27 weeks 1–9**, or have equivalent data-engineering fluency. Specifically:

- The **Postgres star schema** (Week 1–2) with a `fct_orders` fact and its dimensions, and the **incremental Python ETL** (Week 3) that loads it. The GX ingestion gate guards *that* load.
- A working **Airflow** install (Week 4) and at least one DAG you can read and edit. The gates wire into a DAG; you must be comfortable adding tasks, setting dependencies, and reading task logs.
- The **dbt project on DuckDB** (Week 5) with `staging` / `intermediate` / `marts` layers. The mart-boundary checks and the revisited dbt tests live here.
- The **Parquet + Iceberg lakehouse on MinIO** (Weeks 6–7) and the **Kafka / Spark Structured Streaming** work (Weeks 8–9), as the data sources the gates sit in front of. The streaming source matters for freshness.
- **Python** (pandas, SQLAlchemy or a Postgres driver) and **SQL** from memory — `GROUP BY`, window functions, CTEs, date functions. The anomaly-detection checks are SQL.
- **Docker** and ~6 GB free — Postgres, DuckDB, MinIO, and Airflow run as containers, plus a Python environment for Great Expectations.

You do **not** need prior experience with Great Expectations or with data contracts. We start at the taxonomy and build up to a halting gate. The canonical week titles and the course contract live in [`../../SYLLABUS.md`](../../SYLLABUS.md) (Week 10).

## Topics covered

- **The six data-quality dimensions.** Completeness (is the data there — nulls, missing rows, missing partitions), validity (is it well-formed — types, ranges, formats, value sets), uniqueness (no unintended duplicates — keys, compound keys), freshness (is it recent enough — load timestamp vs SLA), volume (is the amount right — row counts vs baseline), distribution (is the *shape* right — mean, null-rate, cardinality drift). The right check for each.
- **The halting gate.** Why a check that only logs is worthless; how a gate `raise`s and stops the downstream; `warn` vs `error` (fail) severity; the cost of a false halt vs a missed bad load, and how to tune the threshold between them.
- **Great Expectations (GX Core 1.x).** The full object model — Data Context (file/ephemeral), Datasource, Data Asset, Batch Definition, Batch, Expectation, Expectation Suite, Validation Definition, Checkpoint, Action, Data Docs. Real expectations: `expect_column_values_to_not_be_null`, `expect_column_values_to_be_between`, `expect_column_values_to_be_in_set`, `expect_column_values_to_be_unique`, `expect_table_row_count_to_be_between`, `expect_column_values_to_match_regex`, `expect_compound_columns_to_be_unique`. The honest 0.x-vs-1.x API difference.
- **dbt tests revisited.** Generic tests (`unique`, `not_null`, `accepted_values`, `relationships`), singular tests (SQL in `tests/`), `dbt_utils` (`expression_is_true`, `accepted_range`) and `dbt_expectations` (`expect_column_values_to_be_between`, `expect_row_values_to_have_recent_data`), severity (`warn`/`error`, `error_if`, `warn_if`), and `store_failures`.
- **Freshness, volume, and distribution anomaly detection.** `dbt source freshness` with `loaded_at_field` / `warn_after` / `error_after`; row-count deltas vs a rolling baseline; mean / null-rate / cardinality drift vs history.
- **Data contracts.** What a contract contains — schema, grain/semantics, freshness + volume SLAs, ownership, change policy, PII flags — expressed in real YAML, and how to enforce each clause as an automated check the *producer* runs.
- **Pipeline observability.** Run metadata (start/end, status, rows read/written), stage latency, result freshness, emitting metrics to a metadata table; the data-quality report as a versioned artifact.

## Weekly schedule

The schedule below adds up to approximately **36 hours**. Treat it as a target, not a contract.

| Day       | Focus                                                              | Lectures | Exercises | Challenges | Quiz/Read | Homework | Mini-Project | Self-Study | Daily Total |
|-----------|-------------------------------------------------------------------|---------:|----------:|-----------:|----------:|---------:|-------------:|-----------:|------------:|
| Monday    | The six DQ dimensions; the halting gate; warn vs fail             |   2h     |    1h     |    0h      |   0.5h    |   1h     |     0h       |   0.5h     |    5h       |
| Tuesday   | Great Expectations: context, suites, checkpoints, Data Docs       |   1.5h   |    2h     |    0h      |   0.5h    |   1h     |     0h       |   0.5h     |    5.5h     |
| Wednesday | dbt tests revisited; dbt_utils, dbt_expectations; source freshness|   1.5h   |    2h     |    1h      |   0.5h    |   1h     |     0h       |   0h       |    6h       |
| Thursday  | Data contracts; freshness/volume/distribution anomalies; observ.  |   1.5h   |    1h     |    1h      |   0.5h    |   1h     |     1.5h     |   0.5h     |    7h       |
| Friday    | Wire the gates into the DAG; the DQ report artifact               |   0h     |    0h     |    0h      |   0.5h    |   0h     |     3h       |   0.5h     |    4h       |
| Saturday  | Mini-project deep work                                            |   0h     |    0h     |    0h      |   0h      |   0h     |     3.5h     |   0h       |    3.5h     |
| Sunday    | Quiz, review, DQ report polish                                    |   0h     |    0h     |    0h      |   1h      |   0h     |     3h       |   0h       |    4h       |
| **Total** |                                                                   | **8h**   | **8h**    | **3h**     | **4h**    | **4h**   | **15h**      | **2.5h**   | **44.5h**   |

> The totals above run hot on purpose — Week 10 is a heavy week and most cohorts spend the full 36 in the mini-project plus lectures and skip some self-study. Budget the core 36 and treat the rest as stretch.

## How to navigate this week

| File | What's inside |
|------|---------------|
| [README.md](./README.md) | This overview (you are here) |
| [resources.md](./resources.md) | The GX docs and Expectations Gallery, dbt tests + source-freshness docs, `dbt_utils` / `dbt_expectations` / Elementary / Soda, the two O'Reilly books, and the data-contract references |
| [lecture-notes/01-the-data-quality-taxonomy-and-the-halting-gate.md](./lecture-notes/01-the-data-quality-taxonomy-and-the-halting-gate.md) | The six DQ dimensions and the right check for each; why a gate must halt; fail vs warn severity |
| [lecture-notes/02-great-expectations-suites-checkpoints-and-dbt-tests.md](./lecture-notes/02-great-expectations-suites-checkpoints-and-dbt-tests.md) | GX Core 1.x end to end (context → checkpoint → Data Docs) and dbt tests revisited in depth |
| [lecture-notes/03-data-contracts-freshness-volume-anomalies-and-observability.md](./lecture-notes/03-data-contracts-freshness-volume-anomalies-and-observability.md) | Data contracts in YAML; freshness/volume/distribution anomaly detection; pipeline observability |
| [exercises/SOLUTIONS.md](./exercises/SOLUTIONS.md) | Worked solutions to the four exercises, with code and verification output |
| [exercises/exercise-01-ingestion-gx-suite.py](./exercises/exercise-01-ingestion-gx-suite.py) | Build a GX suite over the raw `orders` ingestion asset |
| [exercises/exercise-02-gx-checkpoint-and-datadocs.py](./exercises/exercise-02-gx-checkpoint-and-datadocs.py) | Wire a Checkpoint, run validation, render Data Docs |
| [exercises/exercise-03-dbt-mart-tests.yml](./exercises/exercise-03-dbt-mart-tests.yml) | A dbt `schema.yml` with generic tests + `dbt_expectations` to complete |
| [exercises/exercise-04-dbt-source-freshness.yml](./exercises/exercise-04-dbt-source-freshness.yml) | A dbt `sources.yml` with a freshness config to complete |
| [challenges/challenge-01-fail-the-pipeline-on-bad-data.md](./challenges/challenge-01-fail-the-pipeline-on-bad-data.md) | Wire a GX checkpoint into the DAG so a corrupted file *halts* the pipeline and alerts |
| [challenges/challenge-02-build-a-data-contract-and-enforce-it.md](./challenges/challenge-02-build-a-data-contract-and-enforce-it.md) | Author a producer/consumer data contract and enforce its schema/freshness/volume clauses |
| [mini-project/README.md](./mini-project/README.md) | Lab 10 — the quality-gated pipeline end to end, plus the human-readable DQ report |
| [quiz.md](./quiz.md) | 10 questions with a worked answer key |
| [homework.md](./homework.md) | Six practice problems extending the week |

## Reading order if short on time

If you have only a few hours this week, read in this order and skip the rest:

1. **[lecture-notes/01](./lecture-notes/01-the-data-quality-taxonomy-and-the-halting-gate.md)** — the taxonomy and the halting gate. This is the *why*, and it is the part you carry to every future pipeline. Non-negotiable.
2. **[lecture-notes/02](./lecture-notes/02-great-expectations-suites-checkpoints-and-dbt-tests.md) §1–§4** — enough GX to write a suite and a checkpoint, plus the dbt-test severity section. The *how* for the gates.
3. **[mini-project/README.md](./mini-project/README.md)** — skim the brief so you know what the week builds toward, then do as much of it as time allows. The lab is where the ideas become reflexes.
4. **[lecture-notes/03](./lecture-notes/03-data-contracts-freshness-volume-anomalies-and-observability.md) §1 and §2** — the data-contract anatomy and `dbt source freshness`. Read the anomaly-detection sections if you have time.

Everything else — the second challenge, the homework, the full Data Docs walkthrough — is valuable but deferrable. The taxonomy and one working halting gate are the irreducible core.

---

*If you find errors in this material, please open an issue or send a PR. Future learners will thank you.*
