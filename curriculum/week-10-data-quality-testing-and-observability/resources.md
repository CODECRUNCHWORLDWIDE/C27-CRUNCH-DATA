# Week 10 — Resources

Every tool this week is **open source** and runs locally: Great Expectations, dbt Core, the `dbt_utils` and `dbt_expectations` packages, Elementary, and Soda all publish full docs openly. The two books are the only paid material and neither is required to complete the week — they're the deeper "why." One warning that saves hours: **Great Expectations had a hard API break between 0.x and 1.x** (Lecture 2 §1); when you open the GX docs, confirm the version selector says **1.x** before you copy anything, or you'll fight syntax that doesn't exist in your install.

When a link is versioned, the current stable URL is given. If a property name or method signature differs in your version, the project's own docs are authoritative.

## Great Expectations (the ingestion gate)

- **Great Expectations — documentation home** (GX Core 1.x; Data Context, Datasources, suites, Validation Definitions, Checkpoints, Actions, Data Docs): <https://docs.greatexpectations.io/docs/>
- **GX — core concepts overview** (the object model the whole framework hangs on): <https://docs.greatexpectations.io/docs/core/introduction/gx_overview>
- **GX — Expectations Gallery** (the full catalog of expectation classes — search here before writing a custom one): <https://greatexpectations.io/expectations/>
- **`airflow-provider-great-expectations`** (the `GreatExpectationsOperator` that raises on a failed checkpoint, turning a check into a gate): <https://github.com/great-expectations/airflow-provider-great-expectations>

## dbt — tests and source freshness (the transformation + source gates)

- **dbt — Add data tests to your DAG** (generic + singular tests, `severity`, `error_if`/`warn_if`, `store_failures`): <https://docs.getdbt.com/docs/build/data-tests>
- **dbt — Snapshotting source data freshness** (`loaded_at_field`, `warn_after`, `error_after`, the `dbt source freshness` command): <https://docs.getdbt.com/docs/build/sources#snapshotting-source-data-freshness>
- **dbt — documentation home**: <https://docs.getdbt.com/>
- **dbt-utils** (`expression_is_true`, `accepted_range`, `unique_combination_of_columns` and more): <https://github.com/dbt-labs/dbt-utils>
- **dbt-expectations** (the Great Expectations vocabulary ported into dbt — `expect_column_values_to_be_between`, `expect_row_values_to_have_recent_data`): <https://github.com/calogica/dbt-expectations>

## Data observability — the open-source next steps

- **Elementary — data observability for dbt** (collects run/test results and adds anomaly detection on top of dbt's artifacts): <https://docs.elementary-data.com/>
- **Soda — documentation** (SodaCL, a declarative checks-as-config language with freshness and anomaly checks across many sources): <https://docs.soda.io/>
- *(Commercial, for comparison only — not used in this course)* **Monte Carlo** — automated, ML-based data observability at enterprise scale; this is what the DIY `meta.load_metrics` table grows into when an org will pay for hands-off monitoring.

## Data contracts

- **Data Contract Specification** (the open, machine-readable YAML spec for contracts — the shape used in Lecture 3 §1 and Challenge 2): <https://datacontract.com/>
- **Data Contract CLI** (`datacontract test` — derive and run checks from a contract): <https://github.com/datacontract/datacontract-cli>
- **GoCardless and PayPal data-contract write-ups** — the two most-cited industry accounts of contracts in production; search the respective engineering blogs for the current canonical posts (titles drift, so search rather than hard-link).

## The books (the "why")

- **Joe Reis & Matt Housley, *Fundamentals of Data Engineering*** (O'Reilly, 2022) — the data-quality dimensions, SLAs, and the place of validation in the lifecycle. ISBN **978-1-098-10830-4**: <https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/>
- **Barr Moses, Lior Gavish & Molly Vorwerck, *Data Quality Fundamentals*** (O'Reilly, 2022) — "data downtime," the dimensions of quality at scale, and observability as a discipline: <https://www.oreilly.com/library/view/data-quality-fundamentals/9781098112035/>

## Course cross-references

- **C27 Syllabus** (Week 10 spec and where this sits in Phase III): [`../../SYLLABUS.md`](../../SYLLABUS.md)
- Week 4 (Airflow — the DAG the gates wire into), Week 5 (dbt on DuckDB — the transformation tests), Week 3 (idempotent ETL — what the uniqueness checks *prove*).

## Reading-time budget

If you have limited time, spend it in this order. The taxonomy and one working gate are the irreducible core; everything else is depth.

| Resource | Time | When |
|---|---:|---|
| Lecture 01 — taxonomy + halting gate | 35 min | Monday, non-negotiable |
| Lecture 02 §1–§4 — GX suite + checkpoint | 30 min | Tuesday, before Exercise 01 |
| GX docs — core concepts overview | 20 min | Tuesday, alongside the exercise |
| Lecture 02 §6 — dbt test severity | 15 min | Wednesday |
| dbt — data tests doc | 20 min | Wednesday, before Exercise 03 |
| Lecture 03 §1–§2 — contracts + source freshness | 30 min | Thursday |
| dbt — source freshness doc | 15 min | Thursday, before Exercise 04 |
| Lecture 03 §3–§5 — anomalies + observability | 25 min | Thursday/Friday |
| *Fundamentals of Data Engineering*, ch. 9 | 60 min | weekend / stretch |
| *Data Quality Fundamentals* (skim) | 90 min | stretch |

---

*If a link 404s, please open an issue so we can replace it.*
