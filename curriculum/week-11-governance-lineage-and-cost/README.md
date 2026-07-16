# Week 11 — Governance, Lineage and Cost

> The senior engineer reasons about the bill even when no bill arrives. This week you learn where cost hides in a data platform, how to make it disappear with compaction and partition pruning, how to trace a wrong number from a dashboard back to the source that produced it, and how to delete one user's data from a table that was built to be append-only forever.

For ten weeks the metric has been *correctness*: the pipeline that does not double-count, the join that does not explode, the stream that does not lose an event. Correctness is the floor. This week we add the two things that separate a competent data engineer from a senior one — **cost** and **governance** — and they turn out to be the same discipline wearing two hats. Both are about knowing exactly which bytes your queries touch, where they live, who is allowed to read them, and how to make them go away.

Cost is the part nobody teaches because on a laptop there is no invoice. That is precisely why it is dangerous. The query that scans 40 GB to return one number runs fine on your machine and bankrupts a team on BigQuery. The lakehouse that lands ten thousand 64 KB files per day works in the demo and brings a Spark cluster to its knees in production. You will not learn cost from a bill — by the time the bill arrives the damage is months old. You learn it by reading the plan, counting the bytes scanned, and internalizing the cost model: a scan costs bytes read, a shuffle costs the network, and storage costs money per tier per month whether anyone queries it or not. We make all three visible on the laptop — Iceberg metadata files, the Spark UI, and DuckDB's `EXPLAIN ANALYZE` — and then name the cloud analogues honestly so the instinct transfers to Snowflake credits and S3 storage classes without you ever touching a vendor console.

The small-files problem is the canonical cost trap and the one you will fix this week with your own hands. Streaming jobs and frequent micro-batches write many tiny files; a query then opens, seeks, and closes thousands of file handles to read a gigabyte of logical data, and the metadata overhead dwarfs the actual scan. The fix is **compaction** — Iceberg's `rewrite_data_files`, Delta's `OPTIMIZE` bin-packing — which rewrites those tiny files into a handful of right-sized ones. The second half of the fix is **partition pruning**: lay the data out so the engine can skip whole directories it knows cannot match the predicate, instead of opening every file to check. You will measure the bytes-scanned difference before and after, and the number will be large enough to change how you write every pipeline afterward.

Lineage is the governance tool you reach for first in an incident, and the reason is simple: when an executive emails "this revenue number looks wrong," the only question that matters is *where did this number come from*. Lineage answers it. Table-level lineage tells you which models feed the dashboard; column-level lineage tells you that `total_revenue` is derived from `orders.amount` joined to a currency-conversion table that was deployed yesterday. dbt gives you this for free in its DAG and `dbt docs`; **OpenLineage** standardizes it across tools so Airflow, dbt, and Spark all emit the same event model into one metadata server (**Marquez**); and catalogs like **DataHub** and **OpenMetadata** add search, ownership, a business glossary, and cross-tool column-level lineage on top. You will walk a real "trace the bad number" incident this week using nothing but the lineage graph, and it will feel less like detective work and more like reading a map.

Governance also means knowing which columns are personal data and treating them differently. **PII classification** is tagging — marking a column as containing personal data so policy can be enforced and audited — and dbt `meta` tags plus catalog tags are where that tagging lives. **Masking** is showing the right people the real value and everyone else a hashed, tokenized, or redacted one, enforced through views, dynamic masking, or column-level encryption. **Access control** narrows it further: Postgres row-level security with `CREATE POLICY` so a regional analyst sees only their region's rows, and column-level security through views so the support team sees a customer's order history but never their full card number. These are not abstractions you will hear about — you will write the policies and prove they hold.

The hardest governance problem in a lakehouse is the one that sounds easiest: delete a user. GDPR Article 17 gives a person the right to have their personal data erased, and you have a legal clock to comply. But the lakehouse was *designed* to be append-only and immutable — Parquet files do not change, and time travel deliberately keeps old snapshots so you can query the past. Deleting a row from the current table does nothing if the old data files and snapshots still hold it. The real answer is a two-step: a logical `DELETE` (Iceberg row-level deletes — copy-on-write vs merge-on-read, position vs equality deletes; Delta `DELETE` writing deletion vectors) followed by *physically purging the old files* (`expire_snapshots`, `VACUUM`) so no time-travel query can resurrect the deleted person. We also teach **crypto-shredding** — encrypt each user's data with a per-user key and throw the key away — as the pragmatic alternative when rewriting petabytes is not viable.

By Friday you will have compacted a real lakehouse and measured the scan-cost drop, re-partitioned it for pruning and proven the pruning fired, exposed end-to-end lineage from a source table to a dashboard metric, classified and masked a PII column behind row- and column-level security, and documented a compliant hard-delete that survives a time-travel audit. That is **Lab 11 — Optimize, prune, and trace**, and it is the last lab before the capstone. Everything you build here goes straight into the capstone's cost-and-governance grading axis.

## Learning objectives

By the end of Week 11 you will be able to:

- **Reason about the cost model of a data platform from first principles** — articulate what a scan costs (bytes read), what a shuffle costs (network and spill), and what storage costs (capacity × tier × time), and locate where each hides in a query you have never seen.
- **Diagnose and fix the small-files problem** — recognize the symptom (file count ≫ data size warrants), compact with Iceberg `rewrite_data_files` or Delta `OPTIMIZE`, and explain why metadata overhead, not bytes, is the cost.
- **Optimize a lakehouse for scan cost** — choose a partition layout and transform (Iceberg hidden partitioning, day/bucket/truncate; Delta partitioning and liquid clustering) that lets the engine prune, and measure the bytes-scanned reduction on a representative query before and after.
- **Measure bytes scanned three ways** — from Iceberg metadata and manifest statistics, from the Spark UI, and from DuckDB `EXPLAIN ANALYZE` — and map each to its cloud-billing analogue without a cloud account.
- **Distinguish table-level from column-level lineage** and explain why lineage is the first tool you reach for during an incident.
- **Expose end-to-end lineage** — generate and serve dbt docs with its DAG and exposures, emit OpenLineage events from Airflow and dbt into Marquez, and read the resulting source-to-dashboard graph.
- **Classify, mask, and access-control PII** — tag a column as personal data, build a masking view (hashing, tokenization, redaction), and enforce row-level and column-level security with Postgres `CREATE POLICY` and view-based projection.
- **Perform a GDPR-compliant hard-delete in an immutable lakehouse** — issue the row-level delete, then physically purge old data files with snapshot expiration / `VACUUM`, and verify a time-travel query can no longer resurrect the deleted person. Explain crypto-shredding as the alternative.

## Prerequisites

This week assumes everything Phase II and the first half of Phase III built. Specifically you must have, working in Docker:

- The **Iceberg (and Delta) lakehouse on MinIO** from Week 6 — partitioned Parquet, an Iceberg catalog, time-travel queries.
- The **dbt project on DuckDB** from Week 5 — staging / intermediate / mart layering, `ref()`/`source()`, generated docs.
- The **Airflow deployment** from Week 4 — a running scheduler and at least one DAG you can attach a plugin to.
- The **Spark Structured Streaming sink** from Week 9 — because the small-files problem is *its* exhaust; that streaming job is what produced the thousands of tiny files you will compact.
- Comfort reading a query plan (Week 2) and the Spark UI (Week 7). We build directly on both.
- Postgres running in Docker (Week 1) — the row- and column-level security examples are taught against it.

If your Week 9 streaming job is not producing small files yet, the exercises include a one-command generator that lands a few thousand tiny Parquet files so the compaction numbers are real.

## Topics covered

- The cost model of a data platform: scan cost (bytes read), shuffle cost (network + spill), storage cost (capacity × tier × time).
- Where cost hides: full scans, oversized and undersized files, unpruned partitions, runaway shuffles, missing predicate pushdown.
- The small-files problem at scale, and why the cost is metadata overhead rather than bytes.
- Compaction: Iceberg `rewrite_data_files`, Delta `OPTIMIZE` bin-packing and Z-ORDER.
- Partitioning for pruning: Iceberg hidden partitioning and partition transforms (`day`, `bucket`, `truncate`); Delta partitioning and liquid clustering.
- Measuring bytes scanned: Iceberg metadata and manifest statistics, the Spark UI, DuckDB `EXPLAIN ANALYZE`.
- Cloud cost analogues named honestly: BigQuery bytes-billed, Snowflake credits, S3 storage classes — and the open-source path that runs on the laptop.
- Table-level vs column-level lineage; why lineage is the first incident tool.
- dbt lineage: the DAG, `dbt docs generate` / `dbt docs serve`, exposures, `manifest.json`.
- The OpenLineage spec (run / job / dataset facets), the Airflow OpenLineage provider, the dbt-OpenLineage integration, and the Marquez metadata server.
- Data catalogs: DataHub and OpenMetadata — search, glossary, ownership, column-level lineage ingestion.
- PII classification (column tagging via dbt `meta` and catalog tags) and masking strategies (hashing, tokenization, deterministic vs non-deterministic, dynamic masking views, column-level encryption).
- Access control: Postgres row-level security (`CREATE POLICY`) and column-level security via views.
- GDPR Article 17 hard-delete in an immutable lakehouse: Iceberg row-level deletes (copy-on-write vs merge-on-read, position vs equality deletes) + `expire_snapshots`; Delta `DELETE` + `VACUUM` + deletion vectors; crypto-shredding as the alternative.

## Weekly schedule

Roughly **36 hours**, mirroring the Phase III cadence. Lectures are the three notes in `lecture-notes/`; exercises and challenges are in their folders; the lab is the mini-project.

| Day | Focus | Lectures | Exercises | Challenges | Quiz | Mini-Project | Self-Study | Daily Total |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Mon | Cost: where it hides and how to kill it | 2.0h | 1.5h | — | — | — | 1.5h | **5.0h** |
| Tue | Compaction & partition pruning (hands-on) | — | 2.5h | 2.0h | — | — | 1.0h | **5.5h** |
| Wed | Lineage, catalogs, and the incident | 2.0h | 1.5h | — | — | — | 1.5h | **5.0h** |
| Thu | PII, masking, access control, GDPR delete | 2.0h | 2.0h | 1.5h | — | — | 1.0h | **6.5h** |
| Fri | Studio: cost-and-lineage review + Lab 11 | — | — | — | — | 4.5h | 1.0h | **5.5h** |
| Sat | Lab 11 build + homework | — | — | — | — | 4.0h | 1.5h | **5.5h** |
| Sun | Quiz + reading + wrap-up | — | — | — | 0.5h | 1.0h | 1.5h | **3.0h** |
| | | **6.0h** | **7.5h** | **5.5h** | **0.5h** | **9.5h** | **9.0h** | **≈36.0h** |

## How to navigate this week

| File | What it is | When to read it |
| --- | --- | --- |
| [`README.md`](./README.md) | This overview | First |
| [`lecture-notes/01-cost-where-it-hides-and-how-to-kill-it.md`](./lecture-notes/01-cost-where-it-hides-and-how-to-kill-it.md) | The cost model, the small-files problem, compaction, partition pruning, measuring bytes scanned | Monday |
| [`lecture-notes/02-lineage-catalogs-and-the-incident.md`](./lecture-notes/02-lineage-catalogs-and-the-incident.md) | Table vs column lineage, dbt docs, OpenLineage + Marquez, DataHub / OpenMetadata, the incident walkthrough | Wednesday |
| [`lecture-notes/03-pii-masking-access-control-and-gdpr-deletion.md`](./lecture-notes/03-pii-masking-access-control-and-gdpr-deletion.md) | PII classification, masking, RLS / column security, GDPR hard-delete in an immutable lakehouse | Thursday |
| [`exercises/exercise-01-compact-and-measure.sql`](./exercises/exercise-01-compact-and-measure.sql) | Compact small files and measure bytes scanned before/after | Tuesday |
| [`exercises/exercise-02-partition-for-pruning.sql`](./exercises/exercise-02-partition-for-pruning.sql) | Re-partition and prove pruning fired | Tuesday |
| [`exercises/exercise-03-mask-pii-view.sql`](./exercises/exercise-03-mask-pii-view.sql) | Build a masking view + a Postgres RLS policy | Thursday |
| [`exercises/exercise-04-gdpr-hard-delete.sql`](./exercises/exercise-04-gdpr-hard-delete.sql) | Row-level delete + snapshot expiration / VACUUM | Thursday |
| [`exercises/SOLUTIONS.md`](./exercises/SOLUTIONS.md) | Worked solutions with verification output | After attempting the exercises |
| [`challenges/challenge-01-cut-the-scan-cost.md`](./challenges/challenge-01-cut-the-scan-cost.md) | Cut a representative query's bytes-scanned with measured before/after | Tuesday → Friday |
| [`challenges/challenge-02-trace-the-bad-number.md`](./challenges/challenge-02-trace-the-bad-number.md) | Trace a wrong dashboard number source-to-dashboard with lineage | Wednesday → Friday |
| [`mini-project/README.md`](./mini-project/README.md) | **Lab 11 — Optimize, prune, and trace.** The graded end-to-end build | Friday / Saturday |
| [`homework.md`](./homework.md) | 5–6 practice problems with deliverables | Throughout the week |
| [`quiz.md`](./quiz.md) | ~10 questions + worked answer key | Sunday |
| [`resources.md`](./resources.md) | Grouped real references with a reading-time budget | As needed |

## Reading order if short on time

If you have only a few hours, do this in order and stop wherever you run out:

1. **`lecture-notes/01`** — the cost model and the small-files / compaction / pruning core. This is the part you will use every week for the rest of your career, and it is the one with no cloud bill to teach it for you.
2. **`mini-project/README.md`** — read the Lab 11 brief end-to-end so you know what you are building toward, even before you do the exercises.
3. **`exercises/exercise-01` and `exercise-02`** — compact and prune with your own hands, then read the matching sections of **`exercises/SOLUTIONS.md`**. Measuring the bytes-scanned drop yourself is the single most valuable hour of the week.
4. **`lecture-notes/02`** (lineage) and **`lecture-notes/03`** (PII + GDPR delete) — read for understanding; the deletion-in-an-immutable-lakehouse section is the one most engineers get wrong in interviews.
5. **`quiz.md`** — use it as a self-check; if you can answer the small-files, partition-pruning, OpenLineage, and immutable-delete questions cold, you have the week.

Week spine and skills earned: [`../../SYLLABUS.md`](../../SYLLABUS.md) (§ Week 11). Track design rationale: [`../../CHARTER.md`](../../CHARTER.md).
