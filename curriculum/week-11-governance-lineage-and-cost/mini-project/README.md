# Lab 11 — Optimize, Prune, and Trace

> The last lab before the capstone. Take the lakehouse you have been building since Week 6 and make it cheap, traceable, and compliant: compact it, prune it, prove the scan-cost drop, expose end-to-end lineage from source to dashboard, mask a PII column behind access control, and document a GDPR hard-delete that survives a time-travel audit.

This is the governance-and-cost capstone-feeder. Everything you produce here maps directly onto the capstone's *Data quality, lineage & cost discipline* grading axis. It is one continuous build over Friday's studio and Saturday, on the Iceberg/Delta lakehouse on MinIO, the dbt project on DuckDB, the Airflow deployment, and Postgres — all already running in Docker.

## What you are building

Four deliverables, one platform:

1. **A cost optimization** — compact the small files and re-partition for pruning, with a measured before/after bytes-scanned reduction on a representative query.
2. **End-to-end lineage** — source → staging → mart → dashboard, exposed via dbt docs (with an exposure) and emitted through OpenLineage into Marquez so the graph is one continuous map.
3. **PII governance** — one PII column classified, masked, and locked behind row- and column-level access control.
4. **A compliant hard-delete** — a documented GDPR Article 17 erasure on the lakehouse table that no time-travel query can resurrect.

## Architecture and lineage map

```text
  SOURCES                  INGEST / TRANSFORM             LAKEHOUSE (MinIO)            CONSUME
  =======                  ==================             =================            =======

  raw.orders ────┐                                    ┌─ Iceberg/Delta tables ─┐
                 │     Airflow DAG (OpenLineage)       │  raw → staged → mart    │
  raw.fx_rates ──┤────► Spark / dbt-ol ───────────────►│  PARTITIONED for pruning│──► dbt mart
                 │       │                             │  COMPACTED small files  │     (DuckDB)
  raw.customers ─┘       │ emits run/job/dataset       └──────────┬──────────────┘        │
   (has PII)             │  + columnLineage facets                │                       ▼
                         ▼                                        │              exec_revenue_dashboard
                   ┌──────────────┐                               │                (dbt EXPOSURE)
                   │   MARQUEZ     │◄──── OpenLineage events ──────┘                       │
                   │ (lineage UI)  │◄──── dbt docs DAG + exposures ────────────────────────┘
                   └──────────────┘

  GOVERNANCE OVERLAY (applies across the stack)
  ---------------------------------------------
   PII column (e.g. customers.email)
     ├─ classified:   dbt meta {classification: pii} + catalog tag
     ├─ masked:       deterministic salted hash via a base-locked masking VIEW
     ├─ row-scoped:   Postgres RLS  CREATE POLICY (region isolation)
     └─ col-scoped:   projecting view (card_last4 only)

   GDPR Art. 17 hard-delete on the lakehouse table
     Step 1  DELETE FROM ... WHERE customer_id = X        (logical, COW or MOR)
     Step 2  expire_snapshots + remove_orphan_files       (Iceberg)  /  VACUUM (Delta)
     Verify  time-travel to the pre-delete snapshot now FAILS
```

## Build steps

### Part A — Optimize (cost)

1. Identify the small-files table (your Week 9 streaming sink, or generate one) and freeze a representative date-filtered query.
2. Capture baseline scan metrics (Spark UI: size of files read, files read, partitions pruned) and table metadata (file count, avg size).
3. Compact with `rewrite_data_files` (Iceberg) or `OPTIMIZE` (Delta); record file-count collapse.
4. Re-create the table with a partition layout aligned to the query (`days(event_ts)` + `bucket(N, customer_id)`, or Delta liquid clustering).
5. Re-measure. Produce the before/after table and screenshots. Target **≥10× bytes-scanned reduction**.

### Part B — Lineage

6. Add a dbt `exposure` for the dashboard so the DAG reaches the consumer; `dbt docs generate` and `dbt docs serve` and confirm the source-to-dashboard graph renders.
7. Stand up Marquez (`./docker/up.sh`), run dbt via `dbt-ol run` into it, and (if ingesting via Airflow) enable `apache-airflow-providers-openlineage`. Confirm the continuous graph and at least one `columnLineage` facet on a metric column.
8. Optionally ingest the dbt project into DataHub or OpenMetadata and screenshot the column-level lineage there.

### Part C — PII governance

9. Classify one PII column with dbt `meta` tags and (if running a catalog) a catalog tag.
10. Build the masking view (deterministic, secret-salted hash; base table locked) and the access controls: Postgres RLS `CREATE POLICY` for rows, a projecting view for columns. Verify from two roles.

### Part D — Compliant deletion

11. Pick a customer and perform the two-step hard-delete on the lakehouse table: logical `DELETE`, then physical purge (`expire_snapshots` + `remove_orphan_files`, or `VACUUM RETAIN 0 HOURS`).
12. Document the runbook with the four proof lines: delete → time-travel still finds them → purge → time-travel now fails.

## Deliverables

Submit a `lab-11/` directory containing:

1. `cost/` — the frozen query, the before/after metrics table, and the two Spark UI screenshots (baseline + final pruned).
2. `lineage/` — the dbt `exposures.yml`, a `dbt docs` DAG screenshot reaching the dashboard, and a Marquez (and/or catalog) screenshot showing a `columnLineage` edge into a metric.
3. `governance/` — the masking + RLS SQL, and the two-role verification output proving row-scoped, masked, column-limited access.
4. `deletion/` — the hard-delete runbook with the four proof lines and the time-travel-fails evidence.
5. `README.md` — a one-page summary tying the four together, including the headline bytes-scanned reduction and one paragraph on why `DELETE` alone is not GDPR erasure.

## Grading rubric (100 points)

| Area | Criteria | Points |
| --- | --- | --- |
| Cost optimization | Compaction + partitioning applied; **≥10×** bytes-scanned reduction proven with before/after screenshots; latency vs bytes contributions correctly attributed | 25 |
| Lineage | dbt exposure + docs reach the dashboard; OpenLineage events in Marquez form a continuous source-to-dashboard graph; at least one column-level edge shown | 25 |
| PII governance | One column classified; deterministic masking via a base-locked view; working RLS policy + column-limiting view; two-role verification | 20 |
| Compliant deletion | Two-step delete documented; time-travel proven to find the user *before* purge and **fail after**; correct reasoning on immutability | 20 |
| Communication | The summary README correctly explains the four results and the immutable-delete subtlety | 10 |

## Pass criteria

To pass Lab 11 you must hit **70/100 overall** AND satisfy both non-negotiables:

- The bytes-scanned reduction is **measured with before/after evidence** (not asserted), and `partitions pruned > 0` on the final query.
- The GDPR delete is proven **physically complete** — a time-travel query that found the user before the purge fails after it. A logical `DELETE` alone does not pass.

## References

- [`../README.md`](../README.md) — week overview and schedule.
- [`../lecture-notes/01-cost-where-it-hides-and-how-to-kill-it.md`](../lecture-notes/01-cost-where-it-hides-and-how-to-kill-it.md), [`02-lineage-catalogs-and-the-incident.md`](../lecture-notes/02-lineage-catalogs-and-the-incident.md), [`03-pii-masking-access-control-and-gdpr-deletion.md`](../lecture-notes/03-pii-masking-access-control-and-gdpr-deletion.md).
- [`../exercises/SOLUTIONS.md`](../exercises/SOLUTIONS.md) — the mechanics for all four parts.
- Apache Iceberg <https://iceberg.apache.org/docs/latest/> · Delta Lake <https://docs.delta.io/latest/index.html> · OpenLineage <https://openlineage.io/docs/> · Marquez <https://marquezproject.ai/> · dbt docs/exposures <https://docs.getdbt.com/docs/build/documentation> · Postgres RLS <https://www.postgresql.org/docs/current/ddl-rowsecurity.html> · GDPR Art. 17 <https://gdpr-info.eu/art-17-gdpr/>.
- Track spine: [`../../../SYLLABUS.md`](../../../SYLLABUS.md) (§ Week 11, Lab 11).
