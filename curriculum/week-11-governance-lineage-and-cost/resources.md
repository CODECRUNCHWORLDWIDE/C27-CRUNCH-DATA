# Week 11 — Resources

Curated, real references for governance, lineage, and cost. Every link is to primary documentation or the canonical book. Grouped by topic; a reading-time budget is at the bottom. You do not need to read all of it — see the budget table for the minimum.

---

## Cost, compaction, and partitioning

- **Apache Iceberg — Documentation (latest).** The spec, partitioning and hidden partitioning, partition transforms (`days`/`bucket`/`truncate`), metadata tables, and maintenance. <https://iceberg.apache.org/docs/latest/>
- **Apache Iceberg — Maintenance.** `rewrite_data_files` (compaction / binpack / sort), `rewrite_manifests`, `expire_snapshots`, `remove_orphan_files`. <https://iceberg.apache.org/docs/latest/maintenance/>
- **Apache Iceberg — Spark Procedures.** The `CALL` syntax for all maintenance procedures used in the exercises. <https://iceberg.apache.org/docs/latest/spark-procedures/>
- **Apache Iceberg — Partitioning.** Why hidden partitioning exists, and partition evolution. <https://iceberg.apache.org/docs/latest/partitioning/>
- **Delta Lake — Documentation (latest).** <https://docs.delta.io/latest/index.html>
- **Delta Lake — Optimizations (OSS).** `OPTIMIZE` bin-packing, Z-ORDER, file compaction, data skipping. <https://docs.delta.io/latest/optimizations-oss.html>
- **Delta Lake — Best practices.** File sizing, partitioning vs liquid clustering. <https://docs.delta.io/latest/best-practices.html>
- **DuckDB — `EXPLAIN` / `EXPLAIN ANALYZE`.** Reading the plan and the scan's pushdown / files-skipped reporting. <https://duckdb.org/docs/sql/statements/explain.html>

## Lineage and catalogs

- **OpenLineage — Documentation.** The standard, the run/job/dataset model, the producer integrations. <https://openlineage.io/docs/>
- **OpenLineage — Facets.** `schema`, `columnLineage`, `dataQuality`, `sql`, and how facets attach to runs/jobs/datasets. <https://openlineage.io/docs/spec/facets/>
- **Marquez — Documentation.** The reference OpenLineage metadata server and UI; quickstart and API. <https://marquezproject.ai/docs>
- **Marquez — Source / quickstart.** `docker/up.sh`, ports, and the seed data. <https://github.com/MarquezProject/marquez>
- **Airflow — OpenLineage provider.** Auto-instrumenting DAGs with `apache-airflow-providers-openlineage`. <https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/index.html>
- **dbt — Documentation (docs, the DAG).** `dbt docs generate` / `serve`, `manifest.json`, `catalog.json`. <https://docs.getdbt.com/docs/build/documentation>
- **dbt — Exposures.** Declaring downstream consumers (dashboards) as DAG nodes. <https://docs.getdbt.com/docs/build/exposures>
- **DataHub — Documentation.** Ingestion, column-level lineage, glossary, the OpenLineage endpoint. <https://datahubproject.io/docs/>
- **OpenMetadata — Documentation.** Connectors, lineage, glossary, classification, data quality. <https://docs.open-metadata.org/>

## PII, masking, access control, and deletion

- **PostgreSQL — Row Security Policies.** `CREATE POLICY`, `ENABLE` / `FORCE ROW LEVEL SECURITY`, `USING` and `WITH CHECK`. <https://www.postgresql.org/docs/current/ddl-rowsecurity.html>
- **PostgreSQL — `pgcrypto`.** `digest`, `encode`, `crypt` for hashing and column-level encryption. <https://www.postgresql.org/docs/current/pgcrypto.html>
- **Apache Iceberg — Spark Writes.** Row-level `DELETE`/`UPDATE`/`MERGE`, copy-on-write vs merge-on-read, position vs equality deletes. <https://iceberg.apache.org/docs/latest/spark-writes/>
- **Delta Lake — Table utility commands.** `DELETE`, `VACUUM`, retention, deletion vectors. <https://docs.delta.io/latest/delta-utility.html>
- **GDPR — Article 17 (Right to erasure / "right to be forgotten").** The legal text the deletion lab implements. <https://gdpr-info.eu/art-17-gdpr/>

## The book

- **Joe Reis & Matt Housley, _Fundamentals of Data Engineering_, O'Reilly, 2022.** ISBN 978-1-098-10830-4. Ch. 6 (storage / cost), Ch. 8 (queries and compute cost), Ch. 9 (metadata, lineage, data management), Ch. 10 (security, privacy, governance). <https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/>

---

## Reading-time budget

| Priority | Resource | Time | Why |
| --- | --- | --- | --- |
| **Must** | Iceberg — Maintenance + Partitioning | 40 min | Compaction and pruning are the lab's core; you run these procedures |
| **Must** | OpenLineage — Documentation (model + facets) | 35 min | The event model and `columnLineage` facet drive the lineage lab |
| **Must** | GDPR Article 17 | 10 min | Short; it is the obligation the deletion lab satisfies |
| **Must** | Postgres — Row Security Policies | 25 min | The RLS exercise depends on `CREATE POLICY` + `FORCE` |
| **Should** | Delta — Optimizations (OPTIMIZE / Z-ORDER) | 25 min | The Delta path for compaction + clustering |
| **Should** | dbt — docs + exposures | 20 min | How lineage reaches the dashboard |
| **Should** | Marquez — quickstart | 20 min | Standing up the lineage server for the lab/challenge |
| **Should** | _Fundamentals of Data Engineering_, Ch. 6 & 9 | 90 min | The cost and metadata/lineage mental models |
| **Optional** | DataHub or OpenMetadata docs (pick one) | 45 min | Catalog column-level lineage beyond Marquez |
| **Optional** | DuckDB — EXPLAIN ANALYZE | 15 min | A second, single-node way to measure pruning |
| **Optional** | Iceberg — Spark Procedures + Writes | 30 min | Reference for the exact `CALL`/`DELETE` syntax |

**Minimum viable path (~110 min):** the four **Must** rows. They cover compaction, pruning, the lineage event model, deletion, and access control — enough to do the exercises and pass the quiz. Add the **Should** rows before the lab.

Week spine and skills earned: [`../../SYLLABUS.md`](../../SYLLABUS.md) (§ Week 11).
