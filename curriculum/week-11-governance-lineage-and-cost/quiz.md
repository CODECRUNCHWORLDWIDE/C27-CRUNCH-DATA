# Week 11 Quiz — Governance, Lineage and Cost

Ten questions on the cost model, the small-files problem, compaction, partition pruning, lineage, the OpenLineage event model, PII masking, row-level security, and compliant deletion in an immutable lakehouse. Aim for ~30 minutes. Worked answers follow in **## Answers** — do not scroll until you have committed to your own.

---

### Q1. The cost model

A query returns a single integer but the metered engine reports it scanned 38 GB. Which of the three cost categories is being billed, and what does that number represent physically?

### Q2. The small-files problem

A table is logically 2 GB but lives in 18,000 files of ~110 KB each and queries are slow out of proportion to its size. Is the dominant cost the *bytes* or something else? Name the specific overheads, and name the operation that fixes it without changing the table's logical contents.

### Q3. Compaction vs pruning

After running `rewrite_data_files` (binpack), file count drops from 18,000 to 4 and the query is 13× faster — but the Spark UI shows *size of files read* is essentially unchanged. Explain why, and state what you must do additionally to reduce bytes scanned.

### Q4. Partition pruning fires — or doesn't

You partition an Iceberg table by `days(event_ts)`. Two queries filter the same single day:

```sql
-- A
WHERE event_ts >= TIMESTAMP '2026-06-01' AND event_ts < TIMESTAMP '2026-06-02'
-- B
WHERE date_trunc('day', event_ts) = TIMESTAMP '2026-06-01'
```

One prunes to one partition; one scans the whole table. Which is which, and why?

### Q5. Over-partitioning

A teammate partitions a table that ingests ~20 MB/day by `hours(event_ts)`. Why is this a mistake, and what failure mode does it (re)introduce?

### Q6. Table vs column lineage in an incident

A dashboard revenue number is wrong. You have both table-level and column-level lineage. What does each give you during the incident, and which one actually closes the investigation?

### Q7. The OpenLineage event model

Name the three core entities in the OpenLineage event model and what each represents. Which *facet* carries column-level lineage, and on which entity does it appear?

### Q8. Deterministic vs non-deterministic masking

An analyst needs to count distinct customers and join on customer identity but must never see an email address. You can hash `email` with a per-row random salt or with a fixed server-side secret. Which do you choose and why? What is the residual risk of your choice and how do you mitigate it?

### Q9. Row-level security

In Postgres, you write `CREATE POLICY ... USING (region = current_setting('app.current_region', true))` on `orders` and `ENABLE ROW LEVEL SECURITY`, but the table owner still sees every row. What did you forget, and what is the one statement that fixes it?

### Q10. GDPR delete in an immutable lakehouse

A teammate runs `DELETE FROM db.customers WHERE customer_id = 42` on an Iceberg table and declares the GDPR erasure complete. They are wrong. Explain precisely why, give the two-step sequence that *is* complete, and name the single query that proves it.

---

## Answers

### A1.
**Scan cost** — bytes read off storage. The 38 GB is the number of bytes the engine physically read from files to satisfy the query, regardless of the one-row result. It is large because the query did a full scan (no partition pruning / no predicate pushdown), so it read every byte of every file even though it needed almost none of them. This is exactly what BigQuery bills as *bytes billed* and what drives Snowflake warehouse time.

### A2.
The dominant cost is **not the bytes** — 2 GB is small. It is **per-file and metadata overhead**: 18,000 file opens / object-store `GET` requests (each with latency), 18,000 manifest/transaction-log entries the planner must read *before the query starts*, and 18,000 tiny tasks whose scheduling overhead dwarfs their work. The fix is **compaction** — Iceberg `rewrite_data_files` (binpack) or Delta `OPTIMIZE` — which rewrites the tiny files into a few right-sized ones, leaving the logical contents identical.

### A3.
Compaction reduced **file count and overhead** (fewer opens, a tiny manifest, less planning), which is where the 13× latency win came from — but the table is still **unpartitioned**, so the query still has to read every file to find matching rows. Bytes scanned is a function of *layout relevance*, not file size. To reduce bytes scanned you must **partition for pruning** (Iceberg hidden partitioning on the filtered column, or Delta partitioning / liquid clustering) so the engine can skip the partitions that cannot match.

### A4.
**A prunes; B scans everything.** A filters the raw `event_ts` column with a half-open range the reader can push down and map onto the `days(event_ts)` partition values, so Iceberg prunes to one partition. B wraps the column in `date_trunc(...)`, a function the scan cannot push down or relate to the partition transform, so Iceberg cannot prune and reads every partition. Rule: filter the raw column with comparisons, never a function of it.

### A5.
20 MB/day split into 24 hourly partitions is ~830 KB per partition — far below any sensible target file size. It **reintroduces the small-files problem** (a huge number of tiny partitions, each one or more tiny files), inflates metadata and planning, and gains nothing because no query needs hour granularity on a 20 MB/day table. Partition at the coarsest granularity that still prunes for your queries, with partitions at least a target-file-size each — here `days` or even `months` is right.

### A6.
**Table-level lineage** gives you the *blast radius* — which datasets feed the dashboard — narrowing the search from the whole warehouse to a handful of upstream models. **Column-level lineage** gives you the *root-cause path* — that the wrong metric is computed from specific columns (`amount_usd` ← `fx_rates.rate`). Column-level closes the investigation, because it points at the one or two leaf columns to inspect rather than every model in the blast radius.

### A7.
The three core entities are **Job** (a process definition — a dbt model, an Airflow task, a Spark app; stable across executions), **Run** (one execution of a Job, with a UUID and a state like `START`/`COMPLETE`/`FAIL`), and **Dataset** (an input or output, identified by `namespace` + `name`). Column-level lineage is carried by the **`columnLineage` facet**, which appears on an **output Dataset** and maps each output field to the input fields it derives from.

### A8.
Choose the **fixed server-side secret (deterministic hash)**: the same email always hashes to the same value, so the analyst can `GROUP BY` and join on the masked identity and still count distinct customers — a per-row random salt would make every occurrence of the same email different and destroy those analytics. The residual risk is a **dictionary / rainbow-table attack** on low-cardinality or guessable values; mitigate it by salting with a **secret** (not a public constant), keeping that secret in a secret store, and rotating it if compromised.

### A9.
You forgot **`FORCE ROW LEVEL SECURITY`**. `ENABLE ROW LEVEL SECURITY` applies policies to ordinary users but, by default, the **table owner (and superusers) bypass RLS**. The fix is one statement: `ALTER TABLE orders FORCE ROW LEVEL SECURITY;`, which subjects the owner to the policies as well.

### A10.
`DELETE` alone removes the row from the **current** table, but the **old snapshot still references the old data file**, which still contains customer 42's data in plaintext on object storage — a time-travel query (`VERSION AS OF <old snapshot>`) resurrects them, so it is not erasure. The complete sequence is two steps: **(1)** `DELETE FROM db.customers WHERE customer_id = 42;` (logical), then **(2)** `CALL local.system.expire_snapshots(table => 'db.customers', older_than => current_timestamp(), retain_last => 1);` followed by `CALL local.system.remove_orphan_files(...)` to physically delete the now-unreferenced files (Delta: `VACUUM ... RETAIN 0 HOURS`). The proof is a **time-travel query to the pre-delete snapshot** — `SELECT count(*) FROM db.customers VERSION AS OF <pre_delete_snapshot>;` — which must now **fail** (snapshot expired) rather than return the deleted row.
