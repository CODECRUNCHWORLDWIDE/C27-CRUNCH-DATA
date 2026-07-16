# Week 11 — Exercise Solutions

Worked solutions for the five exercises. Read these *after* you have attempted the starters in this folder. Every solution shows the completed code, the verification output you should see, and the one or two sentences of reasoning that matter. Numbers are representative of a ~2 GB / 5M-row synthetic `events` table on a laptop; yours will differ in absolute size but not in the *ratio*, which is the point.

Exercises referenced:

- [`exercise-01-compact-and-measure.sql`](./exercise-01-compact-and-measure.sql)
- [`exercise-02-partition-for-pruning.sql`](./exercise-02-partition-for-pruning.sql)
- [`exercise-03-mask-pii-view.sql`](./exercise-03-mask-pii-view.sql)
- [`exercise-04-gdpr-hard-delete.sql`](./exercise-04-gdpr-hard-delete.sql)

---

## Exercise 01 — Compact small files and measure

**Completed compaction call:**

```sql
CALL local.system.rewrite_data_files(
  table    => 'db.events',
  strategy => 'binpack',
  options  => map(
    'target-file-size-bytes', '536870912',   -- 512 MB
    'min-input-files',         '5',
    'min-file-size-bytes',     '134217728'    -- 128 MB
  )
);
```

**Baseline metadata (step 1):**

```text
 file_count | total_bytes  | avg_file_kb | total_rows
------------+--------------+-------------+-----------
      18342 |  2106373120  |       112.1 |   5000000
```

Eighteen thousand files averaging ~112 KB — the small-files problem, textbook.

**Compaction result row:**

```text
 rewritten_data_files_count | added_data_files_count | rewritten_bytes_count
----------------------------+------------------------+----------------------
                      18342 |                      4 |            2106373120
```

**After metadata (step 4):**

```text
 file_count | avg_file_mb
------------+-------------
          4 |       502.3
```

```text
 snapshot_id          | operation | added | deleted
----------------------+-----------+-------+--------
 6914...               | replace   | 4     | 18342
```

**Scan metrics (Spark UI, steps 2 and 5), same daily query:**

| Metric | Before compaction | After compaction |
| --- | --- | --- |
| number of files read | 18,342 | 4 |
| size of files read | ~2.0 GB | ~2.0 GB |
| query wall-clock | ~41 s | ~3 s |

**Reasoning.** Compaction collapsed 18,342 files into 4 and cut wall-clock ~13× — but *bytes scanned barely moved*, because the table is still unpartitioned and the daily query still has to read every file to find the matching rows. Compaction buys **latency and planning cost** (fewer file opens, a tiny manifest); it does **not** buy bytes-scanned. That is exercise 02's job. This is the most common misconception: people compact, see the speedup, and think they have solved cost — they have solved overhead, not scan.

---

## Exercise 02 — Re-partition for pruning

**Completed table DDL and load:**

```sql
CREATE TABLE local.db.events_pruned (
  event_id BIGINT, customer_id BIGINT, event_ts TIMESTAMP, amount DECIMAL(12,2)
)
USING iceberg
PARTITIONED BY (days(event_ts), bucket(16, customer_id))
TBLPROPERTIES ('write.target-file-size-bytes' = '536870912');

INSERT INTO local.db.events_pruned
SELECT event_id, customer_id, event_ts, amount FROM local.db.events
ORDER BY event_ts;
```

**Partition inspection (step 3):** the synthetic data spans ~58 days, so:

```text
 partition                      | record_count | file_count
--------------------------------+--------------+-----------
 {2026-04-01, 0}                |        5388  |          1
 {2026-04-01, 1}                |        5402  |          1
 ...
(928 partitions = 58 days x 16 buckets)
```

**Scan metrics for the daily query (step 4):**

| Metric | events (unpartitioned, compacted) | events_pruned |
| --- | --- | --- |
| size of files read | ~2.0 GB | ~34 MB |
| number of files read | 4 | 16 (one day × 16 buckets) |
| partitions pruned | 0 | 912 of 928 |

**Bytes scanned dropped from ~2.0 GB to ~34 MB — a ~60× reduction.** That ratio is exactly what a metered engine would have billed: the same query that "cost" 2 GB now costs 34 MB.

**Step 5 — the footgun.** With `WHERE date_trunc('day', event_ts) = TIMESTAMP '2026-04-15'`, the Spark UI shows *size of files read ≈ 2.0 GB again, partitions pruned = 0*. One-sentence explanation: **`date_trunc(...)` wraps the partition column in a function the scan cannot push down, so Iceberg cannot map the predicate to partition values and reads every partition.** The fix is the half-open range from step 4, which the reader pushes down and which prunes correctly. *Always write predicates against the raw column, never a function of it.*

**DuckDB cross-check** shows the same story in its plan:

```text
ICEBERG_SCAN  events_pruned
  Filters: event_ts >= '2026-04-15' AND event_ts < '2026-04-16'
  Files scanned: 16 / 928     Row groups scanned: 16 / 928
```

---

## Exercise 03 — Mask a PII column and lock it down with RLS

**Completed masking function and view branch:**

```sql
CREATE OR REPLACE FUNCTION mask_email(addr TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE AS $$
  SELECT encode(digest(addr || current_setting('app.pii_secret'), 'sha256'), 'hex')
$$;

-- in the view:
CASE WHEN pg_has_role(current_user, 'pii_reader', 'MEMBER')
     THEN email ELSE mask_email(email) END AS email,
```

**Completed RLS:**

```sql
ALTER TABLE customers ENABLE ROW LEVEL SECURITY;
ALTER TABLE customers FORCE ROW LEVEL SECURITY;
```

**Verification (a) — analyst_eu, scoped to EU:**

```text
 customer_id | region |                            email                             | full_name |    card_last4
-------------+--------+--------------------------------------------------------------+-----------+------------------
           1 | EU     | 6b8...e2a (sha256 hex, deterministic)                        | ***       | ****-****-****-1111
(1 row)
```

Only the EU row is returned (RLS), the email is a deterministic hash and the name is redacted (masking view), and the card is reduced to last-4 (column-level). The analyst can still `GROUP BY email` to count distinct customers without ever seeing one.

**Verification (b) — pii_reader:**

```text
 customer_id | region |       email         |   full_name   |    card_last4
-------------+--------+---------------------+---------------+------------------
           1 | EU     | ada@example.com     | Ada Lovelace  | ****-****-****-1111
           2 | US     | grace@example.com   | Grace Hopper  | ****-****-****-5559
           3 | APAC   | kanade@example.com  | Kanade Sato   | ****-****-****-0009
(3 rows)
```

All rows (RLS bypass via the `pg_has_role` clause), real email and name. Note the card is *still* last-4 only — masking the PAN is unconditional here because nobody in this scenario needs the full number; PII reveal and PAN reveal are independent decisions.

**Verification (c) — determinism:**

```text
 deterministic
---------------
 t
```

The same email hashes identically, which is what lets analysts join/group on the masked identity. If you had used a random per-row salt this would be `f` and analytics on the column would be impossible — the deterministic-vs-non-deterministic trade in lecture 3, made concrete.

---

## Exercise 04 — GDPR hard-delete

**Completed two-step (copy-on-write chosen for a clean one-off erasure):**

```sql
ALTER TABLE local.db.customers SET TBLPROPERTIES ('write.delete.mode' = 'copy-on-write');
DELETE FROM local.db.customers WHERE customer_id = 2;

CALL local.system.expire_snapshots(
  table => 'db.customers', older_than => current_timestamp(), retain_last => 1);
CALL local.system.remove_orphan_files(
  table => 'db.customers', older_than => current_timestamp());
```

**The four lines that prove compliance:**

```text
-- after DELETE, current table:
 still_present
---------------
             0          <-- gone from current view

-- BEFORE purge, time-travel to the pre-delete snapshot:
SELECT * FROM local.db.customers VERSION AS OF 4471...0913 WHERE customer_id = 2;
 customer_id | full_name
-------------+--------------
           2 | Grace Hopper   <-- STILL THERE. DELETE alone is NOT erasure.

-- AFTER expire_snapshots, the same time-travel query:
SELECT count(*) FROM local.db.customers VERSION AS OF 4471...0913;
ERROR: Cannot find snapshot with ID 4471...0913   <-- unreachable. Purged.

-- current table, post-purge:
 still_present
---------------
             0
```

**Reasoning.** The middle two lines are the whole lesson. After `DELETE`, the *current* table is clean but the *old snapshot* still references the *old data file*, which still contains Grace Hopper's row in plaintext on MinIO — a time-travel query (or anyone reading the file directly) resurrects her, and that is a compliance failure. `expire_snapshots` removes the snapshot pointer and `remove_orphan_files` deletes the now-unreferenced Parquet file from object storage. Only after step 2 is the erasure real, proven by the time-travel query now erroring. The Delta path is identical in shape: `DELETE` then `VACUUM RETAIN 0 HOURS` (with the retention safety check disabled), after which the pre-delete version is no longer reachable.

If rewriting were infeasible (petabyte tables, deletes arriving constantly), the answer is **crypto-shredding** instead: each customer's PII is encrypted with a per-customer key from day one, and "deletion" destroys the key — the ciphertext stays in the immutable files but is permanently unreadable. That avoids the rewrite entirely at the cost of building per-user encryption up front.

---

## What "done" looks like

You have completed the exercise set when you can show, end to end:

1. A before/after table proving file count collapsed under compaction (exercise 01).
2. A before/after table proving bytes-scanned dropped ≥10× under partition pruning, plus the date_trunc footgun and its fix (exercise 02).
3. Two role outputs proving row-scoped, masked PII access with the PAN never fully exposed (exercise 03).
4. The four-line runbook proving compliant erasure survives a time-travel audit (exercise 04).

Those four artifacts are exactly what the mini-project (Lab 11) asks you to assemble into one deliverable.
