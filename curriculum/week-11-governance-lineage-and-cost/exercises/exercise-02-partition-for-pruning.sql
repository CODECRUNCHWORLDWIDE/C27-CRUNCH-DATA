-- ============================================================================
-- Exercise 02 — Re-partition for pruning, and PROVE pruning fired
-- C27 · Crunch Data · Week 11 — Governance, Lineage and Cost
-- ----------------------------------------------------------------------------
-- GOAL
--   The compacted `local.db.events` table (exercise 01) is right-sized but
--   unpartitioned, so the daily-revenue query still scans the whole table.
--   Re-create it with an Iceberg hidden-partition transform on event_ts, then
--   prove the same query now prunes to a single day.
--
-- KEY IDEA
--   With Iceberg hidden partitioning you partition on a TRANSFORM of the real
--   column (days(event_ts)). The user keeps filtering the natural column and
--   Iceberg prunes automatically — no synthetic partition column to remember.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. CREATE the partitioned table. Choose the transform granularity that
--    matches the query (the query filters a single DAY).
-- ----------------------------------------------------------------------------
CREATE TABLE local.db.events_pruned (
  event_id     BIGINT,
  customer_id  BIGINT,
  event_ts     TIMESTAMP,
  amount       DECIMAL(12,2)
)
USING iceberg
PARTITIONED BY ( <<< time transform on event_ts at DAY granularity >>>,
                 <<< hash high-cardinality customer_id into 16 buckets >>> )
TBLPROPERTIES ('write.target-file-size-bytes' = '536870912');

-- ----------------------------------------------------------------------------
-- 2. LOAD it from the compacted source.
--    Sorting on the partition column before insert produces cleaner files.
-- ----------------------------------------------------------------------------
INSERT INTO local.db.events_pruned
SELECT event_id, customer_id, event_ts, amount
FROM local.db.events
ORDER BY event_ts;   -- helps Iceberg lay rows out per-partition

-- ----------------------------------------------------------------------------
-- 3. INSPECT the partition layout. Iceberg's partitions metadata table shows
--    one row per partition with its file count and record count.
-- ----------------------------------------------------------------------------
SELECT partition, record_count, file_count
FROM local.db.events_pruned.partitions
ORDER BY partition
LIMIT 10;
-- >>> RECORD the total number of partitions (should be ~ one per day).

-- ----------------------------------------------------------------------------
-- 4. PROVE PRUNING — run the SAME daily query against the partitioned table.
--    In the Spark UI SQL tab, the Iceberg scan node now reports:
--      - "number of partitions" vs partitions actually read
--      - far smaller "size of files read"
-- ----------------------------------------------------------------------------
SELECT sum(amount) AS daily_revenue
FROM local.db.events_pruned
WHERE event_ts >= TIMESTAMP '2026-04-15 00:00:00'
  AND event_ts <  TIMESTAMP '2026-04-16 00:00:00';
-- >>> files read = ____   size of files read = ____   partitions pruned = ____
-- >>> COMPARE to exercise 01's "after" numbers. The bytes-scanned drop is the
-- >>> headline metric for challenge 01 and the lab.

-- ----------------------------------------------------------------------------
-- 5. THE FOOTGUN — show that a function-wrapped predicate KILLS pruning.
--    This filters the same single day but wraps the column in date_trunc,
--    which the reader cannot push down, so it scans everything. Confirm in the
--    Spark UI that "size of files read" jumps back up.
-- ----------------------------------------------------------------------------
SELECT sum(amount) AS daily_revenue
FROM local.db.events_pruned
WHERE date_trunc('day', event_ts) = TIMESTAMP '2026-04-15 00:00:00';
-- >>> EXPLAIN: why does pruning NOT fire here? Write one sentence. Then rewrite
-- >>> it as a half-open range (>= ... AND < ...) and confirm pruning returns.

-- ----------------------------------------------------------------------------
-- 6. DuckDB cross-check (single-node) — read the same Iceberg table and confirm
--    row groups / files skipped in the plan:
--      EXPLAIN ANALYZE
--      SELECT sum(amount) FROM iceberg_scan('s3://lake/db/events_pruned')
--      WHERE event_ts >= TIMESTAMP '2026-04-15' AND event_ts < TIMESTAMP '2026-04-16';
-- ----------------------------------------------------------------------------

-- DELTA EQUIVALENT
--   CREATE TABLE db.events_pruned ( ... , event_date DATE
--     GENERATED ALWAYS AS (CAST(event_ts AS DATE)) ) USING delta
--     PARTITIONED BY (event_date);            -- classic
--   -- OR, preferred for new tables:
--   CREATE TABLE db.events_pruned ( ... ) USING delta
--     CLUSTER BY (customer_id, event_date);    -- liquid clustering

-- DELIVERABLE: before/after bytes-scanned for the daily query, the partition
-- count, and the one-sentence explanation of the date_trunc footgun.
