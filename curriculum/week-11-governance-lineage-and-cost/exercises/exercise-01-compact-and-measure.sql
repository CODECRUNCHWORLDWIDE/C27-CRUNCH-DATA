-- ============================================================================
-- Exercise 01 — Compact small files and measure bytes scanned (before/after)
-- C27 · Crunch Data · Week 11 — Governance, Lineage and Cost
-- ----------------------------------------------------------------------------
-- GOAL
--   Your Week 9 Structured Streaming sink (or the generator below) has produced
--   an Iceberg table `local.db.events` made of thousands of tiny Parquet files.
--   Compact it, and PROVE the improvement by measuring file count and the
--   bytes a representative query touches, before and after.
--
-- RUN CONTEXT
--   spark-sql / pyspark with the Iceberg + Hadoop catalog `local` pointed at
--   MinIO, exactly as configured in Weeks 6 and 9. Delta equivalents are noted
--   at the bottom. Fill in every  <<< ... >>>  blank.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0. (Optional) Generate small files if your stream has not yet.
--    From a shell, NOT this file:
--      spark-sql -e "
--        SET spark.sql.shuffle.partitions=2000;
--        INSERT INTO local.db.events
--        SELECT id AS event_id, id % 5000 AS customer_id,
--               TIMESTAMP '2026-04-01 00:00:00' + make_interval(0,0,0,0,0,0, id) AS event_ts,
--               CAST(rand()*100 AS DECIMAL(12,2)) AS amount
--        FROM range(0, 5000000);"
--    The high shuffle-partition count deliberately fragments the write.
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- 1. BASELINE — how many files, how big, how many rows?
--    Iceberg exposes its bookkeeping as metadata tables. Read them.
-- ----------------------------------------------------------------------------
SELECT
  count(*)                          AS file_count,
  sum(file_size_in_bytes)           AS total_bytes,
  round(avg(file_size_in_bytes)/1024.0, 1) AS avg_file_kb,
  sum(record_count)                 AS total_rows
FROM local.db.events.files;
-- >>> RECORD the baseline file_count and avg_file_kb. A healthy avg is
-- >>> hundreds of MB; if you see double-digit KB you have the small-files problem.

-- ----------------------------------------------------------------------------
-- 2. BASELINE SCAN — run the representative query under EXPLAIN and note the
--    files-read / bytes-read the scan reports. (Open the Spark UI :4040 SQL tab
--    and read "size of files read" / "number of files read" for the scan node.)
-- ----------------------------------------------------------------------------
EXPLAIN FORMATTED
SELECT sum(amount) AS daily_revenue
FROM local.db.events
WHERE event_ts >= TIMESTAMP '2026-04-15 00:00:00'
  AND event_ts <  TIMESTAMP '2026-04-16 00:00:00';
-- >>> RUN the query (without EXPLAIN) and capture the Spark UI scan metrics:
-- >>>   files read = ____   size of files read = ____

-- ----------------------------------------------------------------------------
-- 3. COMPACT — bin-pack the small files into ~512 MB targets.
--    Complete the procedure call.
-- ----------------------------------------------------------------------------
CALL local.system.rewrite_data_files(
  table    => 'db.events',
  strategy => 'binpack',
  options  => map(
    'target-file-size-bytes', '<<< 512 MB in bytes >>>',
    'min-input-files',         '<<< only rewrite groups of at least N small files >>>',
    'min-file-size-bytes',     '<<< treat files smaller than this as "small" >>>'
  )
);
-- >>> The result row reports rewritten_data_files_count and added_data_files_count.
-- >>> RECORD both: you should see thousands rewritten into a handful added.

-- ----------------------------------------------------------------------------
-- 4. AFTER — re-read the metadata. File count should collapse.
-- ----------------------------------------------------------------------------
SELECT count(*) AS file_count,
       round(avg(file_size_in_bytes)/1024.0/1024.0, 1) AS avg_file_mb
FROM local.db.events.files;

-- Confirm a new snapshot was created by compaction (operation = 'replace'):
SELECT snapshot_id, operation,
       summary['added-data-files']   AS added,
       summary['deleted-data-files'] AS deleted
FROM local.db.events.snapshots
ORDER BY committed_at DESC
LIMIT 3;

-- ----------------------------------------------------------------------------
-- 5. AFTER SCAN — re-run the same query, re-read the Spark UI scan metrics.
--    Compaction alone does NOT reduce bytes scanned (no pruning yet) but file
--    count and planning time collapse. Pruning is exercise 02.
-- ----------------------------------------------------------------------------
SELECT sum(amount) AS daily_revenue
FROM local.db.events
WHERE event_ts >= TIMESTAMP '2026-04-15 00:00:00'
  AND event_ts <  TIMESTAMP '2026-04-16 00:00:00';
-- >>> files read after = ____   size of files read after = ____
-- >>> EXPECTED: files read drops dramatically; bytes ~unchanged (set up exercise 02).

-- ----------------------------------------------------------------------------
-- DELTA EQUIVALENT (if you built the Delta variant in Week 6)
--   DESCRIBE DETAIL db.events;            -- numFiles, sizeInBytes (baseline)
--   OPTIMIZE db.events;                   -- returns numFilesAdded / numFilesRemoved
--   DESCRIBE DETAIL db.events;            -- numFiles (after)
-- ----------------------------------------------------------------------------

-- DELIVERABLE: a before/after table of {file_count, avg_file_size, files_read,
-- size_of_files_read} with the Spark UI screenshots. See SOLUTIONS.md.
