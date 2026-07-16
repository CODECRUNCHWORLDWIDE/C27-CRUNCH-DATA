-- ============================================================================
-- Exercise 04 — GDPR hard-delete in an immutable lakehouse
-- C27 · Crunch Data · Week 11 — Governance, Lineage and Cost
-- ----------------------------------------------------------------------------
-- GOAL
--   A user (customer_id = 2) has exercised their GDPR Art. 17 right to erasure.
--   Delete them from the Iceberg lakehouse COMPLETELY: not just a logical delete
--   (which leaves the old data files reachable by time travel) but a physical
--   purge that no time-travel query can resurrect.
--
-- THE TWO STEPS (this is the whole point):
--   Step 1  logical row-level DELETE
--   Step 2  PHYSICAL purge: expire old snapshots + remove orphan files
--   Verify  a time-travel query can no longer reach the deleted data
--
-- RUN CONTEXT: spark-sql with the Iceberg `local` catalog on MinIO. Delta
-- equivalents at the bottom. Fill in every  <<< ... >>>  blank.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0. Confirm the target exists and capture the CURRENT snapshot id (you will
--    try to time-travel back to it after purging, and it must FAIL).
-- ----------------------------------------------------------------------------
SELECT customer_id, full_name FROM local.db.customers WHERE customer_id = 2;

SELECT snapshot_id, committed_at
FROM local.db.customers.snapshots
ORDER BY committed_at DESC LIMIT 1;
-- >>> RECORD this snapshot_id as PRE_DELETE_SNAPSHOT.

-- ----------------------------------------------------------------------------
-- 1. STEP 1 — logical delete. (Optionally set the delete mode first.)
--    copy-on-write rewrites the affected files now; merge-on-read writes a
--    delete file and applies it at read time. For a one-off erasure, COW keeps
--    the result clean.
-- ----------------------------------------------------------------------------
ALTER TABLE local.db.customers
  SET TBLPROPERTIES ('write.delete.mode' = '<<< copy-on-write or merge-on-read >>>');

DELETE FROM local.db.customers WHERE customer_id = 2;

-- The CURRENT table no longer shows the user...
SELECT count(*) AS still_present FROM local.db.customers WHERE customer_id = 2;
-- >>> EXPECT 0.

-- ...BUT the data is NOT gone yet. Prove it: time-travel to PRE_DELETE_SNAPSHOT
-- and the user reappears, because the old snapshot still points at the old file.
SELECT customer_id, full_name
FROM local.db.customers VERSION AS OF <<< PRE_DELETE_SNAPSHOT id >>>
WHERE customer_id = 2;
-- >>> EXPECT 1 row. THIS is why DELETE alone is NOT GDPR compliance.

-- ----------------------------------------------------------------------------
-- 2. STEP 2 — physically purge. Expire all snapshots older than now so the
--    pre-delete snapshot (and its data file) becomes unreferenced, then remove
--    the now-orphaned data files from object storage.
-- ----------------------------------------------------------------------------
CALL local.system.expire_snapshots(
  table       => 'db.customers',
  older_than  => <<< a timestamp at or after "now", e.g. current_timestamp() >>>,
  retain_last => <<< keep only the newest N snapshots — choose the minimum >>>
);

CALL local.system.remove_orphan_files(
  table       => 'db.customers',
  older_than  => current_timestamp()
);

-- ----------------------------------------------------------------------------
-- 3. VERIFY ERASURE — the time-travel query that worked in step 1 must now FAIL
--    (the snapshot is expired) or return nothing. This is the compliance proof.
-- ----------------------------------------------------------------------------
-- This should now ERROR with "snapshot ... is not known" / cannot find snapshot:
SELECT count(*)
FROM local.db.customers VERSION AS OF <<< PRE_DELETE_SNAPSHOT id >>>;

-- And no data file should remain that contains the user. Confirm file set
-- changed and the current scan is clean:
SELECT count(*) AS remaining_files FROM local.db.customers.files;
SELECT count(*) AS still_present  FROM local.db.customers WHERE customer_id = 2;
-- >>> EXPECT still_present = 0 AND the pre-delete snapshot unreachable.

-- ----------------------------------------------------------------------------
-- DELTA EQUIVALENT
--   DELETE FROM db.customers WHERE customer_id = 2;        -- step 1 (logical)
--   -- step 2: VACUUM physically removes files older than retention. For an
--   -- immediate compliant purge you must override the safety check:
--   SET spark.databricks.delta.retentionDurationCheck.enabled = false;
--   VACUUM db.customers RETAIN 0 HOURS;
--   -- afterwards: SELECT * FROM db.customers VERSION AS OF <pre_delete> should fail.
-- ----------------------------------------------------------------------------

-- DELIVERABLE: a short runbook showing (1) the delete, (2) the time-travel that
-- STILL found the user before purge, (3) the expire_snapshots/VACUUM, and
-- (4) the same time-travel now failing — the four lines that prove compliant
-- erasure. See SOLUTIONS.md.
