-- ============================================================================
-- Exercise 03 — Mask a PII column and lock it down with RLS
-- C27 · Crunch Data · Week 11 — Governance, Lineage and Cost
-- ----------------------------------------------------------------------------
-- GOAL
--   In Postgres (the Week 1 warehouse box), take a customers table that holds
--   PII, and build:
--     (a) a masking VIEW that reveals email/name to a privileged role and a
--         DETERMINISTIC hash / redaction to everyone else, and
--     (b) a row-level-security policy so a regional analyst sees only their
--         region's rows.
--   Then verify both from two different roles.
--
-- RUN CONTEXT: psql against the Postgres container. Requires the pgcrypto
-- extension for digest(). Fill in every  <<< ... >>>  blank.
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ----------------------------------------------------------------------------
-- 0. Seed table (skip if you already have customers).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customers (
  customer_id  BIGINT PRIMARY KEY,
  full_name    TEXT NOT NULL,
  email        TEXT NOT NULL,
  region       TEXT NOT NULL,           -- 'EU', 'US', 'APAC'
  card_number  TEXT
);
INSERT INTO customers VALUES
  (1,'Ada Lovelace','ada@example.com','EU','4111111111111111'),
  (2,'Grace Hopper','grace@example.com','US','5500005555555559'),
  (3,'Kanade Sato','kanade@example.com','APAC','340000000000009')
ON CONFLICT DO NOTHING;

-- ----------------------------------------------------------------------------
-- 1. Roles. pii_reader may see real PII; analyst_eu may not, and is scoped to EU.
-- ----------------------------------------------------------------------------
CREATE ROLE pii_reader  NOLOGIN;
CREATE ROLE analyst_eu  NOLOGIN;

-- A server-side secret for deterministic, salted hashing. In production this
-- comes from a secret store, not a literal. Set it per session for the exercise:
SET app.pii_secret = '<<< choose a non-empty secret string >>>';

-- ----------------------------------------------------------------------------
-- 2. Deterministic masking function: same input -> same hash, so analysts can
--    still GROUP BY / join on the masked identity without seeing the value.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mask_email(addr TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE AS $$
  SELECT encode(
           digest( addr || <<< concatenate the app.pii_secret here via current_setting >>> ,
                   'sha256'),
           'hex')
$$;

-- ----------------------------------------------------------------------------
-- 3. The masking VIEW. pii_reader sees the real value; everyone else the mask.
--    Use pg_has_role(current_user, 'pii_reader', 'MEMBER') to branch.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW customers_secure AS
SELECT
  customer_id,
  region,
  CASE WHEN <<< true when current_user is a member of pii_reader >>>
       THEN email ELSE mask_email(email) END                       AS email,
  CASE WHEN pg_has_role(current_user, 'pii_reader', 'MEMBER')
       THEN full_name ELSE '***' END                               AS full_name,
  -- Column-level: never expose the full PAN. Show last 4 only.
  '****-****-****-' || right(card_number, 4)                       AS card_last4
FROM customers;

-- Lock the base table; grant only the view.
REVOKE ALL ON customers FROM PUBLIC;
GRANT SELECT ON customers_secure TO pii_reader, analyst_eu;

-- ----------------------------------------------------------------------------
-- 4. Row-level security on the base table so the view itself is row-scoped.
--    A row is visible only when its region matches the session's region setting;
--    pii_reader (a global role here) sees all.
-- ----------------------------------------------------------------------------
ALTER TABLE customers ENABLE ROW LEVEL SECURITY;
ALTER TABLE customers <<< force RLS so even the owner is subject to it >>>;

CREATE POLICY customers_region_isolation ON customers
  FOR SELECT
  USING ( region = current_setting('app.current_region', true)
          OR pg_has_role(current_user, 'pii_reader', 'MEMBER') );

-- ----------------------------------------------------------------------------
-- 5. VERIFY.
-- ----------------------------------------------------------------------------
-- (a) As an analyst scoped to EU: should see ONLY region='EU', email hashed.
SET ROLE analyst_eu;
SET app.current_region = 'EU';
SELECT * FROM customers_secure;          -- expect 1 row, hashed email, name '***'
RESET ROLE;

-- (b) As pii_reader: should see ALL rows with REAL email and name.
SET ROLE pii_reader;
SELECT * FROM customers_secure;          -- expect 3 rows, real email/name
RESET ROLE;

-- (c) Prove determinism: the same email always hashes the same way.
SELECT mask_email('ada@example.com') = mask_email('ada@example.com') AS deterministic;

-- DELIVERABLE: the two role outputs (analyst_eu vs pii_reader), proving rows are
-- scoped by region AND email/name are masked for the non-privileged role, with
-- card_number never exposed beyond last 4. See SOLUTIONS.md.
