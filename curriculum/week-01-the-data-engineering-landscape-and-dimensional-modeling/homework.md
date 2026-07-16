# Week 1 — Homework

Six practice problems, roughly **45 minutes each** (~4.5 hours total). They reinforce the lectures from a different angle than the exercises and challenges: where the exercises hand you scaffolding, the homework asks you to produce a complete artifact from a prompt. Each problem names the **deliverable filename** you submit and the **citations** you should ground your answer in. Everything runs against `postgres:16` in Docker (<https://hub.docker.com/_/postgres>).

Submit all six in a `week-01-homework/` directory in your cohort repo.

---

## HW1 — The on-call boundary memo (~45 min)

**Deliverable:** `hw1-on-call.md`

Write a one-page memo, in your own words, that a new hire could read to understand who owns what on a data platform. Cover: the data engineer, the analyst, the data scientist, and the backend engineer; what *artifact* each owns; and one concrete failure that pages each of them. Then describe the five "quiet failures" only the data engineer sees (double-count, silent staleness, schema drift, grain mismatch, late record) and, for each, name the later C27 week that defends against it (use the track `SYLLABUS.md`). Ground the systems-of-record-vs-derived-data framing in Kleppmann (<https://dataintensive.net/>).

**Acceptance:** four roles, four pagers, five quiet failures each mapped to a defense, one citation.

---

## HW2 — Model the same data twice (~45 min)

**Deliverable:** `hw2-oltp-vs-olap.sql`

Pick a business domain that is *not* retail — choose one of: a library (loans), a gym (check-ins), or a clinic (appointments). Write (a) a normalized OLTP schema (≥4 tables, third-normal-form, foreign keys) that the operational app would write into, and (b) a denormalized OLAP star (one fact + ≥2 dimensions) that an analyst would query. In a comment, state one analytical question that is a painful multi-join against the OLTP schema and a single fact-to-dimension join against the star. Cite the PostgreSQL `CREATE TABLE` docs (<https://www.postgresql.org/docs/16/sql-createtable.html>).

**Acceptance:** both schemas run without error; the contrast question is stated; the star uses a surrogate key.

---

## HW3 — Declare and defend three grains (~45 min)

**Deliverable:** `hw3-grains.md`

For three different business processes — (1) a ride-hailing trip, (2) a streaming-service play event, (3) a bank account's monthly statement — write the grain of the fact table in one sentence each (no "and"), list the dimensions and the facts, and classify each fact as additive, semi-additive, or non-additive. For at least one, name a coarser grain you *rejected* and the question it could not answer. Cite the Kimball Group techniques page (<https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/>).

**Acceptance:** three grain sentences with no "and"; every fact classified by additivity; one rejected coarser grain explained.

---

## HW4 — Star vs snowflake, on paper and in SQL (~45 min)

**Deliverable:** `hw4-star-vs-snowflake.sql`

Take a `dim_employee` dimension with a hierarchy (employee → department → division → company). Write it once as a wide star dimension and once as a snowflake (normalized into `dim_department`, `dim_division`). For each, write the SQL to answer "headcount by division". In a comment, state which you would ship and the *one condition* under which you would change your mind (size of the dimension, hierarchy-integrity requirement). Cite the Kimball techniques page and the PostgreSQL `CREATE TABLE` docs.

**Acceptance:** both shapes run; both headcount queries return the same numbers; the ship decision is defended in one paragraph.

---

## HW5 — Implement and audit a Type-2 SCD (~45 min)

**Deliverable:** `hw5-scd2.sql`

Build a `dim_employee` Type-2 dimension tracking an employee's `department` over time. Use `GENERATED ALWAYS AS IDENTITY` for the surrogate key and `valid_from`/`valid_to`/`is_current` (half-open intervals, `9999-12-31` sentinel). Seed two employees, then process a staged department change for one of them with the close-then-open transaction (`MERGE` to close, `INSERT` to open). Finish with (a) a point-in-time audit query for a date before the change and one after, and (b) the well-formedness check. Cite the PostgreSQL `MERGE` (<https://www.postgresql.org/docs/16/sql-merge.html>) and identity-column (<https://www.postgresql.org/docs/16/ddl-identity-columns.html>) docs and the Kimball SCD section.

**Acceptance:** the changed employee has two rows (one closed, one open with a fresh key); the two audit queries return different departments; the well-formedness check returns zero rows.

---

## HW6 — Conformed dimension design (~45 min)

**Deliverable:** `hw6-conformed.md`

Design (on paper / in markdown, with `CREATE TABLE` stubs) a small two-fact warehouse for a *subscription business*: `fact_subscription_event` (one row per sign-up / cancellation event) and `fact_revenue_snapshot` (one row per customer per month, with monthly recurring revenue as a semi-additive measure). Identify which dimensions conform across both facts (`dim_date`, `dim_customer`, `dim_plan`), draw a 2×3 enterprise-bus-matrix (processes × conformed dimensions), and write — in words — the drill-across question the conformance enables and why the two facts must be pre-aggregated to a common grain before joining. Cite the Kimball techniques page (conformed dimensions / bus matrix).

**Acceptance:** two facts at clearly different grains; the bus matrix is drawn; the semi-additive measure is identified as not-summable-over-time; the drill-across question is stated.

---

## Submission

Put `hw1-on-call.md`, `hw2-oltp-vs-olap.sql`, `hw3-grains.md`, `hw4-star-vs-snowflake.sql`, `hw5-scd2.sql`, and `hw6-conformed.md` in `week-01-homework/` in your cohort repo. Every `.sql` file must run clean against `postgres:16` with `\i <file>` from a fresh database. Every `.md` file must carry at least one real citation. A TA will run the SQL and spot-check the writeups.
