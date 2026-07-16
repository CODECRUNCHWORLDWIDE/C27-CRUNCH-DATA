# Week 1 — Quiz

Ten multiple-choice questions covering OLTP vs OLAP, fact-table grain, star vs snowflake schemas, slowly-changing-dimension types, surrogate keys, conformed dimensions, and the warehouse/lake/lakehouse lineage. Treat it as a closed-book check; the answer key with reasoning and citations is at the bottom.

## Question 1 — OLTP vs OLAP

Which statement best captures the difference between an OLTP system and an OLAP system?

- (A) OLTP is for SQL databases and OLAP is for NoSQL databases.
- (B) OLTP handles many small concurrent reads/writes against a normalized store; OLAP handles few large aggregating reads against a denormalized, often columnar store.
- (C) OLTP is always faster than OLAP because it uses indexes.
- (D) OLAP systems cannot use SQL; they require a special query language.

## Question 2 — Grain

You are designing `fact_sales` for a retailer. Which grain statement is the *strongest* choice and correctly stated?

- (A) "One row per order and per shipment."
- (B) "One row per day."
- (C) "One row per product sold on one sales-order line."
- (D) "One row per customer."

## Question 3 — Additivity

At the sales-line grain, which measure is **non-additive** (meaningless to `SUM` across any dimension)?

- (A) `quantity`
- (B) `extended_amount`
- (C) `unit_price`
- (D) `line_count`

## Question 4 — Star vs snowflake

What is the defining difference between a star schema and a snowflake schema?

- (A) A star has one fact table; a snowflake has many fact tables.
- (B) In a star, dimensions are denormalized (a hierarchy stored as text columns in one table); in a snowflake, dimension hierarchies are normalized into separate joined sub-dimension tables.
- (C) A snowflake stores data as columns and a star stores data as rows.
- (D) A star uses surrogate keys and a snowflake uses natural keys.

## Question 5 — SCD types

A product is re-categorized and you must be able to answer "what category was this product in *at the time of each past sale*." Which SCD type do you need?

- (A) Type 0 — retain the original value.
- (B) Type 1 — overwrite the old value.
- (C) Type 2 — add a new effective-dated row.
- (D) Type 3 — add a prior-value column.

## Question 6 — Type-2 mechanics

In a Type-2 SCD using half-open intervals `[valid_from, valid_to)`, a product changes category on 2026-06-19. Which point-in-time predicate correctly returns exactly one version for any given date `D`?

- (A) `D >= valid_from AND D <= valid_to`
- (B) `D >= valid_from AND D < valid_to`
- (C) `D > valid_from AND D <= valid_to`
- (D) `D BETWEEN valid_from AND valid_to`

## Question 7 — Surrogate keys

Why does a Kimball dimension use a warehouse-generated surrogate key instead of the source's natural key as its primary key?

- (A) Natural keys are always slower because they are always strings.
- (B) It is a legal requirement for data warehouses.
- (C) It decouples the warehouse from the source, joins faster as a narrow integer, and — critically — lets one natural entity have multiple dimension rows over time (enabling Type-2 SCDs).
- (D) Surrogate keys are required by the SQL standard for any table.

## Question 8 — Degenerate dimension

In `fact_sales`, the `order_number` is stored directly on the fact row with no `dim_order` table. What is this called?

- (A) A conformed dimension.
- (B) A degenerate dimension.
- (C) A role-playing dimension.
- (D) A factless fact table.

## Question 9 — Conformed dimensions

`fact_sales` (line grain) and `fact_inventory_snapshot` (product-store-day grain) both reference the *same* `dim_product` and `dim_store`. What does this conformance enable, and what must you be careful about when querying across both?

- (A) It enables drilling across both facts via the shared keys; because the facts are at different grains, you must pre-aggregate each to a common grain before joining, or a direct join fans out and double-counts.
- (B) It enables joining the two facts directly on their keys with no risk.
- (C) It means the two facts must always be loaded by the same pipeline.
- (D) It means `units_on_hand` can be summed across days safely.

## Question 10 — The lakehouse lineage

Which statement correctly places the lakehouse in the warehouse → lake → lakehouse lineage?

- (A) The lakehouse replaced the warehouse by removing SQL and dimensional modeling entirely.
- (B) The lakehouse keeps the data lake's cheap object storage and open columnar files but adds ACID transactions, schema enforcement, and table semantics (via formats like Iceberg or Delta) that the bare lake lacked.
- (C) The lakehouse is just a renamed data warehouse with no new properties.
- (D) The lakehouse predates the data warehouse historically.

---

## Answer key

1. **(B)** — OLTP and OLAP are defined by opposite access patterns: many small writes against a normalized store vs few huge aggregating reads against a denormalized (and at scale, columnar) store. Both use SQL; the split is about workload shape, not language or storage engine family. (Lecture 1 § 3; Kleppmann <https://dataintensive.net/>.)
2. **(C)** — A grain must be one sentence with no "and" and should be the finest level the source supports; the line grain is atomic and admits the most dimensions. (A) is two grains; (B) and (D) are coarser grains that cannot answer product-level questions. (Lecture 2 § 3; Kimball techniques <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/>.)
3. **(C)** — `unit_price` is a rate; summing it across rows produces a meaningless number. Store it for display and re-derive averages from additive components (`extended_amount`, `quantity`). `quantity` and `extended_amount` are additive across every dimension. (Lecture 2 § 3.)
4. **(B)** — The defining difference is dimension normalization: the star denormalizes hierarchies into wide single-hop dimension tables; the snowflake normalizes them into separate joined sub-dimensions. Both have one fact table per process and both use surrogate keys. (Lecture 2 § 7; Kimball techniques.)
5. **(C)** — Only Type 2 keeps every historical version as a separate effective-dated row, so a fact stores the surrogate key current at the time of the sale and the join yields point-in-time correctness. Type 1 overwrites and destroys the answer. (Lecture 3 §§ 2, 5.)
6. **(B)** — With a half-open interval `[valid_from, valid_to)` you test `D >= valid_from AND D < valid_to`; the old row's `valid_to` equals the new row's `valid_from`, so `<` ensures the changeover date matches only the new row. `<=` or `BETWEEN` (inclusive) would match both versions on the boundary date. (Lecture 3 §§ 5, 7; `MERGE` <https://www.postgresql.org/docs/16/sql-merge.html>.)
7. **(C)** — Surrogate keys decouple the warehouse from source identifiers, join faster as narrow integers, and — the load-bearing reason — let one natural key map to many dimension rows over time, which is what makes Type-2 SCDs possible. (Lecture 2 § 5; identity columns <https://www.postgresql.org/docs/16/ddl-identity-columns.html>.)
8. **(B)** — A degenerate dimension is a meaningful key kept on the fact with no dimension table of its own (the order number groups lines but has no descriptive attributes). (Lecture 2 § 4; Kimball techniques.)
9. **(A)** — Conformed dimensions let you drill across facts loaded by separate pipelines via shared keys; but because the two facts are at different grains, you must aggregate each to a common grain *before* joining, or a direct join multiplies rows and corrupts both measures. `units_on_hand` is semi-additive and must not be summed over time. (Lecture 2 § 4; Challenge 2; Kimball techniques.)
10. **(B)** — The lakehouse synthesizes the lake (cheap object storage, open columnar files) with the warehouse's guarantees (ACID, schema enforcement, time travel) via table formats like Iceberg and Delta. Dimensional modeling and SQL survive into it. (Lecture 1 § 5; Kleppmann <https://dataintensive.net/>.)

---

## Self-assessment

Score one point per correct answer.

- **9–10 — Solid.** You own the vocabulary and the mechanics. You can declare a grain, defend a star, and reason about Type-2 history. Move into the mini-project with confidence and reach for the stretch goals.
- **7–8 — Nearly there.** Re-read the lecture section cited next to any question you missed; the gaps are usually grain additivity (Q3) or the half-open interval convention (Q6), both of which bite in the mini-project. Patch them before Friday.
- **5–6 — Re-study.** Re-read Lecture 2 (grain, star/snowflake) and Lecture 3 (SCD types and mechanics) and re-run `exercises/exercise-03-type2-scd.sql` against `SOLUTIONS.md` until the close-then-open transaction and the audit query are second nature.
- **Below 5 — Reset.** Start the week's lectures over from Lecture 1, do the three exercises with the solutions open beside you, and retake the quiz. This is the foundation week; every later week assumes grain, surrogate keys, and SCD-2 cold. Time spent here is repaid tenfold in Weeks 3–5.
