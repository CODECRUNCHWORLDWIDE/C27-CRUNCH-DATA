# Lecture 3 — Slowly-Changing Dimensions and Surrogate Keys: Type 0/1/2/3, Effective Dating, the SCD-2 MERGE, and the Point-in-Time Audit

> **Time:** 2 hours. Take Types 0/1/2/3 and the effective-dating model first; take the `MERGE` and the audit query in a second sitting at the keyboard. **Prerequisites:** Lecture 2 (the retail star, surrogate keys, grain). The `postgres:16` container from Lecture 1 § 4 with the star from Lecture 2 § 6 loaded. **Citations:** Kimball & Ross, *The Data Warehouse Toolkit*, 3rd ed. <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/>; Kimball Group "Dimensional Modeling Techniques" (the SCD section) <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/>; PostgreSQL 16 identity columns <https://www.postgresql.org/docs/16/ddl-identity-columns.html>, `MERGE` <https://www.postgresql.org/docs/16/sql-merge.html>, and generated columns <https://www.postgresql.org/docs/16/ddl-generated-columns.html>.

## 1. The problem: dimensions change, and history has opinions

A fact is a frozen measurement — order line #3 sold 2 units for $40 on June 19, and that is true forever. A *dimension attribute*, by contrast, drifts: a customer moves from Miami to Austin, a product is re-categorized from "Snacks" to "Healthy Snacks", a store is reassigned from the "South" region to a new "Southeast" region. The question dimensional modeling forces you to answer explicitly — and the question that separates a data engineer from someone who just writes `UPDATE` — is: **when a dimension attribute changes, what should happen to history?**

There is no single right answer. There is a *decision*, and Kimball numbered the choices so teams could communicate them in one token. "We'll track product category as a Type 2" is a complete, unambiguous instruction. This lecture walks Types 0, 1, 2, and 3, with runnable SQL for each, and then goes deep on Type 2 — the effective-dated row — because it is the technique that makes the warehouse able to answer "what was true *at the time of the fact*", which is the whole reason analytical history is worth keeping. The canonical reference is the SCD section of the Kimball Group techniques page (<https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/>).

## 2. The four types at a glance

```text
              An attribute changes: "Snacks" -> "Healthy Snacks"

TYPE 0  Retain original.   Never changes. (date_of_birth, original_signup_date)
        row stays:  category_name = 'Snacks'  forever, by policy.

TYPE 1  Overwrite.         No history. Dashboard always shows the latest.
        UPDATE row: category_name := 'Healthy Snacks'.  Old value is gone.

TYPE 2  Add a new row.     FULL history. Old row closed, new row opened.
        old row: valid_to=2026-06-18, is_current=false
        NEW row: valid_from=2026-06-19, is_current=true  (new surrogate key)

TYPE 3  Add a column.      ONE prior value kept side by side.
        row: current_category='Healthy Snacks', prior_category='Snacks'
```

| Type | What it does to history | Surrogate keys per natural key | Use when |
|---|---|---|---|
| **0** | Keeps the original value forever | One | The attribute is immutable by policy (birth date, original source system) |
| **1** | Overwrites; no history | One | You only ever care about the *current* value (a corrected typo, a current email) |
| **2** | Adds a new effective-dated row | **Many over time** | You must reconstruct "what was true at the time of the fact" — the default for meaningful attributes |
| **3** | Adds a "previous value" column | One | You need exactly the *current* and *one prior* state, not full history (a re-org with an "old region" view) |

## 3. Type 1 — overwrite (the baseline you must understand to reject)

Type 1 is what a naive `UPDATE` does: the new value replaces the old, and the old value ceases to exist. There is exactly one row per natural key, and it always reflects the current truth.

```sql
-- Type 1: overwrite. dim_customer has ONE row per customer_code.
UPDATE dim_customer
SET    city = 'Austin'
WHERE  customer_code = 'CUST-1007';
-- The fact that this customer used to live in Miami is now unrecoverable.
```

Type 1 is correct when history genuinely does not matter — fixing a misspelled name, refreshing a phone number. It is *wrong* the moment someone asks "how much revenue came from Miami customers last year", because every customer who has since moved now looks like they were always in their current city. That question requires Type 2.

## 4. Type 3 — the prior-value column

Type 3 keeps the current value *and* one previous value, side by side, in two columns. It answers "show me sales under both the old and the new category scheme" without storing full history.

```sql
ALTER TABLE dim_product ADD COLUMN prior_category_name text;

-- On a re-categorization, shift current -> prior, set the new current:
UPDATE dim_product
SET    prior_category_name = category_name,   -- remember the old
       category_name       = 'Healthy Snacks' -- set the new
WHERE  sku = 'SKU-0042';
```

Type 3 is rare and specific: it fits a one-time, organization-wide re-mapping (a fiscal-calendar change, a regional re-org) where analysts want to compare "old way vs new way" but do not need every intermediate state. It cannot represent a *second* change without losing the first. For anything that changes repeatedly and where each historical state matters, you need Type 2.

## 4a. Type 0 — retain the original, by policy

Type 0 is the deliberate decision to *never* change an attribute once it is set, even if the source sends a different value later. It is not laziness; it is policy. A customer's original signup date, a product's original launch SKU, the source system a record was first ingested from — these are facts about the entity's *origin*, and overwriting them would destroy meaning. The enforcement is procedural (the loader simply does not touch Type-0 columns) and can be backed by a trigger or a `CHECK` if you want the database to refuse changes:

```sql
ALTER TABLE dim_customer ADD COLUMN original_signup_date date NOT NULL;
-- Type 0: the loader's UPDATE statements never list original_signup_date.
-- It is set once, at first insert, and read forever.
```

A single dimension routinely mixes SCD types: `dim_customer` might hold `original_signup_date` (Type 0), `email` (Type 1 — always show the current, history irrelevant), and `loyalty_tier` (Type 2 — you must know the tier at the time of each sale to analyze tier-driven revenue). Deciding the type *per attribute* is part of the modeling work, and a good `MODEL.md` documents the choice for every column that can change. The mini-project's stretch goal asks you to do exactly this and defend it.

## 5. Type 2 — the effective-dated row (the centerpiece)

Type 2 keeps **every** version of a dimension row as a separate physical row, distinguished by a fresh surrogate key and bounded by effective dates. This is the technique that makes the surrogate key from Lecture 2 indispensable: a single natural key (`sku`) now maps to *several* rows (`product_key` 7, 88, 134), one per era of that product's life.

Add the Type-2 control columns to `dim_product`:

```sql
ALTER TABLE dim_product
    ADD COLUMN valid_from date    NOT NULL DEFAULT DATE '0001-01-01',
    ADD COLUMN valid_to   date    NOT NULL DEFAULT DATE '9999-12-31',
    ADD COLUMN is_current boolean NOT NULL DEFAULT true;
```

Three columns do all the work:

- **`valid_from`** — the date this version became effective (inclusive).
- **`valid_to`** — the date this version stopped being effective (we use an *exclusive* upper bound; a far-future sentinel `9999-12-31` means "still open").
- **`is_current`** — a convenience flag, `true` for exactly one row per natural key. It is redundant with `valid_to = '9999-12-31'` but makes the common "give me the current version" query trivial and indexable.

A product that started as "Snacks" and was re-categorized to "Healthy Snacks" on 2026-06-19 has two rows:

```text
product_key | sku      | category_name    | valid_from | valid_to   | is_current
------------+----------+------------------+------------+------------+-----------
        7   | SKU-0042 | Snacks           | 0001-01-01 | 2026-06-19 | false
      134   | SKU-0042 | Healthy Snacks   | 2026-06-19 | 9999-12-31 | true
```

The interval convention is **half-open**: a version covers `[valid_from, valid_to)`. The old row's `valid_to` equals the new row's `valid_from` (both `2026-06-19`), so there is exactly one row valid on any given date and no gaps and no overlaps. Get this boundary convention right and the audit query in Section 7 is trivial; get it wrong (inclusive `valid_to`) and the changeover date matches two rows.

**The crucial payoff:** `fact_sales` stores the `product_key` *that was current on the date of the sale*. A sale on 2026-06-10 stored `product_key = 7` ("Snacks"); a sale on 2026-06-20 stored `product_key = 134` ("Healthy Snacks"). Join the fact to the dimension on the surrogate key and every sale automatically shows the category *as it was at the time of the sale* — point-in-time correctness, for free, forever. A Type-1 overwrite throws this away; Type 2 is how you keep it.

## 6. The SCD-2 maintenance: close the old, open the new, in one MERGE

When a new version of a product arrives, two things must happen atomically: the currently-open row must be *closed* (`valid_to` set, `is_current` cleared) and a *new* row opened. PostgreSQL 16's `MERGE` (<https://www.postgresql.org/docs/16/sql-merge.html>) expresses the close-half cleanly; the open-half is an `INSERT`. The standard, robust pattern stages the incoming changed rows and runs two statements in one transaction.

Suppose a staging table holds the latest source state of each product:

```sql
CREATE TABLE stg_product (
    sku           text NOT NULL,
    product_name  text NOT NULL,
    category_name text NOT NULL,
    brand_name    text NOT NULL,
    effective_date date NOT NULL          -- the date the new version takes effect
);
```

**Step 1 — close every current row whose attributes actually changed**, using `MERGE`:

```sql
MERGE INTO dim_product d
USING stg_product s
   ON  d.sku = s.sku
   AND d.is_current = true
   -- only act when a tracked attribute actually differs:
   AND (d.category_name IS DISTINCT FROM s.category_name
        OR d.brand_name  IS DISTINCT FROM s.brand_name
        OR d.product_name IS DISTINCT FROM s.product_name)
WHEN MATCHED THEN
   UPDATE SET valid_to   = s.effective_date,   -- close the old version
              is_current = false;
```

`IS DISTINCT FROM` is the null-safe inequality you want here — it treats two nulls as equal and a null-vs-value as different, so you do not spuriously close a row over a null. Note we only close rows that *changed*; an unchanged product is left alone (no needless version churn).

**Step 2 — open a new current row for each product whose version we just closed (or which is brand new):**

```sql
INSERT INTO dim_product (sku, product_name, category_name, brand_name,
                         valid_from, valid_to, is_current)
SELECT s.sku, s.product_name, s.category_name, s.brand_name,
       s.effective_date, DATE '9999-12-31', true
FROM   stg_product s
WHERE  NOT EXISTS (                       -- no open row matches the new state
          SELECT 1 FROM dim_product d
          WHERE  d.sku = s.sku
            AND  d.is_current = true
            AND  d.category_name = s.category_name
            AND  d.brand_name    = s.brand_name
            AND  d.product_name  = s.product_name
       );
```

Run both inside one transaction so the close and the open are atomic — never expose a moment with zero current rows or two:

```sql
BEGIN;
  -- Step 1 MERGE (close changed current rows)
  -- Step 2 INSERT (open new current rows)
COMMIT;
```

The new row gets a fresh `product_key` from the identity sequence automatically — you never assign it, which is the entire point of the surrogate key being warehouse-generated. After this runs, the natural key `SKU-0042` has two rows, exactly as the table in Section 5 shows, and every future fact load for that product will look up and store the *current* `product_key`, while old facts keep pointing at the old one.

> **Why not a single `MERGE` with both branches?** PostgreSQL 16's `MERGE` can `UPDATE`, `INSERT`, or `DELETE`, but a single `MERGE` cannot both *close an existing row* and *insert a different new row* for the same matched source row in one pass — the matched branch acts on the matched target. The close-then-insert pair in one transaction is the clearest correct expression and is what the exercises and solutions use. (`MERGE` semantics: <https://www.postgresql.org/docs/16/sql-merge.html>.)

## 7. The point-in-time audit — the single query

Here is the payoff and the C27 "audit promise": reconstruct any product's state *as it stood on any past date* with one `SELECT`, no application code. Because the versions form a gap-free, overlap-free half-open timeline, the predicate is exactly one comparison per bound:

```sql
-- What did every product look like on 2026-06-15?
SELECT sku, product_name, category_name, brand_name
FROM   dim_product
WHERE  DATE '2026-06-15' >= valid_from
  AND  DATE '2026-06-15' <  valid_to;      -- half-open: < not <=
```

Change the literal date and you time-travel the dimension to any point in its history. To audit one product's *entire* lifecycle in order:

```sql
SELECT product_key, category_name, valid_from, valid_to, is_current
FROM   dim_product
WHERE  sku = 'SKU-0042'
ORDER  BY valid_from;
```

And to prove a Type-2 dimension is *well-formed* — exactly one current row per natural key, no overlapping intervals — you write a check the mini-project's verification section requires:

```sql
-- Must return ZERO rows: any natural key with != 1 current version is a bug.
SELECT sku, COUNT(*) AS current_versions
FROM   dim_product
WHERE  is_current = true
GROUP  BY sku
HAVING COUNT(*) <> 1;
```

If that query returns rows, your SCD-2 maintenance opened two current rows or failed to close one — the most common Type-2 bug, and exactly why the verification query exists.

## 8. Surrogate-key generation strategies

The surrogate keys that make all of this work need a generation strategy. In Postgres you have three, in descending order of preference for a warehouse:

1. **`GENERATED ALWAYS AS IDENTITY`** (preferred; <https://www.postgresql.org/docs/16/ddl-identity-columns.html>). Standard SQL, backed by an implicit sequence, and `ALWAYS` forbids callers from supplying their own value — exactly the guarantee you want for a surrogate key the warehouse owns. Use `GENERATED BY DEFAULT AS IDENTITY` only if a bulk loader must supply explicit keys (e.g. restoring a dump).
2. **An explicit `SEQUENCE`** (`CREATE SEQUENCE` + `DEFAULT nextval(...)`). Functionally similar; more verbose; useful when several tables must draw from one shared key space (rare in a star). Identity columns are the modern, tidier form of the same machinery.
3. **`bigint` with a generated/derived value** (e.g. a hash of the natural key + version). Avoid for primary surrogate keys; hashing reintroduces source coupling and collision risk. Hashes have a place as *change-detection* helpers (compare a hash of tracked columns to decide whether to open a new version) but not as the key itself.

For `dim_date` only, keep the *smart integer* convention from Lecture 2 (`20260619`) — it is a deliberate, sortable, range-friendly exception to opacity. Every other dimension in this course uses `GENERATED ALWAYS AS IDENTITY`. Generated *columns* (<https://www.postgresql.org/docs/16/ddl-generated-columns.html>) are a separate, useful tool for derived attributes like `is_weekend` on `dim_date`, but they are not how you make a surrogate key — they cannot reference a sequence.

## Exercise pointer

Open [`../exercises/exercise-03-type2-scd.sql`](../exercises/exercise-03-type2-scd.sql): you will add the Type-2 columns to `dim_product`, process a staged re-categorization with the close-then-open transaction, and write the point-in-time audit and the well-formedness check from Sections 7. The full worked version with sample `psql` output is in [`SOLUTIONS.md`](../exercises/SOLUTIONS.md). The mini-project then makes the Type-2 audit a graded deliverable.

## Summary

- A dimension attribute change forces a *decision*, and Kimball numbers it. **Type 0** retains the original; **Type 1** overwrites (no history); **Type 2** adds an effective-dated row (full history); **Type 3** keeps one prior-value column.
- **Type 2** is the centerpiece: `valid_from` / `valid_to` (half-open `[from, to)`) / `is_current`, a fresh surrogate key per version, and a far-future `9999-12-31` sentinel for the open row. A single natural key maps to many surrogate keys over time.
- The fact stores the surrogate key *current at the time of the fact*, so joining fact to dimension yields point-in-time correctness automatically — the entire reason Type 2 exists.
- **Maintenance** is close-then-open in one transaction: a `MERGE` closes changed current rows (`valid_to`, `is_current=false`), an `INSERT` opens the new current rows. `IS DISTINCT FROM` makes the change-detection null-safe.
- **Audit** in one `SELECT`: `WHERE date >= valid_from AND date < valid_to`. Prove well-formedness by checking that every natural key has exactly one `is_current` row.
- **Surrogate keys** come from `GENERATED ALWAYS AS IDENTITY`; `dim_date` keeps its smart `YYYYMMDD` key. Generated columns are for derived attributes, not keys.

## Cited references

Kimball & Ross, *The Data Warehouse Toolkit*, 3rd ed. <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/> · Kimball Group "Dimensional Modeling Techniques" (SCD section) <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/> · PostgreSQL 16 identity columns <https://www.postgresql.org/docs/16/ddl-identity-columns.html>, `MERGE` <https://www.postgresql.org/docs/16/sql-merge.html>, generated columns <https://www.postgresql.org/docs/16/ddl-generated-columns.html>.
