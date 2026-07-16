# Challenge 01 — Snowflake vs Star: Measure the Trade-off, Don't Just Recite It

> **Time:** ~2 hours. **Prerequisites:** Lecture 2 (star vs snowflake, § 7) and a loaded star schema from Exercise 02. **Engine:** PostgreSQL 16 in Docker. **Citations:** Kimball Group "Dimensional Modeling Techniques" <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/>; PostgreSQL 16 `EXPLAIN` <https://www.postgresql.org/docs/16/sql-explain.html> and `CREATE TABLE` <https://www.postgresql.org/docs/16/sql-createtable.html>.

## Premise

Kimball says "prefer the star." This challenge makes you *earn* that opinion by building both shapes of the product dimension, loading enough data that the planner has something to chew on, and reading the actual query plans with `EXPLAIN ANALYZE`. By the end you will be able to say, with numbers in hand, exactly what the snowflake costs you and exactly what it buys you — which is the difference between reciting a rule and owning a design decision.

## Setup

Start from a clean Postgres 16 container (or reuse the week's `cc-pg-w1`):

```bash
docker run --name cc-pg-w1-ch1 -e POSTGRES_PASSWORD=crunch \
  -e POSTGRES_DB=retail -p 5433:5432 -d postgres:16
docker exec -it cc-pg-w1-ch1 psql -U postgres -d retail
```

You will build the product dimension **twice**:

- **Star** — `dim_product_star`: `category_name` and `department` are plain `text` columns, denormalized, repeated on every product row.
- **Snowflake** — `dim_product_snow` → `dim_category` → (`department` inside `dim_category`): the hierarchy normalized into sub-dimensions joined by surrogate keys.

Then load **enough volume to matter**: at least **200,000 fact rows** over **5,000 products** across **40 categories** and **8 departments**, so the planner's join and scan choices are visible. Generate the data with `generate_series` — no external files.

## Tasks

1. **Build both dimensions and a shared fact.** `fact_sales` references the *star* product dimension. Build `dim_product_snow` + `dim_category` alongside it carrying the *same* products, so the two only differ in normalization. Use `GENERATED ALWAYS AS IDENTITY` for every surrogate key.
2. **Bulk-load with `generate_series`.** Generate 5,000 products, assign each to one of 40 categories (each category in one of 8 departments), and generate ≥200,000 fact rows referencing random products and dates. `ANALYZE` every table afterward so the planner has fresh statistics.
3. **Run the same business question against both shapes.** Question: *total revenue by department for 2026 Q2.* Against the star this is `fact → dim_product_star` (one hop, then group by `department`). Against the snowflake it is `fact → dim_product_snow → dim_category` (two hops, group by `dim_category.department`).
4. **`EXPLAIN (ANALYZE, BUFFERS)` both queries.** Capture: the join nodes (hash join? nested loop?), the planned vs actual rows, the total execution time, and the shared-buffer reads. Run each query 3 times and take the warm-cache median (the first run pays for disk reads).
5. **Measure storage.** Compare `pg_total_relation_size('dim_product_star')` against `pg_total_relation_size('dim_product_snow') + pg_total_relation_size('dim_category')`.
6. **Write the defense.** In a short `DECISION.md`, state which shape you would ship for this workload and why, citing your own numbers.

## A correct starting scaffold

This is real and runnable; it builds both shapes and the data so you can focus on the measurement:

```sql
-- 8 departments, 40 categories, 5,000 products.
CREATE TABLE dim_category (
    category_key  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category_name text NOT NULL,
    department    text NOT NULL
);
INSERT INTO dim_category (category_name, department)
SELECT 'Category ' || g,
       'Dept ' || ((g % 8) + 1)
FROM generate_series(1, 40) AS g;

-- Snowflake product dimension: FK to dim_category.
CREATE TABLE dim_product_snow (
    product_key  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku          text NOT NULL,
    product_name text NOT NULL,
    category_key bigint NOT NULL REFERENCES dim_category(category_key)
);
INSERT INTO dim_product_snow (sku, product_name, category_key)
SELECT 'SKU-' || lpad(g::text, 6, '0'),
       'Product ' || g,
       ((g % 40) + 1)
FROM generate_series(1, 5000) AS g;

-- Star product dimension: SAME products, category + department denormalized.
CREATE TABLE dim_product_star (
    product_key   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku           text NOT NULL,
    product_name  text NOT NULL,
    category_name text NOT NULL,
    department    text NOT NULL
);
INSERT INTO dim_product_star (sku, product_name, category_name, department)
SELECT ps.sku, ps.product_name, c.category_name, c.department
FROM   dim_product_snow ps
JOIN   dim_category c ON c.category_key = ps.category_key;

-- A trimmed dim_date for the challenge (Q2 2026):
CREATE TABLE dim_date (
    date_key  int PRIMARY KEY,
    full_date date NOT NULL,
    quarter   int NOT NULL,
    year      int NOT NULL
);
INSERT INTO dim_date
SELECT (EXTRACT(YEAR FROM d)*10000 + EXTRACT(MONTH FROM d)*100 + EXTRACT(DAY FROM d))::int,
       d, EXTRACT(QUARTER FROM d)::int, EXTRACT(YEAR FROM d)::int
FROM generate_series(DATE '2026-01-01', DATE '2026-12-31', INTERVAL '1 day') d;

-- 200,000 fact rows referencing the STAR product key (1..5000) and random Q2 dates.
CREATE TABLE fact_sales (
    sale_key        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_key        int    NOT NULL REFERENCES dim_date(date_key),
    product_key     bigint NOT NULL REFERENCES dim_product_star(product_key),
    quantity        int    NOT NULL,
    extended_amount numeric(14,2) NOT NULL
);
INSERT INTO fact_sales (date_key, product_key, quantity, extended_amount)
SELECT (20260401 + (random()*90)::int * 1)  -- approx Q2 spread; see note below
         , 1, 1, 1   -- placeholder, replaced below
FROM generate_series(1, 1) LIMIT 0;  -- intentional no-op; write the real load yourself

CREATE INDEX ON fact_sales (product_key);
CREATE INDEX ON dim_product_snow (category_key);
```

> **Your job in the load:** the placeholder fact load above is deliberately a no-op so you write a correct one. Generate 200,000 rows whose `date_key` is a *valid* date in `dim_date` (do not fabricate `20260401 + random*90` — that yields invalid dates like `20260445`; instead pick a random real `full_date` from `dim_date` and use its `date_key`). Then `ANALYZE fact_sales; ANALYZE dim_product_star; ANALYZE dim_product_snow; ANALYZE dim_category;`.

## Acceptance criteria

- [ ] Both `dim_product_star` and `dim_product_snow` + `dim_category` exist with surrogate keys and carry the same 5,000 products.
- [ ] `fact_sales` has ≥200,000 valid rows (every `date_key` resolves in `dim_date`, every `product_key` in `dim_product_star`).
- [ ] You captured `EXPLAIN (ANALYZE, BUFFERS)` output for the "revenue by department, Q2 2026" query against **both** shapes.
- [ ] You report warm-cache median execution time for both and identify the extra join node the snowflake introduces.
- [ ] You report storage for both shapes.
- [ ] `DECISION.md` states which you would ship and defends it with your own numbers and one citation to the Kimball techniques page.

## What you should observe (and explain)

The star query has one join (`fact ⋈ dim_product_star`) then a `GROUP BY department`. The snowflake query has two (`fact ⋈ dim_product_snow ⋈ dim_category`) then a `GROUP BY dim_category.department`. The extra hash join is cheap on 40 categories but it is *not free*, and on a wider or deeper hierarchy the cost grows. The snowflake's storage is smaller because `department` and `category_name` are stored once in `dim_category` instead of 5,000 times in the star — but the saving is tiny next to the 200,000-row fact, which is identical in both. That asymmetry *is* Kimball's argument: you save storage where it does not matter (the small dimension) and pay query complexity where it does (every analytical read). Write that argument up with your numbers attached.

## Stretch goals

1. **Make the hierarchy deeper.** Add a `dim_department` level so the snowflake is `product → category → department`, three hops. Re-measure. Does the gap widen?
2. **Force the planner.** Run `SET enable_hashjoin = off;` and re-`EXPLAIN`. Watch the planner fall back to nested-loop or merge joins, and reason about why the hash join was the right call.
3. **Filtered query.** Add a `WHERE department = 'Dept 3'` predicate to both. Does the snowflake's ability to filter on the small `dim_category` first change the trade?
4. **Wide dimension.** Add 30 text columns to `dim_product_star` and re-measure storage and scan time, modeling a realistically wide real-world dimension.

## Citations

Kimball Group, "Dimensional Modeling Techniques" (star vs snowflake) <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/> · PostgreSQL 16 `EXPLAIN` <https://www.postgresql.org/docs/16/sql-explain.html> · PostgreSQL 16 `CREATE TABLE` <https://www.postgresql.org/docs/16/sql-createtable.html>.
