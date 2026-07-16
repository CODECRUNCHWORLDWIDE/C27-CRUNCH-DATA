# Week 1 — Exercise Solutions

Full worked solutions for the three exercise files. Every block here is real, runnable PostgreSQL 16 SQL — paste it into the `psql` session from Lecture 1 § 4 (`docker exec -it cc-pg-w1 psql -U postgres -d retail`) and it executes as written. Read the "What success looks like" output first so you know the target, then the SQL, then the annotations, then the pitfalls.

References used throughout: PostgreSQL 16 `CREATE TABLE` <https://www.postgresql.org/docs/16/sql-createtable.html>, identity columns <https://www.postgresql.org/docs/16/ddl-identity-columns.html>, `MERGE` <https://www.postgresql.org/docs/16/sql-merge.html>; Kimball Group "Dimensional Modeling Techniques" <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/>.

---

## Exercise 01 — Model the Grain

### What success looks like

```text
GRAIN: One row per product sold on one sales-order line.

retail=# \d fact_sales
                                  Table "public.fact_sales"
     Column      |     Type      | Nullable |                Default
-----------------+---------------+----------+----------------------------------------
 sale_key        | bigint        | not null | generated always as identity
 date_key        | integer       | not null |
 product_key     | bigint        | not null |
 store_key       | bigint        | not null |
 customer_key    | bigint        | not null |
 order_number    | text          | not null |
 order_line_no   | integer       | not null |
 quantity        | integer       | not null |
 unit_price      | numeric(12,2) | not null |
 extended_amount | numeric(14,2) | not null |
Indexes:
    "fact_sales_pkey" PRIMARY KEY, btree (sale_key)
    "fact_sales_order_number_order_line_no_key" UNIQUE CONSTRAINT, btree (order_number, order_line_no)
Foreign-key constraints:
    "fact_sales_customer_key_fkey" FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key)
    ...
```

### The grain

> **GRAIN: One row per product sold on one sales-order line.**

One sentence. No "and". This is the finest grain the source supports.

### The solution SQL

```sql
-- GRAIN: One row per product sold on one sales-order line.
CREATE TABLE fact_sales (
    sale_key        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_key        int    NOT NULL REFERENCES dim_date(date_key),
    product_key     bigint NOT NULL REFERENCES dim_product(product_key),
    store_key       bigint NOT NULL REFERENCES dim_store(store_key),
    customer_key    bigint NOT NULL REFERENCES dim_customer(customer_key),
    order_number    text   NOT NULL,                  -- degenerate dimension
    order_line_no   int    NOT NULL,
    quantity        int           NOT NULL CHECK (quantity > 0),  -- additive
    unit_price      numeric(12,2) NOT NULL,            -- NON-additive: never SUM
    extended_amount numeric(14,2) NOT NULL,            -- additive: qty * unit_price
    UNIQUE (order_number, order_line_no)               -- one row per real line
);
```

### The defense (Answer 3)

> The line grain can answer **"units sold per *product*"** and **"average lines per order"**; the order grain cannot, because an order touches many products and a single order row cannot attribute units to one product. The order grain avoids the **double-count of an order total** — its total is stored once per order — whereas the line grain must compute order totals by `SUM`ming lines, and must never store an order-level measure (like shipping cost) on every line, or that measure double-counts. We choose the line grain because Kimball's rule is to model at the finest grain the source supports: you can always `SUM` lines up to an order, but you can never split an order total back down to lines.

### Annotations

- **`GENERATED ALWAYS AS IDENTITY`** mints the surrogate `sale_key`; `ALWAYS` forbids a loader from supplying its own value, which is the guarantee you want for a warehouse-owned key (<https://www.postgresql.org/docs/16/ddl-identity-columns.html>).
- **The foreign keys are surrogate keys**, never natural keys. The fact never stores a SKU or an email — only the integer surrogate of the dimension version that was current at the time of the sale. This is what makes Type-2 SCDs work (Exercise 03).
- **`order_number` is a degenerate dimension**: a meaningful identifier with no descriptive attributes of its own, so it lives on the fact with no `dim_order` table.
- **`UNIQUE (order_number, order_line_no)`** is the idempotency guard. A re-run that tries to insert the same real line twice fails loudly instead of double-counting — the quiet failure from Lecture 1 § 2, prevented at the schema level.
- **`unit_price` is stored but flagged non-additive.** You keep it for display and for re-deriving averages, but `SUM(unit_price)` is meaningless; aggregate `extended_amount` instead.

### Common pitfalls

- **Choosing the order grain "because it's simpler".** It is simpler and it cannot answer product-level questions, which is most of what the business asked. Finest grain by default.
- **Storing a SKU or email in the fact.** That couples the fact to the source and breaks Type-2 history. Always the surrogate.
- **Putting an order-level measure (shipping, order discount) on the line fact.** It double-counts across the lines. Order-level measures belong in a separate order-grain fact, or are allocated down to lines deliberately.
- **Omitting the `UNIQUE` constraint.** Without it, the most common production incident — a re-run — silently doubles your revenue.

---

## Exercise 02 — Build the Star Schema

### What success looks like

```text
 week_of_year |  category_name  | region |  revenue
--------------+-----------------+--------+----------
           24 | Snacks          | South  |   141.00
           24 | Beverages       | South  |    57.00
           25 | Snacks          | West   |    76.00
(3 rows)
```

(Your exact numbers depend on the sample rows you load; the *shape* — week × category × region with a summed revenue — is the target.)

### The solution SQL

`dim_date` is provided in the exercise. The three remaining dimensions and the fact:

```sql
CREATE TABLE dim_product (
    product_key   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku           text   NOT NULL,
    product_name  text   NOT NULL,
    category_name text   NOT NULL,    -- denormalized into the star
    brand_name    text   NOT NULL
);

CREATE TABLE dim_store (
    store_key   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_code  text   NOT NULL UNIQUE,
    store_name  text   NOT NULL,
    city        text   NOT NULL,
    region      text   NOT NULL,
    country     text   NOT NULL
);

CREATE TABLE dim_customer (
    customer_key  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_code text   NOT NULL UNIQUE,
    full_name     text   NOT NULL,
    email         text   NOT NULL,
    city          text   NOT NULL,
    loyalty_tier  text   NOT NULL
);

-- GRAIN: One row per product sold on one sales-order line.
CREATE TABLE fact_sales (
    sale_key        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_key        int    NOT NULL REFERENCES dim_date(date_key),
    product_key     bigint NOT NULL REFERENCES dim_product(product_key),
    store_key       bigint NOT NULL REFERENCES dim_store(store_key),
    customer_key    bigint NOT NULL REFERENCES dim_customer(customer_key),
    order_number    text   NOT NULL,
    order_line_no   int    NOT NULL,
    quantity        int           NOT NULL CHECK (quantity > 0),
    unit_price      numeric(12,2) NOT NULL,
    extended_amount numeric(14,2) NOT NULL,
    UNIQUE (order_number, order_line_no)
);
CREATE INDEX idx_fact_sales_date     ON fact_sales (date_key);
CREATE INDEX idx_fact_sales_product  ON fact_sales (product_key);
CREATE INDEX idx_fact_sales_store    ON fact_sales (store_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales (customer_key);
```

Load sample rows (note: never hardcode surrogate keys; look them up):

```sql
INSERT INTO dim_product (sku, product_name, category_name, brand_name) VALUES
  ('SKU-0001', 'Trail Mix 200g',  'Snacks',    'CrunchCo'),
  ('SKU-0002', 'Sparkling Water', 'Beverages', 'FizzCo'),
  ('SKU-0003', 'Granola Bar',     'Snacks',    'CrunchCo');

INSERT INTO dim_store (store_code, store_name, city, region, country) VALUES
  ('STR-MIA', 'Miami Downtown', 'Miami',   'South', 'USA'),
  ('STR-SEA', 'Seattle Pike',   'Seattle', 'West',  'USA');

INSERT INTO dim_customer (customer_code, full_name, email, city, loyalty_tier) VALUES
  ('CUST-1001', 'Ada Lovelace',  'ada@example.com',   'Miami',   'gold'),
  ('CUST-1002', 'Alan Turing',   'alan@example.com',  'Seattle', 'silver'),
  ('CUST-1003', 'Grace Hopper',  'grace@example.com', 'Miami',   'bronze');

-- Six fact rows across two weeks (week 24 = mid-June, week 25 = late June 2026).
INSERT INTO fact_sales
  (date_key, product_key, store_key, customer_key,
   order_number, order_line_no, quantity, unit_price, extended_amount)
VALUES
  (20260612, (SELECT product_key FROM dim_product WHERE sku='SKU-0001'),
             (SELECT store_key   FROM dim_store   WHERE store_code='STR-MIA'),
             (SELECT customer_key FROM dim_customer WHERE customer_code='CUST-1001'),
             'ORD-5000', 1, 2, 9.50, 19.00),
  (20260612, (SELECT product_key FROM dim_product WHERE sku='SKU-0003'),
             (SELECT store_key   FROM dim_store   WHERE store_code='STR-MIA'),
             (SELECT customer_key FROM dim_customer WHERE customer_code='CUST-1001'),
             'ORD-5000', 2, 4, 3.00, 12.00),
  (20260613, (SELECT product_key FROM dim_product WHERE sku='SKU-0001'),
             (SELECT store_key   FROM dim_store   WHERE store_code='STR-MIA'),
             (SELECT customer_key FROM dim_customer WHERE customer_code='CUST-1003'),
             'ORD-5001', 1, 6, 9.50, 57.00),
  (20260613, (SELECT product_key FROM dim_product WHERE sku='SKU-0002'),
             (SELECT store_key   FROM dim_store   WHERE store_code='STR-MIA'),
             (SELECT customer_key FROM dim_customer WHERE customer_code='CUST-1003'),
             'ORD-5001', 2, 3, 19.00, 57.00),
  (20260613, (SELECT product_key FROM dim_product WHERE sku='SKU-0003'),
             (SELECT store_key   FROM dim_store   WHERE store_code='STR-MIA'),
             (SELECT customer_key FROM dim_customer WHERE customer_code='CUST-1003'),
             'ORD-5001', 3, 1, 53.00, 53.00),
  (20260619, (SELECT product_key FROM dim_product WHERE sku='SKU-0001'),
             (SELECT store_key   FROM dim_store   WHERE store_code='STR-SEA'),
             (SELECT customer_key FROM dim_customer WHERE customer_code='CUST-1002'),
             'ORD-5002', 1, 8, 9.50, 76.00);
```

The analytical query (joins fact → date → product → store):

```sql
SELECT d.week_of_year,
       p.category_name,
       s.region,
       SUM(f.extended_amount) AS revenue
FROM   fact_sales f
JOIN   dim_date    d ON d.date_key    = f.date_key
JOIN   dim_product p ON p.product_key = f.product_key
JOIN   dim_store   s ON s.store_key   = f.store_key
WHERE  d.quarter = 2 AND d.year = 2026
GROUP  BY d.week_of_year, p.category_name, s.region
ORDER  BY revenue DESC;
```

### Annotations

- **`dim_date` is generated, not typed by hand.** `generate_series` over a date range plus `EXTRACT`/`to_char` builds an entire year in one statement. A real warehouse generates the date dimension out 5–10 years ahead so no fact ever fails its date foreign key.
- **The surrogate-key lookup on insert** (`(SELECT product_key FROM dim_product WHERE sku=...)`) is the realistic shape of a fact load: the loader resolves the natural key to the *current* surrogate key. In Phase I Week 3 this becomes a proper join in a Python loader.
- **Aggregating `extended_amount`, not `unit_price`.** `extended_amount` is additive; the query sums it freely across date, category, and region. Summing `unit_price` would be meaningless.
- **The single-hop star** means every dimension is exactly one join from the fact — readable and fast.

### Common pitfalls

- **Hardcoding surrogate keys** (`product_key = 1`). `IDENTITY` assigns them in insertion order, but you must not depend on that; look them up by natural key.
- **Inserting facts before dimensions.** The foreign keys reject the fact row. Load dimensions first, then facts.
- **`week_of_year` surprises around year boundaries.** Postgres `WEEK` and ISO weeks differ; for serious work use `EXTRACT(WEEK ...)` consistently and document which you mean. For this exercise either is acceptable.
- **Forgetting the indexes.** They are not required for correctness on six rows, but the star pattern always indexes the fact's foreign keys; build the habit now.

---

## Exercise 03 — Type-2 Slowly-Changing Dimension

### What success looks like

After processing the change, `SKU-0001` has two rows and `SKU-0002` has one:

```text
retail=# SELECT product_key, sku, category_name, valid_from, valid_to, is_current
         FROM dim_product ORDER BY sku, valid_from;
 product_key |   sku    | category_name  | valid_from | valid_to   | is_current
-------------+----------+----------------+------------+------------+-----------
           1 | SKU-0001 | Snacks         | 0001-01-01 | 2026-06-19 | f
           3 | SKU-0001 | Healthy Snacks | 2026-06-19 | 9999-12-31 | t
           2 | SKU-0002 | Beverages      | 0001-01-01 | 9999-12-31 | t
(3 rows)
```

Point-in-time audit for 2026-06-15 shows the OLD category:

```text
   sku    | category_name
----------+---------------
 SKU-0001 | Snacks
 SKU-0002 | Beverages
(2 rows)
```

Well-formedness check returns ZERO rows.

### The solution SQL

Add the control columns:

```sql
ALTER TABLE dim_product
    ADD COLUMN valid_from date    NOT NULL DEFAULT DATE '0001-01-01',
    ADD COLUMN valid_to   date    NOT NULL DEFAULT DATE '9999-12-31',
    ADD COLUMN is_current boolean NOT NULL DEFAULT true;
```

Apply the change, close-then-open, in one transaction:

```sql
BEGIN;

-- Step A: close every CURRENT row whose tracked attributes changed.
MERGE INTO dim_product d
USING stg_product s
   ON  d.sku = s.sku
   AND d.is_current = true
   AND (d.category_name IS DISTINCT FROM s.category_name
        OR d.brand_name   IS DISTINCT FROM s.brand_name
        OR d.product_name IS DISTINCT FROM s.product_name)
WHEN MATCHED THEN
   UPDATE SET valid_to   = s.effective_date,
              is_current = false;

-- Step B: open a new current row for any staged product without a
-- matching open row (covers both the just-closed and brand-new products).
INSERT INTO dim_product (sku, product_name, category_name, brand_name,
                         valid_from, valid_to, is_current)
SELECT s.sku, s.product_name, s.category_name, s.brand_name,
       s.effective_date, DATE '9999-12-31', true
FROM   stg_product s
WHERE  NOT EXISTS (
          SELECT 1 FROM dim_product d
          WHERE  d.sku = s.sku
            AND  d.is_current = true
            AND  d.category_name = s.category_name
            AND  d.brand_name    = s.brand_name
            AND  d.product_name  = s.product_name
       );

COMMIT;
```

Point-in-time audit (half-open interval — `<`, not `<=`):

```sql
SELECT sku, category_name
FROM   dim_product
WHERE  DATE '2026-06-15' >= valid_from
  AND  DATE '2026-06-15' <  valid_to
ORDER  BY sku;
```

Full lifecycle of one product:

```sql
SELECT product_key, category_name, valid_from, valid_to, is_current
FROM   dim_product
WHERE  sku = 'SKU-0001'
ORDER  BY valid_from;
```

Well-formedness check (must return zero rows):

```sql
SELECT sku, COUNT(*) AS current_versions
FROM   dim_product
WHERE  is_current = true
GROUP  BY sku
HAVING COUNT(*) <> 1;
```

### Annotations

- **`IS DISTINCT FROM` is null-safe.** Plain `<>` returns `NULL` (not `true`) when either side is `NULL`, so a change *to* or *from* a null would be missed. `IS DISTINCT FROM` treats two nulls as equal and null-vs-value as different — exactly the change-detection semantics you want.
- **The change-detection clause is in the `MERGE` `ON`**, so unchanged products (`SKU-0002`) never match, never get closed, and Step B's `NOT EXISTS` finds their unchanged open row and skips the insert. Result: no version churn for unchanged rows — a hard requirement for a real SCD loader.
- **Half-open interval `[valid_from, valid_to)`.** The old row's `valid_to` (`2026-06-19`) equals the new row's `valid_from` (`2026-06-19`). The audit predicate uses `< valid_to`, so the changeover date `2026-06-19` matches *only* the new row. Exactly one version is valid on any date; no gaps, no overlaps.
- **The new `product_key` is assigned by the identity sequence** — you never set it. That is the whole reason the surrogate key exists: it lets one natural key own many physical rows.
- **One transaction.** Wrapping close + open in `BEGIN … COMMIT` means a reader never sees a moment with zero current rows for a product or two current rows. Atomicity is not optional here.

### Common pitfalls

- **Using `<=` in the audit instead of `<`.** With an inclusive upper bound the changeover date `2026-06-19` matches *both* versions, and you double-count or get an ambiguous result. The half-open convention and `<` are a matched pair — break one and you must break the other.
- **Forgetting the change-detection predicate**, so every load closes and re-opens every row, exploding the dimension with identical-but-churned versions.
- **Plain `<>` instead of `IS DISTINCT FROM`**, silently missing changes that involve nulls.
- **Trying to do it in a single `MERGE`.** PostgreSQL 16's `MERGE` cannot, in one matched branch, close the existing row *and* insert a different new row for the same source row (<https://www.postgresql.org/docs/16/sql-merge.html>). The close-then-insert pair in one transaction is the correct, clear pattern.
- **Running the open `INSERT` outside the transaction.** A crash between close and open leaves a product with *no* current row — the well-formedness check would catch it, but only after the damage.
