# Lecture 2 — Dimensional Modeling: The Four-Step Process, Grain, Facts, Dimensions, and Star vs Snowflake

> **Time:** 2 hours. Take the four-step process and the grain material first; take facts, dimensions, and the star/snowflake trade in a second sitting. **Prerequisites:** Lecture 1 (OLTP vs OLAP, the role boundary) and the running `postgres:16` container from Lecture 1 § 4. **Citations:** Ralph Kimball & Margy Ross, *The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling*, 3rd ed. (Wiley) — book home <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/>; the Kimball Group "Dimensional Modeling Techniques" reference <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/>; PostgreSQL 16 `CREATE TABLE` <https://www.postgresql.org/docs/16/sql-createtable.html> and identity columns <https://www.postgresql.org/docs/16/ddl-identity-columns.html>.

## 1. Why dimensional modeling, and why Kimball

The relational normalization you saw in Lecture 1's OLTP schema is optimized for *writing without anomalies*. Dimensional modeling is optimized for the opposite job: letting a human ask a business question and get an answer with one obvious join, predictable performance, and no chance of a subtle double-count. Ralph Kimball's *The Data Warehouse Toolkit* (3rd ed., with Margy Ross; <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/>) is the canonical text, and its central claim is that you should model around **business processes** as **fact tables** surrounded by **dimension tables** in a **star** — because that shape is simultaneously the easiest for a person to query, the easiest for a query engine to optimize, and the most resilient to schema change. This lecture teaches the discipline; Lecture 3 teaches what happens to dimensions when the world changes underneath them.

The free companion to the book is the Kimball Group's "Dimensional Modeling Techniques" page (<https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/>), which is a numbered catalog of every pattern in this lecture — grain, conformed dimensions, degenerate dimensions, factless facts, the SCD types. Keep it open in a tab; we cite specific techniques from it throughout.

## 2. The four-step design process

Kimball's four steps are not a suggestion; they are an *ordered* recipe, and the order matters. Run them on every business process you ever model:

```text
STEP 1  Select the business process     -> "Retail sales", the act of selling a
                                            product to a customer at a store.
   |
STEP 2  Declare the grain               -> "One row per product sold on one
   |                                        sales-order line." (No "and".)
   |
STEP 3  Identify the dimensions         -> WHO/WHAT/WHERE/WHEN context for that
   |                                        grain: date, product, store, customer.
   |
STEP 4  Identify the facts              -> The numeric measurements at that grain:
                                            quantity sold, extended sale amount.
```

1. **Select the business process.** A business process is a verb the business performs and measures — *selling*, *shipping*, *paying an invoice*. It is *not* a department or a report. "Retail sales" is a process; "the analytics team's weekly deck" is not. Each process becomes one (or a small family of) fact table(s).
2. **Declare the grain.** State, in one sentence, what exactly one row of the fact table represents. This is the most important sentence you will write all week, and Section 3 is devoted to it.
3. **Identify the dimensions.** Once the grain is fixed, the dimensions are the descriptive context that is *true at that grain*: the "by what" you will slice and filter by. For a sales line, that is the date of the sale, the product sold, the store it sold at, and the customer who bought it.
4. **Identify the facts.** The facts are the numeric measurements that exist *at exactly that grain*. For a sales line: the quantity and the extended amount (quantity × unit price). A fact that is *not* at the grain — say, the order's total shipping cost, which is per-order not per-line — does *not* belong in this fact table, because storing it on every line would double-count it.

Run those four steps and the schema is no longer a matter of taste; it is mostly determined. The art is almost entirely in step 2.

**Working the four steps backward from a business question.** The discipline is best learned by watching it run. A stakeholder says: *"I want weekly revenue by product category and by store region for last quarter, and I want to be able to break it down to the individual product and the individual customer."* Translate that sentence into the four steps:

1. **Business process?** The sentence is about *selling*. The process is retail sales. (Not "the dashboard" — the dashboard is the *output*; the process is the verb being measured.)
2. **Grain?** The stakeholder wants to break down "to the individual product" — so a row that bundles a whole order will not do; we need product-level detail. The finest the source offers is the order *line*. Grain: *one row per product sold on one sales-order line.* Notice the grain was *chosen by the most detailed thing the question asks for*, then pushed to the finest the source supports.
3. **Dimensions?** The "by" clauses name them: "by category" and "to the individual product" → `dim_product`; "by store region" → `dim_store`; "weekly … last quarter" → `dim_date`; "to the individual customer" → `dim_customer`.
4. **Facts?** "Revenue" is the headline measure; at the line grain the additive measure is `extended_amount` (quantity × unit price), and `quantity` itself. Revenue *by week* is just `SUM(extended_amount)` grouped by the week attribute of `dim_date`.

The schema fell out of the sentence. That is the whole method: the business question *is* the design document, and the four steps are how you read it.

## 3. Grain is THE word

The **grain** of a fact table is the meaning of one row, and it is the *contract* every downstream consumer relies on. Fix it wrong and three things break at once: which dimensions can attach, whether your measures are safe to `SUM`, and whether two people querying the table get the same number. Some rules earned from pain:

- **State the grain in one sentence with no "and".** "One row per product per sales-order line" is a grain. "One row per order and per shipment" is two grains pretending to be one — split them into two fact tables. The C27 grain promise (see the week README) is that this sentence lives in a comment at the top of every `CREATE TABLE`.
- **Choose the finest grain the source supports, by default.** Kimball's strong recommendation: model at the atomic, lowest level of detail your source data allows — here, the individual order *line*, not the order, not the daily summary. You can always aggregate fine grain *up* (`SUM` lines into an order total) but you can never recover detail you did not store. Fine grain also accepts the most dimensions: a line has a product; an order does not (it has *many* products).
- **Grain determines additivity.** Once the grain is fixed, every measure falls into one of three buckets, and getting this wrong is the classic warehouse bug:

| Additivity | Meaning | Example at the sales-line grain | Safe to `SUM` across… |
|---|---|---|---|
| **Additive** | Sums correctly across *every* dimension | `quantity`, `extended_amount` | date, product, store, customer — all |
| **Semi-additive** | Sums across some dimensions but **not time** | an inventory `units_on_hand` snapshot | store, product — but **not** date (you average over time, not sum) |
| **Non-additive** | Sums across *no* dimension | `unit_price`, a ratio, a percentage | nothing — re-derive from additive components instead |

The single most common analytical mistake is `SUM(unit_price)` — a non-additive measure summed as if it were additive, producing a meaningless number. Store the additive components (`quantity`, `extended_amount`) in the fact and *compute* ratios at query time from their sums. Semi-additive measures (balances, snapshots) are why Lecture 3 and Challenge 2 introduce a separate inventory-snapshot fact: you cannot put a snapshot quantity in the sales fact without lying about its additivity over time.

## 4. Dimensions — the descriptive context

A **dimension table** holds the descriptive "by what" attributes you slice, filter, group, and label by. Dimensions are wide (many text columns), relatively small (thousands to millions of rows, not billions), and *denormalized* in a star. Key sub-types you must know:

- **The conformed dimension.** A single dimension table shared by *multiple* fact tables, so that "product" means the same thing everywhere. This is the backbone of an integrated warehouse and the subject of Challenge 2. (Kimball technique: conformed dimensions and the enterprise bus matrix.)
- **The role-playing dimension.** One physical dimension used in several roles. `dim_date` plays "order date", "ship date", and "return date" — the same table, joined three times under three aliases. You build the date dimension once and view it through roles.
- **The degenerate dimension.** A dimension *key with no dimension table* — the order number itself. It is a high-cardinality identifier you keep *on the fact row* to group lines of the same order, but it has no descriptive attributes worth a table of its own. You store `order_number` directly in `fact_sales`. (Kimball technique: degenerate dimensions.)
- **The factless fact table.** A fact table with *no* numeric measures — only the keys — recording that an *event* happened. "A customer was eligible for a promotion on a date" is factless; you count rows, you do not sum a measure. (Kimball technique: factless fact tables.) We will not build one this week, but you must be able to name it.

## 5. Surrogate keys vs natural keys

Every dimension's primary key in a Kimball star is a **surrogate key**: a meaningless integer the warehouse generates, *not* the source's identifier. The source's identifier (SKU, email, store code) is the **natural key**, kept as an ordinary attribute. In Postgres you mint surrogate keys with `GENERATED ALWAYS AS IDENTITY` (<https://www.postgresql.org/docs/16/ddl-identity-columns.html>):

```sql
CREATE TABLE dim_product (
    product_key   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- surrogate
    sku           text   NOT NULL,   -- natural key, demoted to an attribute
    product_name  text   NOT NULL
);
```

Four reasons the surrogate key is not bureaucracy:

1. **It decouples the warehouse from the source.** If the source recycles a SKU or merges two systems with clashing IDs, your fact rows still point at the right dimension row.
2. **It is fast and narrow.** A 8-byte integer join key beats a variable-length text natural key on every join, and `fact_sales` joins to `dim_product` on every analytical query.
3. **It makes Type-2 SCDs possible.** This is the load-bearing reason. A natural key can identify only *one* dimension row, but a single product can have *many* dimension rows over time as it is re-categorized (Lecture 3). The surrogate key lets each historical version be a distinct row with a distinct key, while they all share one natural key.
4. **It buffers the fact table from source schema changes.** The fact only ever stores surrogate keys; how the source identifies things is the dimension loader's problem, not the fact's.

The one exception by long convention is `dim_date`, which uses a *smart integer* key like `20260619` (year×10000 + month×100 + day). It is technically a surrogate, it sorts and ranges naturally, and it lets you partition and filter on the key directly. Every other dimension gets an opaque identity surrogate.

## 6. The retail star, in full

Here is the complete star you will build in the exercises and the mini-project. The grain comment at the top of `fact_sales` is the contract; everything else hangs off it. Run this against the Lecture-1 container (`docker exec -it cc-pg-w1 psql -U postgres -d retail`). The DDL is standard PostgreSQL 16 `CREATE TABLE` (<https://www.postgresql.org/docs/16/sql-createtable.html>).

```sql
-- =====================================================================
-- dim_date: the conformed date dimension. Smart integer key (YYYYMMDD).
-- Grain: one row per calendar day.
-- =====================================================================
CREATE TABLE dim_date (
    date_key      int  PRIMARY KEY,                 -- 20260619
    full_date     date NOT NULL UNIQUE,
    day_of_week   text NOT NULL,                     -- 'Friday'
    day_of_month  int  NOT NULL,
    week_of_year  int  NOT NULL,
    month_num     int  NOT NULL,
    month_name    text NOT NULL,                     -- 'June'
    quarter       int  NOT NULL,
    year          int  NOT NULL,
    is_weekend    boolean NOT NULL
);

-- =====================================================================
-- dim_product: denormalized (star). Category & brand are TEXT, not FKs.
-- Grain: one row per product version (Type-2 columns added in Lecture 3).
-- =====================================================================
CREATE TABLE dim_product (
    product_key   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku           text   NOT NULL,                   -- natural key
    product_name  text   NOT NULL,
    category_name text   NOT NULL,                   -- denormalized into the star
    brand_name    text   NOT NULL
);

-- =====================================================================
-- dim_store: denormalized. Region & country are TEXT, not FKs.
-- Grain: one row per store.
-- =====================================================================
CREATE TABLE dim_store (
    store_key     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_code    text   NOT NULL UNIQUE,            -- natural key
    store_name    text   NOT NULL,
    city          text   NOT NULL,
    region        text   NOT NULL,
    country       text   NOT NULL
);

-- =====================================================================
-- dim_customer: denormalized.
-- Grain: one row per customer.
-- =====================================================================
CREATE TABLE dim_customer (
    customer_key  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_code text   NOT NULL UNIQUE,            -- natural key
    full_name     text   NOT NULL,
    email         text   NOT NULL,
    city          text   NOT NULL,
    loyalty_tier  text   NOT NULL                    -- 'bronze'/'silver'/'gold'
);

-- =====================================================================
-- fact_sales
-- GRAIN: one row per product sold on one sales-order line.
--        (One sentence, no "and". This is the contract.)
-- =====================================================================
CREATE TABLE fact_sales (
    sale_key        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- foreign keys to the conformed dimensions:
    date_key        int    NOT NULL REFERENCES dim_date(date_key),
    product_key     bigint NOT NULL REFERENCES dim_product(product_key),
    store_key       bigint NOT NULL REFERENCES dim_store(store_key),
    customer_key    bigint NOT NULL REFERENCES dim_customer(customer_key),
    -- degenerate dimension: the order number lives ON the fact, no table:
    order_number    text   NOT NULL,
    order_line_no   int    NOT NULL,
    -- additive facts at the line grain:
    quantity        int           NOT NULL CHECK (quantity > 0),
    unit_price      numeric(12,2) NOT NULL,          -- NON-additive: do not SUM
    extended_amount numeric(14,2) NOT NULL,          -- additive: quantity * unit_price
    UNIQUE (order_number, order_line_no)             -- one row per real order line
);

CREATE INDEX idx_fact_sales_date     ON fact_sales (date_key);
CREATE INDEX idx_fact_sales_product  ON fact_sales (product_key);
CREATE INDEX idx_fact_sales_store    ON fact_sales (store_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales (customer_key);
```

The shape is the **star**:

```text
                       +---------------+
                       |   dim_date    |
                       +-------+-------+
                               |
        +---------------+      |      +----------------+
        | dim_customer  |------+------|   dim_store    |
        +---------------+      |      +----------------+
                               |
                       +-------+--------+
                       |   fact_sales   |   <- center: keys + additive facts
                       |  (line grain)  |      + order_number (degenerate dim)
                       +-------+--------+
                               |
                       +-------+-------+
                       |  dim_product  |
                       +---------------+
```

Every dimension is *one hop* from the fact. Any business question — by date, by category, by region, by loyalty tier, or any combination — is one join per dimension you touch and a `GROUP BY`. That single-hop property is the whole point of the star.

## 7. Star vs snowflake

A **snowflake** schema normalizes a dimension's hierarchy out into sub-dimensions, third-normal-form style, instead of denormalizing it into one wide table. Take `dim_product`: in the star, `category_name` and `brand_name` are plain text columns repeated on every product row. In the snowflake, you replace them with foreign keys to separate tables:

```sql
-- SNOWFLAKE variant of the product dimension (contrast with §6's star):
CREATE TABLE dim_category (
    category_key  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category_name text   NOT NULL UNIQUE,
    department    text   NOT NULL                    -- the next level up
);

CREATE TABLE dim_product_snow (
    product_key   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku           text   NOT NULL,
    product_name  text   NOT NULL,
    category_key  bigint NOT NULL REFERENCES dim_category(category_key)  -- FK, not text
);
```

Now "revenue by department" requires `fact_sales → dim_product_snow → dim_category` — two hops instead of one. The trade:

| | **Star** (denormalized dimension) | **Snowflake** (normalized hierarchy) |
|---|---|---|
| Joins from fact to attribute | One hop | Two or more hops |
| Query readability | High — fewer joins | Lower — more joins to reason about |
| Storage | More (text repeated per row) | Less (text stored once) |
| Hierarchy integrity | Enforced by the loader, not the schema | Enforced relationally by the FK |
| Update of a category name | Many rows | One row |
| Kimball's default | **Preferred** | Only when justified |

Kimball is famously opinionated: **prefer the star.** Storage is cheap, query simplicity is not, and the repeated text in a wide dimension is exactly what makes analytical queries fast and obvious. Snowflake a dimension only when it is genuinely enormous (the saved storage is material) or when a hierarchy must be enforced relationally rather than by the loader. Challenge 1 makes you build both versions of the product dimension, run the same query against each with `EXPLAIN ANALYZE` (<https://www.postgresql.org/docs/16/sql-explain.html>), and defend which you would ship — so the trade-off is something you have measured, not just memorized.

## Exercise pointer

Open [`../exercises/exercise-02-build-the-star-schema.sql`](../exercises/exercise-02-build-the-star-schema.sql): you will write the four dimension `CREATE TABLE`s and the `fact_sales` DDL (with its grain comment), load a handful of sample rows into each, and run an analytical query that joins the fact to all four dimensions. Then carry the star into [`../challenges/challenge-01-snowflake-vs-star-tradeoff.md`](../challenges/challenge-01-snowflake-vs-star-tradeoff.md) to feel the snowflake trade with real query plans.

## Summary

- **Kimball's four steps**, in order: select the business process, declare the grain, identify the dimensions, identify the facts. The order matters; the art is in step 2.
- **Grain** is the meaning of one fact row and the contract everyone downstream relies on. State it in one sentence with no "and", choose the finest grain the source supports, and let it determine which measures are additive, semi-additive, or non-additive. Never `SUM` a non-additive measure like `unit_price`.
- **Dimensions** are the wide, denormalized, descriptive context. Know the conformed, role-playing, and degenerate dimensions and the factless fact table by name.
- **Surrogate keys** (`GENERATED ALWAYS AS IDENTITY`) are the warehouse's own integer keys; natural keys become attributes. Surrogates decouple from the source, join fast, and — the load-bearing reason — make Type-2 SCDs possible. `dim_date` uses a smart `YYYYMMDD` key by convention.
- The **retail star** centers `fact_sales` (line grain, additive measures, the order number as a degenerate dimension) on four one-hop dimensions.
- **Star beats snowflake** by default: prefer denormalized one-hop dimensions; snowflake only when a dimension is enormous or a hierarchy must be relationally enforced.

## Cited references

Kimball & Ross, *The Data Warehouse Toolkit*, 3rd ed. (Wiley) <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/> · Kimball Group "Dimensional Modeling Techniques" <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/> · PostgreSQL 16 `CREATE TABLE` <https://www.postgresql.org/docs/16/sql-createtable.html>, identity columns <https://www.postgresql.org/docs/16/ddl-identity-columns.html>, `EXPLAIN` <https://www.postgresql.org/docs/16/sql-explain.html>.
