# Challenge 02 — Conformed Dimensions: Drill Across Two Facts at Different Grains

> **Time:** ~2 hours. **Prerequisites:** Lecture 2 (conformed dimensions, § 4; additive vs semi-additive facts, § 3) and a loaded star from Exercise 02. **Engine:** PostgreSQL 16 in Docker. **Citations:** Kimball Group "Dimensional Modeling Techniques" (conformed dimensions, the enterprise bus matrix) <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/>; PostgreSQL 16 `CREATE TABLE` <https://www.postgresql.org/docs/16/sql-createtable.html>.

## Premise

A single fact table is a table. A *warehouse* is many fact tables that share dimensions, so a question can drill across processes that were loaded by completely separate pipelines. This challenge adds a second fact — `fact_inventory_snapshot` — at a *different grain* than `fact_sales`, deliberately reusing the *same* `dim_date`, `dim_product`, and `dim_store`. Because the dimensions conform, you can answer a question neither fact can answer alone: *for each product and store, how did units sold compare to units on hand?* You will also meet a semi-additive measure for real, and learn why you must never `SUM` it across time.

## The two facts

| | `fact_sales` (you built this) | `fact_inventory_snapshot` (you build this) |
|---|---|---|
| Grain | One row per product per sales-order line | One row per product per store per **day** |
| Conformed dims | date, product, store, customer | date, product, store |
| Measure | `quantity`, `extended_amount` (additive) | `units_on_hand` (**semi-additive** — not over time) |
| Loaded by | the sales pipeline | the inventory pipeline (separate!) |

`units_on_hand` is a **snapshot**: it is additive across product and store (total stock across stores is a valid sum) but **not across time** (summing Monday's + Tuesday's stock double-counts the same physical goods). Over time you *average* a snapshot, never sum it. This is exactly why it lives in its own fact at its own grain and not as a column on `fact_sales`.

## Setup

Reuse the week's container (or a fresh one) with `dim_date`, `dim_product`, `dim_store`, `dim_customer`, and a loaded `fact_sales` from Exercise 02:

```bash
docker exec -it cc-pg-w1 psql -U postgres -d retail
```

## Tasks

1. **Build `fact_inventory_snapshot`.** Grain in a comment: *one row per product per store per day.* Surrogate `inventory_snapshot_key`; foreign keys to the **conformed** `dim_date`, `dim_product`, `dim_store` (the very same tables `fact_sales` uses — do not create new ones); measure `units_on_hand int NOT NULL`; a `UNIQUE (date_key, product_key, store_key)` so each cell is unique. Index the three foreign keys.
2. **Load a few days of snapshots** for the same products and stores that appear in your `fact_sales` data, across the same dates, so the drill-across has overlap. At minimum: 2 products × 1 store × 3 days = 6 rows.
3. **Write the drill-across query.** For 2026 Q2, per product and store, report: total `quantity` sold (from `fact_sales`), total `extended_amount` (from `fact_sales`), and *average* `units_on_hand` (from `fact_inventory_snapshot`). The two facts are at different grains, so you **cannot join them directly** — that would multiply rows and corrupt both measures. Aggregate each fact to the common grain *first* (in CTEs or subqueries), then join the aggregates on the conformed keys.
4. **Prove the trap.** Write the *wrong* version too — a naive `fact_sales JOIN fact_inventory_snapshot ON date_key, product_key, store_key` — and show, with row counts and a wrong sum, how the direct join fans out and double-counts. Document it.
5. **Audit additivity.** Show that `SUM(units_on_hand)` over multiple days is wrong (double-counts), and that `AVG(units_on_hand)` (or a single end-of-period snapshot) is the correct over-time aggregation.

## A correct starting scaffold

Real and runnable; build on it:

```sql
-- GRAIN: one row per product per store per day.
CREATE TABLE fact_inventory_snapshot (
    inventory_snapshot_key bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_key      int    NOT NULL REFERENCES dim_date(date_key),
    product_key   bigint NOT NULL REFERENCES dim_product(product_key),
    store_key     bigint NOT NULL REFERENCES dim_store(store_key),
    units_on_hand int    NOT NULL CHECK (units_on_hand >= 0),  -- SEMI-additive
    UNIQUE (date_key, product_key, store_key)
);
CREATE INDEX ON fact_inventory_snapshot (date_key);
CREATE INDEX ON fact_inventory_snapshot (product_key);
CREATE INDEX ON fact_inventory_snapshot (store_key);

-- Load 3 days of snapshots for SKU-0001 and SKU-0003 at the Miami store:
INSERT INTO fact_inventory_snapshot (date_key, product_key, store_key, units_on_hand)
SELECT d.date_key,
       p.product_key,
       s.store_key,
       (100 - (d.date_key % 10))           -- arbitrary but stable stock level
FROM   dim_date d
JOIN   dim_product p ON p.sku IN ('SKU-0001','SKU-0003')
JOIN   dim_store   s ON s.store_code = 'STR-MIA'
WHERE  d.full_date IN (DATE '2026-06-12', DATE '2026-06-13', DATE '2026-06-19');
```

The drill-across query (the right way — pre-aggregate each fact to the common grain):

```sql
WITH sales AS (
    SELECT f.product_key, f.store_key,
           SUM(f.quantity)        AS units_sold,
           SUM(f.extended_amount) AS revenue
    FROM   fact_sales f
    JOIN   dim_date d ON d.date_key = f.date_key
    WHERE  d.quarter = 2 AND d.year = 2026
    GROUP  BY f.product_key, f.store_key
),
inv AS (
    SELECT i.product_key, i.store_key,
           AVG(i.units_on_hand) AS avg_units_on_hand   -- AVG, never SUM, over time
    FROM   fact_inventory_snapshot i
    JOIN   dim_date d ON d.date_key = i.date_key
    WHERE  d.quarter = 2 AND d.year = 2026
    GROUP  BY i.product_key, i.store_key
)
SELECT p.sku, p.product_name, s.store_name,
       COALESCE(sales.units_sold, 0)       AS units_sold,
       COALESCE(sales.revenue, 0)          AS revenue,
       ROUND(inv.avg_units_on_hand, 1)     AS avg_units_on_hand
FROM   inv
LEFT JOIN sales ON sales.product_key = inv.product_key
               AND sales.store_key   = inv.store_key
JOIN   dim_product p ON p.product_key = inv.product_key
JOIN   dim_store   s ON s.store_key   = inv.store_key
ORDER  BY p.sku, s.store_name;
```

## Acceptance criteria

- [ ] `fact_inventory_snapshot` exists at its stated grain, reuses the *conformed* `dim_date` / `dim_product` / `dim_store` (no duplicate dimension tables), and has the `UNIQUE` grain constraint.
- [ ] Snapshot rows are loaded overlapping the `fact_sales` dates/products/stores.
- [ ] The correct drill-across query pre-aggregates each fact to the common (product, store) grain, then joins the aggregates, and returns sensible numbers.
- [ ] You demonstrate the *wrong* direct join and quantify how it fans out and double-counts.
- [ ] You demonstrate that `SUM(units_on_hand)` over time is wrong and `AVG` (or a point snapshot) is right, with output for both.

## Why this is the whole point of conformance

Two teams, two pipelines, two facts — and yet one query integrates them, because both facts speak the *same* product key and store key. Had the inventory pipeline built its own `dim_store2` with a different surrogate key for the Miami store, no query could ever drill across the two without a fragile string match on store names. Conformed dimensions are the contract that lets an organization integrate instead of fragment; the Kimball Group formalizes this as the *enterprise data warehouse bus matrix* (<https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/>) — a grid of business processes (rows) against conformed dimensions (columns) that you fill in before you build, so every fact shares the same dimensions by design.

## Stretch goals

1. **Add a third conforming fact** — `fact_returns` at the line grain (product, store, customer, date) — and write a query reporting sold vs returned vs on-hand per product.
2. **Build the bus matrix.** Draw the processes × dimensions grid for sales, inventory, and returns and mark which conformed dimension each uses. Commit it as `BUS-MATRIX.md`.
3. **Period-end snapshot.** Instead of `AVG`, report the *last* snapshot in the quarter per product/store using a window function (`ROW_NUMBER() OVER (PARTITION BY product_key, store_key ORDER BY date_key DESC)`), the correct way to get an end-of-period balance.
4. **Conformance violation drill.** Deliberately build a second store dimension with mismatched keys, attempt the drill-across, watch it fail or mislead, then document why conformance is non-negotiable.

## Citations

Kimball Group, "Dimensional Modeling Techniques" (conformed dimensions, bus matrix, semi-additive facts) <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/> · PostgreSQL 16 `CREATE TABLE` <https://www.postgresql.org/docs/16/sql-createtable.html>.
