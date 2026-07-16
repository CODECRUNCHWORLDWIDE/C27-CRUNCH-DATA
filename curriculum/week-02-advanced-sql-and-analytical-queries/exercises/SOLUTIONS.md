# Week 2 — Exercise Solutions

Full worked solutions for the three exercise files. Every query is real and runs on **PostgreSQL 16** against the Week-1 retail star schema; the window queries and the anti-joins also run unchanged on **DuckDB 1.x**. Sample output and plan excerpts are illustrative — your exact numbers depend on how much data you loaded — but the *shape* of the output and the plans is what you are matching. Citations: window functions <https://www.postgresql.org/docs/16/functions-window.html>, grouping sets <https://www.postgresql.org/docs/16/queries-table-expressions.html#QUERIES-GROUPING-SETS>, "Using EXPLAIN" <https://www.postgresql.org/docs/16/using-explain.html>.

---

## Exercise 01 — Window Functions

### Task 1 — Rank products by revenue within category

```sql
WITH product_revenue AS (
    SELECT p.category,
           p.product_name,
           SUM(f.extended_price) AS revenue
    FROM   fact_sales f
    JOIN   dim_product p ON p.product_key = f.product_key
    GROUP  BY p.category, p.product_name
)
SELECT category,
       product_name,
       revenue,
       DENSE_RANK() OVER (PARTITION BY category ORDER BY revenue DESC) AS category_rank
FROM   product_revenue
ORDER  BY category, category_rank, product_name;
```

**What success looks like:**

```text
  category   | product_name      |  revenue   | category_rank
-------------+-------------------+------------+---------------
 Electronics | 4K Television     | 1842300.00 |             1
 Electronics | Wireless Earbuds  |  998140.00 |             2
 Electronics | Phone Case        |  998140.00 |             2
 Electronics | USB-C Cable       |  410220.00 |             3
 Home        | Espresso Machine  |  765400.00 |             1
 ...
```

Note the two Electronics products tied at `998140.00` both get rank `2`, and the next product gets rank `3` (not `4`) — that is `DENSE_RANK`.

**Why `DENSE_RANK` here, and when you'd want the others:**

- `ROW_NUMBER()` would have given the two tied products `2` and `3` arbitrarily — wrong if you want "tied means tied," right if you must return *exactly N rows*.
- `RANK()` would have given them `2` and `2`, then skipped to `4` — right if you want the "Olympic" skip-after-tie.
- `DENSE_RANK()` gives `2`, `2`, `3` — right for "the top 3 *revenue tiers*."

**Stretch — only the #1 per category.** PostgreSQL:

```sql
WITH product_revenue AS ( ... ),
ranked AS (
    SELECT category, product_name, revenue,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) AS rn
    FROM   product_revenue
)
SELECT category, product_name, revenue FROM ranked WHERE rn = 1;
```

DuckDB equivalent (identical result):

```sql
SELECT category, product_name, revenue
FROM   product_revenue
QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) = 1;
```

### Task 2 — 7-day running total per store

```sql
WITH daily AS (
    SELECT f.store_key, f.date_key, SUM(f.extended_price) AS daily_revenue
    FROM   fact_sales f
    GROUP  BY f.store_key, f.date_key
)
SELECT s.store_name,
       d.full_date,
       daily.daily_revenue,
       SUM(daily.daily_revenue) OVER (
           PARTITION BY daily.store_key
           ORDER BY d.full_date
           ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
       ) AS revenue_7day
FROM   daily
JOIN   dim_store s ON s.store_key = daily.store_key
JOIN   dim_date  d ON d.date_key  = daily.date_key
ORDER  BY s.store_name, d.full_date;
```

**What success looks like** (first 8 rows of one store):

```text
 store_name | full_date  | daily_revenue | revenue_7day
------------+------------+---------------+--------------
 Crunch-001 | 2026-01-01 |      12300.00 |     12300.00
 Crunch-001 | 2026-01-02 |       9800.00 |     22100.00
 Crunch-001 | 2026-01-03 |      11050.00 |     33150.00
 Crunch-001 | 2026-01-04 |      14200.00 |     47350.00
 Crunch-001 | 2026-01-05 |       8700.00 |     56050.00
 Crunch-001 | 2026-01-06 |      10100.00 |     66150.00
 Crunch-001 | 2026-01-07 |      13400.00 |     79500.00   <- 7 days summed
 Crunch-001 | 2026-01-08 |       9900.00 |     77100.00   <- drops 01-01, adds 01-08
```

Row 7 is the sum of all seven days; row 8 has slid the frame forward — it dropped Jan 1 and added Jan 8, so it is the trailing-7 total, not the running-since-start total. That sliding is the whole point of the `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` frame.

**Why the explicit frame matters:** if you write the window with `ORDER BY` and *no* frame, PostgreSQL defaults to `RANGE UNBOUNDED PRECEDING AND CURRENT ROW`, which is a *running total since the beginning of the partition*, not a trailing-7. The query would still run and return plausible-looking numbers — a silent wrong answer.

### Task 3 — Month-over-month change with LAG

```sql
WITH monthly AS (
    SELECT d.year, d.month, SUM(f.extended_price) AS revenue
    FROM   fact_sales f
    JOIN   dim_date d ON d.date_key = f.date_key
    GROUP  BY d.year, d.month
)
SELECT year, month, revenue,
       LAG(revenue) OVER (ORDER BY year, month) AS prev_month_revenue,
       revenue - LAG(revenue) OVER (ORDER BY year, month) AS mom_change,
       ROUND( 100.0 * (revenue - LAG(revenue) OVER (ORDER BY year, month))
              / NULLIF(LAG(revenue) OVER (ORDER BY year, month), 0), 1) AS mom_pct
FROM   monthly
ORDER  BY year, month;
```

**What success looks like:**

```text
 year | month |  revenue   | prev_month_revenue | mom_change | mom_pct
------+-------+------------+--------------------+------------+---------
 2026 |     1 | 1840200.00 |             (null)  |    (null)  |  (null)
 2026 |     2 | 1655100.00 |        1840200.00   | -185100.00 |   -10.1
 2026 |     3 | 2103400.00 |        1655100.00   |  448300.00 |    27.1
 2026 |     4 | 1990050.00 |        2103400.00   | -113350.00 |    -5.4
```

The first month is `NULL` for prev/change/pct — correct, because there is no earlier month to compare. The `NULLIF(..., 0)` is what stops a zero-revenue previous month from raising a division error.

**Common pitfalls (Exercise 01):**

- Filtering on a window result in `WHERE` (`WHERE DENSE_RANK() OVER (...) = 1`) — errors with "window functions are not allowed in WHERE." Wrap it in a CTE (or use `QUALIFY` on DuckDB).
- Forgetting the in-window `ORDER BY` on a running total — without it the running sum is undefined.
- Using the default frame and getting a since-beginning running total when you wanted a trailing window.
- Repeating the `LAG(...)` expression three times is fine for correctness; PostgreSQL computes the window once. Readability aside, it is not a performance problem.

---

## Exercise 02 — Grouping Sets & Anti-Joins

### Task 1 — Subtotal report with GROUPING SETS + GROUPING()

```sql
SELECT CASE WHEN GROUPING(s.region)   = 1 THEN 'ALL REGIONS'    ELSE s.region   END AS region,
       CASE WHEN GROUPING(p.category) = 1 THEN 'ALL CATEGORIES' ELSE p.category END AS category,
       SUM(f.extended_price) AS revenue
FROM   fact_sales f
JOIN   dim_store   s ON s.store_key   = f.store_key
JOIN   dim_product p ON p.product_key = f.product_key
GROUP  BY ROLLUP (s.region, p.category)
ORDER  BY GROUPING(s.region), s.region, GROUPING(p.category), p.category;
```

`ROLLUP(s.region, p.category)` is exactly `GROUPING SETS ((s.region, p.category), (s.region), ())`; either spelling is accepted.

**What success looks like:**

```text
    region    |    category     |   revenue
--------------+-----------------+-------------
 North        | Electronics     |  2410300.00
 North        | Home            |  1180400.00
 North        | ALL CATEGORIES  |  3590700.00   <- region subtotal
 South        | Electronics     |  2980150.00
 South        | Home            |   905900.00
 South        | ALL CATEGORIES  |  3886050.00   <- region subtotal
 ALL REGIONS  | ALL CATEGORIES  |  7476750.00   <- grand total
```

`GROUPING(col)` returns `1` on a row where that column was rolled up, `0` otherwise; the `CASE` turns the rolled-up `NULL` into a readable label so a genuine `NULL` category (a real data gap) stays visibly distinct as `(null)`.

### Task 2 — Customers who never ordered

```sql
-- Form A (preferred)
SELECT c.customer_key, c.customer_name
FROM   dim_customer c
WHERE  NOT EXISTS (SELECT 1 FROM fact_sales f WHERE f.customer_key = c.customer_key);

-- Form B (equivalent)
SELECT c.customer_key, c.customer_name
FROM   dim_customer c
LEFT   JOIN fact_sales f ON f.customer_key = c.customer_key
WHERE  f.customer_key IS NULL;
```

Both return the same rows:

```text
 customer_key | customer_name
--------------+-----------------
        90112 | Dana Okoro
        90377 | Sam Whitfield
```

**Task 2c — the `NOT IN` trap, explained.** With no `NULL` in the subquery, `NOT IN` returns the same rows as the anti-joins. The moment one `fact_sales.customer_key` is `NULL` (a guest/anonymous sale), the `NOT IN` query returns **zero rows for everyone**. Reason: `key NOT IN (10, 20, NULL)` expands to `key<>10 AND key<>20 AND key<>NULL`; `key<>NULL` is `NULL` (unknown) in three-valued logic, and `TRUE AND TRUE AND NULL = NULL`, which is not `TRUE`, so every row is dropped. `NOT EXISTS` is immune because it tests existence row-by-row rather than comparing against a value list. **Always use `NOT EXISTS` for anti-joins.**

### Task 3 — Products sold in every store

```sql
-- Form A — double NOT EXISTS (relational division)
SELECT p.product_key, p.product_name
FROM   dim_product p
WHERE  NOT EXISTS (
    SELECT 1 FROM dim_store s
    WHERE NOT EXISTS (
        SELECT 1 FROM fact_sales f
        WHERE f.product_key = p.product_key AND f.store_key = s.store_key
    )
);

-- Form B — count form (usually a cleaner plan)
SELECT f.product_key
FROM   fact_sales f
GROUP  BY f.product_key
HAVING COUNT(DISTINCT f.store_key) = (SELECT COUNT(*) FROM dim_store);
```

Both return the same product keys:

```text
 product_key | product_name
-------------+---------------
          14 | USB-C Cable
          22 | AA Batteries
```

Form A reads as "there is no store where this product did *not* sell." Form B counts distinct stores per product and keeps the ones whose count equals the total store count. On a large fact table Form B usually plans as a single `HashAggregate` over `fact_sales`, while Form A can become a correlated nested loop — compare them with `EXPLAIN ANALYZE`.

**Common pitfalls (Exercise 02):**

- Using `NOT IN` for the anti-join and silently getting zero rows when a `NULL` exists.
- Forgetting `COUNT(DISTINCT store_key)` and using `COUNT(store_key)` in Form B — a product that sold *twice in one store and nowhere else* would wrongly count as 2.
- Ordering a `ROLLUP` report by the plain columns instead of by `GROUPING(...)` first, which scatters the subtotal rows through the middle of the output instead of placing them after their group.

---

## Exercise 03 — Read the Plan

### Step 2 — the slow plan (BEFORE)

```text
GroupAggregate  (cost=98958.10..98958.40 rows=1 width=44)
                (actual time=441.20..441.21 rows=1 loops=1)
  Group Key: customer_key
  ->  Seq Scan on fact_sales f  (cost=0.00..98958.00 rows=137 width=12)
                                (actual time=0.31..440.05 rows=137 loops=1)
        Filter: (customer_key = 80421)
        Rows Removed by Filter: 4999863           <-- the smoking gun
        Buffers: shared read=36330                <-- whole table read from disk
Planning Time: 0.10 ms
Execution Time: 441.28 ms
```

**Diagnosis.** The bottleneck node is the `Seq Scan on fact_sales`. It read all 5,000,000 rows and discarded 4,999,863 of them (`Rows Removed by Filter`) to return 137. The estimate (`rows=137`) was actually *accurate* — the problem is not a bad estimate, it is that there is no index on `customer_key`, so the only access path available is a full scan. `Buffers: shared read=36330` confirms the whole table came off disk.

### Step 3 — the fix

```sql
CREATE INDEX idx_fact_customer ON fact_sales (customer_key);
ANALYZE fact_sales;
```

### Step 5 — the fast plan (AFTER)

```text
GroupAggregate  (cost=480.30..480.55 rows=1 width=44)
                (actual time=0.28..0.29 rows=1 loops=1)
  Group Key: customer_key
  ->  Bitmap Heap Scan on fact_sales f  (cost=5.20..479.80 rows=137 width=12)
                                        (actual time=0.06..0.24 rows=137 loops=1)
        Recheck Cond: (customer_key = 80421)
        Heap Blocks: exact=131
        Buffers: shared hit=140                   <-- 140 pages, all cached
        ->  Bitmap Index Scan on idx_fact_customer
                                  (actual time=0.04..0.04 rows=137 loops=1)
              Index Cond: (customer_key = 80421)
Planning Time: 0.22 ms
Execution Time: 0.34 ms
```

**Result:**

```text
BEFORE: time = 441.28 ms, shared read = 36330
AFTER:  time =   0.34 ms, shared hit  =   140
SPEEDUP: ~1300x
```

The `Seq Scan` became a `Bitmap Index Scan` feeding a `Bitmap Heap Scan`; the database now touches ~140 pages instead of 36,330, and they are cache hits, not disk reads.

### Step 6 — when the Seq Scan is correct

```text
Finalize Aggregate  (cost=98960.50..98960.51 rows=1 width=8) (actual ... rows=1)
  ->  Seq Scan on fact_sales  (rows=5000000)   <-- planner CHOSE this, on purpose
Execution Time: 295.10 ms
```

Even with `idx_fact_customer` present, `SUM(extended_price)` over the whole table uses a `Seq Scan`, because the query needs *every* row. Using the index would mean reading every leaf entry *and* fetching every heap row — strictly more work than one sequential pass. The planner is right; a seq scan is the correct access path when you need most of the table.

**Common pitfalls (Exercise 03):**

- Forgetting `ANALYZE` after `CREATE INDEX` — the planner may keep using the old plan because its statistics do not yet reflect the index.
- Reading wall-clock time only and ignoring `Buffers` — on a warm cache a slow query can look fast; buffers tell the truth about work done.
- Concluding "indexes are always faster" and trying to force one on the full-table aggregate. They are not; Step 6 is the counterexample.
- Changing two things at once (index *and* `work_mem`) and not knowing which helped. Change one thing, re-measure.
