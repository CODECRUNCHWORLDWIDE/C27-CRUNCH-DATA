# Mini-Project — Model and Load a Retail Analytics Warehouse in Postgres

> **Time budget:** ~11 hours across Thursday–Sunday. **Engine:** PostgreSQL 16 in Docker; the whole thing comes up with `docker compose up`. **Citations:** Kimball & Ross, *The Data Warehouse Toolkit*, 3rd ed. <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/>; PostgreSQL 16 docs <https://www.postgresql.org/docs/16/index.html> (`CREATE TABLE` <https://www.postgresql.org/docs/16/sql-createtable.html>, `MERGE` <https://www.postgresql.org/docs/16/sql-merge.html>, identity columns <https://www.postgresql.org/docs/16/ddl-identity-columns.html>); Docker Postgres image <https://hub.docker.com/_/postgres>.

This is the Week-1 capstone-in-miniature. You will take everything from the three lectures — grain, the four-step process, the star schema, surrogate keys, and the Type-2 SCD — and turn it into a single, reproducible, `docker compose up`-runnable retail analytics warehouse that a reviewer can stand up on their laptop and verify in five minutes. It is the first link in the Phase I chain; in Week 3 you will replace the SQL seed loader with a Python ETL job, and in Week 4 you will orchestrate that job in Airflow. Build it clean now and the rest of the phase has a foundation.

## The business

You are modeling **retail sales** for a mid-size retailer with multiple stores. Stakeholders ask questions like:

- Weekly and monthly revenue by product category, by region, and by loyalty tier.
- Units sold per product per store per day.
- *What category was a product in at the time of each sale* — even after it was re-categorized. (This is the SCD-2 requirement; it is non-negotiable.)
- Average basket size (lines per order) and average order value.

Run Kimball's four steps before writing DDL: the business process is **retail sales**, the grain is **one row per product per sales-order line**, the dimensions are **date / product / store / customer**, and the facts are **quantity** and **extended amount**. The SCD-2 question fixes `dim_product` as a Type-2 dimension.

## Functional requirements

- **F1 — The star schema.** Build `dim_date`, `dim_product`, `dim_store`, `dim_customer`, and `fact_sales` exactly to the grain above. `fact_sales` carries its grain in a comment, references every dimension by surrogate key, carries `order_number` as a degenerate dimension, and has a `UNIQUE (order_number, order_line_no)` idempotency guard.
- **F2 — Surrogate keys.** Every dimension uses `GENERATED ALWAYS AS IDENTITY`, except `dim_date`, which uses the smart `YYYYMMDD` integer key. No natural key is ever a primary key.
- **F3 — A generated date dimension.** Populate `dim_date` for all of 2026 (≥365 rows) from a single `generate_series` statement, with `year`, `quarter`, `month_num`, `month_name`, `week_of_year`, `day_of_week`, and `is_weekend`.
- **F4 — A Type-2 product dimension.** `dim_product` has `valid_from`, `valid_to` (half-open, `9999-12-31` sentinel), and `is_current`. Loading a re-categorization closes the old version and opens a new one with a fresh surrogate key, atomically.
- **F5 — A realistic seed.** Load ≥30 products, ≥4 stores across ≥2 regions, ≥20 customers across ≥3 loyalty tiers, and ≥500 fact rows spanning ≥3 months, with surrogate keys looked up (never hardcoded).
- **F6 — A documented re-categorization.** At least **two** products must change category mid-dataset, with facts *before and after* the change date, so the point-in-time audit demonstrably returns different categories for the same product on different sale dates.
- **F7 — Verification queries.** Ship a `verify.sql` that runs the well-formedness check (exactly one current row per SKU → zero rows), the point-in-time audit on a chosen date, and the three headline business queries (revenue by category/region/week, units per product/store/day, average order value).
- **F8 — One-command bring-up.** `docker compose up` starts Postgres and runs the schema, seed, and SCD-change scripts in order, leaving a fully loaded, queryable warehouse with no manual steps.

## Non-functional requirements

- **NF1 — Reproducible.** A clean checkout plus `docker compose up` produces an identical warehouse every time. No hidden manual steps, no "first edit this file".
- **NF2 — Idempotent bring-up.** Re-running `docker compose up` (after `docker compose down -v`) re-creates the warehouse cleanly; the `UNIQUE` guards mean a re-applied seed cannot double-count.
- **NF3 — Self-documenting grain.** Every fact table's grain is a comment at the top of its `CREATE TABLE`. A reviewer can read the contract without asking you.
- **NF4 — Standard SQL only.** PostgreSQL 16 standard features (`GENERATED ALWAYS AS IDENTITY`, `MERGE`, `generate_series`, generated columns). No extensions, no superuser tricks.
- **NF5 — Laptop-sized.** The whole thing runs in under ~256 MB of Postgres memory and a few seconds of load time on a laptop.

## Suggested project layout

```text
retail-warehouse/
├── docker-compose.yml          # Postgres 16 + init scripts mounted
├── README.md                   # how to run, the grain decisions, the model diagram
├── sql/
│   ├── 01_schema.sql           # F1, F2, F4 — all CREATE TABLE + indexes
│   ├── 02_dim_date.sql         # F3 — generate the date dimension
│   ├── 03_seed.sql             # F5 — load dims (initial versions) + facts
│   ├── 04_scd_change.sql       # F6 — staged re-categorization, close-then-open txn
│   └── verify.sql              # F7 — well-formedness, point-in-time audit, KPIs
└── docs/
    ├── MODEL.md                # the four-step writeup + an ASCII star diagram
    └── GRAIN.md                # the grain sentence and its defense
```

## Starter notes

A `docker-compose.yml` that runs every script in order on first boot (the official image runs files in `/docker-entrypoint-initdb.d` alphabetically — see <https://hub.docker.com/_/postgres>):

```yaml
services:
  warehouse:
    image: postgres:16
    environment:
      POSTGRES_PASSWORD: crunch
      POSTGRES_DB: retail
    ports:
      - "5432:5432"
    volumes:
      - ./sql/01_schema.sql:/docker-entrypoint-initdb.d/01_schema.sql:ro
      - ./sql/02_dim_date.sql:/docker-entrypoint-initdb.d/02_dim_date.sql:ro
      - ./sql/03_seed.sql:/docker-entrypoint-initdb.d/03_seed.sql:ro
      - ./sql/04_scd_change.sql:/docker-entrypoint-initdb.d/04_scd_change.sql:ro
```

Bring it up and verify:

```bash
docker compose up -d
# wait a few seconds for init scripts to finish, then:
docker compose exec warehouse psql -U postgres -d retail -f /docker-entrypoint-initdb.d/../verify.sql
# or copy verify.sql in and run it:
docker compose exec -T warehouse psql -U postgres -d retail < sql/verify.sql
```

The schema, the SCD-2 `MERGE`-then-`INSERT`, and the audit query are all worked out in the lecture notes and `exercises/SOLUTIONS.md` — you are assembling and extending them into one coherent, loaded, documented system, not inventing new SQL.

## Measurement and verification

Your `verify.sql` must produce, and your README must show, all of the following:

1. **Well-formedness** — `SELECT sku, COUNT(*) FROM dim_product WHERE is_current GROUP BY sku HAVING COUNT(*) <> 1;` returns **zero rows**.
2. **Point-in-time audit** — for one re-categorized product, the same SKU shows the *old* category for a date before the change and the *new* category for a date after it, each from one `SELECT`.
3. **Point-in-time correctness of facts** — a query that joins `fact_sales` to `dim_product` and shows that sales *before* the change carry the old category and sales *after* carry the new — proving the surrogate key did its job.
4. **Headline KPIs** — revenue by category × region × week; units per product/store/day; average order value (`SUM(extended_amount) / COUNT(DISTINCT order_number)`).
5. **Idempotency proof** — re-applying the seed (or attempting to) does not change `COUNT(*)` of `fact_sales`, because of the `UNIQUE` guard.

## Grading rubric (100 points)

| Area | Points | What earns them |
|---|---:|---|
| Star schema correctness (F1, F2) | 20 | Four dims + fact at the stated grain; surrogate keys everywhere; degenerate dim and idempotency guard present |
| Grain declared and defended (NF3, `GRAIN.md`) | 10 | One-sentence grain, no "and", in the DDL comment and defended in `GRAIN.md` |
| Date dimension (F3) | 10 | Full-year `dim_date` generated in one statement with all required attributes |
| Type-2 SCD mechanics (F4, F6) | 25 | Effective dating, half-open intervals, atomic close-then-open, fresh surrogate per version, two real re-categorizations with before/after facts |
| Point-in-time audit + well-formedness (F7, verify) | 15 | Audit returns correct historical state; well-formedness check returns zero rows; fact point-in-time correctness demonstrated |
| One-command reproducible bring-up (F8, NF1, NF2) | 10 | `docker compose up` from clean checkout produces the full warehouse; re-run is idempotent |
| Documentation (MODEL.md, README) | 10 | Four-step writeup, ASCII star diagram, how-to-run, KPI output captured |

Minimum to pass: **70/100, AND a correct Type-2 audit** (you cannot pass this mini-project with a broken SCD — it is the point of the week).

## Stretch goals

1. **Snowflake one dimension** and benchmark it against the star (folds in Challenge 1) — document the trade with `EXPLAIN ANALYZE` numbers.
2. **Add `fact_inventory_snapshot`** conforming to the same dimensions (folds in Challenge 2) and add a drill-across KPI.
3. **Add a Type-1 attribute** to `dim_customer` (corrected email) and a Type-2 attribute (loyalty tier changes over time) in the *same* dimension, and explain in `MODEL.md` why one attribute is Type 1 and the other Type 2 — mixed SCD types in one dimension is real and common.
4. **Generate a larger seed** (50k+ facts via `generate_series`) and add the fact-foreign-key indexes, then show one KPI's `EXPLAIN ANALYZE` plan.

## Submission

Push the `retail-warehouse/` directory to your cohort repo. The submission must contain: the `docker-compose.yml`, all `sql/` scripts, `verify.sql`, `docs/MODEL.md` and `docs/GRAIN.md`, and a `README.md` that shows the captured output of `verify.sql` (paste the `psql` results). A reviewer will clone, run `docker compose up`, run `verify.sql`, and confirm every measurement above. If it does not come up with one command, it is not done.

The course is GPL-3.0; if you build something reusable, PR it back to <https://github.com/CODE-CRUNCH-CLUB>.
