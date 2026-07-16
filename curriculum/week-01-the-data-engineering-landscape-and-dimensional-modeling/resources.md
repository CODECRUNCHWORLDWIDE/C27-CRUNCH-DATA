# Week 1 — Resources

Real, verifiable references only, grouped by topic. Read the starred (★) ones this week; the rest are for depth and for the homework citations. Every link below resolves to a real document; nothing here is invented.

## Dimensional modeling — the canonical source

- ★ **Ralph Kimball & Margy Ross — *The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling*, 3rd edition (Wiley).** The book this week is built on: the four-step design process, grain, facts and dimensions, the SCD types, conformed dimensions, the enterprise bus matrix. If you read one book in C27, read this one. Publisher/book home: <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/>.
- ★ **Kimball Group — "Dimensional Modeling Techniques."** The free, numbered online catalog of every technique in the book — grain, conformed dimensions, degenerate dimensions, factless fact tables, the SCD types, the bus matrix. Keep it open while you model. <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/>.

## PostgreSQL 16 — the engine

- ★ **PostgreSQL 16 documentation (index).** The reference for everything you run this week. <https://www.postgresql.org/docs/16/index.html>.
- ★ **`CREATE TABLE`.** Columns, constraints, foreign keys — how you express a schema and a grain. <https://www.postgresql.org/docs/16/sql-createtable.html>.
- ★ **Identity columns (`GENERATED ALWAYS AS IDENTITY`).** How you mint surrogate keys the warehouse owns. <https://www.postgresql.org/docs/16/ddl-identity-columns.html>.
- ★ **`MERGE`.** The statement that closes the old version of a Type-2 SCD row; also the upsert you will lean on in Week 3. <https://www.postgresql.org/docs/16/sql-merge.html>.
- **Generated columns.** Derived, stored/virtual columns (e.g. `is_weekend` on `dim_date`) — useful, but *not* how you make a surrogate key. <https://www.postgresql.org/docs/16/ddl-generated-columns.html>.
- **`EXPLAIN` / `EXPLAIN ANALYZE`.** Read the query plan; measure the star-vs-snowflake trade in Challenge 1. <https://www.postgresql.org/docs/16/sql-explain.html>.
- **`generate_series` (set-returning functions).** How you build a full date dimension and bulk-load test data from one statement. <https://www.postgresql.org/docs/16/functions-srf.html>.

## Running it locally

- ★ **Docker official Postgres image (`postgres:16`).** The container the entire week runs in; note the `/docker-entrypoint-initdb.d` init-script convention the mini-project uses. <https://hub.docker.com/_/postgres>.
- **Docker Compose reference.** For the mini-project's one-command bring-up. <https://docs.docker.com/compose/>.
- **`psql` reference.** The client (ships inside the container); `\i`, `\d`, `\dt`, `\timing`. <https://www.postgresql.org/docs/16/app-psql.html>.

## Foundations & the bigger picture

- ★ **Martin Kleppmann — *Designing Data-Intensive Applications* (O'Reilly).** The conceptual backbone for systems-of-record vs derived-data systems (the role boundary), row vs columnar storage (OLTP vs OLAP), and why the warehouse/lakehouse is fast. Book home with chapter outlines: <https://dataintensive.net/>.

## How these map to the week

| You are working on… | Read first |
|---|---|
| Lecture 1 (roles, OLTP/OLAP, lineage) | Kleppmann (systems of record, columnar storage); PostgreSQL docs index |
| Lecture 2 (four-step, grain, star/snowflake) | Kimball book + techniques page; `CREATE TABLE`; identity columns |
| Lecture 3 (SCD types, surrogate keys) | Kimball techniques (SCD section); `MERGE`; identity columns |
| Exercises 01–03 | `CREATE TABLE`, identity columns, `MERGE`; the Docker Postgres image |
| Challenge 1 (snowflake trade-off) | `EXPLAIN`; `generate_series`; Kimball star-vs-snowflake |
| Challenge 2 (conformed dimensions) | Kimball conformed-dimensions / bus matrix |
| Mini-project | All starred items; Docker Compose reference |

All curriculum in `C27-CRUNCH-DATA/` is GPL-3.0. Improvements welcome at <https://github.com/CODE-CRUNCH-CLUB>.
