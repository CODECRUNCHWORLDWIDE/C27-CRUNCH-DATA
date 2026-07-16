# Week 3 — Resources

Every resource on this page is **free** to read. The psycopg 3 documentation is open. The PostgreSQL documentation is open. The Python documentation is open. The Kimball Group technique pages are free. The Docker Hub Postgres image is free. Kleppmann's *Designing Data-Intensive Applications* has a free companion site with chapter references (the book itself is paid; the site and its bibliography are free). No resource below requires an account.

## Required reading (work it into your week)

### psycopg 3 — the Postgres driver you are using

- **psycopg 3 documentation home** — the current generation of the Python Postgres adapter; install with `pip install "psycopg[binary]"`:
  <https://www.psycopg.org/psycopg3/docs/>
- **Using COPY TO and COPY FROM** — `cursor.copy()`, `copy.write_row()`, `copy.write()`, the block-vs-row API, the bulk-load path you use for staging:
  <https://www.psycopg.org/psycopg3/docs/basic/copy.html>
- **Transactions management** — `autocommit`, the connection `with` block, `conn.transaction()` explicit blocks, commit-on-clean-exit / rollback-on-exception:
  <https://www.psycopg.org/psycopg3/docs/basic/transactions.html>
- **Passing parameters to SQL queries** — the `%s` / `%(name)s` placeholders, `execute`, `executemany`, and why you never string-format SQL:
  <https://www.psycopg.org/psycopg3/docs/basic/params.html>
- **Connection pools (`psycopg_pool`)** — for the long-lived-service case (not the one-shot batch job, but read it so you know when you would reach for it):
  <https://www.psycopg.org/psycopg3/docs/advanced/pool.html>

### PostgreSQL 16 — the database

- **INSERT (including ON CONFLICT)** — the upsert that makes a load idempotent; the conflict target, the `EXCLUDED` pseudo-table, `DO UPDATE` vs `DO NOTHING`:
  <https://www.postgresql.org/docs/16/sql-insert.html>
  (the ON CONFLICT subsection specifically: <https://www.postgresql.org/docs/16/sql-insert.html#SQL-ON-CONFLICT>)
- **MERGE** — the SQL-standard alternative upsert with `WHEN MATCHED` / `WHEN NOT MATCHED` branches; the shape you meet again in the lakehouse:
  <https://www.postgresql.org/docs/16/sql-merge.html>
- **COPY** — the server-side bulk-load command underneath `cursor.copy()`; the formats, the performance notes:
  <https://www.postgresql.org/docs/16/sql-copy.html>
- **TRUNCATE** — fast whole-table emptying for resetting the staging table each run:
  <https://www.postgresql.org/docs/16/sql-truncate.html>
- **Transactions tutorial** — the conceptual model of atomicity that restartability rests on:
  <https://www.postgresql.org/docs/16/tutorial-transactions.html>
- **Concurrency control (MVCC)** — how Postgres isolates concurrent writers; relevant to the concurrent-re-run stretch goal:
  <https://www.postgresql.org/docs/16/mvcc.html>

### Python — the standard `logging` module

- **Logging HOWTO** — the practical guide: loggers, handlers, formatters, levels, and `extra=`:
  <https://docs.python.org/3/howto/logging.html>
- **`logging` module reference** — `LogRecord`, `Formatter`, `LoggerAdapter`, `StreamHandler`:
  <https://docs.python.org/3/library/logging.html>
- **Logging cookbook** — recipes including custom formatters (the basis of the `JsonFormatter` in Lecture 1) and contextual logging with `LoggerAdapter`:
  <https://docs.python.org/3/howto/logging-cookbook.html>
- **`itertools.batched`** — the standard-library batching helper added in Python 3.12, an alternative to the hand-rolled `batched()` in Lecture 1:
  <https://docs.python.org/3/library/itertools.html#itertools.batched>

### Conceptual foundations — batch, idempotency, and time

- **Kleppmann, *Designing Data-Intensive Applications* — companion site** — chapter 10 (batch processing) and chapter 11 (stream processing: event time vs processing time, the straggler, idempotence and effective exactly-once):
  <https://dataintensive.net/>
- **Kimball Group — Dimensional Modeling Techniques** — the canonical catalog; the ETL and incremental-load techniques live here:
  <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/>
- **Kimball Group — Late-Arriving Dimensions** — the inferred-member / placeholder pattern for a fact that references a not-yet-loaded dimension member:
  <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/late-arriving-dimensions/>

### Running it locally

- **Postgres official Docker image** — `docker run --name pg-week3 -e POSTGRES_PASSWORD=crunch -p 5432:5432 -d postgres:16`; the env vars, the volumes, the tags:
  <https://hub.docker.com/_/postgres>
- **Docker Compose file reference** — for the mini-project's `docker-compose.yml` (the `services`, `depends_on`, `healthcheck` keys):
  <https://docs.docker.com/compose/compose-file/>

## Recommended reading (after the required set)

- **The Log: What every software engineer should know about real-time data's unifying abstraction** (Jay Kreps) — the log as the foundation of incremental processing; the conceptual bridge from this week's batch loader to Week 8's Kafka:
  <https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying>
- **PostgreSQL — Populating a Database** — the official performance guide for bulk loading: `COPY`, removing indexes during load, `maintenance_work_mem`, disabling autocommit:
  <https://www.postgresql.org/docs/16/populate.html>
- **psycopg 3 — Differences from psycopg2** — read this once if you have used the old driver; the COPY and transaction APIs this week relies on are different:
  <https://www.psycopg.org/psycopg3/docs/basic/from_pg2.html>
- **dbt — incremental models** (forward reference to Week 5) — how the ELT world expresses the same incremental + late-data ideas as configuration rather than Python:
  <https://docs.getdbt.com/docs/build/incremental-models>
- **PostgreSQL — Write-Ahead Logging (WAL)** — why a giant single transaction stresses the WAL, the mechanism behind batch-size discipline:
  <https://www.postgresql.org/docs/16/wal-intro.html>

## Tools you will install this week

- **Python 3.12** — verify with `python --version` (you want `3.12.x`). Install from <https://www.python.org/downloads/> if needed.
- **psycopg 3** — `pip install "psycopg[binary]"` (the `[binary]` extra ships a prebuilt libpq, so no C toolchain). Verify: `python -c "import psycopg; print(psycopg.__version__)"` (expect `3.x`).
- **PostgreSQL 16 via Docker** — `docker run --name pg-week3 -e POSTGRES_PASSWORD=crunch -p 5432:5432 -d postgres:16`. Verify: `docker exec pg-week3 pg_isready` (returns `accepting connections`).
- **`psql`** — use the one inside the container with no local install: `docker exec -it pg-week3 psql -U postgres`.
- **(Optional) `pytest`** — for the mini-project's idempotency test: `pip install pytest`.

## Citations policy

This curriculum cites the psycopg 3 documentation, the PostgreSQL 16 documentation, the Python standard-library documentation, the Kimball Group technique pages, Kleppmann's *DDIA* companion site, and the Docker Hub Postgres image as its primary references. Every code example in the lecture notes and exercises is traced to one of these. When a third-party essay (Jay Kreps's "The Log") is the clearer reference, it is cited explicitly with a URL — never paraphrased without attribution. If a citation is missing from a section of these notes, treat it as a bug and open an issue against the C27 curriculum repository at <https://github.com/CODE-CRUNCH-CLUB>.
