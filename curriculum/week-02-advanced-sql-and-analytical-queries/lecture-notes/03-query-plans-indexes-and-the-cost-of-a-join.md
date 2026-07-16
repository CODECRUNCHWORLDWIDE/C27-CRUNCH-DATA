# Lecture 3 — Query Plans, Indexes, and the Cost of a Join

> **Time:** 2 hours. Plan-reading in one sitting; indexes and join algorithms in a second. **Prerequisites:** Lectures 1 and 2; the Week-1 retail star schema with a fact table large enough to make scans visible (the mini-project loads ~5M fact rows; even 100k shows the effects). **Citations:** PostgreSQL `EXPLAIN` reference at <https://www.postgresql.org/docs/16/sql-explain.html>, the "Using EXPLAIN" walkthrough at <https://www.postgresql.org/docs/16/using-explain.html>, indexes at <https://www.postgresql.org/docs/16/indexes.html>, DuckDB `EXPLAIN` at <https://duckdb.org/docs/guides/meta/explain>, and Martin Kleppmann, *Designing Data-Intensive Applications*, for storage and index internals at <https://dataintensive.net/>.

## 1. Read the plan before you guess

The discipline of this lecture, and of the rest of the course, is one sentence: **you do not get to say why a query is slow until you have read the plan.** "I think it's the join" is a guess. A plan that shows a `Nested Loop` with `actual rows=4012398` where the estimate said `rows=11` is evidence. Tuning without reading the plan is guessing with extra steps.

PostgreSQL gives you two commands. `EXPLAIN` shows the plan the optimizer *would* use, with cost estimates, *without running the query*. `EXPLAIN ANALYZE` *runs* the query and reports the actual time and actual row counts alongside the estimates, so you can see where the optimizer's guess was wrong. The reference is <https://www.postgresql.org/docs/16/sql-explain.html>; the indispensable walkthrough is <https://www.postgresql.org/docs/16/using-explain.html>.

```sql
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT TEXT)
SELECT s.region, SUM(f.extended_price)
FROM   fact_sales f
JOIN   dim_store s ON s.store_key = f.store_key
GROUP  BY s.region;
```

Always run `EXPLAIN ANALYZE` with `BUFFERS` on. Buffers tell you how many 8KB pages were read from shared memory (`shared hit`) versus from disk (`shared read`) — the truest measure of how much work the query did, more honest than wall-clock time on a warm cache.

## 2. Anatomy of a plan node

A plan is a tree, printed inside-out: the most indented node runs first, feeds its parent, and so on up to the top. Read a node like this:

```text
Seq Scan on fact_sales f  (cost=0.00..86458.00 rows=5000000 width=16)
                          (actual time=0.012..֊412.380 rows=5000000 loops=1)
   Buffers: shared hit=128 read=36330
```

- `cost=0.00..86458.00` — two numbers in arbitrary "cost units." The first is the *startup cost* (work before the first row can be emitted); the second is the *total cost* (work to return all rows). Units are calibrated so that reading one sequential page costs 1.0 (`seq_page_cost`); a random page costs 4.0 (`random_page_cost`) by default. The numbers are only meaningful *relative to each other*, for comparing plans.
- `rows=5000000` — the optimizer's **estimate** of how many rows this node emits. This number drives every downstream decision.
- `width=16` — estimated average row width in bytes.
- `actual time=0.012..412.380` — startup and total time in milliseconds (only present with `ANALYZE`).
- `actual ... rows=5000000 loops=1` — how many rows the node *actually* emitted, and how many times it ran. `loops` matters enormously on the inner side of a nested loop: the printed `actual rows` is *per loop*, so multiply by `loops` for the true total.
- `Buffers` — pages touched. `read` means it went to disk.

**The single most important habit: compare estimated `rows` to `actual rows` at every node.** When they agree, the optimizer understood the data and probably picked a good plan. When they diverge by 100x or 1000x, the optimizer was flying blind and likely picked a bad plan — and that node is where your bug lives.

## 3. The node types you will meet

### Scans — how a table is read

- **`Seq Scan`** — read every page of the table top to bottom. Cost is proportional to table size, *independent* of how many rows match. Correct when you need most of the table (a `SUM` over everything), or the table is tiny, or no useful index exists.
- **`Index Scan`** — descend a B-tree to find matching index entries, then fetch each matching row from the heap (the table). Great when *few* rows match; terrible when many match, because each heap fetch is a random page read (cost 4.0 each).
- **`Index Only Scan`** — the query's needed columns are *all in the index*, so PostgreSQL never touches the heap at all. The fastest scan when applicable; this is what a *covering* index buys you (section 6).
- **`Bitmap Heap Scan`** (usually under a `Bitmap Index Scan`) — when a moderate number of rows match, PostgreSQL first builds a bitmap of which *pages* contain matches, then reads those pages in physical order. This avoids the random-fetch penalty of a plain index scan when the matches are spread out. It is the optimizer's middle ground between a sharp index scan and a full seq scan.

### Joins — how two inputs are combined

- **`Nested Loop`** — for each row of the outer input, probe the inner input. Cost ≈ `outer_rows × cost_of_one_inner_lookup`. Cheap when the outer side is *tiny* and the inner side has an index to probe. Catastrophic when the outer side is large — the inner side gets scanned millions of times. This is the join that "explodes."
- **`Hash Join`** — build an in-memory hash table on the smaller input (the *build* side), then stream the larger input through it (the *probe* side). Cost ≈ `build_rows + probe_rows`, linear. The default choice for joining two large unsorted inputs. Watch for "Batches: N" — if N > 1 the hash table spilled to disk because `work_mem` was too small.
- **`Merge Join`** — sort both inputs on the join key (or read them already-sorted from indexes), then walk them together like a zipper. Good when both inputs are already sorted, or for very large joins where the sort amortizes. Cost includes the sorts unless an index provides the order for free.

### Everything else

- **`Sort`** — orders rows for `ORDER BY`, a merge join, or `DISTINCT`. The plan tells you `Sort Method: quicksort Memory: 25kB` (fit in RAM, good) or `Sort Method: external merge Disk: 14200kB` (**spilled to disk** because the sort exceeded `work_mem` — a major slowdown and a clear tuning target).
- **`Aggregate`** / **`HashAggregate`** / **`GroupAggregate`** — compute `GROUP BY` / aggregate results. `HashAggregate` hashes by group key (no sort needed); `GroupAggregate` requires sorted input. A `HashAggregate` that exceeds `work_mem` also spills.
- **`WindowAgg`** — computes window functions (Lecture 1). It usually sits above a `Sort` that orders rows by the `PARTITION BY` / `ORDER BY` of the window.
- **`CTE Scan`** — reads a materialized CTE (Lecture 2). Its presence tells you the CTE was *not* inlined.

## 4. A plan that is correct, and a plan that is wrong

**Correct seq scan.** Summing revenue per region touches essentially the whole fact table, so a seq scan is the *right* answer — an index would be slower:

```text
HashAggregate  (cost=98958.00..98962.50 rows=4 width=40)
               (actual time=701.2..701.2 rows=4 loops=1)
  Group Key: s.region
  ->  Hash Join  (cost=14.00..86458.00 rows=5000000 width=14)
                 (actual time=0.3..520.1 rows=5000000 loops=1)
        Hash Cond: (f.store_key = s.store_key)
        ->  Seq Scan on fact_sales f  (rows=5000000)   -- correct: we need all rows
        ->  Hash  (rows=50)
              ->  Seq Scan on dim_store s  (rows=50)    -- tiny dimension, hashed
```

Estimates match actuals at every node; the `Hash Join` builds on the 50-row dimension and probes the 5M-row fact. Nothing to fix.

**Wrong: a seq scan that should be an index seek.** Now fetch one customer's orders:

```text
Seq Scan on fact_sales f  (cost=0.00..98958.00 rows=120 width=16)
                          (actual time=0.2..438.9 rows=137 loops=1)
   Filter: (customer_key = 80421)
   Rows Removed by Filter: 4999863     -- read 5M rows to keep 137
   Buffers: shared read=36330
```

`Rows Removed by Filter: 4999863` is the smoking gun: PostgreSQL read every one of five million rows and threw away all but 137, because there is no index on `customer_key`. The fix is one statement:

```sql
CREATE INDEX idx_fact_customer ON fact_sales (customer_key);
ANALYZE fact_sales;   -- refresh statistics so the planner notices the new index
```

Re-run and the plan becomes:

```text
Bitmap Heap Scan on fact_sales f  (cost=5.2..480.1 rows=137 width=16)
                                  (actual time=0.05..0.31 rows=137 loops=1)
   Recheck Cond: (customer_key = 80421)
   Buffers: shared hit=140
   ->  Bitmap Index Scan on idx_fact_customer  (actual time=0.03..0.03 rows=137)
         Index Cond: (customer_key = 80421)
```

438ms to 0.3ms, and `shared read=36330` collapses to `shared hit=140`. That is the entire loop of this week: read the plan, find the scan reading rows it throws away, add the index, re-measure.

## 5. Indexes: B-trees and what they cost

PostgreSQL's default index is a **B-tree** — a balanced tree of sorted keys with `O(log n)` lookup, documented at <https://www.postgresql.org/docs/16/indexes.html>. Kleppmann's *Designing Data-Intensive Applications* (chapter 3, "Storage and Retrieval", <https://dataintensive.net/>) is the canonical explanation of *why* a B-tree gives log-time lookups and how it differs from a log-structured store: a B-tree stores keys in sorted pages, each lookup descends a few levels, and a range scan walks the leaf level in order.

The trade-off every index makes: it speeds up reads that match its key and *slows down* writes, because every `INSERT`/`UPDATE`/`DELETE` must also maintain the index, and the index consumes disk. An index you never use is pure cost. So you index columns that appear in `WHERE`, `JOIN`, and `ORDER BY` clauses of queries you actually run, and no others.

**When a seq scan is correct and an index would lose:** if a query needs more than roughly 5–10% of a table's rows, the random heap fetches of an index scan cost *more* than just reading the whole table sequentially. The optimizer knows this and will *choose* the seq scan. Forcing an index there (people try, with `enable_seqscan = off`) makes the query slower. The seq scan is not a bug; it is sometimes the answer.

## 6. Composite, partial, and covering indexes

**Composite (multi-column) index** — `CREATE INDEX ix ON fact_sales (store_key, date_key)`. Column *order* is everything: this index serves `WHERE store_key = ?`, `WHERE store_key = ? AND date_key = ?`, and `WHERE store_key = ? ORDER BY date_key`, but it does **not** efficiently serve `WHERE date_key = ?` alone (the leading column is missing — like looking up a phone book by first name). The rule: put the column used for *equality* first, the column used for *range/sort* second. Documented under multicolumn indexes at <https://www.postgresql.org/docs/16/indexes-multicolumn.html>.

**Partial index** — `CREATE INDEX ix ON fact_sales (customer_key) WHERE discount_amount > 0`. Indexes only the rows matching the predicate. Smaller, cheaper to maintain, and ideal when your queries always filter on the same condition (e.g. only discounted sales).

**Covering / `INCLUDE` index** — `CREATE INDEX ix ON fact_sales (customer_key) INCLUDE (extended_price)`. The `INCLUDE` columns are stored in the index leaves but not part of the key. A query that filters on `customer_key` and reads only `extended_price` can be answered entirely from the index — an `Index Only Scan`, no heap fetch at all. Covering indexes are documented at <https://www.postgresql.org/docs/16/indexes-index-only-scans.html>.

## 7. The cost of a join, and the join that explodes

The optimizer's hardest job is estimating *how many rows a join will produce*, because that drives which join algorithm it picks and in what order it joins tables. It estimates from statistics gathered by `ANALYZE` — per-column histograms, most-common-values, and distinct-value counts.

When the estimate is right, it picks well: tiny outer side → `Nested Loop` with an indexed inner probe; two large inputs → `Hash Join`. When the estimate is **wrong** — stale statistics, or two columns correlated in a way single-column stats can't model — it picks a `Nested Loop` expecting 11 rows on the outer side, gets 4 million, and scans the inner side 4 million times. The query that should take 50ms takes 50 seconds. The tell in `EXPLAIN ANALYZE`:

```text
Nested Loop  (cost=0.4..931.0 rows=11 width=20)
             (actual time=0.1..51284.0 rows=4012398 loops=1)   -- est 11, actual 4,012,398
  ->  Seq Scan on big_outer  (rows=4012398)
  ->  Index Scan on dim_product  (actual time=0.01..0.01 rows=1 loops=4012398)  -- 4M loops!
```

`rows=11` estimate, `rows=4012398` actual, and `loops=4012398` on the inner index scan — the inner side ran four million times. The fixes, in order of preference: (1) run `ANALYZE` to refresh statistics; (2) add `CREATE STATISTICS` for correlated columns so the planner models the correlation (<https://www.postgresql.org/docs/16/sql-createstatistics.html>); (3) ensure the join columns are indexed so even a nested loop is cheap; (4) as a last resort, restructure the query. The first thing to try is almost always `ANALYZE` — a stale-stats explosion is the most common cause and the cheapest fix. You will diagnose exactly this in Challenge 2.

## 8. Reading a DuckDB plan, and why columnar reads fewer bytes

DuckDB is an in-process **columnar, vectorized** analytical engine. PostgreSQL stores rows together (row-oriented): to sum one column it still reads whole rows off disk. DuckDB stores each column together (column-oriented): a `SUM(extended_price)` reads *only* the `extended_price` column and skips every other byte. For wide fact tables and analytical aggregates, that is a large constant-factor win, and it is the storage idea Kleppmann describes as column-oriented storage (<https://dataintensive.net/>). DuckDB also processes data in *vectors* (batches of ~2048 values) rather than one row at a time, which keeps the CPU pipeline full.

DuckDB's plan command is the same idea with different output:

```sql
EXPLAIN ANALYZE
SELECT region, SUM(extended_price)
FROM fact_sales JOIN dim_store USING (store_key)
GROUP BY region;
```

```text
┌─────────────────────────────┐
│      HASH_GROUP_BY           │
│   Groups: region             │
│   ... 4 rows                 │
└──────────────┬──────────────┘
┌──────────────┴──────────────┐
│         HASH_JOIN            │
│   store_key = store_key      │
└──────────────┬──────────────┘
┌──────────────┴──────────────┐
│         SEQ_SCAN             │
│   fact_sales                 │
│   Projection: extended_price,│   <- reads ONLY these columns
│               store_key      │
└─────────────────────────────┘
```

The `Projection` line under the scan is the columnar payoff: DuckDB reads only the two columns the query needs, not the whole row. The plan command and its output are documented at <https://duckdb.org/docs/guides/meta/explain>. The practical lesson: the *same* analytical query — a big aggregate over a wide fact table — is often dramatically faster on DuckDB than on a single Postgres box, not because DuckDB is "better," but because columnar storage reads fewer bytes for that *shape* of query. Postgres wins on the OLTP shape (fetch a few whole rows by key); DuckDB wins on the OLAP shape (aggregate a few columns over many rows). Knowing which engine fits which shape is a senior-engineer judgment.

## 9. A tuning checklist

When a query is slow, in order:

1. `EXPLAIN (ANALYZE, BUFFERS)` it. Read the tree inside-out.
2. Find the node where **estimated rows and actual rows diverge most**. That is usually the problem.
3. Look for `Rows Removed by Filter: <huge>` under a `Seq Scan` → a missing index.
4. Look for `Sort Method: external merge Disk:` or `Batches: N (N>1)` → a spill; raise `work_mem` for the session or reduce the data sorted/hashed.
5. Look for a `Nested Loop` with huge `loops` on the inner side → a cardinality explosion; run `ANALYZE`, consider `CREATE STATISTICS`, or index the join key.
6. Make **one** change, re-run `EXPLAIN ANALYZE`, and compare. Never change two things at once — you will not know which one helped.

## Exercise pointer

Go to [`exercises/exercise-03-read-the-plan.sql`](../exercises/exercise-03-read-the-plan.sql). You will run `EXPLAIN ANALYZE` on a deliberately slow query, read it to find the `Seq Scan` with `Rows Removed by Filter`, add the correct index in the marked answer slot, and re-measure to prove the improvement. Solutions with before/after plan excerpts are in [`exercises/SOLUTIONS.md`](../exercises/SOLUTIONS.md), and Challenge 2 makes you tune a harder one and report the diff.

## Summary

- `EXPLAIN` shows the planned cost; `EXPLAIN ANALYZE` runs it and shows actual rows and time. Always add `BUFFERS`.
- A plan is a tree read inside-out. The most valuable habit is comparing estimated vs actual `rows` at every node; large divergence is where the bug lives.
- Scans: `Seq Scan` (all pages, correct when you need most rows), `Index Scan` (few matches), `Index Only Scan` (covering index, no heap), `Bitmap Heap Scan` (moderate, scattered matches).
- Joins: `Nested Loop` (tiny outer + indexed inner; explodes when outer is large), `Hash Join` (two large inputs, linear), `Merge Join` (pre-sorted inputs). Watch for spills (`Batches > 1`, `external merge Disk:`).
- A B-tree gives `O(log n)` lookups; an index speeds matching reads and slows writes. A seq scan is *correct* when a query needs more than ~5–10% of the table.
- Composite index column order matters (equality column first); partial indexes shrink the index; covering/`INCLUDE` indexes enable `Index Only Scan`.
- The join explodes when the row estimate is wrong; the first fix is `ANALYZE`, then `CREATE STATISTICS`, then indexing the join key.
- DuckDB's columnar/vectorized model reads only the needed columns; the same big-aggregate query is often far faster there. Postgres wins OLTP shapes, DuckDB wins OLAP shapes.

Cited references: PostgreSQL `EXPLAIN` <https://www.postgresql.org/docs/16/sql-explain.html>; "Using EXPLAIN" <https://www.postgresql.org/docs/16/using-explain.html>; indexes <https://www.postgresql.org/docs/16/indexes.html>; multicolumn indexes <https://www.postgresql.org/docs/16/indexes-multicolumn.html>; index-only scans <https://www.postgresql.org/docs/16/indexes-index-only-scans.html>; `CREATE STATISTICS` <https://www.postgresql.org/docs/16/sql-createstatistics.html>; DuckDB `EXPLAIN` <https://duckdb.org/docs/guides/meta/explain>; Kleppmann, *Designing Data-Intensive Applications* <https://dataintensive.net/>.
