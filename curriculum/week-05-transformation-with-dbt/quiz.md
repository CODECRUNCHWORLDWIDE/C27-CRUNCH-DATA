# Week 5 — Quiz

Ten multiple-choice questions on `ref` vs `source`, materializations, tests, snapshots, the staging/intermediate/mart layering, incremental models, and lineage. Take it with the lecture notes closed. Aim for 9/10 before the mini-project. Answer key at the bottom — do not peek.

---

**Q1.** What does dbt do when it encounters `{{ ref('stg_orders') }}` in a model?

- A) It runs `stg_orders` immediately and inlines the result as a literal table.
- B) It resolves to the fully-qualified table/view name for the current target AND records a DAG edge from this model to `stg_orders`.
- C) It downloads `stg_orders` from dbt Cloud's metadata store.
- D) Nothing — `ref` is only documentation and has no effect on build order.

---

**Q2.** You have a raw table loaded by an upstream Python job that dbt does not create. The correct way to reference it in a model is:

- A) Hard-code `select * from raw.orders`.
- B) `{{ ref('raw_orders') }}`.
- C) `{{ source('raw', 'orders') }}`, after declaring it in a `sources` YAML block.
- D) `{{ seed('orders') }}`.

---

**Q3.** Which statement about the staging layer is correct?

- A) Staging models should join multiple sources to denormalize early.
- B) Staging models clean and rename exactly one source each, with no joins or aggregation.
- C) Staging models must always be materialized as tables.
- D) Staging models contain the dimensional star schema.

---

**Q4.** An `ephemeral` materialization means:

- A) The model is rebuilt as a temporary table on every query.
- B) The model produces no database object; its SQL is inlined as a CTE into every model that refs it.
- C) The model is stored only in memory and lost on restart.
- D) The model is a view that auto-expires after 24 hours.

---

**Q5.** A dbt test is considered to **pass** when its query:

- A) Returns at least one row.
- B) Returns exactly one row.
- C) Returns zero rows.
- D) Returns a boolean `true`.

---

**Q6.** Which test enforces that every `fct_orders.customer_sk` exists in `dim_customer.customer_sk`?

- A) `unique`.
- B) `not_null`.
- C) `accepted_values`.
- D) `relationships` (with `to: ref('dim_customer')`, `field: customer_sk`).

---

**Q7.** In an incremental model, what is the role of `is_incremental()`?

- A) It deletes the table before every run.
- B) It returns `true` only when the table already exists and the run is not a `--full-refresh`, gating the high-water-mark `where` clause that limits processing to new rows.
- C) It converts the model to a view.
- D) It runs the model's tests.

---

**Q8.** You run an incremental `fct_orders` with `incremental_strategy='delete+insert'` and `unique_key='order_id'`, then re-run the exact same input batch. The row count:

- A) Doubles, because incremental always appends.
- B) Stays the same, because dbt deletes existing rows matching the incoming `order_id`s and re-inserts them (an upsert).
- C) Drops to zero, because the batch is treated as a delete.
- D) Is undefined.

---

**Q9.** In a dbt snapshot, a row whose `dbt_valid_to IS NULL` represents:

- A) A deleted record.
- B) An invalid record that failed a test.
- C) The currently-active version of that natural key.
- D) A record with no surrogate key.

---

**Q10.** `dbt build` differs from running `dbt run` then `dbt test` because `dbt build`:

- A) Skips tests entirely for speed.
- B) Runs all models first, then all tests, regardless of dependencies.
- C) Interleaves models and their tests in DAG order, and **skips** models downstream of a failed test.
- D) Only works in dbt Cloud.

---

## Answer key

<details>
<summary>Click to reveal answers</summary>

1. **B** — `ref` does two things at once: it resolves to the correct fully-qualified name *for the active target* (so the same model points at dev objects in dev and prod objects in prod) and it declares a dependency edge that dbt uses to build the DAG and order the run. It does not inline (that is ephemeral) and it is emphatically not just documentation. See Lecture 1 §5. (<https://docs.getdbt.com/reference/dbt-jinja-functions/ref>)

2. **C** — A table dbt reads but does not create is a **source**. Declare it in a `sources:` YAML block and reference it with `{{ source('raw','orders') }}`. This gives a stable name, enables `dbt source freshness`, and makes the table appear as a true origin in lineage. Hard-coding (A) loses all three; `ref` (B) is for dbt models; `seed` (D) is for CSVs you load. Lecture 1 §4. (<https://docs.getdbt.com/docs/build/sources>)

3. **B** — Staging is the trust boundary: one model per source table, clean and rename only, no joins, no aggregation, no business logic. Joins belong in intermediate, the star in marts. Staging is typically `view` (cheap, always fresh), not table (C). Lecture 1 §6.1. (<https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview>)

4. **B** — Ephemeral produces no database object; dbt inlines the model's compiled SQL as a CTE into every consumer that refs it. Consequences: you cannot test it directly and cannot `run --select` it alone. Good for intermediate glue. Lecture 2 §1.3. (<https://docs.getdbt.com/docs/build/materializations>)

5. **C** — A dbt test returns the rows that *violate* the assertion. Zero offending rows = pass; one or more = fail (and a non-zero exit). This "return the bad rows" model is why both generic and singular tests are just `SELECT`s. Lecture 2 §2. (<https://docs.getdbt.com/docs/build/data-tests>)

6. **D** — `relationships` is the referential-integrity / foreign-key test: every value in the column must exist in the referenced model's column. `unique` and `not_null` are key-shape checks; `accepted_values` checks set membership. Lecture 2 §2.1. (<https://docs.getdbt.com/docs/build/data-tests>)

7. **B** — `is_incremental()` returns `true` only when the model's table already exists *and* the run is not `--full-refresh`. The `{% if is_incremental() %} where ... {% endif %}` block is the high-water-mark filter — the dbt expression of Week 3's watermark. On the first run (or a full refresh) it is false and the whole table is built. Lecture 2 §1.4. (<https://docs.getdbt.com/docs/build/incremental-models>)

8. **B** — With `delete+insert` on `unique_key='order_id'`, dbt deletes existing rows whose `order_id` matches the incoming batch and re-inserts them — an upsert. Re-running the same batch deletes-then-reinserts the same rows, so the count is unchanged. That is the idempotency property from Week 3. `append` (A) would double-count. Lecture 2 §1.4 and Challenge 01. (<https://docs.getdbt.com/docs/build/incremental-models>)

9. **C** — In a Type-2 snapshot, `dbt_valid_to IS NULL` marks the current version of that natural key; superseded versions have a non-null `dbt_valid_to`. This is the open-ended current row, identical to your Week 1 current-flag / open `effective_to`. Lecture 2 §3.1. (<https://docs.getdbt.com/docs/build/snapshots>)

10. **C** — `dbt build` interleaves models and their tests in dependency order: build a model, test it, and only proceed downstream if its tests pass. A failed test causes dbt to **skip** everything downstream, so a broken staging model never silently feeds a mart. `run` then `test` (B's behavior, roughly) does not give you this skip-on-failure ordering. It is dbt-core, not Cloud-only (D). Lecture 2 §2.4. (<https://docs.getdbt.com/docs/build/data-tests>)

</details>

---

## Self-assessment

- **9–10 correct** — Solid. You understand refs vs sources, the four materializations, the test model, snapshots, and `dbt build` ordering. Start the mini-project.
- **7–8 correct** — Good enough to proceed, but re-read the lecture section behind each miss before the lab; the lab assumes all of this.
- **5–6 correct** — Re-read Lecture 1 §5–6 (refs and layering) and Lecture 2 §1–2 (materializations and tests), then retake. These are load-bearing for everything downstream.
- **Below 5** — Do not start the mini-project yet. Re-read all three lectures, run the three exercises against DuckDB, and retake. The concepts here are the foundation of all of Phase II.
