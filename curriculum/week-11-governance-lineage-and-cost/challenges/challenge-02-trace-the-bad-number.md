# Challenge 02 — Trace the Bad Number

> The exec dashboard says one revenue number; Finance says another. You did not build the model. Using end-to-end lineage, trace the wrong number from the dashboard back to the source that produced it and write up the root-cause path.

## Why it matters

This is the interview question — "tell me about a time you debugged a data incident" — and the daily reality of owning a pipeline. When a number is wrong, the only useful first question is *where did it come from*, and lineage is the tool that answers it without reading every model's SQL by hand. An engineer who can walk a lineage graph from a dashboard tile to a root-cause column in fifteen minutes is worth a great deal more than one who greps the warehouse for four hours. This challenge makes you do it once, with evidence, so you can do it under pressure later.

## The setup

Your instructor (or you, on a partner's project) introduces a **deliberate defect** upstream that makes a dashboard metric wrong. Pick one to inject if you are doing this solo:

- A duplicate-loading bug in a staging model that inflates a joined dimension (e.g. duplicate FX rates double-counting revenue).
- A unit/scale error (cents vs dollars) introduced in an intermediate model.
- A silently-changed join key that drops or fans out rows.

The dashboard now shows a number that disagrees with a trusted reference. Your job is to find *why*, using lineage, and produce the traced path.

## Procedure

### Phase 0 — Build the lineage (do this BEFORE the incident) (≈45 min)

1. Ensure dbt **exposures** declare the dashboard as a node (`exposures.yml`) so the graph reaches the consumer.
2. Generate and serve dbt docs (`dbt docs generate` / `dbt docs serve`) — confirm the DAG renders from sources to the exposure.
3. Wire **OpenLineage**: run dbt via `dbt-ol run` into a running **Marquez** (`OPENLINEAGE_URL=http://localhost:5000`), and if your ingestion is in Airflow, enable `apache-airflow-providers-openlineage`. Confirm jobs, datasets, and run history appear in the Marquez UI.

### Phase 1 — Scope the blast radius (table-level) (≈20 min)

4. From the wrong tile, find its exposure and its single upstream model. Walk the table-level DAG upstream and list every candidate model (`dbt ls --select +exposure:<name>`).

### Phase 2 — Localize to a column (column-level) (≈30 min)

5. Trace the specific metric column down through the column-level lineage (catalog column-lineage view, or the OpenLineage `columnLineage` facet in Marquez) until you reach the leaf source columns it depends on. You should end with one or two columns, not five tables.

### Phase 3 — Correlate with change and confirm (≈30 min)

6. In Marquez, inspect the **run history** of the jobs that produce those leaf columns. Find the run whose code version (`sql` facet) or row counts (`dataQuality` facet) changed, lining up with when the number went wrong.
7. Confirm the root cause by querying the suspect column/table directly (e.g. count duplicates, check the scale).

### Phase 4 — Write the root-cause path (≈25 min)

8. Document the *traced path*, not just the fix.

## Deliverable

A report (`challenge-02-root-cause.md`) containing:

1. The symptom: the two disagreeing numbers and which is trusted.
2. The **traced path**, written as a chain: `dashboard → mart.<metric> → int.<column> → stg.<column> → source.<column> → the change that broke it`.
3. Evidence: a dbt-docs DAG screenshot (table-level blast radius), a column-lineage screenshot (the metric's leaf dependencies), and a Marquez run-history screenshot showing the changed run.
4. The confirming query and its output (e.g. the duplicate count).
5. Two sentences on how lineage shortened the search — what you would have had to do without it.

## Pass criteria

- [ ] dbt docs + an exposure render the dashboard as a node in the DAG.
- [ ] OpenLineage events from at least dbt land in Marquez and the graph is visible.
- [ ] The writeup gives a complete column-level traced path from the dashboard metric to the root-cause source column.
- [ ] The root cause is confirmed with a direct query, not just asserted.
- [ ] Evidence screenshots (DAG, column lineage, run history) are included.

## References

- [`../lecture-notes/02-lineage-catalogs-and-the-incident.md`](../lecture-notes/02-lineage-catalogs-and-the-incident.md) — table vs column lineage, dbt docs + exposures, OpenLineage + Marquez, the worked incident walkthrough.
- dbt — documentation and exposures. <https://docs.getdbt.com/docs/build/documentation> · <https://docs.getdbt.com/docs/build/exposures>
- OpenLineage — spec and the `columnLineage` facet. <https://openlineage.io/docs/> · <https://openlineage.io/docs/spec/facets/>
- Marquez — the metadata server and UI. <https://marquezproject.ai/docs> · <https://github.com/MarquezProject/marquez>
- Airflow OpenLineage provider. <https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/index.html>
