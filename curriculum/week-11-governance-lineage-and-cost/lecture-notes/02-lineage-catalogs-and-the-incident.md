# Lecture 11.2 — Lineage, Catalogs, and the Incident

> "Lineage is the first thing you reach for during an incident."

It is 9:14 AM. The VP of Sales emails: "Yesterday's revenue on the exec dashboard says $4.1M. Finance closed the month at $3.7M. Which one is wrong?" You did not write the dashboard. You did not build the model behind it. You have one question to answer and a clock running: *where did this number come from?* The tool that answers it is **lineage** — the recorded map of which data flowed into which, column by column, from the source system to the pixel on the dashboard. This lecture is about producing that map and reading it under pressure.

---

## 1. Table-level vs column-level lineage

Lineage comes at two resolutions, and the difference matters in an incident.

**Table-level lineage** answers *which datasets feed which*. The `exec_revenue` dashboard reads `mart_daily_revenue`, which reads `int_orders_enriched`, which reads `stg_orders` and `stg_fx_rates`, which read the raw `orders` and `fx_rates` source tables. It is a directed acyclic graph of datasets. It tells you the *blast radius* — if `stg_fx_rates` is wrong, everything downstream of it is suspect — and it narrows the search from "the whole warehouse" to "these five models."

**Column-level lineage** answers *which fields feed which*. It records that `mart_daily_revenue.total_revenue` is computed as `sum(int_orders_enriched.amount_usd)`, and that `amount_usd` is `orders.amount * fx_rates.rate`. This is the resolution that closes an incident: it tells you the wrong $4.1M came from `amount_usd`, which depends on `fx_rates.rate`, which — you now check — was deployed yesterday with a model change. Table-level gets you to the neighborhood; column-level gets you to the house.

Both are *metadata about data flow*. Neither moves data; both are produced as a side effect of the transformation tools knowing what they read and wrote.

---

## 2. dbt's built-in lineage

You already produce lineage and may not have noticed. Every time a dbt model uses `ref()` or `source()`, dbt records an edge in its dependency graph. That graph *is* table-level lineage, and dbt ships the tooling to see it.

### 2.1 The DAG and `dbt docs`

```bash
# Compile the project, build the catalog (column types from the warehouse), and
# emit the static documentation site including the lineage graph.
dbt docs generate

# Serve it locally — the "View Lineage Graph" button renders the full DAG.
dbt docs serve --port 8080
```

`dbt docs generate` writes two artifacts you will use programmatically:

- **`target/manifest.json`** — the full project graph: every model, its `depends_on` (the `ref`/`source` edges), its compiled SQL, its `meta` tags, and its tests. This is the machine-readable lineage. Tools (and lecture 3's PII tagging) read it.
- **`target/catalog.json`** — column names and types for every model, harvested from the warehouse. Combined with the manifest it is what lets some tools infer column-level lineage.

### 2.2 Exposures — extending lineage past dbt

dbt's DAG ends at the last model. The dashboard lives outside dbt, so by default lineage stops short of the thing the VP is looking at. **Exposures** close that gap — they declare a downstream consumer as a node in the dbt graph:

```yaml
# models/exposures.yml
exposures:
  - name: exec_revenue_dashboard
    label: "Executive Revenue Dashboard"
    type: dashboard
    maturity: high
    url: https://bi.internal/dashboards/exec-revenue
    owner:
      name: Analytics Platform
      email: data-platform@example.com
    depends_on:
      - ref('mart_daily_revenue')
    description: "Daily revenue tile the exec team reads at 9 AM."
```

Now `dbt docs` shows the dashboard as a node, and `dbt ls --select +exposure:exec_revenue_dashboard` lists every model upstream of it — the exact set you must inspect when that dashboard is wrong. Exposures also let you run only what feeds a given dashboard: `dbt build --select +exposure:exec_revenue_dashboard`.

dbt's native lineage is table-level out of the box; column-level lineage from dbt is available in dbt Cloud and via OSS tools that parse the compiled SQL in `manifest.json`. For column-level across *all* your tools — not just dbt — you reach for OpenLineage.

---

## 3. OpenLineage — one lineage model across every tool

dbt knows its own lineage. Airflow knows which tasks ran. Spark knows which DataFrames it read and wrote. The problem is they each know it in their own format, and an incident crosses all three. **OpenLineage** is an open standard for emitting lineage as a common event model, so Airflow, dbt, and Spark all speak the same language into one metadata server.

### 3.1 The event model — run, job, dataset, facets

OpenLineage models the world as three core entities plus extensible *facets*:

- A **Job** is a process definition — a dbt model, an Airflow task, a Spark application. It is stable across runs.
- A **Run** is one execution of a Job, with a UUID, a state (`START` / `RUNNING` / `COMPLETE` / `FAIL` / `ABORT`), and timestamps.
- A **Dataset** is an input or output — a table, a file, a topic. Identified by a `namespace` (e.g. the warehouse) and a `name` (e.g. `db.mart_daily_revenue`).
- **Facets** are typed JSON blobs attached to any of the above to carry detail: a `schema` facet (columns and types), a `columnLineage` facet (which output columns derive from which input columns — *this is column-level lineage*), a `dataQuality` facet (row counts, assertions), a `sql` facet (the query that ran).

A single OpenLineage event is a JSON document like:

```json
{
  "eventType": "COMPLETE",
  "eventTime": "2026-06-18T02:00:13.412Z",
  "run":  { "runId": "d46e465b-...-9f2a" },
  "job":  { "namespace": "dbt", "name": "mart_daily_revenue" },
  "inputs":  [ { "namespace": "duckdb://warehouse", "name": "int_orders_enriched" } ],
  "outputs": [ {
    "namespace": "duckdb://warehouse",
    "name": "mart_daily_revenue",
    "facets": {
      "columnLineage": { "fields": {
        "total_revenue": { "inputFields": [
          { "namespace": "duckdb://warehouse", "name": "int_orders_enriched", "field": "amount_usd" }
        ] } }
      }
    }
  } ]
}
```

Read that `columnLineage` facet and you have answered the incident question for `total_revenue` in one document.

### 3.2 Marquez — the metadata server

OpenLineage events have to go somewhere. **Marquez** is the reference OpenLineage server: it receives events on an HTTP endpoint, stores the run/job/dataset graph in Postgres, and renders an interactive lineage UI (jobs, datasets, run history, and the edges between them). It is the reference implementation of the OpenLineage standard and runs in Docker:

```bash
git clone https://github.com/MarquezProject/marquez && cd marquez
./docker/up.sh    # Marquez API on :5000, the web UI on :3000
```

Point producers at `http://localhost:5000` (the `OPENLINEAGE_URL`) and the graph populates as jobs run.

### 3.3 Wiring the producers

**Airflow** — the OpenLineage provider auto-instruments your DAGs; no per-task code:

```bash
pip install apache-airflow-providers-openlineage
```

```ini
# airflow.cfg
[openlineage]
transport = {"type": "http", "url": "http://marquez:5000", "endpoint": "api/v1/lineage"}
namespace = airflow-prod
```

Every task run now emits `START`/`COMPLETE`/`FAIL` events with input/output datasets it can infer (e.g. from a `PostgresOperator`'s SQL). This is also how you get *run-level* lineage — which orchestration run produced the bad data.

**dbt** — the OpenLineage integration wraps `dbt run` and parses `manifest.json` + `run_results.json` to emit one event per model, including the `columnLineage` facet:

```bash
pip install openlineage-dbt
OPENLINEAGE_URL=http://localhost:5000 OPENLINEAGE_NAMESPACE=dbt \
  dbt-ol run            # drop-in replacement for `dbt run`
```

**Spark** — add the OpenLineage listener jar and configure it; every Spark action emits dataset-level (and, for SQL, column-level) lineage:

```python
spark = (SparkSession.builder
    .config("spark.jars.packages", "io.openlineage:openlineage-spark_2.12:1.+")
    .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
    .config("spark.openlineage.transport.type", "http")
    .config("spark.openlineage.transport.url", "http://localhost:5000")
    .config("spark.openlineage.namespace", "spark-prod")
    .getOrCreate())
```

With all three wired, Marquez shows one continuous graph: the Airflow run that triggered the Spark ingestion that landed `stg_orders`, the dbt models that transformed it, all the way to the model behind the dashboard — across three tools, one map.

---

## 4. Data catalogs — DataHub and OpenMetadata

Marquez is lineage-centric. A **data catalog** is the broader product: it ingests lineage *and* metadata from across the stack and adds the things a large org needs to govern data — discovery, ownership, meaning, and policy.

What a catalog adds on top of raw lineage:

- **Search and discovery** — "where is the `customers` table, who owns it, is it deprecated?" across every system.
- **Business glossary** — link the column `mrr` to the agreed definition of "Monthly Recurring Revenue," so two teams mean the same thing.
- **Ownership and stewardship** — every dataset has an owner to page; this is what closes the "who do I email" loop in an incident.
- **Column-level lineage ingestion** — both parse SQL and consume OpenLineage to build column-level graphs across tools.
- **Classification and policy** — tag columns as PII (lecture 3), attach access policies, and audit them.

**DataHub** (LinkedIn-origin, open source) and **OpenMetadata** (Collate-origin, open source) are the two leading open catalogs. Both:

- Ingest from dbt (manifest + catalog), Airflow, Spark, Postgres, Kafka, and dozens of other sources via pull-based connectors or push-based emitters.
- Consume OpenLineage events (DataHub has a native OpenLineage endpoint; OpenMetadata ingests lineage via its connectors and API).
- Provide column-level lineage, a glossary, ownership, tags, and search through a web UI.

They differ in architecture and emphasis — DataHub leans real-time, stream-based metadata via Kafka; OpenMetadata leans a unified schema with ingestion connectors and built-in data-quality and profiler workflows — but for this course the takeaway is: **the catalog is where lineage stops being a debugging tool and becomes an organizational asset.** You will run one of them in the lab to ingest the dbt project and view the column-level graph.

---

## 5. The incident walkthrough — trace the bad number

Back to 9:14 AM. The dashboard says $4.1M; Finance says $3.7M. Here is the lineage-driven procedure, the one you will execute in challenge 2.

**Step 0 — establish the consumer.** Open the catalog (or `dbt docs`) and locate the dashboard's exposure: `exec_revenue_dashboard`. Its single upstream dependency is `mart_daily_revenue`. The blast radius is everything upstream of that node.

**Step 1 — table-level, walk upstream.** Read the DAG upstream from `mart_daily_revenue`:

```text
exec_revenue_dashboard (exposure)
  └─ mart_daily_revenue
       └─ int_orders_enriched
            ├─ stg_orders     ← source: raw.orders
            └─ stg_fx_rates   ← source: raw.fx_rates
```

Five candidate nodes. Too many to inspect blind — go to column level.

**Step 2 — column-level, follow the metric.** In the catalog's column-lineage view (or the `columnLineage` facet in Marquez), trace `mart_daily_revenue.total_revenue`:

```text
total_revenue = sum(int_orders_enriched.amount_usd)
amount_usd    = stg_orders.amount * stg_fx_rates.rate
```

The metric depends on exactly two leaf columns: `orders.amount` and `fx_rates.rate`. The search is now two columns, not five tables.

**Step 3 — correlate with change.** In Marquez, look at the *run history* of the jobs that produce those columns. `stg_fx_rates`'s job has a new run from last night with a different code version (the `sql` facet changed). Someone deployed an FX-rate model change yesterday. The `dataQuality` facet on that run shows the row count for `fx_rates` jumped — duplicate rates were loaded, inflating the join and double-counting revenue. **Root cause found: a duplicate-rate bug in yesterday's `stg_fx_rates` change inflated `amount_usd`, so `total_revenue` over-reported by ~11%.** The dashboard's $4.1M is wrong; Finance's $3.7M is right.

**Step 4 — write the root-cause path.** The deliverable is not "I fixed it" — it is the *traced path*: dashboard → `mart_daily_revenue.total_revenue` → `amount_usd` → `stg_fx_rates.rate` → the duplicate-loading change in last night's run, with the lineage screenshots and the run-version diff as evidence. That path is what you could not have produced without lineage, and it is what turns a four-hour panic into a fifteen-minute diagnosis.

The lesson: **lineage is incident infrastructure.** You build it before the incident — in calm, by wiring dbt exposures and OpenLineage — so that when the VP emails, the map already exists and you only have to read it.

---

## 6. What to carry into the lab and the capstone

- Lineage comes at **table** resolution (blast radius) and **column** resolution (root cause). You need both; column-level is what closes incidents.
- dbt gives you table-level lineage free via `ref`/`source`, surfaced by `dbt docs generate`/`serve`, with `manifest.json` as the machine-readable form and **exposures** extending the graph to the dashboard.
- **OpenLineage** standardizes lineage across tools as a **run / job / dataset + facets** event model; the `columnLineage` facet is column-level lineage; **Marquez** is the server that collects and renders it. Wire Airflow, dbt (`dbt-ol`), and Spark to one Marquez and you get a single end-to-end graph.
- **DataHub / OpenMetadata** are catalogs that add search, glossary, ownership, and policy on top of lineage — turning it from a debugging tool into a governance asset.
- The lab artifact is the **source-to-dashboard lineage graph**; challenge 2 is the **traced root-cause path** of a deliberately broken number.

---

## References

- OpenLineage documentation — the spec, the run/job/dataset model, facets (including `columnLineage`), and integrations. <https://openlineage.io/docs/>
- OpenLineage — facets reference (schema, columnLineage, dataQuality, sql). <https://openlineage.io/docs/spec/facets/>
- Marquez documentation and source — the reference OpenLineage metadata server. <https://marquezproject.ai/docs> · <https://github.com/MarquezProject/marquez>
- Apache Airflow — OpenLineage provider (`apache-airflow-providers-openlineage`). <https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/index.html>
- dbt documentation — documentation, the DAG, and exposures. <https://docs.getdbt.com/docs/build/documentation> · <https://docs.getdbt.com/docs/build/exposures>
- DataHub documentation — ingestion, column-level lineage, and the OpenLineage endpoint. <https://datahubproject.io/docs/>
- OpenMetadata documentation — connectors, lineage, glossary, and classification. <https://docs.open-metadata.org/>
- Joe Reis & Matt Housley, *Fundamentals of Data Engineering*, O'Reilly, 2022. ISBN 978-1-098-10830-4 — Ch. 9 on metadata, lineage, and data management. <https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/>
