# Resources — Week 12

Every link below is free unless explicitly marked otherwise. This is the capstone-and-career week; the references are about demoing, postmortem writing, interview preparation, and portfolio packaging — not new data-engineering techniques. The SYLLABUS capstone spec and the Google SRE book are the load-bearing references.

---

## Primary references — read this week

### 1. The SYLLABUS — Capstone spec and career pack

- **Source:** `../../SYLLABUS.md` in this course.
- **Sections we use:** the "Capstone" section (the architecture diagram, the nine required deliverables, the chaos-drill menu, the six grading axes) and the "Career engineering pack" (interview-prep topics, runbook contents, portfolio recommendations).
- **Why it is primary:** the capstone build-and-defense brief (`mini-project/README.md`) is the operational expansion of this section. Everything this week builds, defends, or packages is specified here. Read it first.

### 2. Google SRE Book — postmortems and incident management

- **Source:** Beyer, Jones, Petoff, Murphy (eds.), *Site Reliability Engineering*, O'Reilly, 2016. Free online.
- **URL:** <https://sre.google/sre-book/table-of-contents/>
- **Chapters we use:**
  - "Postmortem Culture: Learning from Failure" — <https://sre.google/sre-book/postmortem-culture/> — the structure and the blamelessness norm for your capstone postmortem.
  - "Managing Incidents" — <https://sre.google/sre-book/managing-incidents/> — incident command, useful framing for the chaos-drill timeline.
- **Cite as:** "Google SRE Book, §chapter".

### 3. Etsy — Blameless PostMortems

- **Source:** John Allspaw, "Blameless PostMortems and a Just Culture", Etsy Code as Craft.
- **URL:** <https://www.etsy.com/codeascraft/blameless-postmortems/>
- **What it covers:** the canonical argument for why "human error" is never a root cause and how a just culture extracts more truth from incidents. Read before writing your postmortem (Lecture 2).

### 4. Principles of Chaos Engineering

- **URL:** <https://principlesofchaos.org/>
- **What it covers:** the discipline of deliberately injecting failure to learn how a system behaves — the framing for your chaos drill (Challenge 1, Lecture 2).

---

## Data-engineering reference (the through-line)

### Fundamentals of Data Engineering

- **Source:** Joe Reis and Matt Housley, *Fundamentals of Data Engineering*, O'Reilly, 2022. ISBN 978-1-098-10830-4. (Library / purchase.)
- **URL:** <https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/>
- **What it covers:** the data-engineering lifecycle (generation → ingestion → storage → transformation → serving) that the capstone walks end to end, plus idempotency, exactly-once, and the failure modes the chaos drills exercise. The single best book to re-skim before the defense.

### The Data Warehouse Toolkit

- **Source:** Ralph Kimball and Margy Ross, *The Data Warehouse Toolkit*, 3rd ed., Wiley, 2013. ISBN 978-1-118-53080-1. (Library / purchase.)
- **What it covers:** dimensional modeling — grain, facts, dimensions, SCD types, conformed dimensions — the source for the Week 1 material the modeling interview round probes.

---

## Demo and communication references

### Demo craft

- **Brett Victor, "Inventing on Principle"** — <https://www.youtube.com/watch?v=PUv66718DII> — a masterclass in demos that *communicate* rather than impress; watch the first ten minutes for the structure.
- **The Heilmeier Catechism** — <https://www.darpa.mil/about/heilmeier-catechism> — the question set ("What are you trying to do? Why is it hard? What's new in your approach?") that forces a clear explanation of what you built and why anyone should care. Use it to structure your architecture overview.

### Technical writing (the architecture doc and the blog post)

- **Google "Technical Writing" courses** — <https://developers.google.com/tech-writing> — free, short, and directly applicable to the architecture document and the incident-story blog post.
- **Julia Evans, "How to write a good technical blog post"** — <https://jvns.ca/blog/2017/03/20/blogging-principles/> — the model for the one-incident post in your portfolio (specific, with the artifact, with the fix).

---

## Tool documentation (cite the right one per axis)

- **dbt** — <https://docs.getdbt.com/> — incremental models, `unique_key`, tests, snapshots, docs and lineage (Weeks 5, 10, 11).
- **Apache Airflow** — <https://airflow.apache.org/docs/> — DAG design, idempotent tasks, backfills, `on_failure_callback` (Week 4).
- **Apache Spark Structured Streaming** — <https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html> — event-time watermarks, windows, output modes, checkpointing, exactly-once (Weeks 7, 9).
- **Apache Iceberg** — <https://iceberg.apache.org/docs/latest/> — table format, snapshots, schema evolution, compaction/rewrite (Week 6).
- **Delta Lake** — <https://docs.delta.io/latest/index.html> — the alternative table format; `OPTIMIZE`, time travel, schema evolution (Week 6).
- **Apache Kafka** — <https://kafka.apache.org/documentation/> — topics, partitions, offsets, consumer groups, delivery semantics, the schema registry (Week 8).
- **MinIO** — <https://min.io/docs/minio/linux/index.html> — the S3-compatible object store under the lakehouse (Week 6).
- **Great Expectations** — <https://docs.greatexpectations.io/docs/> — checkpoints, expectation suites, and Data Docs (the data-quality report) (Week 10).
- **DuckDB** — <https://duckdb.org/docs/> — the embedded query layer for the dashboard / ad-hoc analysis (Weeks 2, 6).
- **OpenLineage / Marquez** — <https://openlineage.io/docs/> — the lineage standard and the metadata server, for the end-to-end lineage deliverable (Week 11).
- **Docker Compose `depends_on` / healthcheck** — <https://docs.docker.com/reference/compose-file/services/#depends_on> — for a cold `docker compose up` that does not race its dependencies (Lecture 1, §4).

---

## Interview-preparation references

### Data-engineering interview prep

- **Chip Huyen, "Machine Learning Interviews" book** (free online) — <https://huyenchip.com/ml-interviews-book/> — adjacent field, but the chapters on system design and the behavioral round transfer directly to data-engineering loops.
- **"Awesome Data Engineering" / community question banks** — search "data engineering interview questions" on GitHub; use to broaden your prep beyond the SYLLABUS rounds.
- **The original references for the rounds themselves** — your own C27 weeks: Week 1 (modeling), Week 2 (SQL/tuning), Week 3 (idempotency), Week 4 (orchestration), Week 6 (storage), Week 7 (Spark), Week 9 (streaming), Week 12 (the incident story). The best interview prep is the course you just finished.

### General interview-motion practice

- **"Ace the Data Science Interview"** (Nick Singh, Kevin Huo) — for the SQL and modeling round structure. (Purchase.)
- **Interviewing.io / Pramp** — free peer mock-interview platforms (general SWE/data), useful for the think-out-loud reps if you cannot find a data-engineering peer.

---

## Portfolio and open-source references

### Open-source contribution (Challenge 2)

- **Open Source Guides, "How to Contribute"** — <https://opensource.guide/how-to-contribute/> — the etiquette and mechanics of a first PR.
- **dbt contributing guide** — <https://github.com/dbt-labs/dbt-core/blob/main/CONTRIBUTING.md>.
- **Apache Airflow contributing guide** — <https://github.com/apache/airflow/blob/main/contributing-docs/README.rst>.
- **dbt, Airflow, DuckDB, Iceberg, Delta, Great Expectations, Dagster** — each repo's `CONTRIBUTING.md`. Read before contributing.

### Portfolio packaging

- **GitHub "About READMEs"** — <https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-readmes> — the README is what a hiring manager opens first; make it count.
- **The SYLLABUS portfolio recommendations** — public GPL-3.0 capstone repo with a real README, architecture diagram, and the data-quality report; one merged PR; one technical incident blog post; a landing page without the phrase "data-driven."

---

## Reading time budget

| Reference                                            | Time     | When               |
|------------------------------------------------------|----------|--------------------|
| SYLLABUS capstone spec + career pack                 | 25 min   | Monday morning     |
| mini-project/README.md (the build-and-defense brief) | 25 min   | Monday morning     |
| Google SRE "Postmortem Culture" + Etsy blameless     | 35 min   | Tuesday morning    |
| Brett Victor + Heilmeier (demo craft)                | 30 min   | Monday afternoon   |
| Data-engineering interview question banks            | 45 min   | Wednesday          |
| Open Source Guides + your target's CONTRIBUTING.md   | 20 min   | Monday (start early — review is slow) |
| exercises/SOLUTIONS.md (model Q&A and rounds)        | 40 min   | Before dress rehearsal |
| Total                                                | ~3.5 h   | spread across week |

The reading is light this week by design — the work is finishing, drilling, rehearsing the cold-start demo, and defending the pipeline, not absorbing new material. Read the defense brief and the postmortem references early; spend the rest of the week on the platform and in front of your peer.
