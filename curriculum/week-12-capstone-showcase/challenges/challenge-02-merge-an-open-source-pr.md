# Challenge 2 — Merge One Pull Request into an Open-Source Data Project

## Brief

Land one real, merged pull request into an established open-source data project. The SYLLABUS names this as a portfolio recommendation, and it is the single most predictive signal a hiring manager has for "can this person contribute to my codebase on day one." This is not a toy fork or a typo fix in your own repo — it is a contribution, to someone else's project, that passes their review and gets merged.

You should spend ~3–4 hours on this challenge, spread across the week (review cycles take days, so start Monday). The deliverable is the merged-PR link and a `CONTRIBUTION.md` writeup for your portfolio.

## Why this challenge matters

Anyone can write "I built a lakehouse" on a resume. A merged PR into dbt, Airflow, or DuckDB is a claim a hiring manager can *verify in one click*, and it proves something no personal project can: that you can read an unfamiliar codebase, work to its conventions, pass its CI, respond to a maintainer's review, and get your change accepted by people with no reason to be nice to you. That is the actual job. A candidate with one merged PR into a serious data project is, in most hiring funnels, immediately more credible than one with five impressive-but-solo repos.

## Choose a target

Pick a project you have *actually used* in C27 — your contribution will be better and your motivation more credible if you've felt the friction you're fixing:

- **dbt** (<https://github.com/dbt-labs/dbt-core>) — the transformation layer from Week 5; doc clarifications, adapter fixes, and test additions are realistic first PRs.
- **Apache Airflow** (<https://github.com/apache/airflow>) — the orchestrator from Week 4; provider fixes, doc corrections, and example DAGs are good first contributions. Huge surface area with "good first issue" labels.
- **DuckDB** (<https://github.com/duckdb/duckdb>) — the query layer from Week 2/6; doc fixes and reproducible-bug reports with fixes.
- **Apache Iceberg / Delta** (<https://github.com/apache/iceberg>, <https://github.com/delta-io/delta>) — the table formats from Week 6; doc and binding fixes.
- **Great Expectations** (<https://github.com/great-expectations/great_expectations>) — the quality layer from Week 10; expectation docs, new expectations, test coverage.
- **Dagster** (<https://github.com/dagster-io/dagster>) — an orchestration alternative; integrations and docs.
- **A Spark or Kafka client library** (e.g., `confluent-kafka-python`, a Spark connector) — from Weeks 7, 8.

## What counts (and what does not)

**Counts:** a bug fix, a documentation fix that clarifies something genuinely confusing, a missing test, a new dbt test or Great Expectations expectation, an adapter/connector fix, a corrected example, a reproducible-bug report with a working fix.

**Does not count:** a whitespace-only change, a typo fix so trivial maintainers close it, a change to your own fork that was never proposed upstream, or a PR that is still open and unreviewed at submission (it must be *merged*, or at minimum *approved and merging*, with maintainer engagement visible).

## Procedure

### Phase 1 — Find a real gap (Monday)

Use the project from your C27 work. The best PR fixes something *you* tripped over: a doc that was wrong, an expectation that didn't exist, an adapter behavior that surprised you, a missing example. Check the project's "good first issue" / "help wanted" labels. Read `CONTRIBUTING.md` *before* you write a line — every serious project has one, and ignoring it is the fastest way to get a PR closed.

### Phase 2 — Make the change to their standards (Tuesday–Wednesday)

- Match the existing code style exactly (most projects have a formatter and a linter — run them).
- Write the commit message in their convention (many projects require a sign-off, an area prefix, or a linked issue).
- Add the test or doc the project requires for that kind of change (a new dbt test needs a test; a new expectation needs an example and a test).
- Run their CI locally if you can; a PR that fails CI on submission signals you didn't read the contribution guide.

### Phase 3 — Submit and respond to review (Wednesday–Friday)

Open the PR. A maintainer will likely request changes — this is normal and is the part that proves you can collaborate. Respond promptly, make the requested changes, and be gracious. The review exchange is itself a portfolio artifact: it shows you can take feedback in someone else's house.

### Phase 4 — Document it (Friday/Sunday)

Write `CONTRIBUTION.md`:

1. The PR link and the project.
2. What gap it filled and how you found it (ideally: "I hit this in C27 Week N").
3. What the maintainer asked for in review and how you responded.
4. What you learned about the project's conventions and contribution process.

## Deliverable

1. The merged (or approved-and-merging) PR link.
2. `CONTRIBUTION.md` — the writeup above.

Commit `CONTRIBUTION.md` to your capstone repo and link it from your portfolio landing page. Commit message like `week-12/challenge-02: upstream contribution writeup`.

## Pass criteria

- The PR is to an *established, third-party* open-source data project (not your own repo).
- The PR is merged, or approved with visible maintainer engagement and on track to merge.
- The change is substantive per the "what counts" list (not whitespace/trivial-typo).
- `CONTRIBUTION.md` documents the gap, the review exchange, and the lesson.

## A note on rejection and timing

Your first PR might be rejected, or sit for weeks, or get bikeshedded over a detail you think is trivial. This is the real open-source experience, and surviving it gracefully is the skill. If your chosen PR stalls past the week, document the *open, engaged* PR (with the maintainer exchange) as your deliverable and note its status — a thoughtfully-argued open PR with maintainer back-and-forth still demonstrates the competency. Start Monday precisely because review cycles are slow; a PR opened Friday cannot merge by Sunday. And once you have one merged PR, keep going after C27 — the second is far easier than the first, and a contribution history is a compounding asset for your whole career.

## References

- SYLLABUS, "Portfolio recommendations" — the merged-PR recommendation.
- Each project's `CONTRIBUTING.md` (read before contributing).
- dbt contributing guide. <https://github.com/dbt-labs/dbt-core/blob/main/CONTRIBUTING.md>
- Apache Airflow contributing guide. <https://github.com/apache/airflow/blob/main/contributing-docs/README.rst>
- "How to Contribute to Open Source" (Open Source Guides). <https://opensource.guide/how-to-contribute/>
