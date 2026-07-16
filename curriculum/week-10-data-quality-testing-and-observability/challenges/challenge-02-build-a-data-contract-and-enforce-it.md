# Challenge 2 — Build a Data Contract and Enforce It

**Time estimate:** ~90 minutes.

## Problem statement

Two teams share the `orders` feed: the **orders-platform** team *produces* it, and your **analytics** team *consumes* it. Last quarter the producer renamed `total_cents` to `amount` without telling anyone; your marts silently broke for two days. There was no written agreement, so there was nothing to enforce and nobody to hold accountable. Your job is to author a **data contract** between the two teams — schema, grain, semantics, freshness + volume SLAs, ownership, change policy, PII flags — as machine-readable YAML, then **enforce its clauses as automated checks**, and prove a breaking change is caught *mechanically* before it reaches your marts.

The contract is the thing that turns "they broke us again" from a Slack fight into a CI failure on the *producer's* side (Lecture 3 §1). This challenge is the discipline that separates a team that gets surprised by upstream changes from one that doesn't.

## Why it matters

Schema drift and semantic drift are the leading causes of silent data incidents, and they are unsolvable without a written agreement: you cannot enforce a promise that was never made. A data contract is the senior-shop convention for this in 2026 — the producer owns it, CI tests it on both sides, and a breaking change requires notice + a version bump + sign-off. After this challenge you can author one and wire it to checks, which is the exact artifact a reviewer asks for when they probe "how do you handle upstream changes?"

## Procedure

### Phase 1 — Author the contract

1. Write `contracts/orders.yaml` covering every clause (model it on Lecture 3 §1 / the Data Contract Specification at <https://datacontract.com/>):
   - **Schema** — every field, its type, `required`.
   - **Grain + semantics** — what one row is (`(order_id, line_number)`), what each field means.
   - **SLAs** — freshness (`error_after: 2h`, `warn_after: 1h`) and volume (`minRows: 30000`, `maxRows: 50000`).
   - **Ownership** — the producing team and a contact.
   - **Change policy** — additive allowed; breaking requires 14 days' notice + major version bump + consumer sign-off.
   - **PII flags** — mark `customer_email` (and any other personal field) `pii: true`.
2. Version the contract (`info.version`) and commit it where *both* teams can see it.

### Phase 2 — Generate checks from the contract clauses

3. Turn each clause into an automated check. The mapping (Lecture 3 §1.1):
   - `required: true` → a `not_null` (dbt) / `ExpectColumnValuesToNotBeNull` (GX) check.
   - `enum` → `accepted_values` / `ExpectColumnValuesToBeInSet`.
   - `minimum` / `pattern` → range / regex checks.
   - `primaryKey` (grain) → compound-uniqueness check.
   - freshness SLA → `dbt source freshness` with the contract's `warn_after`/`error_after`.
   - volume SLA → `ExpectTableRowCountToBeBetween` / a row-count test.
4. You may hand-write these, or use the **`datacontract` CLI** (`datacontract test contracts/orders.yaml --server ...`) which derives and runs checks from the contract directly. Either is acceptable; the point is the checks *come from the contract*, not from a separate hand-maintained list that drifts.

### Phase 3 — Enforce on both sides

5. **Producer side:** add a CI step the producer runs *before shipping* — `datacontract test` (or your generated checks) against the data they're about to publish. If they drop/rename a field or blow an SLA, *their* CI fails. This is the important side: it catches the break before it reaches you.
6. **Consumer side:** run the schema + SLA subset at your ingestion boundary (the GX suite from Challenge 1) as defense in depth.

### Phase 4 — Prove a breaking change is caught

7. Simulate the incident: produce a dataset where `total_cents` has been renamed to `amount` (the real Q3 break). Run the contract checks. The schema check must **fail** — `total_cents` is `required` and is missing — on the *producer's* side, before it ships.
8. Simulate a non-breaking change: add a new *nullable* column `discount_cents`. The contract checks must **pass** (additive change is `allowed` by the change policy). This proves the contract distinguishes breaking from additive, rather than failing on every change.
9. Simulate an SLA breach: age the feed past `error_after`. `dbt source freshness` must fail.

## Deliverable

A directory `challenge-02/` containing:

- [ ] `contracts/orders.yaml` — the full contract with all seven clause types.
- [ ] The generated/authored checks (or the `datacontract` CLI invocation).
- [ ] The producer-side and consumer-side enforcement config (CI step + ingestion checks).
- [ ] `PROOF.md` showing: the breaking change (renamed column) **caught and failing**; the additive change (new nullable column) **passing**; and the SLA breach **failing** `dbt source freshness`. Include the check output for each.

## Pass criteria

- [ ] The contract contains all of: schema, grain/semantics, freshness SLA, volume SLA, ownership, change policy, PII flags.
- [ ] Each enforced clause traces to a concrete check (a renamed `total_cents` fails a schema/`not_null` check; a blown freshness SLA fails `dbt source freshness`; an out-of-band row count fails a volume check).
- [ ] The breaking change (rename) is **caught**; the additive change (new nullable column) **passes** — proving the policy distinguishes the two.
- [ ] Enforcement runs on the **producer side** (catches the break before it ships), not only the consumer side.
- [ ] At least one field is correctly flagged `pii: true`.

## Stretch

- **Wire it into the producer's CI for real.** Put `datacontract test` in a GitHub Actions / GitLab CI job that runs on the producer's PRs, so a breaking schema change fails the PR check and cannot merge.
- **Auto-generate the GX suite from the contract.** Write a small script that reads `orders.yaml` and emits the GX `orders_ingestion` suite, so the contract is the single source of truth and the suite can never drift from it.
- **Add a `lineage` note.** Record which downstream marts depend on the contract, so a proposed breaking change can be assessed for blast radius before the 14-day notice clock starts (a bridge to Week 11's lineage work).

## References

- **Data Contract Specification** (the open YAML spec): <https://datacontract.com/>
- **GoCardless — "Data contracts at GoCardless"** and **PayPal — "The Next Generation of Data Platforms is the Data Mesh / data contracts"** — the two most-cited industry write-ups on contracts in practice; search the engineering blogs for the current canonical posts.
- **Joe Reis & Matt Housley, *Fundamentals of Data Engineering*** (O'Reilly, 2022), ISBN 978-1-098-10830-4 — the producer/consumer boundary and SLAs.
- Lecture 3 §1 (contract anatomy and enforcement) and §2 (freshness gate).
