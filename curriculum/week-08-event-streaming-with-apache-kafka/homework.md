# Week 8 — Homework

Six practice problems that consolidate the week's material. They are sized to ~45 minutes each. Do them after the lectures and the exercises; do them before (or alongside) the mini-project. Cite the docs you used in your commit messages. All problems assume the Kafka + schema-registry Docker stack from Challenge 1 / the mini-project.

## Problem 1 — Reason about offsets and consumer lag

Produce 100 records to a single-partition topic `hw-offsets`. Then:

1. Start a consumer group `lag-demo` with `auto.offset.reset="earliest"`, read and commit 40 records, then stop it.
2. Produce 60 more records (160 total now).
3. Run `kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group lag-demo` and read the `CURRENT-OFFSET`, `LOG-END-OFFSET`, and `LAG` columns.
4. Explain, in your own words, what each column means and why `LAG = LOG-END-OFFSET − CURRENT-OFFSET`. What number do you predict for `LAG` before you run the command, and does it match?

Then restart the consumer and confirm it resumes at offset 40 (not 0 and not 160), and watch `LAG` drain to 0.

Cite <https://kafka.apache.org/documentation/#intro_concepts_and_terms> and <https://docs.confluent.io/platform/current/clients/consumer.html>.

Deliverable: `homework/01-offsets-and-lag.md` with the command output, the predicted-vs-actual lag, and the explanation.

## Problem 2 — Prove per-partition ordering (and its limit)

Create a 3-partition topic `hw-ordering`. Write a producer that emits, for each of two keys `A` and `B`, a sequence `0..49` (so 100 records, each tagged with its key and a `seq`). Consume the whole topic and:

1. Confirm that for key `A` the `seq` values arrive strictly increasing, and likewise for `B`.
2. Now interleave the *global* read order (across partitions) and show that `A`'s and `B`'s records are **not** globally ordered relative to each other — e.g. you might see `A:5` before `B:2` and `B:7` before `A:6`.
3. Write one paragraph stating precisely what Kafka guarantees here and what it does not, and why a single key on a single partition is the only way to get an order.

Cite <https://kafka.apache.org/documentation/#semantics> and <https://kafka.apache.org/documentation/#intro_concepts_and_terms>.

Deliverable: `homework/02-ordering/` (the producer/consumer scripts) plus a `NOTES.md` with the two orderings and the guarantee paragraph.

## Problem 3 — The three delivery semantics, in code

For a single-partition topic, write three tiny consumer loops over the same input and force a crash mid-stream in each (an `os._exit(1)` after ~half the records):

1. **At-most-once:** commit the offset *before* processing. After restart, show that some records were never processed (loss).
2. **At-least-once:** process *before* committing, with a delayed/batched commit. After restart, show that some records were processed twice (duplication).
3. Explain why **exactly-once** cannot be achieved by a different commit ordering alone — it needs the idempotent producer + transactions (point at Lecture 2 Section 7; you do not have to implement it here, just explain).

Cite <https://kafka.apache.org/documentation/#semantics>.

Deliverable: `homework/03-delivery-semantics/` with the three scripts and a `RESULTS.md` showing the loss count, the duplicate count, and the exactly-once explanation.

## Problem 4 — Retention vs. compaction

Create two topics: `hw-events` with `cleanup.policy=delete` and a short `retention.ms` (e.g. 60000), and `hw-state` with `cleanup.policy=compact`. Then:

1. To `hw-events`, produce a stream of timestamped events; explain (and, if you wait out the retention, observe) that old records age out by time.
2. To `hw-state`, produce several records for the same set of keys (e.g. `user_1` → v1, v2, v3; `user_2` → v1, v2), trigger/await compaction, and show a fresh consumer reads only the **latest** value per key.
3. Produce a tombstone (a record with a key and a `null` value) for one key on `hw-state` and explain what it does.
4. Write a short decision guide: for each of these — a clickstream, a fraud-alert log, a current-account-balance store, an audit trail — say whether you would use retention or compaction and why.

Cite <https://kafka.apache.org/documentation/#compaction>.

Deliverable: `homework/04-retention-compaction.md` with the commands, the observed compaction result, and the decision guide.

## Problem 5 — Schema evolution cases

Starting from the v1 `Click` Avro schema (`user_id: string`, `url: string`, `ts: double`), classify each of the following proposed changes as `BACKWARD`-compatible or not, *predict* the registry's verdict, then verify with `test_compatibility` against a registry whose subject is in `BACKWARD` mode:

1. Add `referrer: ["null","string"]` with `default: null`.
2. Add `country: string` with **no** default.
3. Remove the `url` field.
4. Change `ts` from `double` to `long`.
5. Rename `url` to `page_url` (no alias).
6. Add `referrer: ["null","string"]` with `default: null` **and** an Avro `alias` mapping an old name.

For each, write the predicted verdict, the actual `test_compatibility` result, and a one-sentence reason. Then briefly explain what would change if the subject were in `FORWARD` mode instead (which party upgrades first).

Cite <https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html> and the Avro spec at <https://avro.apache.org/docs/>.

Deliverable: `homework/05-schema-evolution.md` with the prediction/verification table and the FORWARD-mode note.

## Problem 6 — Redpanda parity

Bring up the Redpanda single-binary compose from Challenge 1. Without changing any application code, run a small producer and consumer against it (same `confluent-kafka` client, same `bootstrap.servers="localhost:9092"`), and register an Avro schema against its built-in registry on `:8081`. Then:

1. Confirm the producer/consumer/Avro round-trip works identically to Kafka.
2. Use `rpk` to inspect the topic (`rpk topic list`, `rpk topic describe clicks`) and the group lag (`rpk group describe ...`); note the `rpk` equivalents of the `kafka-*` commands you used earlier in the week.
3. Compare startup time and memory footprint (`docker stats`) between the Kafka stack (broker + registry, JVM) and Redpanda (single binary). Report the numbers.
4. Write one paragraph: when would you reach for Redpanda over Kafka on a laptop, and what is the trade-off (ecosystem/maturity vs. footprint)?

Cite <https://docs.redpanda.com/> and the quickstart at <https://docs.redpanda.com/current/get-started/quick-start/>.

Deliverable: `homework/06-redpanda-parity.md` with the round-trip confirmation, the `rpk` command mapping, the resource comparison, and the trade-off paragraph.

## Submission

Push the six deliverables on a branch named `week08-homework/<your-handle>` and open a PR against the C27 curriculum repository. The PR description should link to each deliverable and include a 100-word summary of what you learned.

The teaching staff reviews homework PRs within 5 business days. Reviews focus on whether you can *demonstrate* (not just assert) the per-partition ordering guarantee (Problem 2), whether the delivery-semantics crash tests actually show the loss/duplication (Problem 3), and whether your schema-evolution predictions match the registry's verdicts (Problem 5). The most common review comment is "you asserted at-least-once but did not force a failure to prove the duplicate" — preempt it by actually crashing the consumer mid-stream and counting.

Cited pages this homework draws from: <https://kafka.apache.org/documentation/#intro_concepts_and_terms>, <https://kafka.apache.org/documentation/#semantics>, <https://kafka.apache.org/documentation/#compaction>, <https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html>, <https://avro.apache.org/docs/>, <https://docs.redpanda.com/>.
