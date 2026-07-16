# Challenge 2 — Delivery Semantics: From Duplicates to Exactly-Once

> **Time:** ~2 hours. **Prerequisites:** Lecture 2 (delivery semantics, transactions), Challenge 1 (you have a clickstream on a topic). **Citations:** the Kafka delivery-semantics section at <https://kafka.apache.org/documentation/#semantics>, the idempotent-producer config at <https://kafka.apache.org/documentation/#producerconfigs_enable.idempotence>, the Kafka transactions guide at <https://docs.confluent.io/platform/current/clients/producer.html#kafka-transactions>, and the consumer `isolation.level` config at <https://kafka.apache.org/documentation/#consumerconfigs_isolation.level>.

## Premise

Everyone says their pipeline is "exactly-once." Almost none are, and most authors cannot tell you which guarantee they actually have. This challenge makes the difference *empirical*. You will build a read-process-write job that consumes the `clicks` topic, enriches each event, and writes to a `clicks-enriched` topic — first in a naive at-least-once way that produces **observable duplicates** when you kill and restart it mid-flight, then in an exactly-once way using the idempotent producer plus Kafka transactions that produces **no duplicates and no loss** across the same forced restart. You prove each claim by counting, not by asserting.

## Setup

Use the Kafka (or Redpanda) Docker compose from Challenge 1, and the `clicks` topic populated by Challenge 1's producer. Create the output and (for the exactly-once part) the offsets-tracking topics:

```sh
docker compose -f docker-compose.kafka.yml exec kafka \
  kafka-topics --bootstrap-server localhost:9092 \
  --create --topic clicks-enriched --partitions 3 --replication-factor 1
```

Keep Challenge 1's producer running in a loop in the background so there is a steady stream to process, OR pre-load a fixed, known number of records (say exactly 5,000) so your duplicate/loss counts are exact. The fixed-count approach makes the proof cleaner; prefer it.

## Tasks

### T1 — Establish the baseline: at-least-once with duplicates

Write `enrich_at_least_once.py`:

- A consumer on `clicks` with `group.id="enricher-alo"`, `enable.auto.commit=False`, `auto.offset.reset="earliest"`.
- A plain (non-transactional) producer to `clicks-enriched`.
- The loop, deliberately in **process-then-commit** order with the commit *delayed*: poll a record, produce its enrichment, and commit the offset only every N records (e.g. every 100) or every few seconds — simulating realistic batched commits.
- Add an artificial way to crash: after producing ~half the input, `os._exit(1)` (or just `kill -9` it from another terminal) so the process dies **after producing many records but before committing their offsets.**

On restart with the same `group.id`, the consumer resumes from the last *committed* offset — which is behind where it actually produced — so it **reprocesses and re-produces** the uncommitted span. Those are real duplicates on `clicks-enriched`.

### T2 — Measure the duplicates

Write `count_output.py`: consume all of `clicks-enriched` from `earliest`, and count how many times each unique input event (use a stable event key, e.g. `(user_id, seq)` from Challenge 1) appears on the output. With at-least-once + a forced restart, some events appear **2 (or more) times**. Report:

- total output records,
- distinct input events represented,
- the count of events appearing more than once (the duplicates),
- a few example duplicated keys with their occurrence counts.

This is the concrete, countable evidence that at-least-once duplicates under failure.

### T3 — Achieve exactly-once with transactions

Write `enrich_exactly_once.py` using the read-process-write transaction pattern from Lecture 2:

- A transactional producer: `transactional.id="enricher-eos-1"`, `enable.idempotence=True` (implied), `acks="all"`. Call `init_transactions()` once at startup.
- A consumer with `group.id="enricher-eos"`, `enable.auto.commit=False`, `isolation.level="read_committed"`, `auto.offset.reset="earliest"`.
- The loop, per batch:
  1. `producer.begin_transaction()`
  2. produce the enriched record(s) for the batch to `clicks-enriched`
  3. `producer.send_offsets_to_transaction(offsets, consumer.consumer_group_metadata())` — atomically include the consumer's offsets
  4. `producer.commit_transaction()` — output **and** offset advance commit together, or neither does on abort.
- Crash it at the same point (`os._exit(1)` mid-stream). On restart, the consumer resumes from the last *committed-in-a-transaction* offset, reprocesses the in-flight batch, and re-produces — but the earlier aborted batch's output is invisible to a `read_committed` reader.

### T4 — Prove no duplicates and no loss

Re-run `count_output.py` (it must use `isolation.level="read_committed"` so it ignores aborted-transaction records) against the exactly-once output and show:

- **no duplicates** — every distinct input event appears exactly once, and
- **no loss** — the number of distinct input events on the output equals the number of input events on `clicks` (count the input topic the same way).

Put the at-least-once and exactly-once numbers side by side in a table.

## Acceptance criteria

- The at-least-once run, after a forced restart, produces a **measurable, nonzero** count of duplicated events on `clicks-enriched`, demonstrated by `count_output.py`, with example keys shown at occurrence count ≥ 2.
- The exactly-once run, after the *same* forced restart, produces **zero** duplicates when read with `isolation.level="read_committed"`, **and** loses nothing — distinct output events equal the input event count.
- A `read_committed` reader on the exactly-once output sees only committed-transaction records; if you read the same topic with `isolation.level="read_uncommitted"` you may see the aborted records (note the difference in `RESULTS.md` — it is a good demonstration of what the isolation level does).
- `RESULTS.md` contains: the side-by-side duplicate/loss table for the two runs, the exact crash point and restart procedure, and a paragraph on the honest limit of "exactly-once" — that it is exactly-once *within Kafka* (consume→produce→offset-commit atomic) and does **not** automatically extend to an external side effect (a database write, an API call) unless that side effect is itself made idempotent or transactional.

## Why this challenge matters

The gap between "we have exactly-once" and "we actually have at-least-once and our downstream silently double-counts" is where a startling number of production data bugs live — double-charged customers, inflated metrics, duplicated emails. The only way to know which you have is to *force the failure and count*, which is exactly what you do here. You also learn the real shape of Kafka exactly-once: it is not a flag you flip, it is a transaction that binds the produce and the offset-commit into one atomic unit, plus a `read_committed` consumer downstream. And you learn its boundary — the most important caveat in streaming — that the guarantee is Kafka-to-Kafka, and the moment your processing touches an external system, exactly-once is your responsibility to preserve, not Kafka's to provide. An engineer who has personally watched at-least-once duplicate a record and then watched transactions prevent it does not hand-wave about delivery semantics again.

## References

- Kafka delivery semantics (at-most/at-least/exactly-once) — <https://kafka.apache.org/documentation/#semantics>
- Idempotent producer — <https://kafka.apache.org/documentation/#producerconfigs_enable.idempotence>
- Kafka transactions (`transactional.id`, `send_offsets_to_transaction`) — <https://docs.confluent.io/platform/current/clients/producer.html#kafka-transactions>
- Consumer `isolation.level` (`read_committed`) — <https://kafka.apache.org/documentation/#consumerconfigs_isolation.level>
- `confluent-kafka-python` transactional API — <https://docs.confluent.io/kafka-clients/python/current/overview.html#transactional-api>
- The original exactly-once design (KIP-98) — <https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging>
