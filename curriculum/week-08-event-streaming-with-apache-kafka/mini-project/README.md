# Mini-Project — Lab 08: Produce and Consume a Stream

> **Time:** 8 hours across Friday–Saturday–Sunday. **Prerequisites:** the three lectures, Exercises 1–3, and ideally both challenges. **Citations:** the Apache Kafka docs, the Confluent schema-registry docs, the `confluent-kafka-python` docs. **This topic is the input to Week 9 (Spark Structured Streaming + Flink).**

## The spec

You are building **Lab 08**: a complete, running Kafka clickstream in Docker. By the end you will have stood up Kafka (KRaft mode) and a schema registry in containers, produced a synthetic clickstream of keyed, Avro-serialized events into a partitioned topic, run a two-member consumer group through an observed rebalance, and proven that a backward-compatible Avro schema change registers while an incompatible one is rejected. The grade is on the topology, the per-key ordering, the rebalance, and the schema-compatibility proof — and on leaving the `clicks` topic in a state Week 9 can consume cleanly.

This is not a throwaway. **Week 9's Spark Structured Streaming and Flink jobs read this exact topic**, deserializing the value against the Avro schema you register here. Build it as the durable source it is.

## Topology

```text
                         ┌─────────────────────────────────────────┐
                         │  Docker (docker compose, KRaft mode)     │
                         │                                          │
   clickstream_          │   ┌──────────┐        ┌───────────────┐  │
   producer.py  ───Avro──┼──▶│  Kafka   │◀──────▶│ schema-       │  │
   (key=user_id)         │   │  broker  │  schema│ registry      │  │
                         │   │ topic:   │  by ID │  :8081        │  │
                         │   │ "clicks" │        │ subject:      │  │
                         │   │ 3 parts  │        │ clicks-value  │  │
                         │   └────┬─────┘        └───────────────┘  │
                         │        │  partitions 0,1,2               │
                         └────────┼─────────────────────────────────┘
                                  │
              consumer group "lab08" (auto.offset.reset=earliest)
                                  │
                ┌─────────────────┴───────────────────┐
                │                                      │
        consumer member A                      consumer member B
        (start first: owns 0,1,2)      (start second: REBALANCE -> takes some)
                │                                      │
                └──────────────┬───────────────────────┘
                               ▼
                  prints partition / offset / key / decoded Avro value
                  -- on_assign/on_revoke logs show the rebalance --

   (downstream, NEXT WEEK):  "clicks" topic ──▶ Week 9 Spark/Flink streaming job
```

## The Docker setup

Use the KRaft Kafka + schema-registry compose from Challenge 1 (`docker-compose.kafka.yml`), or the Redpanda drop-in (`docker-compose.redpanda.yml`) — both expose Kafka on `:9092` and a schema registry on `:8081`, and your Python code is identical against either. Bring it up and create the topic:

```sh
docker compose -f docker-compose.kafka.yml up -d
docker compose -f docker-compose.kafka.yml exec kafka \
  kafka-topics --bootstrap-server localhost:9092 \
  --create --topic clicks --partitions 3 --replication-factor 1
pip install "confluent-kafka[avro]"
```

## Functional requirements

### F1 — Kafka + schema registry in Docker (KRaft, no ZooKeeper)

- A `docker compose up -d` brings up a Kafka broker in **KRaft mode** (`KAFKA_PROCESS_ROLES: broker,controller`, no ZooKeeper container) and the Confluent Schema Registry on `:8081`.
- The `clicks` topic exists with **3 partitions**, `replication-factor 1` (single node).
- `kafka-topics --describe --topic clicks` shows the 3 partitions, their leaders, and the ISR.

### F2 — A keyed, Avro-serialized clickstream producer

- `clickstream_producer.py` emits synthetic click events `{user_id, session_id, url, ts, seq}` where `seq` is a per-user monotonic counter (the ordering witness).
- Events are **keyed by `user_id`** so each user's events share a partition and are ordered.
- The value is serialized with `AvroSerializer` against a registered Avro schema (the v1 `Click` schema), so every record is `[magic byte][schema id][avro payload]` and the schema lives in the registry under subject `clicks-value`.
- Produced with `acks="all"` and `enable.idempotence=True`; `flush()` before exit.
- It produces a known total (e.g. 5,000 events across ~50 users) so downstream counts are exact.

### F3 — Per-key ordering, proven

- A consumer reads the whole topic from `earliest` and verifies that, per `user_id`, the `seq` values arrive strictly increasing within the partition (no gaps, no reordering).
- Report the per-user `(partition, first_seq, last_seq, count)` and **zero** ordering violations.

### F4 — A two-member consumer group with an observed rebalance

- Run two members of the `lab08` consumer group (the Exercise 2 pattern) with `on_assign`/`on_revoke` callbacks.
- Start member A alone (it owns partitions 0, 1, 2). Start member B; capture the rebalance in the logs — A revokes/reassigns and B picks up partition(s). Stop B; capture A reclaiming them.
- Save the captured rebalance log (both `cooperative-sticky` and, optionally, the eager default for comparison).

### F5 — Avro schema in the registry, with compatibility proven

- The v1 `Click` schema is registered under `clicks-value` (this happens automatically on the first F2 produce).
- Demonstrate a **backward-compatible** evolution: add an optional `referrer` field (`["null","string"]`, default `null`) and show the registry **accepts** it as version 2, then produce a v2 record and consume it back (old consumers still read v1 records; the new field reads as `null`).
- Demonstrate an **incompatible** evolution: change `ts` from `double` to `string` (or add a no-default required field) and show the registry **rejects** it with HTTP 409.

### F6 — The Week 9 handoff check

- A brand-new consumer group, starting from `auto.offset.reset="earliest"`, reads the *entire* `clicks` topic and deserializes every record against the registry without error. This is the proof that Week 9 can consume the topic. Record the total record count read and confirm it equals what F2 produced.

## Non-functional requirements

### NF1 — Reproducibility

- The whole lab comes up from `docker compose up -d` plus the topic-create command and `pip install`. A `Makefile` or `run.sh` that does setup → produce → ordering-check → group demo → schema demo end to end is strongly encouraged.
- No global installs; a `requirements.txt` pins `confluent-kafka[avro]`.

### NF2 — Clean teardown and naming

- `docker compose down -v` removes everything. The topic is named exactly `clicks` (Week 9 expects it), keyed by `user_id`, 3 partitions.

### NF3 — PERF.md

- A `PERF.md` reports: producer **throughput** (events/sec — measure it), the per-partition record counts and the **partition skew**, and the **consumer lag** observed (use `kafka-consumer-groups --describe --group lab08` to read `LAG` per partition) before and after you let a consumer catch up. One paragraph interpreting the numbers: is the load balanced across partitions, and is the consumer keeping up with the producer?

## Suggested layout

```
lab08/
├── docker-compose.kafka.yml        <-- Kafka (KRaft) + schema registry
├── docker-compose.redpanda.yml     <-- the drop-in alternative
├── requirements.txt                <-- confluent-kafka[avro]
├── run.sh                          <-- end-to-end driver
├── schemas/
│   ├── click_v1.avsc               <-- baseline schema
│   ├── click_v2_good.avsc          <-- + optional referrer (accepted)
│   └── click_v3_bad.avsc           <-- ts double->string (rejected)
├── clickstream_producer.py         <-- F2: keyed Avro producer
├── ordering_check.py               <-- F3: per-key seq monotonicity
├── group_member.py                 <-- F4: one member, run twice
├── schema_compat.py                <-- F5: register v2 (ok), reject v3
├── handoff_check.py                <-- F6: fresh group reads it all
├── PERF.md                         <-- NF3
└── RESULTS.md                      <-- the captured outputs + the rebalance log
```

## Grading rubric

- **20 points: the stack.** Kafka in KRaft mode + schema registry come up in Docker from the compose; the `clicks` topic has 3 partitions; `--describe` is clean (F1, NF1, NF2).
- **20 points: the keyed Avro producer.** Events keyed by `user_id`, Avro-serialized via the registry, `acks=all` + idempotence, `flush()`, known total count (F2).
- **15 points: per-key ordering proven.** The `seq` monotonicity check passes with zero violations and reports the per-user partition/seq summary (F3).
- **20 points: the rebalance.** A two-member group with a captured `on_assign`/`on_revoke` log showing partitions moving when B joins and returning when B leaves (F4).
- **15 points: schema compatibility proven.** v2 (optional field) accepted and re-consumed; v3 (type change / no-default field) rejected with 409 (F5).
- **5 points: the Week 9 handoff.** A fresh `earliest` group reads the whole topic and deserializes every record; count matches (F6).
- **5 points: PERF.md.** Throughput, partition skew, and consumer lag measured and interpreted (NF3).

## The demo (the gate)

Beyond the rubric, the Lab 08 gate is a short demo:

1. **The stack is up.** `docker compose ps` shows the broker and registry; `kafka-topics --describe --topic clicks` shows 3 partitions.
2. **The stream flows.** Run the producer; tail a console consumer with `--print.key --print.partition` and show a user's events all on one partition.
3. **Ordering holds.** Run `ordering_check.py`; show zero `seq` violations.
4. **The rebalance.** Start member A, then B; point at the `REVOKED`/`ASSIGNED` lines as B joins and as B leaves.
5. **The schema gate.** Run `schema_compat.py`; show v2 accepted, v3 rejected with the 409 message.
6. **The handoff.** Run `handoff_check.py`; show the full topic read and deserialized — "Week 9 can consume this."

## Stretch goals

1. **Exactly-once enrichment.** Apply Challenge 2: a transactional read-process-write job from `clicks` to `clicks-enriched`, proven duplicate-free across a forced restart.
2. **Compacted companion topic.** Add a `user-profiles` topic with `cleanup.policy=compact`, key it by `user_id`, and show that after producing many updates a fresh consumer reads only the latest profile per user.
3. **Redpanda parity run.** Run the *entire* lab against the Redpanda compose with zero code changes; note the startup time and memory difference in PERF.md.
4. **Protobuf variant.** Register the `Click` value as Protobuf instead of Avro using `ProtobufSerializer` and show the same compatibility behavior.
5. **`kcat`/`rpk` tour.** Inspect the topic, the consumer-group lag, and the registry subjects entirely from the CLI (`kafka-consumer-groups`, `kafka-run-class GetOffsetShell`, `curl` against `:8081/subjects`).

## Submission

Push the `lab08/` directory on a branch named `week08-mini-project/<your-handle>` and open a PR against the C27 curriculum repository. The PR description must include: the `kafka-topics --describe --topic clicks` output, the captured rebalance log (the `REVOKED`/`ASSIGNED` lines), the schema-compatibility output (v2 accepted, v3 rejected 409), the F6 handoff count, and the PERF.md numbers.

The teaching staff reviews mini-project PRs within 7 business days. Reviews focus on (a) the topology and KRaft setup, (b) keyed Avro production with proven per-key ordering, (c) the observed rebalance, and (d) the schema-compatibility proof. Passing Lab 08 is the prerequisite for Week 9 — and the `clicks` topic you build *is* Week 9's input, so a clean F6 handoff is non-negotiable.

Cited pages: <https://kafka.apache.org/documentation/>, <https://kafka.apache.org/documentation/#kraft>, <https://docs.confluent.io/platform/current/schema-registry/index.html>, <https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html>, <https://docs.confluent.io/kafka-clients/python/current/overview.html>, <https://docs.redpanda.com/>.
