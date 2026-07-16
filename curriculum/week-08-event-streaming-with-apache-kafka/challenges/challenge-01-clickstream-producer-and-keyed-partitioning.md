# Challenge 1 — A Synthetic Clickstream Producer with Keyed Partitioning

> **Time:** ~2 hours. **Prerequisites:** Lectures 1–2, Exercises 1–2. **Citations:** the Kafka producer configs at <https://kafka.apache.org/documentation/#producerconfigs>, the partitioning intro at <https://kafka.apache.org/documentation/#intro_concepts_and_terms>, the `confluent-kafka-python` docs at <https://docs.confluent.io/kafka-clients/python/current/overview.html>, the KRaft docs at <https://kafka.apache.org/documentation/#kraft>, and the Redpanda quickstart at <https://docs.redpanda.com/current/get-started/quick-start/>.

## Premise

A clickstream is the textbook Kafka workload: a high-volume, never-ending sequence of user events where you care about the order of *one user's* events but not the global order across users. Build a synthetic clickstream producer that emits keyed events to a partitioned topic, then **prove two properties** that are the heart of the week: (1) all events for a given key land on one partition and arrive in produce order — per-key ordering; and (2) the keys spread evenly across partitions — no hot partition. This producer is also the seed of the Lab 08 mini-project and, through it, the source Week 9 streams from, so build it cleanly.

## Setup — Kafka in Docker (KRaft mode)

Save as `docker-compose.kafka.yml`. This is a single-node Kafka broker in KRaft mode (no ZooKeeper) plus the Confluent Schema Registry (you need it in Challenge 2 and the mini-project; harmless here).

```yaml
services:
  kafka:
    image: confluentinc/cp-kafka:7.6.1
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      # --- KRaft: this node is both broker and controller, no ZooKeeper ---
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:29093"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,PLAINTEXT_HOST://0.0.0.0:9092"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
      # single node -> everything is replication-factor 1
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      # a fixed cluster id keeps the data dir stable across restarts
      CLUSTER_ID: "kraft-cluster-crunch-data"

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.1
    container_name: schema-registry
    depends_on:
      - kafka
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: "PLAINTEXT://kafka:29092"
      SCHEMA_REGISTRY_LISTENERS: "http://0.0.0.0:8081"
```

```sh
docker compose -f docker-compose.kafka.yml up -d
# Create the topic explicitly with 3 partitions:
docker compose -f docker-compose.kafka.yml exec kafka \
  kafka-topics --bootstrap-server localhost:9092 \
  --create --topic clicks --partitions 3 --replication-factor 1
docker compose -f docker-compose.kafka.yml exec kafka \
  kafka-topics --bootstrap-server localhost:9092 --describe --topic clicks
```

### Redpanda alternative (drop-in)

If you would rather avoid the JVM/registry containers, this single-binary Redpanda compose exposes the same Kafka API on `:9092` and a built-in schema registry on `:8081` — your producer code does not change.

```yaml
services:
  redpanda:
    image: redpandadata/redpanda:v24.1.7
    container_name: redpanda
    command:
      - redpanda
      - start
      - --overprovisioned
      - --smp=1
      - --memory=1G
      - --reserve-memory=0M
      - --node-id=0
      - --kafka-addr=PLAINTEXT://0.0.0.0:9092
      - --advertise-kafka-addr=PLAINTEXT://localhost:9092
    ports:
      - "9092:9092"
      - "8081:8081"   # built-in schema registry
```

```sh
docker compose -f docker-compose.redpanda.yml up -d
# Redpanda's CLI is rpk; the wire protocol is identical to Kafka:
docker compose -f docker-compose.redpanda.yml exec redpanda \
  rpk topic create clicks -p 3
```

## Tasks

### T1 — The synthetic clickstream

Write `clickstream_producer.py` (use `confluent-kafka`):

- A pool of ~50 synthetic users (`user_0` … `user_49`) and a small set of pages (`/home`, `/search`, `/product/<id>`, `/cart`, `/checkout`).
- Each event is a dict: `{"user_id", "session_id", "url", "ts", "seq"}`, where `seq` is a **per-user monotonically increasing counter** (0, 1, 2, … for that user). The `seq` field is your ordering witness — it lets you later prove the consumer sees a user's events in order.
- Produce at a steady rate (e.g. ~200 events/sec for ~30 seconds, or a fixed total like 10,000) with `acks="all"` and `enable.idempotence=True`.
- **Key every event by `user_id`** so a user's events share a partition.
- Use the delivery callback to record, per user, which partition its events landed on.

### T2 — Prove per-key ordering

Write `ordering_check.py`, a consumer that reads the whole topic from `earliest` and, for each `user_id`, verifies that the `seq` values arrive **strictly increasing within each partition**. Because a user's events are all on one partition and a partition is read in offset order, a correct run sees `seq = 0, 1, 2, …` for each user with no gaps or reorderings. Report any user whose `seq` sequence is out of order (there should be none) and print, per user, `(partition, first_seq, last_seq, count)`.

### T3 — Verify partition distribution (no hot partition)

From the delivery-callback data (or by consuming and counting), produce a table of **records per partition** and **distinct users per partition**. With ~50 users over 3 partitions and a good hash, expect each partition to hold roughly a third of the records and roughly 15–18 users. Compute the skew (max partition count / min partition count); a healthy run is well under 2.0. Then deliberately break it: add a `--hot` mode that keys *every* event by a single constant (e.g. `"GLOBAL"`), rerun, and show that one partition now holds ~100% of the records — the hot-partition anti-pattern made concrete.

### T4 — Cross-check with the console tooling

Independently confirm your Python findings with the bundled CLI:

```sh
# Per-partition record counts (offsets per partition = record count here):
docker compose -f docker-compose.kafka.yml exec kafka \
  kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic clicks

# Watch keys and partitions live:
docker compose -f docker-compose.kafka.yml exec kafka \
  kafka-console-consumer --bootstrap-server localhost:9092 --topic clicks \
  --from-beginning --property print.key=true --property print.partition=true \
  --max-messages 20
```

## Acceptance criteria

- `clickstream_producer.py` produces N keyed events with `acks=all` + idempotence, keying by `user_id`, and reports the per-user partition.
- `ordering_check.py` reads the whole topic and confirms `seq` is strictly increasing per user, with **zero** ordering violations, and prints the per-user `(partition, first_seq, last_seq, count)` summary.
- The partition-distribution table shows the records and distinct users spread across all 3 partitions with skew < 2.0 in normal mode, and the `--hot` mode demonstrably collapses everything onto one partition.
- `GetOffsetShell` and `kafka-console-consumer --print.partition` independently corroborate the distribution and the key→partition mapping.
- A short `FINDINGS.md` records: the per-partition counts (normal and `--hot`), the computed skew, a one-paragraph explanation of *why* `user_id` is a good key for this stream (cardinality, even distribution, the ordering you actually need), and what would happen to your ordering proof if someone grew the topic from 3 to 6 partitions mid-stream (the `hash % n` remap breaks the cross-resize ordering for a key).

## Why this challenge matters

Key selection is the most consequential design decision you make on a Kafka topic, and it is invisible until it bites. Key by something with too little cardinality and you get a hot partition that caps your throughput at one consumer no matter how many you add. Key by the wrong field and the ordering you assumed you had — "a user's clicks are in order" — silently is not there. This challenge makes both the guarantee and the anti-pattern tangible: you produce a real stream, prove the per-key order holds, watch the distribution spread, and then deliberately collapse it to feel the failure. The `seq` ordering witness is a technique you will reuse for the rest of your streaming career — when you need to *prove* ordering rather than assume it, embed a per-key sequence number and assert it monotonic downstream.

## References

- Kafka producer configs (`acks`, `enable.idempotence`, `partitioner.class`) — <https://kafka.apache.org/documentation/#producerconfigs>
- Topics, partitions, keys, ordering — <https://kafka.apache.org/documentation/#intro_concepts_and_terms>
- `confluent-kafka-python` Producer — <https://docs.confluent.io/kafka-clients/python/current/overview.html#kafka-producer>
- KRaft mode — <https://kafka.apache.org/documentation/#kraft>
- Redpanda quickstart and `rpk` — <https://docs.redpanda.com/current/get-started/quick-start/>
- `kafka-topics` and console tools — <https://kafka.apache.org/documentation/#operations>
