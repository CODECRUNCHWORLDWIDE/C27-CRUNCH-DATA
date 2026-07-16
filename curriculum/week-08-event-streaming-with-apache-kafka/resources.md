# Week 8 — Resources

Every resource on this page is **free** and open. Apache Kafka, Apache Avro, and their documentation are published by the Apache Software Foundation under the Apache-2.0 license at no cost. The Confluent Platform documentation (schema registry, clients) is free to read. `confluent-kafka-python` is open source (Apache-2.0). Redpanda's community edition and docs are free. *Kafka: The Definitive Guide* is available as a free PDF download from Confluent.

## Required reading (work it into your week)

### Apache Kafka official documentation

- **Apache Kafka documentation (home)** — the canonical reference; bookmark it:
  <https://kafka.apache.org/documentation/>
- **Design** — the log abstraction, why Kafka is built the way it is; the section that motivates the whole week:
  <https://kafka.apache.org/documentation/#design>
- **Introduction — concepts and terms** — topics, partitions, offsets, brokers, consumer groups, keys:
  <https://kafka.apache.org/documentation/#intro_concepts_and_terms>
- **Replication** — leaders, followers, the in-sync replica set (ISR), durability:
  <https://kafka.apache.org/documentation/#replication>
- **Message delivery semantics** — at-most-once, at-least-once, exactly-once, and what each costs:
  <https://kafka.apache.org/documentation/#semantics>
- **Log compaction** — retention vs. compaction, tombstones, the changelog model:
  <https://kafka.apache.org/documentation/#compaction>
- **KRaft (no ZooKeeper)** — the self-managed metadata quorum you run all week:
  <https://kafka.apache.org/documentation/#kraft>
- **Producer configs** — `acks`, `enable.idempotence`, `linger.ms`, `batch.size`, `partitioner.class`:
  <https://kafka.apache.org/documentation/#producerconfigs>
- **Consumer configs** — `group.id`, `auto.offset.reset`, `enable.auto.commit`, `partition.assignment.strategy`, `isolation.level`:
  <https://kafka.apache.org/documentation/#consumerconfigs>

### The book

- **Kafka: The Definitive Guide, 2nd Edition** — Gwen Shapira, Todd Palino, Rajini Sivaram, and Krit Petty (O'Reilly, 2021). The standard reference; the chapters on producers, consumers, reliable delivery, and exactly-once map directly onto this week. Free PDF download via Confluent:
  <https://www.confluent.io/resources/kafka-the-definitive-guide-v2/>

### The schema registry and the Python client

- **Confluent Schema Registry — documentation** — subjects, versions, IDs, the wire format, the REST API:
  <https://docs.confluent.io/platform/current/schema-registry/index.html>
- **Schema Registry — schema evolution and compatibility** — `BACKWARD`/`FORWARD`/`FULL`/`NONE`, the concrete rules, the upgrade order:
  <https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html>
- **Schema Registry — Avro fundamentals** — Avro in the registry, serializers/deserializers, reader/writer schema resolution:
  <https://docs.confluent.io/platform/current/schema-registry/fundamentals/avro.html>
- **Schema Registry — REST API reference** — registering, listing versions, testing compatibility, setting modes:
  <https://docs.confluent.io/platform/current/schema-registry/develop/api.html>
- **confluent-kafka-python — overview and API** — `Producer`, `Consumer`, `SerializingProducer`/`DeserializingConsumer`, `AvroSerializer`, `SchemaRegistryClient`, the transactional API:
  <https://docs.confluent.io/kafka-clients/python/current/overview.html>

### Avro

- **Apache Avro — documentation** — the data model, the binary encoding, schema resolution, the rules that drive compatibility:
  <https://avro.apache.org/docs/>
- **Apache Avro — specification** — the precise encoding and schema-resolution semantics:
  <https://avro.apache.org/docs/1.11.1/specification/>

## Redpanda (the drop-in alternative)

- **Redpanda documentation (home)** — the Kafka-API-compatible, single-binary, no-JVM broker with a built-in schema registry:
  <https://docs.redpanda.com/>
- **Redpanda — quickstart** — bring up a single node and produce/consume:
  <https://docs.redpanda.com/current/get-started/quick-start/>
- **Redpanda — `rpk` CLI reference** — the `rpk topic`/`rpk group` commands that replace the `kafka-*` scripts:
  <https://docs.redpanda.com/current/reference/rpk/>
- **Redpanda — schema registry** — the built-in registry on `:8081`:
  <https://docs.redpanda.com/current/manage/schema-reg/schema-reg-overview/>

## The KIPs (the design proposals behind the mechanisms)

- **KIP-500 — Replace ZooKeeper with a Self-Managed Metadata Quorum (KRaft)** — why and how Kafka manages its own metadata:
  <https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum>
- **KIP-98 — Exactly Once Delivery and Transactional Messaging** — the design of the idempotent producer and transactions:
  <https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging>
- **KIP-429 — Incremental Cooperative Rebalancing** — the cooperative-sticky protocol:
  <https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol>

## Operations and tooling

- **Kafka operations** — the `kafka-topics`, `kafka-consumer-groups`, `kafka-console-consumer/producer` CLI tools:
  <https://kafka.apache.org/documentation/#operations>
- **Confluent Platform — clients (producer/consumer guides)** — deeper producer and consumer documentation:
  <https://docs.confluent.io/platform/current/clients/index.html>
- **`kcat`** — the netcat-style Kafka CLI for quick produce/consume/inspect:
  <https://github.com/edenhill/kcat>

## Recommended reading (after the required set)

- **"The Log: What every software engineer should know about real-time data's unifying abstraction"** — Jay Kreps. The essay that motivates the log as the universal data-infrastructure abstraction; the conceptual backbone of this week:
  <https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying>
- **Confluent — "Schema Evolution and Compatibility"** (blog/docs companion) — worked compatibility examples beyond the reference:
  <https://docs.confluent.io/platform/current/schema-registry/fundamentals/index.html>
- **Designing Data-Intensive Applications** — Martin Kleppmann (O'Reilly, 2017), Chapter 11 ("Stream Processing"). The deeper treatment of logs, change data capture, and stream-table duality that Week 9 builds on:
  <https://dataintensive.net/>

## Citations policy

This curriculum cites the official Apache Kafka and Apache Avro documentation, the Confluent Platform docs (schema registry, clients), and the Redpanda docs as the primary references, plus the relevant KIPs for the design rationale and *Kafka: The Definitive Guide* (free PDF) for depth. Every code example is written against `confluent-kafka` (the librdkafka-based Python client) targeting a Kafka broker run in KRaft mode in Docker. *Designing Data-Intensive Applications* is cited only for depth beyond the free material; the graded path never requires it. If a citation is missing from a section of these notes, treat it as a bug and open an issue against the C27 curriculum repository.
