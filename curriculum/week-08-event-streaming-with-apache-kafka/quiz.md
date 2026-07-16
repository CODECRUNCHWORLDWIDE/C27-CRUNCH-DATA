# Week 8 — Quiz

Ten multiple-choice questions covering the log abstraction, partitions and offsets, consumer groups and rebalancing, delivery semantics, ordering, retention vs. compaction, and schema-registry compatibility. Treat the quiz as a closed-book check; the answer key with reasoning is at the bottom.

## Question 1 — The log abstraction

How does a Kafka consumer reading a record differ from a classic message queue consumer reading a message?

- (A) There is no difference; both delete the message on read.
- (B) The Kafka record stays on the log until retention expires — the consumer is a cursor (tracked by offset), so many independent consumers can read the same record and rewind/replay.
- (C) Kafka records can only be read once, by exactly one consumer.
- (D) Kafka deletes a record as soon as any consumer reads it.

## Question 2 — Offsets

Which statement about offsets is correct?

- (A) An offset is a global, cluster-wide sequence number across all partitions.
- (B) An offset is a per-partition, monotonically increasing, permanent position; the same offset number names different records in different partitions.
- (C) Offsets are reassigned when old records are deleted.
- (D) An offset is the timestamp at which a record was produced.

## Question 3 — Ordering

What ordering does Kafka guarantee?

- (A) Total order across the entire topic, regardless of partitions.
- (B) Order only within a single partition; no order is guaranteed across partitions.
- (C) Order across partitions but not within a partition.
- (D) No ordering guarantees at all.

## Question 4 — Keys and partitioning

You key your records by `user_id`. What does this achieve, assuming a non-null key and the default partitioner?

- (A) Every record goes to a random partition for even spread.
- (B) All records with the same `user_id` go to the same partition (via `hash(key) % num_partitions`) and are therefore ordered relative to each other.
- (C) Records are sorted globally by `user_id`.
- (D) The key is ignored unless you write a custom partitioner.

## Question 5 — Consumer groups

A topic has 4 partitions and a consumer group has 6 consumers. What happens?

- (A) Each partition is split across consumers so all 6 are busy.
- (B) Four consumers each get one partition; two consumers are idle (the partition count caps useful parallelism).
- (C) All 6 consumers read all 4 partitions.
- (D) Kafka rejects the 5th and 6th consumers.

## Question 6 — Rebalancing

What is the difference between eager and cooperative-sticky rebalancing?

- (A) Eager moves only the partitions that must move; cooperative stops the whole group.
- (B) Eager revokes all partitions from all members and reassigns from scratch (stop-the-world); cooperative-sticky incrementally moves only the partitions that need to change, letting consumers keep processing the rest.
- (C) They are the same; the names are aliases.
- (D) Cooperative-sticky disables rebalancing entirely.

## Question 7 — Delivery semantics

You commit the consumer offset *before* processing each record. Which delivery semantic is this, and what is its failure mode?

- (A) Exactly-once; no failure mode.
- (B) At-least-once; records may be duplicated.
- (C) At-most-once; a crash after committing but before processing loses the record.
- (D) At-most-once; records may be duplicated.

## Question 8 — `acks` and durability

A producer uses `acks=1`. Under what condition can an acknowledged record still be lost?

- (A) Never; `acks=1` guarantees durability.
- (B) If the partition leader acknowledges the write and then dies before any follower has replicated the record, and a follower is elected leader.
- (C) Only if all brokers fail simultaneously.
- (D) If the consumer does not commit its offset.

## Question 9 — Retention vs. compaction

You want a topic whose consumers can always reconstruct the *current* value for each key (e.g. the latest profile per `user_id`), even a consumer joining much later. Which cleanup policy fits?

- (A) `cleanup.policy=delete` with a long `retention.ms`.
- (B) `cleanup.policy=compact`, which keeps the latest record per key (and uses tombstones for deletes).
- (C) No cleanup policy; keep everything forever.
- (D) `cleanup.policy=delete` with `retention.bytes` set low.

## Question 10 — Schema compatibility

A subject is in `BACKWARD` compatibility mode with a registered schema. Which proposed change does the registry **reject**?

- (A) Adding a new field that has a default value.
- (B) Removing a field.
- (C) Adding a new required field with no default value.
- (D) Adding a nullable field (`["null","string"]`) with `default: null`.

---

## Answer key

- **Q1: (B).** Kafka is a *log*, not a queue: a record is appended and stays until retention expires, regardless of who has read it. A consumer is a cursor tracked by offset, so many independent consumers (and consumer groups) can each read the same record at their own pace, rewind to an old offset, or replay. Destructive, single-recipient delivery is the queue model Kafka deliberately does not use. Cite <https://kafka.apache.org/documentation/#design>.
- **Q2: (B).** An offset is *per-partition*: a monotonically increasing integer naming a record's position within one partition, permanent (never reused, never changed). Offset 3 in partition 0 and offset 3 in partition 1 are different records; there is no global offset. Old-record deletion does not renumber surviving records. Cite <https://kafka.apache.org/documentation/#intro_concepts_and_terms>.
- **Q3: (B).** Ordering is guaranteed *only within a partition*, never across partitions. This is the price of horizontal scale — a total order would require funneling all appends through one coordination point. To order related records, put them in the same partition with the same key. Cite <https://kafka.apache.org/documentation/#semantics>.
- **Q4: (B).** With a non-null key the default partitioner computes `partition = hash(key) % num_partitions` (murmur2 hash), so every record with the same `user_id` lands in the same partition and is ordered relative to the others for that key. There is no global sort and no random spread for keyed records. Cite <https://kafka.apache.org/documentation/#intro_concepts_and_terms>.
- **Q5: (B).** A group assigns each partition to exactly one member, so 4 partitions support at most 4 active consumers; the 5th and 6th sit idle with no assignment. The partition count caps useful consumer parallelism — to scale consumption past 4, you need more partitions. Cite <https://docs.confluent.io/platform/current/clients/consumer.html>.
- **Q6: (B).** Eager rebalancing is "stop-the-world": every member revokes all its partitions and the coordinator reassigns from scratch, so no one processes during the rebalance. Cooperative-sticky (the modern default) is incremental: only the partitions that must move are revoked, members keep processing what they retain, and assignments stay "sticky" across rebalances. Cite <https://kafka.apache.org/documentation/#consumerconfigs_partition.assignment.strategy>.
- **Q7: (C).** Committing before processing is **at-most-once**: if the process crashes after committing but before (or during) processing, the offset is already advanced, so the record is never reprocessed on restart — it is lost. At-most-once means zero-or-one processings: never duplicated, possibly lost. Cite <https://kafka.apache.org/documentation/#semantics>.
- **Q8: (B).** `acks=1` waits only for the *leader* to write. If the leader acknowledges and then dies before a follower has replicated the record, and that follower becomes the new leader, the record is gone — the producer was told "success" but the data did not survive. `acks=all` with `min.insync.replicas≥2` closes this window. Cite <https://kafka.apache.org/documentation/#producerconfigs_acks>.
- **Q9: (B).** Log compaction (`cleanup.policy=compact`) retains the most recent record per key (with null-value tombstones marking deletions), so a late-joining consumer can read the compacted topic and reconstruct the current state of every key without replaying full history. Time/size retention (`delete`) suits event streams, not current-state-per-key. Cite <https://kafka.apache.org/documentation/#compaction>.
- **Q10: (C).** Under `BACKWARD` compatibility, a new schema must be able to read data written with the old schema. Adding a *required* field with **no default** breaks this — old data has no value for the new required field and there is no default to fill in — so the registry rejects it (HTTP 409). Adding a field *with* a default (A, D) and removing a field (B) are all backward-compatible. Cite <https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html>.

## Self-assessment

- 9-10: you can ship Lab 08 and pass the gate without further reading; your `clicks` topic will be clean for Week 9.
- 7-8: re-read the lecture notes on the questions you missed; the ordering (Q3/Q4) and delivery-semantics (Q7) questions are the ones that bite during the mini-project and the challenges.
- 5-6: re-read all three lecture notes and redo the exercises, especially Exercise 2 (consumer groups/rebalance) and Exercise 3 (schema compatibility).
- 0-4: rewind to Lecture 1. The lab will not come together without the log/partition/offset model and the per-partition-ordering rule firmly in hand.
