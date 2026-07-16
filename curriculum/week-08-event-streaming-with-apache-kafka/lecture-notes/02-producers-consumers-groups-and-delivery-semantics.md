# Lecture 2 — Producers, Consumers, Groups, and Delivery Semantics

> **Time:** 2 hours. Take the producer/durability material in one sitting and the consumer-group/rebalance/delivery-semantics material in a second — the second half is the conceptual core of the week. **Prerequisites:** Lecture 1 (the log, partitions, offsets, keys). **Citations:** the producer configs at <https://kafka.apache.org/documentation/#producerconfigs>, the consumer configs at <https://kafka.apache.org/documentation/#consumerconfigs>, the delivery-semantics section at <https://kafka.apache.org/documentation/#semantics>, the Confluent consumer docs at <https://docs.confluent.io/platform/current/clients/consumer.html>, and the Kafka transactions docs at <https://docs.confluent.io/platform/current/clients/producer.html#kafka-transactions>.

## 1. The producer, in depth: `acks` and durability

Lecture 1 produced records with `acks="all"` and moved on. Now we earn that choice. When a producer sends a record, it can wait for the broker to acknowledge with three different strictnesses, set by `acks`:

- **`acks=0` — fire and forget.** The producer does not wait for any acknowledgement. Lowest latency, highest throughput, *zero* durability guarantee: if the broker is down or the network drops the packet, the record is gone and the producer never knows. Use only for data where loss is acceptable (some metrics, some logs).
- **`acks=1` — leader acknowledges.** The producer waits until the partition *leader* has written the record to its log. Better, but there is a window: if the leader acknowledges and then dies *before* a follower replicates the record, and a follower is elected leader, the record is lost. This was the old default.
- **`acks=all` (a.k.a. `acks=-1`) — all in-sync replicas acknowledge.** The producer waits until every replica in the ISR has the record. Combined with `min.insync.replicas=2` (Lecture 1), this means an acknowledged record survives the loss of any single broker. Highest durability, highest latency. This is the modern default and what you use for data you cannot lose.

The trade-off is latency-and-throughput versus the guarantee against loss. There is no universally right answer; there is a right answer *for your data*. A clickstream feeding analytics can usually tolerate `acks=1`; a payment event cannot. The reference is <https://kafka.apache.org/documentation/#producerconfigs_acks>.

Two more producer configs shape throughput without changing the guarantee:

- **`linger.ms`** — wait up to this long to batch more records into one request (default 0, i.e. send immediately). A small `linger.ms` (5–20ms) dramatically improves throughput under load by amortizing the per-request overhead across many records, at the cost of a few ms of latency.
- **`batch.size`** — the maximum bytes per partition batch. Bigger batches, fewer requests, more throughput.

## 2. The idempotent producer — dedupe within a session

Here is a subtle failure. A producer sends a record, the broker writes it and sends an acknowledgement, but the acknowledgement is lost on the network. The producer's `retries` kick in and it sends the record *again*. Now the broker has the record twice — a duplicate, caused by a retry of an actually-successful send.

The **idempotent producer** fixes this. Set `enable.idempotence=true` and the producer attaches a **producer ID (PID)** and a per-partition **sequence number** to every record. The broker tracks the last sequence number it accepted per (PID, partition) and *rejects a duplicate* sequence number — so a retried record is deduplicated at the broker. The result: within a single producer session, retries never create duplicates, and you also get ordering preservation under retries (without idempotence, a retry could reorder records).

Enabling idempotence sets sane defaults automatically: `acks=all`, `retries` to a high value, and `max.in.flight.requests.per.connection<=5`. There is essentially no reason *not* to enable it for a producer that matters, which is why this course turns it on by default. Note its scope: it dedupes *retries within one producer session* — it does not dedupe records you deliberately `produce()` twice, and it does not span producer restarts (that is what transactions, Section 7, add). The reference is <https://kafka.apache.org/documentation/#producerconfigs_enable.idempotence>.

## 3. The consumer and the poll loop

A consumer reads records. The `confluent-kafka` consumer is a poll loop:

```python
from confluent_kafka import Consumer

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "analytics",            # the consumer GROUP this member joins
    "auto.offset.reset": "earliest",    # where to start if no committed offset
    "enable.auto.commit": False,        # we commit manually (see Section 6)
}
consumer = Consumer(conf)
consumer.subscribe(["clicks"])

try:
    while True:
        msg = consumer.poll(timeout=1.0)   # returns one message or None
        if msg is None:
            continue                       # no record within the timeout; loop
        if msg.error():
            print(f"consumer error: {msg.error()}")
            continue
        # Process the record.
        print(f"p{msg.partition()} @ {msg.offset()}: "
              f"key={msg.key()} value={msg.value()}")
        # Commit AFTER processing -> at-least-once (Section 6).
        consumer.commit(msg)
finally:
    consumer.close()   # leave the group cleanly; triggers a rebalance for peers
```

The pieces:

- **`group.id`** names the consumer group this member belongs to. It is the single most important consumer config — it determines how partitions are shared (Section 4) and where offsets are stored.
- **`auto.offset.reset`** decides where a *brand-new* group (one with no committed offset) starts: `earliest` (the beginning of the log — read all history) or `latest` (only records produced after the consumer joins). This only applies when there is no committed offset; an existing group resumes from its commit. The classic confusion — "I started my consumer but it read nothing" — is `latest` on a group that joined after the data was produced.
- **`poll(timeout)`** fetches the next available record (or returns `None` after the timeout). The loop *must* call `poll` regularly — it is also how the consumer sends heartbeats to the group coordinator and services rebalances. A loop that does heavy work between polls and stops heartbeating gets kicked out of the group (Section 5).
- **`close()`** leaves the group gracefully, which triggers an immediate, clean rebalance for the remaining members rather than waiting for a session timeout.

The reference is <https://docs.confluent.io/platform/current/clients/consumer.html>.

## 4. Consumer groups — scaling consumption

A **consumer group** is a set of consumer processes that share the work of reading a topic, identified by a common `group.id`. The fundamental guarantee:

> Each partition of a subscribed topic is assigned to **exactly one** consumer in the group at any time.

So with a 3-partition topic and a group:

- **1 consumer:** it gets all 3 partitions.
- **2 consumers:** one gets 2 partitions, the other gets 1.
- **3 consumers:** each gets exactly 1 partition — maximum parallelism for this topic.
- **4 consumers:** three get 1 partition each; the fourth gets *nothing* and sits idle. **The partition count caps useful consumer parallelism** — you cannot have more active consumers in a group than partitions.

This is how you scale: add consumers (up to the partition count) to read faster. It is also how you get fault tolerance: if a consumer crashes, its partitions are reassigned to the survivors (Section 5), so no partition goes unread.

Two different groups (`group.id=analytics` and `group.id=archival`) are *independent* — each gets its own full copy of the partition assignment and its own offsets, so both read every record. That is the fan-out from Lecture 1: many independent pipelines over one topic. Within a group, the records are *divided*; across groups, they are *duplicated*.

Note: `subscribe()` opts into group management and automatic assignment. There is also `assign()`, which manually pins specific partitions to a consumer and bypasses the group — useful for special cases, but `subscribe` is the normal path and the one that rebalances.

## 5. Rebalancing — when the assignment changes

A **rebalance** is Kafka recomputing the partition-to-consumer assignment within a group. It is triggered by:

- A consumer **joining** the group (new member → redistribute partitions to include it).
- A consumer **leaving** — gracefully (`close()`) or by crashing (the coordinator notices missed heartbeats after `session.timeout.ms`).
- The **partition count changing** (someone added partitions to the topic).

The **group coordinator** (a broker role) manages this. The two protocols matter because they differ in how disruptive they are:

### 5.1 Eager rebalancing (the older default)

Every consumer in the group *revokes all* its partitions, then the coordinator reassigns everything from scratch. During the rebalance, *no consumer is processing anything* — the whole group stops. This "stop-the-world" pause is fine for small groups but painful at scale, where a rolling deploy can cause repeated full stalls.

### 5.2 Cooperative-sticky rebalancing (the modern default)

The cooperative protocol (`CooperativeStickyAssignor`) rebalances *incrementally*: only the partitions that actually need to move are revoked, and consumers keep processing the partitions they retain. A new member joining a 3-consumer group steals one partition from one peer rather than stopping all three. It also tries to keep partitions on the same consumer across rebalances ("sticky") to preserve any local state. You select it with:

```python
conf = {
    # ...
    "partition.assignment.strategy": "cooperative-sticky",
}
```

The reference is <https://kafka.apache.org/documentation/#consumerconfigs_partition.assignment.strategy>.

### 5.3 Watching a rebalance

`confluent-kafka` lets you pass `on_assign` and `on_revoke` callbacks to `subscribe`, which fire exactly when partitions are handed to or taken from this consumer:

```python
def on_assign(consumer, partitions):
    print(f"ASSIGNED: {[(p.topic, p.partition) for p in partitions]}")

def on_revoke(consumer, partitions):
    print(f"REVOKED: {[(p.topic, p.partition) for p in partitions]}")

consumer.subscribe(["clicks"], on_assign=on_assign, on_revoke=on_revoke)
```

Run one consumer (it gets all 3 partitions, you see `ASSIGNED: [..., 0, 1, 2]`), then start a second member of the same group, and you watch the first consumer's callback fire with a revoke/reassign as it gives up partitions to the newcomer. That is the live rebalance you produce in Exercise 2 — it is one of the most clarifying things you do all week.

A practical note: **static membership.** Setting `group.instance.id` to a stable value per consumer marks it a "static" member, so a quick restart (a rolling deploy, a brief crash) within `session.timeout.ms` does *not* trigger a rebalance — the returning consumer reclaims its old partitions. This avoids the rebalance storm of restarting a large group.

## 6. Delivery semantics — the offset-commit ordering is everything

Now the conceptual core. Kafka offers three delivery semantics, and the difference between them is *when you commit the offset relative to when you process the record*.

A consumer's **committed offset** is the durable bookmark it resumes from after a restart. The question is: do you commit *before* or *after* you act on the record?

### 6.1 At-most-once — commit first, then process

```python
msg = consumer.poll(1.0)
consumer.commit(msg)     # commit BEFORE processing
process(msg)             # if we CRASH here, the offset is already committed...
```

If the process crashes *after* committing but *before* (or during) processing, the record is never reprocessed on restart — it is **lost**. At-most-once means: every record is processed zero or one times — *never duplicated, but possibly lost*. Rarely what you want, but cheap.

### 6.2 At-least-once — process first, then commit (the common default)

```python
msg = consumer.poll(1.0)
process(msg)             # process FIRST
consumer.commit(msg)     # commit AFTER processing succeeds
```

If the process crashes *after* processing but *before* committing, the record's offset was never committed, so on restart the consumer reprocesses it — a **duplicate**. At-least-once means: every record is processed one or more times — *never lost, but possibly duplicated*. This is the default and most common semantic in practice. **The consequence you must design for: your downstream must tolerate duplicates** — make the processing idempotent (upsert by a key, dedupe by an event ID), so reprocessing the same record twice is harmless.

This is why `enable.auto.commit=False` and manual `commit` after processing is the standard pattern. Auto-commit (`enable.auto.commit=True`, the default) commits periodically on a timer in the background — convenient but it commits offsets for records that may not have been *processed* yet (they were only `poll`ed), which can silently turn into at-most-once on a crash. For anything that matters, commit manually, after processing.

### 6.3 Exactly-once — neither lost nor duplicated

Each record affects the result exactly once — no loss, no duplication. This is the expensive one, and it requires transactions. Section 7.

The reference is <https://kafka.apache.org/documentation/#semantics>.

## 7. Exactly-once with transactions (read-process-write)

True exactly-once *within Kafka* is real, since Kafka 0.11, via **transactions** layered on the idempotent producer. The canonical use case is the **read-process-write** loop: consume from an input topic, transform, produce to an output topic, and make the *consume-offset-commit* and the *produce* atomic — either both happen or neither does.

The mechanism: a transactional producer is given a stable `transactional.id`. It can `begin_transaction()`, produce output records, *atomically include* the input consumer's offsets in the same transaction with `send_offsets_to_transaction()`, and `commit_transaction()`. If anything fails, `abort_transaction()` rolls back both the produced records and the offset commit. Consumers downstream set `isolation.level=read_committed` so they only see records from *committed* transactions, never aborted ones.

```python
from confluent_kafka import Producer, Consumer, TopicPartition, KafkaError

producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "transactional.id": "clicks-enricher-1",   # stable ID -> transactional
    "enable.idempotence": True,                 # implied, but explicit is clear
})
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "enricher",
    "enable.auto.commit": False,                # offsets committed via the txn
    "auto.offset.reset": "earliest",
    "isolation.level": "read_committed",        # only see committed output
})

producer.init_transactions()      # one-time handshake with the broker
consumer.subscribe(["clicks"])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    producer.begin_transaction()
    # 1) produce the transformed output
    enriched = transform(msg.value())
    producer.produce("clicks-enriched", key=msg.key(), value=enriched)
    # 2) include THIS consumer's offset in the transaction atomically
    producer.send_offsets_to_transaction(
        [TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)],
        consumer.consumer_group_metadata(),
    )
    # 3) commit: the produced record AND the offset advance, atomically
    producer.commit_transaction()
```

If the process dies mid-transaction, the broker aborts the open transaction; on restart, the consumer resumes from the last *committed* offset (because the offset advance was part of the transaction), so the failed record is reprocessed and re-produced — but its earlier, aborted output is invisible to `read_committed` consumers. Net effect: each input record produces exactly one visible output and advances the offset exactly once.

**The honest caveat:** this is exactly-once *processing within Kafka's boundaries*. If your "process" step writes to an external database or calls a payment API, that external side effect is *not* in the Kafka transaction — exactly-once does not extend there unless that system also participates in a two-phase commit or you make the external write idempotent. "Exactly-once" is a precise claim about Kafka-to-Kafka pipelines, not a universal guarantee. The reference is <https://docs.confluent.io/platform/current/clients/producer.html#kafka-transactions>.

## 8. Choosing your semantic

The decision in practice:

- **Default to at-least-once** with manual commit after processing, and make your downstream idempotent. This covers the large majority of pipelines and is the simplest correct choice.
- **Use exactly-once transactions** when the pipeline is Kafka-to-Kafka, duplicates are genuinely unacceptable, and you can pay the throughput cost (transactions add coordination overhead).
- **Use at-most-once** only when loss is acceptable and you want the lowest latency — rare.

The trap is having a semantic *by accident*: a consumer with default auto-commit that does slow processing is silently somewhere between at-most-once and at-least-once depending on timing, and its author usually cannot say which. Know your semantic; prove it (Challenge 2 makes you prove it across a forced restart).

## 9. Exercise pointer

Now do **Exercise 2 — Consumer groups and rebalance**. You will run two members of one consumer group against the 3-partition `clicks` topic, wire up the `on_assign`/`on_revoke` callbacks, start the second member while the first is running, and capture the rebalance in the output — the first consumer giving up a partition to the newcomer. The acceptance criterion is a log showing the assignment before (one consumer owns 0,1,2), the revoke/reassign during, and after (the two consumers split the partitions).

## 10. Summary

- **Producer durability is `acks`**: `0` (fire-and-forget, can lose), `1` (leader only, small loss window), `all` (every ISR member, survives one broker loss with `min.insync.replicas=2`). `linger.ms`/`batch.size` trade a little latency for throughput.
- The **idempotent producer** (`enable.idempotence=true`) attaches a PID + sequence number so the broker dedupes *retries within a session* and preserves order under retries. Turn it on.
- A **consumer** is a poll loop keyed by `group.id`; `auto.offset.reset` (`earliest`/`latest`) decides where a brand-new group starts; `poll()` also heartbeats; `close()` leaves cleanly.
- A **consumer group** assigns each partition to exactly one member. More consumers = more parallelism, capped at the partition count. Different `group.id`s read the topic independently (fan-out).
- A **rebalance** redistributes partitions when a member joins/leaves or partitions change. **Eager** stops the world; **cooperative-sticky** (the modern default) moves only what must move. `on_assign`/`on_revoke` let you watch it; `group.instance.id` (static membership) avoids rebalances on quick restarts.
- **Delivery semantics are the commit ordering:** commit-then-process = **at-most-once** (can lose); process-then-commit = **at-least-once** (can duplicate — the common default, so make downstream idempotent); transactions = **exactly-once**.
- **Exactly-once** uses a transactional producer (`transactional.id`) + `send_offsets_to_transaction` + `commit_transaction`, with `read_committed` consumers. It is exactly-once *within Kafka*, not across external side effects.

Cited pages: <https://kafka.apache.org/documentation/#producerconfigs_acks>, <https://kafka.apache.org/documentation/#producerconfigs_enable.idempotence>, <https://kafka.apache.org/documentation/#consumerconfigs>, <https://kafka.apache.org/documentation/#semantics>, <https://docs.confluent.io/platform/current/clients/consumer.html>, <https://docs.confluent.io/platform/current/clients/producer.html#kafka-transactions>.
