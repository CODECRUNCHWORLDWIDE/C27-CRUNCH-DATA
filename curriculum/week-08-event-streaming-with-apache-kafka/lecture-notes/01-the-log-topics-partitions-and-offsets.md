# Lecture 1 — The Log: Topics, Partitions, and Offsets

> **Time:** 2 hours. Take the log/partition/offset material in one sitting and the replication/retention/keys material in a second. **Prerequisites:** Weeks 1–7 (you can run services in Docker and you finished Spark). **Citations:** the Apache Kafka design docs at <https://kafka.apache.org/documentation/#design>, the introduction at <https://kafka.apache.org/documentation/#intro>, the replication design at <https://kafka.apache.org/documentation/#replication>, log compaction at <https://kafka.apache.org/documentation/#compaction>, and KRaft at <https://kafka.apache.org/documentation/#kraft>.

## 1. The one idea: Kafka is a log, not a queue

Most engineers meet Kafka with a mental model from message queues — RabbitMQ, SQS, ActiveMQ — and that model is *wrong* in the one way that matters most. In a classic queue, a message is delivered to a consumer and then it is *gone*: the broker holds the message only until someone takes it, consumption is destructive, and a message has exactly one rightful recipient (or one per fan-out copy). Kafka does none of that.

A Kafka **topic** is a *log*: an append-only, immutable, ordered sequence of records. You only ever append to the end. You never update a record in place, and a consumer reading a record does not remove it — the record stays on disk until a *retention* policy expires it, regardless of who has or has not read it. A consumer is not a recipient that drains a queue; it is a **cursor** over the log, tracking how far it has read by an integer position called an **offset**. Ten different consumers can read the same topic completely independently, each at its own position, each rewinding or fast-forwarding at will, none affecting the others.

That difference is the whole week. Because the log is durable and re-readable:

- A new consumer can join later and read the *entire history* from the beginning.
- A consumer that crashes and restarts resumes from its last committed offset — it does not lose data and does not need the broker to have held messages for it specifically.
- You can stand up a *second* independent pipeline over the same topic (a real-time dashboard alongside a batch loader) without touching the first.
- You can replay: reset a consumer's offset to an old position and reprocess, e.g. after fixing a bug.

Jay Kreps's essay "The Log: What every software engineer should know about real-time data's unifying abstraction" is the canonical motivation for why this single data structure is so powerful; the formal treatment is the Kafka design docs at <https://kafka.apache.org/documentation/#design>. Hold this distinction in your head — *log, not queue* — and the rest of Kafka is detail on it.

## 2. Topics, partitions, and the offset

A topic is a named log. But a single log on a single machine has a ceiling: one machine's disk and network bandwidth. So Kafka splits a topic into **partitions** — independent logs, each of which can live on a different broker (server) and be written and read in parallel.

```text
Topic "clicks", 3 partitions:

  partition 0:  [r0][r1][r2][r3][r4] ->  (append here)
                  ^offset 0      ^offset 4

  partition 1:  [r0][r1][r2] ->
                  ^0       ^2

  partition 2:  [r0][r1][r2][r3] ->
                  ^0           ^3
```

Two facts about this picture are the foundation of everything:

1. **The offset is per-partition, monotonic, and permanent.** Within partition 0, the records are numbered 0, 1, 2, 3, 4 in the order they were appended, and those numbers never change or get reused. Offset 3 in partition 0 names that record *forever*. Offset 3 in partition 1 is a completely different record; offsets are not global, they are per-partition coordinates.

2. **Partitions are the unit of parallelism.** Three partitions can be served by up to three brokers and read by up to three consumers simultaneously. This is how a topic scales past one machine. The partition count is the cap on consumer parallelism — more on that in Lecture 2.

A few more terms you will see in tooling and logs:

- The **log-end offset (LEO)** of a partition is the offset that the *next* appended record will get — one past the last record.
- The **high-water mark (HW)** is the highest offset that has been replicated to all in-sync replicas and is therefore safe for consumers to read. A consumer never reads past the high-water mark.
- A consumer's **current position** is the offset it will read next; its **committed offset** is the offset it has durably recorded as "done" (used to resume after a restart). These two are not the same, and the gap between them is where delivery semantics live (Lecture 2).
- **Consumer lag** is `LEO − committed offset` — how far behind the consumer is. Watching lag is how you know a consumer is keeping up.

The reference is the Kafka intro at <https://kafka.apache.org/documentation/#intro_concepts_and_terms>.

## 3. The broker, the cluster, and KRaft

A **broker** is a single Kafka server. It stores some partitions on its disk and serves reads and writes for them. A **cluster** is a set of brokers. Partitions of a topic are distributed across the cluster's brokers so that load and storage spread out.

Historically Kafka used **ZooKeeper** — a separate distributed-coordination service — to store cluster metadata (which broker leads which partition, topic configs, ACLs). That is going away. **KRaft** (Kafka Raft, introduced by KIP-500) lets Kafka manage its own metadata using a built-in Raft consensus quorum, with no ZooKeeper at all. KRaft is production-ready and is the default for new clusters; ZooKeeper mode is deprecated. For this week, every Docker setup runs Kafka in **KRaft mode** — one fewer container, faster startup, simpler. You will see a `KAFKA_PROCESS_ROLES: broker,controller` line in the compose file; that is the broker also acting as the metadata controller in a single-node KRaft setup. The reference is <https://kafka.apache.org/documentation/#kraft> and the KIP at <https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum>.

## 4. Replication, leaders, followers, and the ISR

A partition that exists on only one broker disappears if that broker dies. So Kafka **replicates** each partition across several brokers, controlled by the topic's `replication.factor`. With `replication.factor=3`, each partition has three copies on three different brokers.

The copies are not equal. For each partition, one replica is the **leader** and the rest are **followers**:

- All reads and writes for the partition go to the **leader**. (Followers exist for durability, not for serving load.)
- **Followers** continuously fetch from the leader to stay current — they replicate the leader's log.
- The set of replicas that are caught up "enough" with the leader is the **in-sync replica set (ISR)**. A follower that falls too far behind drops out of the ISR.

If the leader broker dies, Kafka elects a new leader *from the ISR* — that is why the ISR matters: only an in-sync replica has the data, so only an in-sync replica may become leader. Two configs govern the durability/availability trade-off:

- `replication.factor` — how many copies exist.
- `min.insync.replicas` — the minimum ISR size required to accept a write when the producer asks for the strongest acknowledgement. With `min.insync.replicas=2` and a producer using `acks=all`, a write is acknowledged only once at least two replicas (leader + one follower) have it, so losing one broker loses no acknowledged data.

On a single-node laptop cluster you will run `replication.factor=1` — there is only one broker, so there is nothing to replicate to — and that is fine for learning, but understand that it means *zero* durability against broker loss. Production runs `replication.factor=3`. The reference is <https://kafka.apache.org/documentation/#replication>.

## 5. Keys and partitioning — the heart of ordering

Here is the rule that titles the whole week: **ordering is guaranteed only within a partition, never across partitions.** Within partition 0, records are read in append order, period. Between partition 0 and partition 1, there is *no* defined order — a record at offset 5 in partition 0 and a record at offset 3 in partition 1 have no temporal relationship Kafka will promise.

This is not a deficiency Kafka could fix with more engineering. A total order across all partitions would require coordinating every partition's appends through a single point, which is exactly the bottleneck partitioning exists to remove. Per-partition ordering is the *price* of horizontal scale, and it is a price worth paying because most of the time you do not need a global order — you need related events in order, and "related" means "same key."

A record has an optional **key**. The producer's default **partitioner** decides which partition a record lands in:

- **If the key is non-null:** `partition = hash(key) % num_partitions` (Kafka uses a murmur2 hash of the key bytes). The consequence is the one you must internalize: *every record with the same key goes to the same partition*, and is therefore strictly ordered relative to every other record with that key.
- **If the key is null:** the record is spread across partitions. Modern clients use a *sticky* partitioner (batch many records to one partition, then switch) rather than naive round-robin, but either way there is no per-key ordering because there is no key.

So if you want all of a given user's clicks in order, you key by `user_id`: every click for `user_42` lands in one partition, in the order produced. You do *not* get an order between `user_42`'s clicks and `user_99`'s clicks — but you almost never need that.

Two anti-patterns to avoid:

- **The hot partition.** If you key by something with very low cardinality or heavy skew — say, `country` where 80% of traffic is one country — most records pile into one partition, one broker, one consumer. You have thrown away the parallelism you partitioned for. Choose a key with enough cardinality and even distribution that the load spreads.
- **Changing the partition count later.** Because the mapping is `hash(key) % num_partitions`, adding partitions changes which partition a key maps to going forward. Records for `user_42` produced before the change are in the old partition; records after are in the (possibly different) new partition — so the *global* ordering for that key is broken across the resize. Decide your partition count up front; do not casually grow it on an ordering-sensitive topic.

The reference is <https://kafka.apache.org/documentation/#intro_concepts_and_terms> and the producer's `partitioner` config at <https://kafka.apache.org/documentation/#producerconfigs_partitioner.class>.

## 6. Retention vs. compaction — two ways a log forgets

A log cannot grow forever; brokers have finite disk. Kafka has two cleanup policies, set per topic by `cleanup.policy`.

### 6.1 Retention (`cleanup.policy=delete`, the default)

Records are kept for a window, then the oldest are deleted in whole *segments*:

- `retention.ms` — keep records for this long (default 7 days). After that, old segments are eligible for deletion.
- `retention.bytes` — keep at most this many bytes per partition; older segments are deleted to stay under the cap.

Retention is the right model for an **event stream**: a clickstream, log lines, metrics — data where each event is independently meaningful and old events stop mattering after a while. You keep a rolling window and let the past fall off. This is the model for the `clicks` topic you build this week.

### 6.2 Log compaction (`cleanup.policy=compact`)

Compaction keeps, for each *key*, only the **most recent** record — it garbage-collects superseded values rather than old time windows:

```text
Before compaction (key=value):
  [a=1][b=1][a=2][c=1][a=3][b=2]

After compaction (latest per key retained):
  [c=1][a=3][b=2]     (a=1, a=2, b=1 removed; latest survives)
```

A record with a key and a *null* value is a **tombstone** — it marks the key as deleted, and after a delay the tombstone itself is removed. Compaction is the right model for a **changelog / table**: "the current state of each entity." A topic that holds the latest profile for each `user_id`, or the latest balance for each `account_id`, wants compaction — a new consumer can read the compacted topic and reconstruct the current state of every key without replaying the entire history of changes. This is the foundation of Kafka-as-a-database patterns and of stream-table duality you will meet in Week 9.

The decision rule: **event stream → retention; current-state-per-key → compaction.** The reference is <https://kafka.apache.org/documentation/#compaction>.

## 7. A producer in Python with `confluent-kafka`

Enough concepts. Here is a real producer using the `confluent-kafka` client (the librdkafka-based client, which you install with `pip install confluent-kafka`). It produces keyed records to a partitioned topic and proves the key→partition mapping by reading back which partition each record landed in via the delivery callback.

```python
"""A minimal keyed producer. Run against a broker on localhost:9092."""
import json
import time
from confluent_kafka import Producer

# Configuration is a flat dict of librdkafka keys. Note the dotted keys —
# these are the canonical Kafka config names, NOT Python identifiers.
conf = {
    "bootstrap.servers": "localhost:9092",
    "acks": "all",                 # wait for all in-sync replicas (durability)
    "enable.idempotence": True,    # dedupe retries; safe default (Lecture 2)
    "client.id": "lecture-producer",
}

producer = Producer(conf)


def delivery_report(err, msg):
    """Called once per record when the broker acknowledges (or fails) it.

    This is asynchronous: produce() returns immediately; this fires later
    when poll()/flush() services the delivery queue.
    """
    if err is not None:
        print(f"DELIVERY FAILED for key={msg.key()}: {err}")
        return
    print(
        f"delivered key={msg.key().decode()} "
        f"-> topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
    )


# Produce several records for two keys. Because the key determines the
# partition (hash(key) % num_partitions), all records for "user_42" land in
# ONE partition (and are ordered), and all for "user_99" land in one partition.
users = ["user_42", "user_99"]
for i in range(6):
    user = users[i % 2]
    event = {"user_id": user, "url": f"/page/{i}", "ts": time.time()}
    producer.produce(
        topic="clicks",
        key=user.encode("utf-8"),          # key bytes -> partition assignment
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )
    # poll(0) services delivery callbacks without blocking. Without periodic
    # poll() the callbacks queue up and the internal buffer can fill.
    producer.poll(0)

# flush() blocks until every queued record is delivered (or fails) and all
# callbacks have fired. ALWAYS flush before exit, or you silently drop records.
producer.flush(timeout=10)
```

Run it and the delivery reports show every `user_42` record on one partition and every `user_99` record on one partition — the per-key ordering guarantee made visible. (The pure-Python alternative is `kafka-python`'s `KafkaProducer`; the API differs slightly, and it does **not** ship the schema-registry serializers, which is why this course prefers `confluent-kafka`. The `confluent-kafka` docs are at <https://docs.confluent.io/kafka-clients/python/current/overview.html>.)

Three details worth internalizing now:

- **`produce()` is asynchronous.** It enqueues the record and returns immediately; the actual send and the broker acknowledgement happen later, and the `delivery_report` callback fires then. This is why you must `poll()` periodically (to service callbacks) and `flush()` before exit (to drain the queue). Forgetting `flush()` is the single most common reason "my producer ran but no records showed up."
- **Keys and values are bytes.** Kafka stores opaque bytes; you encode/serialize yourself (here, `str.encode` and `json.dumps`). Lecture 3 replaces the manual JSON with the Avro serializer + schema registry.
- **`acks="all"` and `enable.idempotence=True`** are the durable, safe defaults. Lecture 2 explains exactly what they buy you and what they cost.

## 8. Creating the topic and inspecting it

Topics can be auto-created, but you should create them deliberately so you control the partition count and replication. From inside the broker container (the Docker setup is in the challenges and mini-project), the bundled CLI tools do the job:

```sh
# Create the clicks topic with 3 partitions, replication-factor 1 (single node).
kafka-topics --bootstrap-server localhost:9092 \
  --create --topic clicks --partitions 3 --replication-factor 1

# Describe it: shows partitions, leaders, replicas, and the ISR.
kafka-topics --bootstrap-server localhost:9092 --describe --topic clicks

# Consume from the beginning, printing the key and partition of each record.
kafka-console-consumer --bootstrap-server localhost:9092 --topic clicks \
  --from-beginning --property print.key=true --property print.partition=true
```

`kafka-topics --describe` is the command you reach for constantly: it shows, per partition, the leader broker, the replica set, and the ISR — the picture from Section 4 made concrete. `kafka-console-consumer` with `print.partition=true` lets you *see* the per-key partition mapping from Section 5 without writing code.

## 9. Exercise pointer

Now do **Exercise 1 — Produce and consume**. You will finish a `confluent-kafka` producer that emits keyed records to the 3-partition `clicks` topic and a consumer that reads them back, and you will confirm from the output that every record for a given key lands on the same partition. The acceptance criterion is that you can point at the output and say which partition each key maps to, and that records for one key appear in produce order.

## 10. Summary

- Kafka is a **log, not a queue**: an append-only, immutable, durable sequence of records that consumers read non-destructively as cursors. The log is the source of truth; consumers are independent views that can replay and rewind.
- A **topic** is split into **partitions** — independent logs that are the unit of both parallelism and ordering. The **offset** is a per-partition, monotonic, permanent position; the high-water mark caps what consumers can read; consumer lag is LEO minus the committed offset.
- A **broker** is one server; a cluster is many. **KRaft** (KIP-500) replaces ZooKeeper so Kafka manages its own metadata — the mode you run all week.
- Partitions are **replicated** (`replication.factor`). One replica is the **leader** (serves all reads/writes); **followers** replicate it; the **ISR** is the caught-up set, and only an ISR member can become leader. `min.insync.replicas` + `acks=all` defines the durability floor.
- **Ordering is only per-partition.** The record **key** maps to a partition via `hash(key) % num_partitions`, so same-key records share a partition and are ordered. Beware the hot partition (skewed key) and resizing an ordering-sensitive topic.
- **Retention** (`cleanup.policy=delete`, time/size window) suits event streams; **compaction** (`cleanup.policy=compact`, latest record per key, tombstones for deletes) suits current-state changelogs.
- A `confluent-kafka` `Producer` sends keyed byte records asynchronously; you must `poll()` to service delivery callbacks and `flush()` before exit. `acks="all"` + `enable.idempotence=True` are the safe defaults.

Cited pages: <https://kafka.apache.org/documentation/#design>, <https://kafka.apache.org/documentation/#intro_concepts_and_terms>, <https://kafka.apache.org/documentation/#replication>, <https://kafka.apache.org/documentation/#compaction>, <https://kafka.apache.org/documentation/#kraft>, <https://docs.confluent.io/kafka-clients/python/current/overview.html>.
