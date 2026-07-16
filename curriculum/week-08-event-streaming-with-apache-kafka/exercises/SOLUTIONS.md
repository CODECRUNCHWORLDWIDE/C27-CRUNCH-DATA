# Exercise Solutions — Week 8

Read these after attempting the exercises. Each solution gives the worked Python, the console output you should see, and the canonical mistakes. All three assume Kafka on `localhost:9092` and (for Exercise 3) the schema registry on `http://localhost:8081`, brought up with the Docker compose in `mini-project/README.md`, and the `clicks` topic created with 3 partitions:

```sh
kafka-topics --bootstrap-server localhost:9092 \
  --create --topic clicks --partitions 3 --replication-factor 1
```

---

## Exercise 1 — Produce and consume

### Worked solution

```python
def delivery_report(err, msg):
    if err is not None:
        print(f"DELIVERY FAILED: {err}")
        return
    print(f"key={msg.key().decode()} -> "
          f"partition={msg.partition()} offset={msg.offset()}")


def produce():
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
        "client.id": "ex01-producer",
    }
    producer = Producer(conf)
    for i in range(20):
        user = USERS[i % len(USERS)]
        page = PAGES[i % len(PAGES)]
        event = {"user_id": user, "url": page, "ts": time.time()}
        producer.produce(
            topic=TOPIC,
            key=user.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)
    producer.flush(timeout=10)


def consume():
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "ex01-reader",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    last = time.time()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.time() - last > 3.0:
                    break
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}")
                continue
            last = time.time()
            print(f"p{msg.partition()} @ {msg.offset()}: "
                  f"key={msg.key().decode()} value={msg.value().decode()}")
            consumer.commit(msg)
    finally:
        consumer.close()
```

### What success looks like

`produce` output (the exact partition numbers depend on the murmur2 hash of the key, but each user is *stable*):

```text
key=user_42 -> partition=2 offset=0
key=user_99 -> partition=0 offset=0
key=user_7  -> partition=1 offset=0
key=user_13 -> partition=2 offset=1
key=user_42 -> partition=2 offset=2
key=user_99 -> partition=0 offset=1
...
```

The load-bearing observation: **every `user_42` record shows `partition=2`, every `user_99` shows `partition=0`**, and so on — same key, same partition, every time. That is `hash(key) % 3` made visible. (`user_42` and `user_13` happening to share partition 2 is fine — two keys can collide onto one partition; what matters is one key never spreads across partitions.)

`consume` output, run with the broker CLI as a cross-check:

```sh
kafka-console-consumer --bootstrap-server localhost:9092 --topic clicks \
  --from-beginning --property print.key=true --property print.partition=true
```

```text
Partition:0	user_99	{"user_id": "user_99", "url": "/search", ...}
Partition:0	user_99	{"user_id": "user_99", "url": "/checkout", ...}
Partition:2	user_42	{"user_id": "user_42", "url": "/home", ...}
...
```

### Common pitfalls

- **No records appear after producing.** You forgot `producer.flush()`. `produce()` is asynchronous — it enqueues and returns; without `flush()` (or enough `poll()`) the process exits with records still buffered, and they are silently lost. This is the single most common Kafka-producer bug.
- **The consumer reads nothing.** You used `auto.offset.reset="latest"` (the librdkafka default) on a fresh group that joined *after* the records were produced — `latest` means "only new records from now on." Use `"earliest"` to read history.
- **`TypeError: key must be bytes`** — you passed a `str` for the key/value. The base `Producer` takes bytes; encode with `.encode("utf-8")`. (The `SerializingProducer` in Exercise 3 takes typed objects and serializes for you — different API.)
- **Same user lands on different partitions.** You produced without a key (or with a different key each time), so the partitioner spread the records. Per-key ordering requires the *same* key for related records.

---

## Exercise 2 — Consumer groups and rebalance

### Worked solution

```python
def make_callbacks(member_name):
    def on_assign(consumer, partitions):
        print(f"[{member_name}] ASSIGNED  "
              f"{[(p.topic, p.partition) for p in partitions]}")

    def on_revoke(consumer, partitions):
        print(f"[{member_name}] REVOKED   "
              f"{[(p.topic, p.partition) for p in partitions]}")

    return on_assign, on_revoke


def run_member(member_name):
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "cooperative-sticky",
    }
    consumer = Consumer(conf)
    on_assign, on_revoke = make_callbacks(member_name)
    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    print(f"[{member_name}] started; polling.")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[{member_name}] error: {msg.error()}")
                continue
            print(f"[{member_name}] read p{msg.partition()} @ {msg.offset()}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print(f"[{member_name}] closed.")
```

### The rebalance log lines you should see

**Terminal A**, started alone — it gets all three partitions:

```text
[A] started; polling.
[A] ASSIGNED  [('clicks', 0), ('clicks', 1), ('clicks', 2)]
[A] read p0 @ 0
[A] read p1 @ 0
[A] read p2 @ 0
...
```

Now start **Terminal B** with the same `group.id`. Watch A give up partitions to B. With `cooperative-sticky`, A keeps what it can and only the moving partitions are revoked/reassigned:

```text
# terminal A:
[A] REVOKED   [('clicks', 2)]
[A] ASSIGNED  [('clicks', 0), ('clicks', 1)]

# terminal B:
[B] started; polling.
[B] ASSIGNED  [('clicks', 2)]
[B] read p2 @ 7
```

(With the **eager** default, A would instead show `REVOKED [0,1,2]` then `ASSIGNED [0,1]` — it gives up *everything* and gets a subset back, the "stop-the-world" behavior. Running it both ways and comparing the revoke lines is the point of Step 2a.)

Now Ctrl-C **Terminal B**. Its `close()` triggers an immediate rebalance and A reclaims partition 2:

```text
# terminal B:
[B] closed.

# terminal A:
[A] ASSIGNED  [('clicks', 0), ('clicks', 1), ('clicks', 2)]
```

### What this proves

- Each partition is owned by **exactly one** member at a time — you never see both A and B reading p2 simultaneously.
- The partition count (3) **caps** parallelism: start a *third* member and it sits idle with `ASSIGNED []` because there are only three partitions to go around.
- A graceful `close()` causes a fast, clean rebalance; killing the process with `kill -9` instead makes the survivors wait out `session.timeout.ms` (default ~45s) before the coordinator declares the member dead and rebalances — try both and feel the difference.

### Common pitfalls

- **No rebalance fires.** Both members must use the *same* `group.id`. Different group IDs are independent groups — each gets all three partitions, no sharing, no rebalance.
- **The callbacks never print.** You called `subscribe([TOPIC])` without passing `on_assign`/`on_revoke`. The callbacks are how the rebalance is visible; pass them.
- **A consumer gets kicked out mid-loop.** If you do slow work between `poll()` calls and stop heartbeating past `max.poll.interval.ms`, the coordinator evicts the member and rebalances. `poll()` regularly.

---

## Exercise 3 — Avro schema registry compatibility

### Worked solution

```python
def produce_and_consume_v1(registry):
    serializer = AvroSerializer(
        schema_registry_client=registry,
        schema_str=CLICK_V1,
        to_dict=lambda obj, ctx: obj,
    )
    producer = SerializingProducer({
        "bootstrap.servers": BOOTSTRAP,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": serializer,
        "acks": "all",
    })
    producer.produce(
        topic=TOPIC, key="user_42",
        value={"user_id": "user_42", "url": "/home", "ts": 1718800000.0},
    )
    producer.flush()
    print("produced v1 record (CLICK_V1 registered as version 1)")

    deserializer = AvroDeserializer(schema_registry_client=registry)
    consumer = DeserializingConsumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "ex03-reader",
        "auto.offset.reset": "earliest",
        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": deserializer,
    })
    consumer.subscribe([TOPIC])
    msg = consumer.poll(5.0)
    print("consumed:", msg.value())
    consumer.close()


def test_evolution(registry, label, schema_str, expect_accepted):
    print(f"\n--- {label} (expect {'ACCEPT' if expect_accepted else 'REJECT'}) ---")
    compatible = registry.test_compatibility(SUBJECT, Schema(schema_str, "AVRO"))
    print(f"test_compatibility -> {compatible}")
    try:
        schema_id = registry.register_schema(SUBJECT, Schema(schema_str, "AVRO"))
        print(f"REGISTERED as id={schema_id}")
    except SchemaRegistryError as e:
        print(f"REJECTED: http={e.http_status_code} code={e.error_code}\n  {e}")
```

### What success looks like

```text
global default compatibility: BACKWARD
produced v1 record (CLICK_V1 registered as version 1)
consumed: {'user_id': 'user_42', 'url': '/home', 'ts': 1718800000.0}

--- ADD optional referrer field (expect ACCEPT) ---
test_compatibility -> True
REGISTERED as id=2

--- CHANGE ts double->string (expect REJECT) ---
test_compatibility -> False
REJECTED: http=409 code=409
  Schema being registered is incompatible with an earlier schema for subject
  "clicks_avro-value", details: [...reader type: string not compatible with
  writer type: double...]
```

The two lines that matter: `test_compatibility -> True` / `REGISTERED as id=2` for the added optional field, and `test_compatibility -> False` / `REJECTED: http=409` for the type change. The registry refused the bad schema **before any record could be produced with it** — that is the rotting-stream protection.

### Cross-check with the REST API

You can confirm the registered versions and the rejection directly:

```sh
# Versions registered under the subject (should show [1, 2] after the good one):
curl -s http://localhost:8081/subjects/clicks_avro-value/versions
# -> [1,2]

# Ask the registry to check the bad schema (returns is_compatible: false):
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\":\"record\",\"name\":\"Click\",\"namespace\":\"data.crunch.clicks\",\"fields\":[{\"name\":\"user_id\",\"type\":\"string\"},{\"name\":\"url\",\"type\":\"string\"},{\"name\":\"ts\",\"type\":\"string\"}]}"}' \
  http://localhost:8081/compatibility/subjects/clicks_avro-value/versions/latest
# -> {"is_compatible":false}
```

### Common pitfalls

- **`ModuleNotFoundError` for the Avro modules.** You installed `confluent-kafka` but not the Avro extra. Install `confluent-kafka[avro]` (it pulls `fastavro` and the registry serializers).
- **The "bad" schema is *accepted*.** The subject's compatibility mode is `NONE`, not `BACKWARD` — `NONE` disables all checking. Confirm with `curl http://localhost:8081/config/clicks_avro-value`; set it back with a `PUT` of `{"compatibility":"BACKWARD"}`.
- **Adding a field is rejected even though you expected accept.** You added it *without* a `default`. Under `BACKWARD`, a new field needs a default so old data can be read by the new schema — `{"name":"referrer","type":["null","string"],"default":null}`. A bare `{"name":"referrer","type":"string"}` is incompatible.
- **`test_compatibility` says True but you wanted to feel the rejection.** That is the *good* schema — it is supposed to be compatible. The *type change* (`CLICK_V3_BAD`) is the one that returns False and 409.

---

## Running all three

```text
# Exercise 1
$ python exercise-01-produce-and-consume.py produce
$ python exercise-01-produce-and-consume.py consume

# Exercise 2 (two terminals)
$ python exercise-02-consumer-groups-and-rebalance.py A
$ python exercise-02-consumer-groups-and-rebalance.py B   # in a second terminal

# Exercise 3
$ python exercise-03-avro-schema-registry-compatibility.py
```

The thread through all three: Exercise 1 shows the key→partition mapping (ordering substrate), Exercise 2 shows how a group divides those partitions and rebalances, and Exercise 3 shows how the registry keeps the records on those partitions evolvable without rotting. Together they are exactly the Lab 08 mini-project in miniature.
