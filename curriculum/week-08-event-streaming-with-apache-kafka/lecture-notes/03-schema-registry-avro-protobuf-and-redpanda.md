# Lecture 3 — The Schema Registry, Avro/Protobuf, and Redpanda

> **Time:** 1 hour (it builds directly on Lectures 1–2; the producer/consumer mechanics are already in hand). **Prerequisites:** Lectures 1–2 (you can produce and consume bytes; now we give the bytes a contract). **Citations:** the Confluent Schema Registry docs at <https://docs.confluent.io/platform/current/schema-registry/index.html>, the schema-evolution guide at <https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html>, the Avro fundamentals at <https://docs.confluent.io/platform/current/schema-registry/fundamentals/avro.html>, the Apache Avro spec at <https://avro.apache.org/docs/>, the `confluent-kafka-python` serializers, and the Redpanda docs at <https://docs.redpanda.com/>.

## 1. Why bytes need a contract

In Lecture 1 the producer wrote `json.dumps(event).encode("utf-8")` and the consumer was expected to `json.loads` it back. That works on day one. On day ninety it is a liability. Suppose the producer team renames `url` to `page_url`, or changes `ts` from a float to a string timestamp, or removes a field a consumer relied on. Nothing stops them — the records are opaque bytes — so the change ships, and downstream consumers either crash on a `KeyError` or, worse, silently read garbage. You find out in production, on a topic with weeks of retention, where the bad records are already on disk being read by every consumer.

The problem is that **the producer and the consumer have an implicit contract about the shape of the data, and nothing enforces it.** A stream without an enforced schema *rots*: every team that touches it is one careless change away from breaking everyone downstream, and because Kafka decouples producers from consumers in time (a consumer might read a record produced an hour or a week ago), you cannot even coordinate the change with a synchronized deploy.

The **schema registry** makes the contract explicit and enforced. It is a separate service that:

1. Stores **versioned schemas** for each topic's keys and values.
2. Assigns each schema a **global integer ID**.
3. *Enforces a compatibility policy* every time a producer tries to register a new schema version — rejecting an incompatible change **at registration time**, before a single bad record can be produced.

That third point is the one that matters: the registry turns "we broke production" into "the producer got an error on startup and could not register the bad schema." The reference is <https://docs.confluent.io/platform/current/schema-registry/index.html>.

## 2. Subjects, versions, and IDs

The registry's data model is small:

- A **subject** is a named scope under which schemas evolve. With the default `TopicNameStrategy`, the subject for a topic `clicks`'s values is `clicks-value` and for its keys is `clicks-key`. Schemas evolve *per subject*.
- Each subject has an ordered list of **versions** (1, 2, 3, …) — the history of schemas registered under it.
- Each distinct schema, across all subjects, gets a unique **global ID** (an integer). The ID is what travels in the record's bytes (Section 4), so the consumer can fetch exactly the schema the producer used.

So registering the first value schema for `clicks` creates subject `clicks-value`, version 1, with some global ID like 1. Evolving it registers version 2 under the same subject with a new ID — *if* the change passes the compatibility check.

## 3. Avro vs. Protobuf vs. JSON Schema

The registry supports three serialization formats. You should know the trade-offs even though this course uses Avro:

| | Avro | Protobuf | JSON Schema |
|---|---|---|---|
| Encoding | Compact binary | Compact binary | Verbose text (JSON) |
| Schema lives | Externally (in the registry); data carries only the ID | In `.proto` files + registry | In the registry |
| Schema evolution | Reader/writer schema resolution; rich, well-specified rules | Field numbers; add/deprecate, never reuse a number | Rules over the JSON Schema vocabulary |
| Kafka-native? | Yes — the original, deepest integration | Strong (gRPC ecosystem) | Yes, but largest payloads |
| Best when | Streaming data at volume; the default | You already use Protobuf/gRPC | You need human-readable payloads or interop with JSON tooling |

**Avro** is the Kafka-native default and what you use this week. Its key property: the data is serialized against a *writer* schema, and a consumer deserializes against a (possibly different) *reader* schema, with Avro performing **schema resolution** between them — this is exactly what makes compatible evolution work. Because the schema is stored externally in the registry, an Avro record on the wire is tiny: it does not carry field names, only the values plus the 4-byte schema ID needed to look the schema up. The Avro spec is at <https://avro.apache.org/docs/> (the encoding and schema-resolution sections are the load-bearing reading).

**Protobuf** uses numbered fields in a `.proto` definition; its evolution rule is "never reuse a field number, never change a field's type" and it is excellent if you already live in a Protobuf/gRPC world. **JSON Schema** is the most verbose (full JSON on the wire, field names and all) but the most universally readable and the easiest to interoperate with JSON-everything tooling. Pick by your ecosystem; Avro is the safe default for high-volume streaming.

## 4. The wire format — magic byte + ID + payload

This is the elegant trick that ties it together. When the `confluent-kafka` Avro serializer encodes a record, it does not write bare Avro — it writes a small framing prefix first:

```text
byte 0:      0x00            magic byte (format version; always 0 today)
bytes 1-4:   schema ID       4-byte big-endian integer (the registry's global ID)
bytes 5..:   payload         the Avro-encoded record body
```

So every value on the topic is `[0x00][schema-id][avro-bytes]`. A consumer reads the first 5 bytes, extracts the schema ID, fetches that exact schema from the registry (caching it after the first fetch), and deserializes the payload against it — even as producers move to newer schema versions, each record self-describes which schema it was written with. This is why the registry and the wire format are inseparable: the ID in the bytes is the pointer into the registry. The reference is the wire-format section of the Avro fundamentals at <https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format>.

## 5. Compatibility modes — the rules that keep the stream from rotting

When you register a new schema version, the registry checks it against the existing version(s) under the subject according to the subject's **compatibility mode**. The four core modes (plus their `*_TRANSITIVE` variants) answer two questions: *which schema is being checked against which*, and *who must upgrade first*.

- **`BACKWARD` (the default).** A *new* schema can read data written with the *old* schema. Concretely: you may **add a field with a default** (old data lacks it → the default fills in) and **remove a field** (the new reader ignores the missing-from-its-perspective old field). You upgrade **consumers first**, then producers. This is the most common mode because it matches the usual ops reality: roll out the new-reading consumers, then start producing the new shape.
- **`FORWARD`.** An *old* schema can read data written with the *new* schema. You may **add a field** (old readers ignore it) and **remove a field that had a default**. You upgrade **producers first**, then consumers.
- **`FULL`.** Both backward *and* forward — the change must be readable both ways. Only the safest changes pass (adding/removing fields that have defaults). Upgrade order does not matter.
- **`NONE`.** No checking. Any change registers. Use only when you genuinely have no compatibility requirement; it removes the protection that is the whole point of the registry.

The `*_TRANSITIVE` variants (`BACKWARD_TRANSITIVE`, etc.) check against **all** previous versions, not just the immediately preceding one — stronger, guards against a chain of individually-compatible-but-collectively-incompatible changes.

### 5.1 The concrete Avro rules

For Avro under `BACKWARD` compatibility, the rules that bite are:

- **Adding a field WITH a default → compatible.** Old data has no value for it; the default supplies one when read by the new schema. ✅
- **Adding a field WITHOUT a default → incompatible.** Old data has no value and there is no default, so the new reader cannot construct a record from old data. ❌
- **Removing a field → compatible (backward).** The new reader simply does not look for it. ✅
- **Changing a field's type → incompatible.** `int` to `string` cannot be resolved. ❌
- **Renaming a field → incompatible** (it reads as "remove old + add new-without-default" unless you use an Avro `alias`). ❌

So the canonical *accepted* evolution this week is **adding an optional field with a default** (e.g. add `"referrer"` defaulting to `null`), and the canonical *rejected* one is **adding a required field with no default** or **changing a field's type**. The registry returns an HTTP 409 (`Conflict`) with a message naming the incompatibility. The reference is <https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html>.

## 6. Avro serialization in Python with `confluent-kafka`

Here is the real code. `confluent-kafka`'s `SerializingProducer`/`DeserializingConsumer` (or the newer plain `Producer`/`Consumer` with explicit serializers) integrate the registry. The `AvroSerializer` registers the schema on first use, prefixes the wire format, and the `AvroDeserializer` reverses it.

```python
from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer, StringDeserializer

# 1) Connect to the registry (runs on :8081 in the Docker setup).
schema_registry = SchemaRegistryClient({"url": "http://localhost:8081"})

# 2) The Avro schema for a click value (version 1).
CLICK_SCHEMA_V1 = """
{
  "type": "record",
  "name": "Click",
  "namespace": "data.crunch.clicks",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "url",     "type": "string"},
    {"name": "ts",      "type": "double"}
  ]
}
"""

# 3) A serializer bound to the registry. On first produce it registers the
#    schema under subject "clicks-value" and gets back the global ID.
avro_serializer = AvroSerializer(
    schema_registry_client=schema_registry,
    schema_str=CLICK_SCHEMA_V1,
    to_dict=lambda obj, ctx: obj,   # our records are already dicts
)

producer = SerializingProducer({
    "bootstrap.servers": "localhost:9092",
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": avro_serializer,
    "acks": "all",
    "enable.idempotence": True,
})

producer.produce(
    topic="clicks",
    key="user_42",
    value={"user_id": "user_42", "url": "/home", "ts": 1718800000.0},
)
producer.flush()

# --- consumer side ---
avro_deserializer = AvroDeserializer(schema_registry_client=schema_registry)
consumer = DeserializingConsumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "avro-reader",
    "auto.offset.reset": "earliest",
    "key.deserializer": StringDeserializer("utf_8"),
    "value.deserializer": avro_deserializer,
})
consumer.subscribe(["clicks"])
msg = consumer.poll(5.0)
print(msg.value())   # -> {'user_id': 'user_42', 'url': '/home', 'ts': 1718800000.0}
consumer.close()
```

The deserializer needs no schema string — it reads the schema ID from the wire format and fetches the schema from the registry by ID. That is the whole point of Section 4 made concrete.

## 7. Proving compatibility — accepted and rejected

You can register and check schemas directly against the registry. The accepted evolution adds an optional field:

```python
CLICK_SCHEMA_V2_OK = """
{
  "type": "record", "name": "Click", "namespace": "data.crunch.clicks",
  "fields": [
    {"name": "user_id",  "type": "string"},
    {"name": "url",      "type": "string"},
    {"name": "ts",       "type": "double"},
    {"name": "referrer", "type": ["null", "string"], "default": null}
  ]
}
"""
```

`referrer` is a nullable union with a `default: null` — old data simply reads as `referrer=null`, so this is **backward-compatible** and the registry accepts it as version 2.

The rejected evolution changes a type (or adds a no-default required field):

```python
CLICK_SCHEMA_V3_BAD = """
{
  "type": "record", "name": "Click", "namespace": "data.crunch.clicks",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "url",     "type": "string"},
    {"name": "ts",      "type": "string"}    // CHANGED double -> string: incompatible
  ]
}
"""
```

Calling `schema_registry.test_compatibility("clicks-value", Schema(CLICK_SCHEMA_V3_BAD, "AVRO"))` returns `False`, and attempting to *register* it raises a `SchemaRegistryError` carrying HTTP 409 with a message like `Schema being registered is incompatible with an earlier schema for subject "clicks-value"`. That rejection — happening before any bad record is produced — is the protection the whole lecture is about. You prove exactly this in Exercise 3.

You can also inspect and set the compatibility mode over the REST API:

```sh
# Read the global default compatibility:
curl -s http://localhost:8081/config
# Read a subject's compatibility:
curl -s http://localhost:8081/config/clicks-value
# Set a subject to FULL:
curl -s -X PUT -H "Content-Type: application/json" \
  --data '{"compatibility": "FULL"}' \
  http://localhost:8081/config/clicks-value
```

The REST API reference is <https://docs.confluent.io/platform/current/schema-registry/develop/api.html>.

## 8. Redpanda — a drop-in alternative

Kafka is a JVM application and historically needed ZooKeeper (KRaft removed that). **Redpanda** is a from-scratch reimplementation of the Kafka protocol in C++: a single binary, no JVM, no ZooKeeper, with a thread-per-core architecture and a **built-in schema registry** and HTTP proxy. The selling point for this course: it speaks the **same Kafka API**, so *your client code does not change*. The same `confluent-kafka` `Producer`/`Consumer`, the same `bootstrap.servers`, the same Avro serializer pointed at Redpanda's built-in registry — it all just works.

```yaml
# A minimal Redpanda single-node compose (the challenge has the full version).
services:
  redpanda:
    image: redpandadata/redpanda:latest
    command:
      - redpanda start
      - --overprovisioned --smp 1 --memory 1G
      - --kafka-addr PLAINTEXT://0.0.0.0:9092
      - --advertise-kafka-addr PLAINTEXT://localhost:9092
    ports:
      - "9092:9092"     # Kafka API
      - "8081:8081"     # built-in schema registry
```

When does the lighter footprint matter? On a laptop with limited RAM, for fast startup, and when you want one container instead of two or three. Redpanda's CLI is `rpk` (e.g. `rpk topic create clicks -p 3`) rather than the `kafka-topics` shell scripts, but the wire protocol your Python code talks is identical. The challenges give you both a Kafka compose and a Redpanda compose so you can run the same exercise against either. The reference is <https://docs.redpanda.com/>.

## 9. Exercise pointer

Now do **Exercise 3 — Avro schema registry compatibility**. You will register the v1 `Click` schema, produce and consume an Avro record (proving the round trip through the registry), then test two evolutions: add an optional `referrer` field (the registry accepts it as v2) and change `ts`'s type (the registry rejects it with a 409). The acceptance criterion is captured output showing the accepted registration *and* the rejection error.

## 10. Summary

- A stream without an enforced schema **rots**: producers and consumers share an implicit data contract that nothing enforces, and Kafka's time-decoupling means you cannot fix a break with a synchronized deploy. The **schema registry** makes the contract explicit and enforced.
- The registry stores **versioned schemas** per **subject** (`<topic>-value`, `<topic>-key` under `TopicNameStrategy`), assigns each a **global ID**, and checks every new version against the subject's **compatibility mode** at registration time.
- **Avro** (Kafka-native, compact, external schema with reader/writer resolution) is the default; **Protobuf** (field-numbered, gRPC-world) and **JSON Schema** (verbose, universal) are alternatives. Pick by ecosystem.
- The **wire format** is `[0x00 magic byte][4-byte schema ID][payload]` — the ID in the bytes is the pointer into the registry, so each record self-describes its schema and consumers deserialize correctly across schema versions.
- **Compatibility modes:** `BACKWARD` (default; new schema reads old data; upgrade consumers first), `FORWARD` (old schema reads new data; upgrade producers first), `FULL` (both), `NONE` (no check). `*_TRANSITIVE` checks all prior versions.
- The Avro rules that bite: **add field with a default = compatible**; add field without a default, change a type, or rename = **incompatible** (rejected with HTTP 409). The accepted evolution this week adds a nullable defaulted field; the rejected one changes a type.
- `confluent-kafka`'s `AvroSerializer`/`AvroDeserializer` + `SchemaRegistryClient` integrate the registry; the serializer registers on first use and prefixes the wire format, the deserializer fetches the schema by ID.
- **Redpanda** is a single-binary, no-JVM, no-ZooKeeper, Kafka-API-compatible broker with a built-in schema registry — a true drop-in: same `bootstrap.servers`, same client code, lighter on a laptop. `rpk` replaces the `kafka-*` CLI scripts.

Cited pages: <https://docs.confluent.io/platform/current/schema-registry/index.html>, <https://docs.confluent.io/platform/current/schema-registry/fundamentals/avro.html>, <https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html>, <https://avro.apache.org/docs/>, <https://docs.confluent.io/platform/current/schema-registry/develop/api.html>, <https://docs.redpanda.com/>.
