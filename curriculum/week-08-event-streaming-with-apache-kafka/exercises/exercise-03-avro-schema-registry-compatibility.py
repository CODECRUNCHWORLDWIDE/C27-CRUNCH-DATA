"""Exercise 3 — Avro + the schema registry: prove compatibility is enforced.

Goal: register a v1 `Click` Avro schema, produce and consume one Avro record
(round-trip through the registry), then test two evolutions:
  (a) ADD an optional `referrer` field with a default -> BACKWARD-compatible,
      ACCEPTED by the registry as version 2.
  (b) CHANGE the `ts` field's type from double to string -> incompatible,
      REJECTED by the registry (HTTP 409).

Setup:
    pip install "confluent-kafka[avro]"
    # Bring up Kafka + the schema registry (compose in the mini-project).
    # Kafka on localhost:9092, schema registry on http://localhost:8081.

Run:
    python exercise-03-avro-schema-registry-compatibility.py

Reference:
    https://docs.confluent.io/platform/current/schema-registry/index.html
    https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html
    https://avro.apache.org/docs/
"""

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import StringDeserializer, StringSerializer

BOOTSTRAP = "localhost:9092"
REGISTRY_URL = "http://localhost:8081"
TOPIC = "clicks_avro"
SUBJECT = f"{TOPIC}-value"  # default TopicNameStrategy subject for the value.

# Version 1: the baseline Click record.
CLICK_V1 = """
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

# Version 2 (GOOD): adds a nullable `referrer` with default null. Old data reads
# as referrer=null, so this is BACKWARD-compatible -> should be ACCEPTED.
CLICK_V2_GOOD = """
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

# Version 3 (BAD): changes `ts` from double to string. Unresolvable type change
# -> incompatible -> should be REJECTED.
CLICK_V3_BAD = """
{
  "type": "record", "name": "Click", "namespace": "data.crunch.clicks",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "url",     "type": "string"},
    {"name": "ts",      "type": "string"}
  ]
}
"""


def produce_and_consume_v1(registry):
    """Round-trip one Avro record through the registry, proving the wire format."""
    # Step 1: build an AvroSerializer bound to `registry` with schema_str=CLICK_V1
    # and a to_dict that returns the object unchanged. Build a SerializingProducer
    # with key.serializer=StringSerializer("utf_8"), value.serializer=the avro one,
    # acks="all".
    ...

    # Step 2: produce one record, e.g.
    #   value={"user_id": "user_42", "url": "/home", "ts": 1718800000.0}
    # then flush(). This first produce REGISTERS CLICK_V1 under SUBJECT as v1.
    ...

    # Step 3: build an AvroDeserializer (no schema_str needed — it reads the
    # schema ID off the wire and fetches it). Build a DeserializingConsumer with
    # group.id, auto.offset.reset="earliest", the string key deserializer, and
    # the avro value deserializer. Subscribe, poll once, print msg.value(),
    # then close. You should see the dict come back intact.
    ...


def test_evolution(registry, label, schema_str, expect_accepted):
    """Test one schema evolution against the registry's compatibility check.

    Step 4: use registry.test_compatibility(SUBJECT, Schema(schema_str, "AVRO"))
    to ask whether `schema_str` is compatible with the registered version(s).
    It returns a bool. Print the result and compare it to `expect_accepted`.

    Step 5: then actually TRY to register it with
    registry.register_schema(SUBJECT, Schema(schema_str, "AVRO")). For the GOOD
    schema this returns a new schema ID (version 2). For the BAD schema this
    raises SchemaRegistryError with http_status_code 409 — catch it and print
    the message. That raised-on-registration error is the protection the whole
    lecture is about.
    """
    print(f"\n--- {label} (expect {'ACCEPT' if expect_accepted else 'REJECT'}) ---")
    # Step 4 (your code): compatible = registry.test_compatibility(...)
    #   print(f"test_compatibility -> {compatible}")
    ...

    # Step 5 (your code):
    #   try:
    #       schema_id = registry.register_schema(SUBJECT, Schema(schema_str, "AVRO"))
    #       print(f"REGISTERED as id={schema_id}")
    #   except SchemaRegistryError as e:
    #       print(f"REJECTED: http={e.http_status_code} code={e.error_code} {e}")
    ...


def main():
    registry = SchemaRegistryClient({"url": REGISTRY_URL})

    # Step 6: confirm the subject's compatibility mode is BACKWARD (the default).
    # You can read it with registry.get_compatibility(SUBJECT) AFTER v1 exists,
    # or registry.get_compatibility() for the global default. Print it.
    ...

    produce_and_consume_v1(registry)

    # The good evolution: add an optional field -> accepted as v2.
    test_evolution(registry, "ADD optional referrer field", CLICK_V2_GOOD,
                   expect_accepted=True)

    # The bad evolution: change ts type double->string -> rejected with 409.
    test_evolution(registry, "CHANGE ts double->string", CLICK_V3_BAD,
                   expect_accepted=False)


if __name__ == "__main__":
    main()
