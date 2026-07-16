"""Exercise 1 — Produce and consume keyed records on a partitioned topic.

Goal: finish a `confluent-kafka` producer that emits keyed click events to the
3-partition `clicks` topic, then a consumer that reads them back, and confirm
from the output that every record for a given key lands on the SAME partition
(the per-key ordering guarantee from Lecture 1).

Setup (run these first; the full Docker compose is in the mini-project):
    pip install confluent-kafka
    # Bring up Kafka in KRaft mode (see mini-project/README.md for the compose).
    # Create the topic with 3 partitions:
    #   kafka-topics --bootstrap-server localhost:9092 \
    #       --create --topic clicks --partitions 3 --replication-factor 1

Run:
    python exercise-01-produce-and-consume.py produce
    python exercise-01-produce-and-consume.py consume

Reference:
    https://docs.confluent.io/kafka-clients/python/current/overview.html
    https://kafka.apache.org/documentation/#producerconfigs
"""

import json
import sys
import time

from confluent_kafka import Consumer, Producer

BOOTSTRAP = "localhost:9092"
TOPIC = "clicks"

# A small synthetic clickstream: a handful of users browsing a handful of pages.
USERS = ["user_42", "user_99", "user_7", "user_13"]
PAGES = ["/home", "/search", "/product/abc", "/cart", "/checkout"]


def delivery_report(err, msg):
    """Async delivery callback. Fires when the broker acks (or fails) a record.

    Step 1: print the key, the partition it landed on, and the offset. This is
    how you SEE the key->partition mapping. Decode the key bytes for readability.
    """
    if err is not None:
        print(f"DELIVERY FAILED: {err}")
        return
    # Step 1 (your code): print "key=<k> -> partition=<p> offset=<o>".
    #   msg.key() is bytes; msg.partition() and msg.offset() are ints.
    ...


def produce():
    """Produce keyed click events and observe their partition assignment."""
    # Step 2: build the producer config dict. Use the canonical dotted keys:
    #   "bootstrap.servers", "acks" = "all", "enable.idempotence" = True,
    #   and a "client.id". Then construct Producer(conf).
    conf = {
        # ... fill in the config ...
    }
    producer = Producer(conf)

    # Step 3: produce ~20 events. For each, pick a user (the KEY) and a page,
    # build an event dict {user_id, url, ts}, and call producer.produce(...)
    # with:
    #   topic=TOPIC,
    #   key=<user>.encode("utf-8"),          # the key drives the partition
    #   value=json.dumps(event).encode("utf-8"),
    #   callback=delivery_report,
    # Call producer.poll(0) after each produce to service delivery callbacks.
    for i in range(20):
        user = USERS[i % len(USERS)]
        page = PAGES[i % len(PAGES)]
        event = {"user_id": user, "url": page, "ts": time.time()}
        # ... produce + poll(0) ...
        ...

    # Step 4: flush before exit. Without flush(), queued records are dropped
    # silently and the delivery callbacks never fire.
    # producer.flush(timeout=10)
    ...

    # Step 5: after running, look at the output. Confirm that every "user_42"
    # record shows the SAME partition number, every "user_99" the same (possibly
    # different) partition, and so on. Write the user->partition mapping you
    # observe in a comment here:
    #   user_42 -> partition ?
    #   user_99 -> partition ?
    #   user_7  -> partition ?
    #   user_13 -> partition ?


def consume():
    """Consume the topic from the beginning and print each record."""
    # Step 6: build the consumer config. Required keys:
    #   "bootstrap.servers", "group.id" (any name, e.g. "ex01-reader"),
    #   "auto.offset.reset" = "earliest"  (read all history),
    #   "enable.auto.commit" = False      (we will commit manually below).
    conf = {
        # ... fill in the config ...
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])

    # Step 7: poll loop. Poll with a timeout, skip None and errored messages,
    # then print partition / offset / key / value for each record. Commit AFTER
    # printing (this is the at-least-once pattern from Lecture 2). Stop after a
    # few seconds of no new records so the script exits.
    last_msg_time = time.time()
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                # Step 7a: if no record for ~3s, assume drained and break.
                if time.time() - last_msg_time > 3.0:
                    break
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}")
                continue
            last_msg_time = time.time()
            # Step 7b (your code): print
            #   f"p{msg.partition()} @ {msg.offset()}: "
            #   f"key={msg.key().decode()} value={msg.value().decode()}"
            # then consumer.commit(msg) to record progress.
            ...
    finally:
        consumer.close()


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "produce":
        produce()
    elif mode == "consume":
        consume()
    else:
        print("usage: python exercise-01-produce-and-consume.py [produce|consume]")
        sys.exit(2)
