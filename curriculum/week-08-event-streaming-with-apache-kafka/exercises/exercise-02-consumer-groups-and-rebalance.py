"""Exercise 2 — A consumer group with two members; observe a rebalance.

Goal: run TWO members of ONE consumer group against the 3-partition `clicks`
topic. Start one member (it gets all 3 partitions), then start a second in
another terminal (it steals partitions from the first). Wire up the on_assign /
on_revoke callbacks so you SEE the rebalance: the assignment before, the
revoke/reassign during, and the split after.

Setup:
    pip install confluent-kafka
    # The clicks topic must exist with 3 partitions and have some records in it
    # (run Exercise 1's `produce` first, or keep it producing in a loop).

Run (two terminals, SAME group):
    # terminal A:
    python exercise-02-consumer-groups-and-rebalance.py A
    # terminal B (start ~5s later, watch terminal A rebalance):
    python exercise-02-consumer-groups-and-rebalance.py B

Reference:
    https://docs.confluent.io/platform/current/clients/consumer.html
    https://kafka.apache.org/documentation/#consumerconfigs_partition.assignment.strategy
"""

import sys
import time

from confluent_kafka import Consumer

BOOTSTRAP = "localhost:9092"
TOPIC = "clicks"
GROUP = "ex02-group"  # BOTH members use this same group.id -> they share work.


def make_callbacks(member_name):
    """Return (on_assign, on_revoke) callbacks tagged with the member name.

    Step 1: each callback receives (consumer, partitions). Print the member
    name and the list of (topic, partition) pairs it is gaining or losing.
    These fire EXACTLY when the rebalance hands partitions to / takes them from
    this consumer — they are how you observe the rebalance.
    """

    def on_assign(consumer, partitions):
        # Step 1a (your code): print
        #   f"[{member_name}] ASSIGNED  {[(p.topic, p.partition) for p in partitions]}"
        ...

    def on_revoke(consumer, partitions):
        # Step 1b (your code): print
        #   f"[{member_name}] REVOKED   {[(p.topic, p.partition) for p in partitions]}"
        ...

    return on_assign, on_revoke


def run_member(member_name):
    """Run one member of the consumer group."""
    # Step 2: build the consumer config. Required:
    #   "bootstrap.servers", "group.id" = GROUP, "auto.offset.reset" = "earliest".
    # Step 2a (optional but recommended): set
    #   "partition.assignment.strategy" = "cooperative-sticky"
    # and observe that the rebalance moves ONLY the partitions that must move,
    # rather than revoking everything (eager) — compare the on_revoke output
    # between the two strategies.
    conf = {
        # ... fill in the config ...
    }
    consumer = Consumer(conf)

    # Step 3: subscribe WITH the callbacks so the rebalance is visible.
    on_assign, on_revoke = make_callbacks(member_name)
    # consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    ...

    # Step 4: poll loop. Poll continuously (poll() is also what services the
    # rebalance and sends heartbeats — a loop that stops polling gets kicked out
    # of the group). Print each record with its partition so you can see WHICH
    # partitions this member is actually reading. Run until Ctrl-C.
    print(f"[{member_name}] started; polling. Start the other member to rebalance.")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[{member_name}] error: {msg.error()}")
                continue
            # Step 4a (your code): print which partition this member read from,
            #   e.g. f"[{member_name}] read p{msg.partition()} @ {msg.offset()}"
            ...
    except KeyboardInterrupt:
        pass
    finally:
        # Step 5: close() leaves the group cleanly and triggers an immediate
        # rebalance for the OTHER member (it reclaims this member's partitions).
        # Watch the surviving terminal's on_assign fire when you Ctrl-C this one.
        consumer.close()
        print(f"[{member_name}] closed.")


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else "A"
    run_member(name)
