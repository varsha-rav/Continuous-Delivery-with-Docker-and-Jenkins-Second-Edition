"""Week 9 lab — a consumer in the group 'orders-workers'.

Run TWO instances (same GROUP_ID via the same group_id below): Kafka assigns each
its own partitions, so together they split the work with no overlap. Each prints
the partitions it was assigned. Kill one (Ctrl-C, or just close it) and watch the
survivor REBALANCE and take over the freed partitions, then keep processing.

Set NAME=... to label each instance in the output, e.g.:
    NAME=c1 python consumer_group.py      # terminal 1
    NAME=c2 python consumer_group.py      # terminal 2
"""
import json
import os

from kafka import KafkaConsumer
from kafka.consumer.subscription_state import ConsumerRebalanceListener

BROKER = "localhost:9092"
TOPIC = "orders"
NAME = os.environ.get("NAME", "consumer")


class AssignmentLogger(ConsumerRebalanceListener):
    def on_partitions_revoked(self, revoked):
        print(f"[{NAME}] partitions REVOKED: {sorted(p.partition for p in revoked)}", flush=True)

    def on_partitions_assigned(self, assigned):
        print(f"[{NAME}] partitions ASSIGNED: {sorted(p.partition for p in assigned)}", flush=True)


consumer = KafkaConsumer(
    bootstrap_servers=BROKER,
    group_id="orders-workers",
    auto_offset_reset="earliest",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)
consumer.subscribe([TOPIC], listener=AssignmentLogger())
print(f"[{NAME}] joined group 'orders-workers'; waiting for assignment (Ctrl-C to stop)...", flush=True)

try:
    for m in consumer:
        print(f"[{NAME}] p{m.partition} off{m.offset}  order {m.value['order_id']} {m.value['status']}", flush=True)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
