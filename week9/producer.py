"""Week 9 lab — produce keyed messages across a multi-partition topic.

Each message is KEYED by order_id, so all events for the same order land on the
same partition and stay in order, while different orders spread across the three
partitions. Watch the partition each message is sent to: a given key always maps
to the same partition.
"""
import json
import time

from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC = "orders"

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Several events per order_id, across a handful of orders, interleaved in time.
# These ids spread across all three partitions of a 3-partition topic.
events = [
    ("42", "created"), ("43", "created"), ("49", "created"), ("44", "created"),
    ("50", "created"), ("46", "created"), ("42", "paid"),    ("43", "paid"),
    ("49", "paid"),    ("50", "shipped"), ("46", "paid"),    ("44", "shipped"),
    ("42", "shipped"), ("42", "delivered"),
]

for order_id, status in events:
    event = {"order_id": order_id, "status": status}
    md = producer.send(TOPIC, key=order_id, value=event).get(timeout=10)
    print(f"order {order_id:<3} {status:<10} -> partition {md.partition} offset {md.offset}")
    time.sleep(0.3)

producer.flush()
producer.close()
print("done — notice every event for a given order_id landed on the same partition.")
