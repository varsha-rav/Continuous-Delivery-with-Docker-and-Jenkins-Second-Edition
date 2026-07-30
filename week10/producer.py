"""Week 10 reliability lab — produce a batch of order events.

    python producer.py            # 5 well-formed orders
    python producer.py --poison   # same batch, plus one malformed (poison) message

Each order has a STABLE id, so re-running the producer is the same logical message
arriving again — exactly the "a message can arrive more than once" case your
consumer must tolerate.
"""
import json
import sys
import time

from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC = "orders"

orders = [{"id": f"order-{i}", "amount": i * 10} for i in range(1, 6)]
if "--poison" in sys.argv:
    # amount is not a number -> the handler fails on it every time it is tried
    orders.insert(2, {"id": "order-POISON", "amount": "twenty"})

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode(),
    acks="all",
)

for order in orders:
    producer.send(TOPIC, order).get(timeout=10)
    print("produced", order)
    time.sleep(0.2)

producer.flush()
producer.close()
print(f"done — {len(orders)} messages on '{TOPIC}'")
