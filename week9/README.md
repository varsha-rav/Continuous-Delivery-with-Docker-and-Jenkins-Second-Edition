# Week 9 — Message brokers

Two things this week:

- **The lab** (`producer.py` + `consumer_group.py`) — see partitioning, consumer
  groups, per-key ordering, and rebalancing with your own eyes. *(Exercise 9)*
- **The capstone step** (`Jenkinsfile`) — your pipeline publishes one `ImagePushed`
  event to Kafka. *(the running capstone; consuming it comes later)*

## Files
| File | Purpose |
|------|---------|
| `docker-compose.yml` | single-broker Kafka (KRaft), container name `week9-kafka` |
| `requirements.txt` | Kafka Python client (`kafka-python-ng`) |
| `producer.py` | publish messages keyed by `order_id` across a 3-partition `orders` topic |
| `consumer_group.py` | a consumer in group `orders-workers`; run two to split partitions |
| `Jenkinsfile` | capstone: **build image → push to registry → announce `ImagePushed`** |
| `Dockerfile` | capstone: builds the small agent image (Python + Kafka client) for the pipeline |
| `app/Dockerfile` | capstone: the trivial image the pipeline builds and pushes |
| `publish_event.py` | capstone: publishes the `ImagePushed` event (run inside the agent image) |

---

## Lab — partitioning and consumer groups

```bash
# 1. Kafka + a 3-partition topic
docker compose -p week9 up -d
docker exec week9-kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic orders --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092

# 2. Python env
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. produce keyed messages (prints the partition each order lands on)
python producer.py

# 4. run TWO consumers in the same group — separate terminals:
NAME=c1 python consumer_group.py
NAME=c2 python consumer_group.py
```

**Shortcut:** `./run_group.sh` starts both consumers at once (labeled `[c1]`/`[c2]`
in one terminal; Ctrl-C stops both). It also checks Kafka is up first. To see a
rebalance while it runs, in another terminal: `kill "$(cat .c1.pid)"`.
(You don't need to start the two at the same instant — Kafka rebalances whenever a
consumer joins or leaves; the script is just convenience.)

Watch: each consumer prints the partitions it was **assigned** (together they cover
all three, no overlap); re-run the producer and each message is handled by exactly
one consumer; every event for one `order_id` stays in order on a single consumer;
**kill one consumer** and the survivor **rebalances** to cover the freed partitions.

Tear down: `docker compose -p week9 down`.

---

## Capstone — the pipeline announces its build to Kafka

The `Jenkinsfile` does three things: **builds an image, pushes it to the local
registry, then announces it on Kafka** — the CI-to-events bridge in miniature.
Nothing consumes the event yet; that comes in later weeks.

1. **Build & push** — `docker build` the trivial `app/` image and `docker push` it
   to `localhost:5001/calculator:${BUILD_NUMBER}`. Because the registry is plain
   HTTP with no auth, this needs **no certificates and no `docker login`** (Docker
   treats `localhost` registries as insecure) — simpler than the earlier
   TLS + htpasswd setup.
2. **Announce** — publish an `ImagePushed` event (carrying that tag) to the
   `ci.images` topic.

**Prereq — build the agent image once** (Python + the Kafka client) and push it to
the same registry; the announce step runs inside it:

```bash
docker build -t localhost:5001/kafka-python:1 .
docker push  localhost:5001/kafka-python:1
```

Each stage declares its own agent: the **build & push** stage runs on the Jenkins
node (docker CLI + mounted socket); the **announce** stage runs *inside* the
kafka-python image (a per-stage docker agent) on the Kafka network, so it can
reach the broker by name (`week9-kafka:29092`).

**Why the broker has two listeners** (`docker-compose.yml`): it advertises
`localhost:9092` for clients on the host (the lab) and `week9-kafka:29092` for
other containers on the Docker network (the pipeline agent). A container's
`localhost` is itself, not the broker, so it needs the by-name address — same
broker, two reachable addresses.

Create the `ci.images` topic before the first run (auto-create is off, same as `orders`):
```bash
docker exec week9-kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic ci.images --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

Verify a send landed:
```bash
docker exec week9-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic ci.images --from-beginning --timeout-ms 6000 \
  --bootstrap-server localhost:9092
```

And confirm the image was pushed:
```bash
curl localhost:5001/v2/_catalog
curl localhost:5001/v2/calculator/tags/list
```

> **Validated** in real Jenkins (Docker Pipeline plugin, Kafka 3.9.0): the job
> builds `calculator`, pushes it to `localhost:5001`, and announces `ImagePushed`
> (with the build number as the tag) to `ci.images` — read back cleanly. The lab's
> keyed production + two-member group split/rebalance also verified.
