#!/usr/bin/env bash
# Week 10 (optional, Pattern 5) — prove a replicated partition survives a broker
# failure. Create a replication-factor-3 topic, write 3 messages, kill the leader,
# and read the data back from a survivor. Requires the 3-broker cluster:
#   docker compose -f cluster-compose.yml -p w10 up -d
set -euo pipefail
T=resilient

# run a Kafka CLI tool inside a named broker container (-i so piped stdin, e.g. the
# console producer, is actually forwarded into the container)
cli() { docker exec -i "w10-kafka-$1" /opt/kafka/bin/"${@:2}"; }

echo "== create topic '$T' with replication-factor 3 =="
cli 1 kafka-topics.sh --bootstrap-server kafka-1:29092 --create --topic "$T" \
    --partitions 1 --replication-factor 3 --if-not-exists

echo "== initial state (leader + full ISR 1,2,3) =="
cli 1 kafka-topics.sh --bootstrap-server kafka-1:29092 --describe --topic "$T"

echo "== produce 3 messages with acks=all =="
printf 'one\ntwo\nthree\n' | cli 1 kafka-console-producer.sh \
    --bootstrap-server kafka-1:29092 --topic "$T" --producer-property acks=all

leader=$(cli 1 kafka-topics.sh --bootstrap-server kafka-1:29092 --describe --topic "$T" \
    | sed -n 's/.*Leader: \([0-9]*\).*/\1/p')
echo "== current leader is broker $leader — killing it =="
docker stop "w10-kafka-$leader" >/dev/null

surv=1; [ "$leader" = "1" ] && surv=2
echo "== letting the cluster settle (leader + coordinator re-election) =="
sleep 8
echo "== state after failure (new leader, ISR shrunk to 2) =="
cli "$surv" kafka-topics.sh --bootstrap-server "kafka-$surv:29092" --describe --topic "$T"

echo "== read the data back — all 3 messages survived =="
cli "$surv" kafka-console-consumer.sh --bootstrap-server "kafka-$surv:29092" \
    --topic "$T" --from-beginning --max-messages 3 --timeout-ms 20000

echo "== restart the downed broker (it will catch up and rejoin the ISR) =="
docker start "w10-kafka-$leader" >/dev/null
echo "done."
