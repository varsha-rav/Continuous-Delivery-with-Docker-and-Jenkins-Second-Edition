#!/usr/bin/env bash
# Week 9 helper — run two consumers in the same group with one command.
#
# Both consumers run here in the background; their output is interleaved and
# prefixed [c1] / [c2]. Press Ctrl-C to stop both.
#
# To watch a REBALANCE, leave this running and, in another terminal:
#     kill "$(cat .c1.pid)"     # frees c1's partitions; c2 takes them over
#
# Note: you do NOT need to start the two at the same instant — Kafka rebalances
# whenever a consumer joins or leaves. This script is just a convenience.

cd "$(dirname "$0")" || exit 1

# 1. Kafka must be up (this is the usual "NoBrokersAvailable" mistake).
if ! nc -z localhost 9092 2>/dev/null; then
  echo "Kafka isn't reachable on localhost:9092."
  echo "Start it first:  docker compose up -d"
  echo "then create the topic (see README step 1), then re-run this script."
  exit 1
fi

# 2. Make sure the venv is active.
if [ -z "${VIRTUAL_ENV:-}" ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate 2>/dev/null || {
    echo "Activate your virtual environment first (see README step 2):"
    echo "  python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
    exit 1
  }
fi

# 3. Start both consumers in the same group.
NAME=c1 python -u consumer_group.py & C1=$!; echo "$C1" > .c1.pid
NAME=c2 python -u consumer_group.py & C2=$!; echo "$C2" > .c2.pid
trap 'kill "$C1" "$C2" 2>/dev/null; rm -f .c1.pid .c2.pid; exit 0' INT TERM

echo "-------------------------------------------------------------------"
echo "Two consumers running in group 'orders-workers':  c1=$C1  c2=$C2"
echo "Run 'python producer.py' in another terminal to see the split."
echo "Rebalance demo (another terminal):  kill $C1   -> c2 takes over c1's partitions"
echo "Ctrl-C here stops both."
echo "-------------------------------------------------------------------"
wait
