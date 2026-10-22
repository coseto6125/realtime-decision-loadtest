#!/usr/bin/env bash
# Implementation A/B runs on the tuned stack (each on a fresh stack, T1 load, 3 minutes):
#   p1_v2_profile : Sanic native server, py-spy --native --idle profiles of collector and context
#   p2_granian    : collector and decision served by Granian (Rust HTTP server) instead of Sanic's own server
#   client A/B    : cache_bench --client both on the Context Builder op mix, against lt-redis, app stopped
set -uo pipefail
: "${LT_SSH:?set LT_SSH}"
cd "$(dirname "$0")/.."
R=${LT_REMOTE_DIR:-lt-loadtest}
G='granian --interface asgi --host 0.0.0.0 --port 8000 --workers 1 --runtime-threads 1 --loop uvloop --log-level warning'
step() { echo "== $1 $(date -Is)"; }
step p1_v2_profile; scripts/restack.sh >/dev/null; sleep 5
PROFILE_SECS=120 scripts/run_test.sh p1_v2_profile -e DURATION=3m | tail -1
step p2_granian; scripts/restack.sh "COLLECTOR_CMD=$G lt.collector:app" "DECISION_CMD=$G lt.decision:app" >/dev/null; sleep 5
scripts/run_test.sh p2_granian -e DURATION=3m | tail -1
step client_ab; scripts/restack.sh >/dev/null
mkdir -p results/client_ab
ssh "$LT_SSH" "cd $R && docker compose -p lt stop lt-collector lt-context lt-decision lt-payments lt-scheduler lt-model >/dev/null 2>&1
  for eps in 150 2000 6000; do
    docker run --rm --name lt-bench --cpuset-cpus 2 --memory 2g --network lt-net lt-app:1 \
      python -m bench.cache_bench --host lt-redis --port 6379 --mode baseline --events-per-sec \$eps --duration-s 40 --client both
  done" > results/client_ab/run.jsonl 2> results/client_ab/stderr.txt
tail -3 results/client_ab/run.jsonl | cut -c1-400
