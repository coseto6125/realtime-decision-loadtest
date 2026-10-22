#!/usr/bin/env bash
# Rerun of the T7 TLS new-connection-per-call runs as t7_tls_noka2_1x and t7_tls_noka2_5x (3 min each, MODEL_KEEPALIVE=0),
# then T7 slow mock without the breaker, T1 on Dragonfly, T6b, T2 soak.
set -uo pipefail
cd "$(dirname "$0")/.."
faults() { curl -s -m 5 -XPOST 127.0.0.1:17104/admin/faults -d "$1"; echo; }
scripts/deploy.sh >/dev/null || { echo "deploy failed $(date -Is): batch stopped" >&2; exit 1; }; echo "deployed $(date -Is)"
TLS=(MODEL_URL=https://lt-model-tls:8443/judge MODEL_TLS_VERIFY=0)
for load in 1x 5x; do
  rate=$([ $load = 1x ] && echo 5.3 || echo 26.5); mode=$([ $load = 1x ] && echo local || echo host)
  name=t7_tls_noka2_$load; echo "== $name $(date -Is)"
  scripts/restack.sh "${TLS[@]}" MODEL_KEEPALIVE=0 -- -f compose.yaml --profile tls >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5
  ( sleep 90; ssh "$LT_SSH" "for i in 1 2 3; do echo \$(date +%T) model-tls \$(docker exec lt-model-tls cat /proc/net/sockstat | grep TCP) decision \$(docker exec lt-decision cat /proc/net/sockstat | grep TCP); sleep 10; done" ) > results/${name}_sockstat.txt 2>&1 &
  POLL_SECS=5 K6_MODE=$mode scripts/run_test.sh $name -e DURATION=3m -e RATE=$rate | tail -1
  wait
done
echo "== t7_slow_mock_nobreaker $(date -Is)"; scripts/restack.sh BREAKER=0 >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5
faults '{"median_ms":200,"p95_ms":600}'
POLL_SECS=5 scripts/run_test.sh t7_slow_mock_nobreaker -e DURATION=3m | tail -1
faults '{}'
echo "== t1_dragonfly $(date -Is)"
scripts/restack.sh REDIS_IMAGE=docker.dragonflydb.io/dragonflydb/dragonfly:v1.38.0 \
  'REDIS_CMD=dragonfly --proactor_threads=2 --maxmemory=700mb --dbfilename=' 'REDIS_PING=redis-cli ping' >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5
scripts/run_test.sh t1_dragonfly -e DURATION=5m | tail -1
echo "== t6b $(date -Is)"; scripts/restack.sh >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5; scripts/t6b.sh
echo "== t2_soak $(date -Is)"; scripts/restack.sh >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5
scripts/run_test.sh t2_soak -e DURATION=${SOAK:-60m} | tail -1
echo "batch7 done $(date -Is)"
