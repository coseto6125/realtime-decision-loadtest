#!/usr/bin/env bash
# After the T4 root cause: T4 again with the fixes, T7, T1 on Dragonfly, T6b, T2 soak.
set -uo pipefail
cd "$(dirname "$0")/.."
scripts/deploy.sh >/dev/null || { echo "deploy failed $(date -Is): batch stopped" >&2; exit 1; }; echo "deployed $(date -Is)"
echo "== t4_fixed $(date -Is)"; scripts/restack.sh >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5
K6_MODE=host scripts/run_test.sh t4_fixed -e PROFILE=stress -e PEAK_X=25 -e RAMP=270s -e MAX_VUS=4000 \
  --system-tags=proto,status,method,name,error_code,scenario,vu,iter | tail -1
ssh "$LT_SSH" "docker logs lt-collector 2>&1 | grep -c 'event store error'; for c in lt-collector lt-decision; do docker exec \$c sh -c 'ulimit -n'; done" > results/t4_rootcause/t4_fixed_checks.txt 2>&1
scripts/t7.sh
echo "== t1_dragonfly $(date -Is)"
scripts/restack.sh REDIS_IMAGE=docker.dragonflydb.io/dragonflydb/dragonfly:v1.38.0 \
  'REDIS_CMD=dragonfly --proactor_threads=2 --maxmemory=700mb --dbfilename=' 'REDIS_PING=redis-cli ping' >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5
scripts/run_test.sh t1_dragonfly -e DURATION=5m | tail -1
echo "== t6b $(date -Is)"; scripts/restack.sh >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5; scripts/t6b.sh
echo "== t2_soak $(date -Is)"; scripts/restack.sh >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5
scripts/run_test.sh t2_soak -e DURATION=${SOAK:-60m} | tail -1
echo "batch6 done $(date -Is)"
