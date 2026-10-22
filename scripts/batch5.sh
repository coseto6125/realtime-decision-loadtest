#!/usr/bin/env bash
# T4 stress and T5 spike (k6 on the load host, core 2), T6 cache A/B, T1 on Dragonfly, T6b, T2 soak.
set -uo pipefail
cd "$(dirname "$0")/.."
PYTHONPATH=. uv run python scripts/index_audit.py > /dev/null 2> results/index_audit_stderr.txt; echo "index audit $(date -Is)"
scripts/deploy.sh >/dev/null || { echo "deploy failed $(date -Is): batch stopped" >&2; exit 1; }; echo "deployed $(date -Is)"
scripts/pg_fk_check.sh
echo "== t3 rerun (injection now overlaps the load) $(date -Is)"; scripts/restack.sh >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5
scripts/t3.sh context_stop redis_pause
echo "== t4_stress $(date -Is)"; scripts/restack.sh >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5
K6_MODE=host scripts/run_test.sh t4_stress -e PROFILE=stress -e PEAK_X=15 -e RAMP=270s -e MAX_VUS=3000 | tail -1
echo "== t5_spike $(date -Is)"; scripts/restack.sh >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5
K6_MODE=host scripts/run_test.sh t5_spike -e PROFILE=spike -e SPIKE_X=10 -e MAX_VUS=3000 | tail -1
echo "== t6 $(date -Is)"; scripts/t6.sh
echo "== t1_dragonfly $(date -Is)"
scripts/restack.sh REDIS_IMAGE=docker.dragonflydb.io/dragonflydb/dragonfly:v1.38.0 \
  'REDIS_CMD=dragonfly --proactor_threads=2 --maxmemory=700mb --dbfilename=' 'REDIS_PING=redis-cli ping' >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5
scripts/run_test.sh t1_dragonfly -e DURATION=5m | tail -1
echo "== t6b $(date -Is)"; scripts/restack.sh >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5; scripts/t6b.sh
echo "== t2_soak $(date -Is)"; scripts/restack.sh >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5
scripts/run_test.sh t2_soak -e DURATION=${SOAK:-60m} | tail -1
echo "batch5 done $(date -Is)"
