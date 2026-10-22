#!/usr/bin/env bash
# T1 (10 min) on the final stack, audits, T1-R (Rust collector, 5 min), Collector ramps, T3.
set -uo pipefail
cd "$(dirname "$0")/.."
R=${LT_REMOTE_DIR:-lt-loadtest}
ssh "$LT_SSH" "for i in grafana/k6:2.3.0 redis:7.2-alpine docker.dragonflydb.io/dragonflydb/dragonfly:v1.38.0; do docker pull -q \$i; done" > results/private/pulls.txt 2>&1
scripts/deploy.sh >/dev/null || { echo "deploy failed $(date -Is): batch stopped" >&2; exit 1; }; echo "deployed $(date -Is)"
echo "== t1_baseline $(date -Is)"; scripts/restack.sh >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5
scripts/run_test.sh t1_baseline -e DURATION=10m | tail -1
PYTHONPATH=. uv run python scripts/index_audit.py > /dev/null 2> results/t1_baseline/index_audit_stderr.txt
ssh "$LT_SSH" "bash $R/scripts/remote/valkey_audit.sh" > results/t1_baseline/valkey_audit.txt 2>&1
echo "== t1r_rust $(date -Is)"; scripts/restack.sh -- -f compose.yaml -f compose.rust.yaml >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5
scripts/run_test.sh t1r_rust -e DURATION=5m | tail -1
ssh "$LT_SSH" "docker exec lt-redis valkey-cli XLEN events; docker exec lt-redis valkey-cli XINFO GROUPS events" > results/t1r_rust/stream_check.txt 2>&1
echo "== ramp_rust $(date -Is)"
SKIP_INVARIANTS=1 K6_MODE=host K6_SCRIPT=k6/events_ramp.js scripts/run_test.sh ramp_rust -e PEAK_RPS=2000 | tail -1
echo "== ramp_python $(date -Is)"; scripts/restack.sh >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5
SKIP_INVARIANTS=1 K6_MODE=host K6_SCRIPT=k6/events_ramp.js scripts/run_test.sh ramp_python -e PEAK_RPS=2000 | tail -1
echo "== t3 $(date -Is)"; scripts/restack.sh >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5
scripts/t3.sh
echo "batch4 done $(date -Is)"
