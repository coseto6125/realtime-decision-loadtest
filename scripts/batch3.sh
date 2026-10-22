#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/.."
scripts/deploy.sh >/dev/null || { echo "deploy failed $(date -Is): batch stopped" >&2; exit 1; }; echo "deployed $(date -Is)"
scripts/pg_ablation.sh
for mode in pipeline lua; do
  echo "== x_$mode $(date -Is)"; scripts/restack.sh "XADD_MODE=$mode" >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5
  scripts/run_test.sh "x_$mode" -e DURATION=3m | tail -1
done
echo "== p3_v3_profile $(date -Is)"; scripts/restack.sh >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5
PROFILE_SECS=120 scripts/run_test.sh p3_v3_profile -e DURATION=3m | tail -1
echo "== build_rust $(date -Is)"; scripts/build_rust.sh
echo "batch3 done $(date -Is)"
