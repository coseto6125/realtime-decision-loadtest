#!/usr/bin/env bash
# After the soak: audits on the populated stack, T6b rerun of the designs that ran after the red-line
# breach, integration tests on a fresh stack.
set -uo pipefail
cd "$(dirname "$0")/.."
R=${LT_REMOTE_DIR:-lt-loadtest}
echo "== audits $(date -Is)"
PYTHONPATH=. uv run python scripts/index_audit.py > /dev/null 2> results/private/index_audit_stderr.txt; echo "index audit exit $?"
ssh "$LT_SSH" "bash $R/scripts/remote/valkey_audit.sh" > results/t2_soak/valkey_audit.txt 2>&1
scripts/deploy.sh >/dev/null || { echo "deploy failed $(date -Is): batch stopped" >&2; exit 1; }; echo "deployed $(date -Is)"
echo "== t6b_rerun $(date -Is)"; scripts/restack.sh >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 5
T6B_OUT=t6b_rerun DESIGNS="tuned tuned_unlogged" scripts/t6b.sh
echo "== integration $(date -Is)"; scripts/restack.sh >/dev/null || { echo "restack failed $(date -Is): batch stopped" >&2; exit 1; }; sleep 8
LT_IT=1 uv run pytest tests/test_integration.py -q > results/integration_tests.txt 2>&1; tail -2 results/integration_tests.txt
echo "batch8 done $(date -Is)"
