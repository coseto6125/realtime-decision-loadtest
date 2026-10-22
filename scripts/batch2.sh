#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/.."
scripts/deploy.sh >/dev/null || { echo "deploy failed $(date -Is): batch stopped" >&2; exit 1; }; echo "deployed $(date -Is)"
scripts/pg_bench_matrix.sh
scripts/ab_runs.sh
echo "batch2 done $(date -Is)"
