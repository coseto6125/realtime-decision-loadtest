#!/usr/bin/env bash
# Recreate the whole stack from empty volumes with optional overrides, then wait up to 60 s until every service is
# running and healthy; exit 1 when a compose command fails or the deadline passes (compose errors stay on stderr).
# Usage: LT_SSH=<alias> scripts/restack.sh [VAR=value ...] [-- compose -f args]
#   e.g. scripts/restack.sh PG_INITDB_ARGS= 'PG_CMD=postgres -c shared_preload_libraries=pg_stat_statements'
set -euo pipefail
: "${LT_SSH:?set LT_SSH}"
R=${LT_REMOTE_DIR:-lt-loadtest}
envs=(); files=()
while [ $# -gt 0 ]; do
  case $1 in --) shift; files=("$@"); break ;; *) envs+=("$(printf '%q' "$1")"); shift ;; esac
done
# the remote script runs under bash -s with set -e: a failed down or up stops it, and its exit status is ours
ssh "$LT_SSH" bash -s <<REMOTE
set -euo pipefail
cd $R
docker compose -p lt ${files[*]:-} down -v --remove-orphans
env ${envs[*]:-} docker compose -p lt ${files[*]:-} up -d
# ready = at least one service listed and every one running, with no healthcheck or a healthy one
ready() { docker compose -p lt ps --format '{{.Name}} {{.State}} {{.Health}}' | awk '{ n++ } \$2 != "running" || (\$3 != "" && \$3 != "healthy") { bad = 1 } END { exit !(n > 0 && !bad) }'; }
for i in \$(seq 60); do ready && break; sleep 1; done
if ! ready; then
  echo "restack: stack not running and healthy after 60 s" >&2
  docker compose -p lt ps --format '{{.Name}} {{.State}} {{.Status}}' >&2
  exit 1
fi
sleep 3
docker compose -p lt ps --format '{{.Name}} {{.Status}}'
REMOTE
