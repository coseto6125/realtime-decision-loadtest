#!/usr/bin/env bash
# fd repro with real events (EVENTS=1): at nofile 1024 the collector must also open Valkey connections,
# so descriptor exhaustion shows up as fast 503s with the OS error in the collector log.
set -uo pipefail
: "${LT_SSH:?set LT_SSH}"
cd "$(dirname "$0")/.."
E=results/t4_rootcause
scripts/restack.sh REDIS_POOL_BLOCKING=0 >/dev/null; sleep 5
SKIP_INVARIANTS=1 K6_MODE=host K6_SCRIPT=k6/keepalive_repro.js scripts/run_test.sh fd_repro_events_nofile1024 -e KA=10 -e VUS=1200 -e EVENTS=1 | tail -1
{ echo "## collector log (first 20 store errors)"; ssh "$LT_SSH" "docker logs lt-collector 2>&1 | grep 'event store error' | cut -c1-220 | head -20"
  python3 -c "import json; d=json.load(open('results/fd_repro_events_nofile1024/stats_collector.json')); print('collector counters', d['counters']); print({k: v for k, v in d['latency_ms'].items() if '5xx' in k})"
  grep -oE 'msg="status=[^"]*' results/fd_repro_events_nofile1024/k6_stderr.txt | sort | uniq -c | sort -rn | head -5
  grep -E 'reuse_failed|http_req_failed' results/fd_repro_events_nofile1024/k6_stdout.txt; } > "$E/fd_repro_events_nofile1024.txt" 2>&1
cat "$E/fd_repro_events_nofile1024.txt"
