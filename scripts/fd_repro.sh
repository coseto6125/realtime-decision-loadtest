#!/usr/bin/env bash
# Open-file-limit repro: 1,200 VUs each hold one keep-alive connection to lt-collector and POST twice,
# 10 s apart (well inside the 75 s keep-alive). With nofile 1024 the collector runs out of descriptors;
# with nofile 65536 it does not. Evidence: k6 errors, collector logs, /proc/1/fd count, error counters.
set -uo pipefail
: "${LT_SSH:?set LT_SSH}"
cd "$(dirname "$0")/.."
E=results/t4_rootcause
for variant in nofile1024 nofile65536; do
  echo "== fd_repro_$variant $(date -Is)"
  files=(); [ $variant = nofile65536 ] && files=(-- -f compose.yaml -f compose.nofile.yaml)
  scripts/restack.sh "${files[@]}" >/dev/null; sleep 5
  ssh "$LT_SSH" "echo \"collector ulimit -n: \$(docker exec lt-collector sh -c 'ulimit -n')\"" > "$E/fd_repro_$variant.txt"
  ( for i in $(seq 40); do ssh "$LT_SSH" "echo \$(date +%T) fds=\$(docker exec lt-collector sh -c 'ls /proc/1/fd | wc -l')"; sleep 3; done ) >> "$E/fd_repro_${variant}_fds.txt" 2>&1 &
  poll=$!
  SKIP_INVARIANTS=1 K6_MODE=host K6_SCRIPT=k6/keepalive_repro.js scripts/run_test.sh fd_repro_$variant -e KA=10 -e VUS=1200 | tail -1
  kill $poll 2>/dev/null
  ssh "$LT_SSH" "docker logs lt-collector 2>&1 | grep -viE 'Starting worker|Worker ready' | sort | uniq -c | sort -rn | head -15" >> "$E/fd_repro_$variant.txt" 2>&1
  python3 -c "import json; d=json.load(open('results/fd_repro_$variant/stats_collector.json')); print('collector counters', d['counters'])" >> "$E/fd_repro_$variant.txt"
  grep -oE 'level=warning msg="status=[^"]*' "results/fd_repro_$variant/k6_stderr.txt" | sort | uniq -c | sort -rn | head -5 >> "$E/fd_repro_$variant.txt"
  grep -E 'reuse_failed|http_req_failed' "results/fd_repro_$variant/k6_stdout.txt" >> "$E/fd_repro_$variant.txt"
done
