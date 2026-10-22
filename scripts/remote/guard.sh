#!/usr/bin/env bash
# Red-line watchdog: abort the test when the 1-minute load exceeds 3.5 or MemAvailable drops below 6 GiB.
# Writes $1/ABORT with the reason and stops the on-host load generator if it runs.
set -u
dir=$1
rm -f "$dir/ABORT"
while true; do
  load=$(cut -d' ' -f1 /proc/loadavg)
  avail_kb=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)
  if awk -v l="$load" -v a="$avail_kb" 'BEGIN { exit !(l > 3.5 || a < 6291456) }'; then
    echo "$(date -Is) load1=$load mem_available_kb=$avail_kb" >> "$dir/ABORT"
    docker stop -t 1 lt-k6 lt-bench lt-pgbench >/dev/null 2>&1
  fi
  echo "$(date +%s) $load $avail_kb" >> "$dir/guard.log"
  sleep 2
done
