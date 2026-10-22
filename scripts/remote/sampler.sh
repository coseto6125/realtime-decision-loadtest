#!/usr/bin/env bash
# Every $2 seconds: cgroup CPU usage (usec) and memory.current (bytes) for each running container whose name starts with lt-.
# Reads cgroup files directly, so sampling costs almost nothing.
set -u
out=$1; every=${2:-5}
echo "ts,container,cpu_usec,mem_bytes" > "$out"
while true; do
  ts=$(date +%s.%N)
  for c in $(docker ps --format '{{.Names}}' | grep '^lt-'); do
    id=$(docker inspect -f '{{.Id}}' "$c" 2>/dev/null) || continue
    cg=/sys/fs/cgroup/system.slice/docker-$id.scope
    [ -r "$cg/cpu.stat" ] || continue
    cpu=$(awk '/^usage_usec/ {print $2}' "$cg/cpu.stat")
    mem=$(cat "$cg/memory.current")
    echo "$ts,$c,$cpu,$mem" >> "$out"
  done
  sleep "$every"
done
