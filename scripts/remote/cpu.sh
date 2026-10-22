#!/usr/bin/env bash
# One line per running container whose name starts with lt-: name cpu_usec mem_bytes (cgroup v2).
# docker ps without -a: a stopped container has no cgroup to read.
for c in $(docker ps --format '{{.Names}}' | grep '^lt-'); do
  id=$(docker inspect -f '{{.Id}}' "$c")
  cg=/sys/fs/cgroup/system.slice/docker-$id.scope
  echo "$c $(awk '/^usage_usec/ {print $2}' "$cg/cpu.stat") $(cat "$cg/memory.current")"
done
