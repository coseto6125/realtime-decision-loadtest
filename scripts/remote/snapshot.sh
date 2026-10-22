#!/usr/bin/env bash
# Host snapshot before/after a test. Raw output (names existing containers) goes to stdout for
# results/private/; the last line is an anonymised aggregate safe to commit.
set -u
echo "## $(date -Is) $1"
uptime
free -g
echo "## docker stats (existing containers)"
docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}' | grep -v '^lt-'
echo "## docker stats (lt-)"
docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}' | grep '^lt-' || true
n=$(docker ps --format '{{.Names}}' | grep -vc '^lt-')
load=$(cut -d' ' -f1-3 /proc/loadavg)
avail=$(awk '/MemAvailable/ {printf "%.1f", $2/1048576}' /proc/meminfo)
echo "AGGREGATE existing_containers=$n load=\"$load\" mem_available_gib=$avail"
