#!/usr/bin/env bash
# One measured test with k6 running locally through an SSH tunnel (ports 17101-17106 on 127.0.0.1).
#
# Usage: LT_SSH=<alias> scripts/run_test.sh <name> [k6 -e args...]
#   env INJECT='<shell run on the load host during the test>'  (failure injection, runs in background)
#   env PROFILE_SECS=<n>  also record py-spy --native profiles (PROFILE_TARGETS, default lt-collector lt-context)
#   env POLL_SECS=<n>     poll /stats of lt-decision and lt-model every n seconds into stats_poll.jsonl
#   env K6_SCRIPT (default k6/traffic.js), SKIP_INVARIANTS=1
#   env K6_MODE=host  run k6 as container lt-k6 on the load host (core 2, lt-net) instead of locally
# Writes results/<name>/ (anonymised, committable) and results/private/<name>/ (raw host snapshots).
set -uo pipefail
: "${LT_SSH:?set LT_SSH}"
name=$1; shift
cd "$(dirname "$0")/.."
R=${LT_REMOTE_DIR:-lt-loadtest}
out=results/$name; priv=results/private/$name
mkdir -p "$out" "$priv"
rs() { ssh -o BatchMode=yes "$LT_SSH" "$@"; }
psql_lt() { rs "docker exec -i lt-pg psql -U lt -d lt -X $*"; }
services=(collector:17101 decision:17102 payments:17103 model:17104 context:17105 scheduler:17106)

# cool-down: the 1-minute load average lags, so wait until it is below START_LOAD_MAX (default 2.0)
# before a test starts; otherwise the red-line guard (3.5) can fire on the previous step's tail.
rs "for i in \$(seq 60); do awk -v m=${START_LOAD_MAX:-2.0} '{ exit !(\$1 < m) }' /proc/loadavg && break; sleep 5; done; cut -d' ' -f1 /proc/loadavg" > "$priv/start_load.txt"
rs "bash $R/scripts/remote/snapshot.sh before-$name" > "$priv/host_before.txt" 2>&1
grep AGGREGATE "$priv/host_before.txt" > "$out/host_before_aggregate.txt"
rs "mkdir -p $R/results/$name; bash $R/scripts/remote/cpu.sh" > "$out/cgroup_before.txt"
for s in "${services[@]}"; do curl -s -m 5 -XPOST "127.0.0.1:${s#*:}/admin/reset" >/dev/null; done
psql_lt -qAt < sql/reset_stats.sql >/dev/null
rs "docker exec lt-redis sh -c 'valkey-cli CONFIG RESETSTAT >/dev/null 2>&1 || redis-cli CONFIG RESETSTAT >/dev/null 2>&1'; true"
# guard and sampler start through bg.sh, which verifies each PID; no guard, no test
H="\$HOME/$R"
rs "bash $R/scripts/remote/bg.sh start $H/results/$name/guard.pid bash $H/scripts/remote/guard.sh $H/results/$name &&
    bash $R/scripts/remote/bg.sh start $H/results/$name/sampler.pid bash $H/scripts/remote/sampler.sh $H/results/$name/sampler.csv 5" ||
  { echo "guard or sampler did not start: refusing to run $name" | tee "$out/ABORT.txt" >&2; exit 1; }
# whatever ends this script (error, Ctrl-C, kill) stops the remote guard and sampler once, through bg.sh
bg_up=1
stop_bg() {
  [ -n "$bg_up" ] || return 0
  bg_up=
  rs "bash $R/scripts/remote/bg.sh stop $H/results/$name/guard.pid; bash $R/scripts/remote/bg.sh stop $H/results/$name/sampler.pid"
}
trap stop_bg EXIT
trap 'stop_bg; exit 130' INT
trap 'stop_bg; exit 143' TERM

if [ -n "${INJECT:-}" ]; then
  # the ssh session itself is backgrounded here, so k6 starts now and the injection runs alongside it
  rs "cd $R && ( $INJECT ) > results/$name/inject.log 2>&1" </dev/null &
fi
if [ -n "${PROFILE_SECS:-}" ]; then
  for c in ${PROFILE_TARGETS:-lt-collector lt-context}; do
    rs "sleep 30; docker exec $c py-spy record --native --idle --pid 1 --rate 250 --duration $PROFILE_SECS --format raw \
        --output /tmp/$c.collapsed >/dev/null 2>&1; docker cp $c:/tmp/$c.collapsed $R/results/$name/" \
        </dev/null > /dev/null 2>&1 &
  done
fi

if [ -n "${POLL_SECS:-}" ]; then
  ( while true; do
      for p in 17102 17104; do curl -s -m 2 "127.0.0.1:$p/stats" | tr -d '\n'; echo; done
      sleep "$POLL_SECS"
    done ) > "$out/stats_poll.jsonl" 2>/dev/null &
  pollpid=$!
fi
start=$(date -Is)
script=${K6_SCRIPT:-k6/traffic.js}
if [ "${K6_MODE:-local}" = host ]; then
  # k6 in a container on the load host, pinned to core 2, talking to the services over lt-net
  rs "docker run --rm --name lt-k6 --user \$(id -u):\$(id -g) --cpuset-cpus 2 --memory 3g --network lt-net \
      -v \$HOME/$R/k6:/k6:ro -v \$HOME/$R/results/$name:/out grafana/k6:2.3.0 run -q --no-color \
      -e RUN=$name-\$(date +%s) -e SUMMARY=/out/k6_summary.json -e COLLECTOR=http://lt-collector:8000 \
      -e DECISION=http://lt-decision:8000 -e PAYMENTS=http://lt-payments:8000 $* \
      --out csv=/out/k6_points.csv.gz /$script" > "$out/k6_stdout.txt" 2> "$out/k6_stderr.txt" &
else
  k6 run -q --no-color -e RUN="$name-$(date +%s)" -e SUMMARY="$out/k6_summary.json" "$@" "$script" \
    > "$out/k6_stdout.txt" 2> "$out/k6_stderr.txt" &
fi
k6pid=$!
while kill -0 $k6pid 2>/dev/null; do
  sleep 10
  if rs "test -f $R/results/$name/ABORT"; then
    echo "ABORT: red line hit, stopping k6" | tee -a "$out/ABORT.txt"
    rs "cat $R/results/$name/ABORT" | tee -a "$out/ABORT.txt"
    rs "docker stop -t 2 lt-k6 >/dev/null 2>&1"; kill -INT $k6pid; sleep 5; kill $k6pid 2>/dev/null
  fi
done
wait $k6pid; echo "k6 exit $? start=$start end=$(date -Is)" > "$out/k6_exit.txt"
[ -n "${pollpid:-}" ] && kill "$pollpid" 2>/dev/null
wait   # profiles

rs "bash $R/scripts/remote/cpu.sh" > "$out/cgroup_after.txt"
for s in "${services[@]}"; do curl -s -m 5 "127.0.0.1:${s#*:}/stats" > "$out/stats_${s%%:*}.json"; done
curl -s -m 5 127.0.0.1:17107/stats > "$out/stats_scheduler2.json" 2>/dev/null || rm -f "$out/stats_scheduler2.json"
[ -s "$out/stats_scheduler2.json" ] || rm -f "$out/stats_scheduler2.json"
psql_lt -P pager=off < sql/pgstats.sql > "$out/pgstats.txt" 2>&1
psql_lt --csv < sql/pgstats_kv.sql > "$out/pgstats_kv.csv" 2>&1
rs "docker exec lt-redis sh -c 'C=\$(command -v valkey-cli || command -v redis-cli); \$C INFO commandstats; \$C INFO memory | grep -E \"used_memory:|used_memory_rss:|mem_fragmentation_ratio\"; echo \"## SLOWLOG\"; \$C SLOWLOG GET 10; echo \"## LATENCY DOCTOR\"; \$C LATENCY DOCTOR'" > "$out/redis_stats.txt" 2>&1
if [ -z "${SKIP_INVARIANTS:-}" ]; then
  psql_lt -P pager=off < sql/invariants.sql > "$out/invariants.txt" 2>&1
  psql_lt -P pager=off < sql/facts.sql > "$out/facts.txt" 2>&1
fi
stop_bg ||
  echo "WARNING: stopping the guard or sampler of $name reported an error" | tee -a "$out/ABORT.txt" >&2
rs "bash $R/scripts/remote/snapshot.sh after-$name" > "$priv/host_after.txt" 2>&1
grep AGGREGATE "$priv/host_after.txt" > "$out/host_after_aggregate.txt"
rsync -az --exclude ABORT "$LT_SSH:$R/results/$name/" "$out/remote/" 2>/dev/null
[ -f "$out/remote/k6_summary.json" ] && cp "$out/remote/k6_summary.json" "$out/k6_summary.json"
echo "done: $out"
