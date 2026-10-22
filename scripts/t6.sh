#!/usr/bin/env bash
# T6: the same per-event op mix (XADD, XREADGROUP + EVALSHA apply script with in-script XACK,
# HGETALL + LRANGE) against three Redis-protocol stores, one at a time. Store: 2 cores (cpuset 0,1),
# 1 GB, no persistence for all three (Dragonfly has no AOF, so AOF is off for the others too).
# Driver: bench.cache_bench in container lt-bench on core 2. Baseline 150 events/s, then a ramp.
set -uo pipefail
: "${LT_SSH:?set LT_SSH}"
cd "$(dirname "$0")/.."
R=${LT_REMOTE_DIR:-lt-loadtest}
EPS=${EPS:-150}
STEPS=${STEPS:-150,1000,2000,4000,8000,12000,16000,24000}
declare -A IMG=(
  [valkey]="valkey/valkey:8.1.4-alpine valkey-server --save '' --appendonly no --io-threads 2 --maxmemory 900mb --maxmemory-policy noeviction"
  [redis72]="redis:7.2-alpine redis-server --save '' --appendonly no --io-threads 2 --maxmemory 900mb --maxmemory-policy noeviction"
  [dragonfly]="docker.dragonflydb.io/dragonflydb/dragonfly:v1.38.0 dragonfly --proactor_threads=2 --maxmemory=900mb --dbfilename="
)
mkdir -p results/t6 results/private/t6
H="\$HOME/$R"; BG="bash $R/scripts/remote/bg.sh"
ssh "$LT_SSH" "$BG start $H/results/t6/guard.pid bash $H/scripts/remote/guard.sh $H/results/t6" ||
  { echo "guard did not start: T6 not run" | tee results/t6/ABORT.txt >&2; exit 1; }
# whatever ends this script (error, Ctrl-C, kill) stops the guard, the sampler and the cache container once
bg_up=1
stop_bg() {
  [ -n "$bg_up" ] || return 0
  bg_up=
  ssh -o BatchMode=yes "$LT_SSH" "$BG stop $H/results/t6/guard.pid; [ ! -s $H/results/t6/sampler.pid ] || $BG stop $H/results/t6/sampler.pid"
}
trap stop_bg EXIT
trap 'stop_bg; exit 130' INT
trap 'stop_bg; exit 143' TERM
ssh "$LT_SSH" "cd $R && docker compose -p lt stop >/dev/null 2>&1; docker network inspect lt-net >/dev/null 2>&1 || docker network create lt-net >/dev/null"
for s in ${STORES:-valkey redis72 dragonfly}; do
  if ssh "$LT_SSH" "test -f $H/results/t6/ABORT"; then echo "red line hit earlier: $s not started" | tee -a results/t6/ABORT.txt; break; fi
  set -- ${IMG[$s]}; img=$1; shift
  echo "== t6 $s $(date -Is)"
  ssh "$LT_SSH" "bash $R/scripts/remote/snapshot.sh before-t6-$s" > results/private/t6/host_before_$s.txt 2>&1
  ssh "$LT_SSH" "docker run -d --name lt-cache-$s --network lt-net --cpuset-cpus 0,1 --memory 1g -p 127.0.0.1:17120:6379 $img $* >/dev/null
    sleep 4; $BG start $H/results/t6/sampler.pid bash $H/scripts/remote/sampler.sh $H/results/t6/sampler_$s.csv 2 || exit 1
    for mode in baseline ramp; do
      docker run --rm --name lt-bench --cpuset-cpus 2 --memory 2g --network lt-net lt-app:1 \
        python -m bench.cache_bench --host lt-cache-$s --port 6379 --mode \$mode --events-per-sec $EPS --duration-s 60 \
        --ramp-steps $STEPS --step-s 20 --p99-limit-ms 10
    done
    $BG stop $H/results/t6/sampler.pid; docker exec lt-cache-$s sh -c 'redis-cli INFO server 2>/dev/null || valkey-cli INFO server' | grep -E 'version|dragonfly' | head -3
    docker stop lt-cache-$s >/dev/null; docker rm -v lt-cache-$s >/dev/null" > results/t6/$s.jsonl 2> results/t6/${s}_stderr.txt
  rsync -az "$LT_SSH:$R/results/t6/sampler_$s.csv" results/t6/
  ssh "$LT_SSH" "bash $R/scripts/remote/snapshot.sh after-t6-$s" > results/private/t6/host_after_$s.txt 2>&1
  grep '"kind": *"summary"' results/t6/$s.jsonl | cut -c1-300
done
stop_bg
ssh "$LT_SSH" "cat $H/results/t6/ABORT 2>/dev/null" | tee -a results/t6/ABORT.txt
[ -s results/t6/ABORT.txt ] || rm -f results/t6/ABORT.txt
