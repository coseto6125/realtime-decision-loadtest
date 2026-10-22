#!/usr/bin/env bash
# T6b: can Postgres replace Valkey as both the Context store and the event stream?
# lt-pg as configured (1 vCPU / 2 GB, cores 0,1), app services stopped, driver lt-bench on core 2,
# database lt_t6b (dropped at the end). Per design: baseline 150 events/s for 60 s, then a ramp (max 3 min).
# The red-line guard runs for the whole script; after a breach no further design is started.
set -uo pipefail
: "${LT_SSH:?set LT_SSH}"
cd "$(dirname "$0")/.."
R=${LT_REMOTE_DIR:-lt-loadtest}
H="\$HOME/$R"
OUT=${T6B_OUT:-t6b}
DSN=postgres://lt:${PG_PASSWORD:-lt-test-only}@lt-pg:5432/lt_t6b
STEPS=${STEPS:-150,300,600,1200,2400,4800,9600}
BG="bash $R/scripts/remote/bg.sh"
rs() { ssh -o BatchMode=yes "$LT_SSH" "$@"; }
mkdir -p results/$OUT results/private/$OUT
rs "cd $R && docker compose -p lt stop lt-collector lt-context lt-decision lt-payments lt-scheduler lt-model >/dev/null 2>&1; cd
  docker exec lt-pg psql -U lt -d lt -qc 'DROP DATABASE IF EXISTS lt_t6b' -c 'CREATE DATABASE lt_t6b'
  $BG start $H/results/$OUT/guard.pid bash $H/scripts/remote/guard.sh $H/results/$OUT" ||
  { echo "guard did not start: T6b not run" | tee results/$OUT/ABORT.txt >&2; exit 1; }
# whatever ends this script (error, Ctrl-C, kill) stops the guard and a running sampler once, through bg.sh
bg_up=1
stop_bg() {
  [ -n "$bg_up" ] || return 0
  bg_up=
  rs "$BG stop $H/results/$OUT/guard.pid; [ ! -s $H/results/$OUT/sampler.pid ] || $BG stop $H/results/$OUT/sampler.pid"
}
trap stop_bg EXIT
trap 'stop_bg; exit 130' INT
trap 'stop_bg; exit 143' TERM
bench() { rs "docker run --rm --name lt-bench --cpuset-cpus 2 --memory 2g --network lt-net lt-app:1 python -m bench.pg_bench --dsn $DSN $*"; }
aborted() { rs "test -f $H/results/$OUT/ABORT"; }
for d in ${DESIGNS:-naive tuned tuned_unlogged}; do
  if aborted; then echo "red line hit earlier: $d not started" | tee -a results/$OUT/ABORT.txt; break; fi
  echo "== t6b $d $(date -Is)"
  rs "bash $R/scripts/remote/snapshot.sh before-t6b-$d" > results/private/$OUT/host_before_$d.txt 2>&1
  rs "$BG start $H/results/$OUT/sampler.pid bash $H/scripts/remote/sampler.sh $H/results/$OUT/sampler_$d.csv 2" ||
    { echo "sampler did not start: $d not run" | tee -a results/$OUT/ABORT.txt >&2; break; }
  { bench --setup "$d" --mode baseline --events-per-sec 150 --duration-s 60 --max-s 75
    aborted || bench --setup "$d" --mode ramp --ramp-steps "$STEPS" --step-s 20 --max-s 180; } > results/$OUT/$d.jsonl 2> results/$OUT/${d}_stderr.txt
  rs "$BG stop $H/results/$OUT/sampler.pid"
  rsync -az "$LT_SSH:$R/results/$OUT/sampler_$d.csv" results/$OUT/
  grep '"kind": *"summary"' results/$OUT/$d.jsonl | cut -c1-300
done
bench --teardown > /dev/null 2>&1
stop_bg
rs "docker exec lt-pg psql -U lt -d lt -qc 'DROP DATABASE IF EXISTS lt_t6b'; cat $H/results/$OUT/ABORT 2>/dev/null" >> results/$OUT/ABORT.txt
[ -s results/$OUT/ABORT.txt ] || rm -f results/$OUT/ABORT.txt
