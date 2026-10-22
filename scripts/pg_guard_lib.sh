# shellcheck shell=bash
# Source from the pg_*.sh scripts (after cd to the repo root, with LT_SSH and R set).
# Each pgbench run gets the red-line guard (scripts/remote/guard.sh, started through bg.sh) in its run folder.
#   pg_may_run <name>    return 1 (skip the run, before restack) once an earlier run hit the red line or lost its guard
#   pg_run_begin <name>  start the guard in results/<name> on the load host; return 1 (skip the run) when it
#                        does not start: no guard, no run
#   pg_run_end <name>    stop the guard, fetch results/<name>, record an ABORT, delete the remote folder
# PG_ABORTED is set once a run hit the red line or lost its guard; every later pg_run_begin then refuses.
# An EXIT, INT or TERM trap stops the guard of the run in flight. Finish the calling script with `pg_status`.
PG_RUN=
PG_ABORTED=
_pg_ssh() { ssh -o BatchMode=yes "$LT_SSH" "$@"; }

pg_may_run() {
  if [ -n "$PG_ABORTED" ]; then echo "red line hit earlier ($PG_ABORTED): $1 not run" >&2; return 1; fi
}

pg_run_begin() {
  PG_RUN=$1
  _pg_ssh "cd $R && mkdir -p results/$1 && chmod 777 results/$1 && bash scripts/remote/bg.sh start \$HOME/$R/results/$1/guard.pid bash \$HOME/$R/scripts/remote/guard.sh \$HOME/$R/results/$1" || {
    echo "guard did not start: $1 not run" >&2; PG_RUN=; PG_ABORTED="guard did not start for $1"; return 1; }
}

pg_guard_stop() {
  [ -n "$PG_RUN" ] || return 0
  local run=$PG_RUN
  PG_RUN=
  _pg_ssh "[ ! -s \$HOME/$R/results/$run/guard.pid ] || bash $R/scripts/remote/bg.sh stop \$HOME/$R/results/$run/guard.pid"
}

pg_run_end() {
  pg_guard_stop
  mkdir -p "results/$1"; rsync -az "$LT_SSH:$R/results/$1/" "results/$1/"
  if [ -f "results/$1/ABORT" ]; then PG_ABORTED=$1; echo "red line hit in $1: the remaining runs are skipped" >&2; fi
  _pg_ssh "rm -rf $R/results/$1"
}

pg_status() { [ -z "$PG_ABORTED" ]; }

trap pg_guard_stop EXIT
trap 'pg_guard_stop; exit 130' INT
trap 'pg_guard_stop; exit 143' TERM
