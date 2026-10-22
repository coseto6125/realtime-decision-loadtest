#!/usr/bin/env bash
# Background helpers for the load host. Source it, or call: bash bg.sh start|stop|alive <pidfile> [cmd...]
#   start: run cmd under nohup, write its PID to pidfile, verify the PID is non-empty and alive; else exit 1
#   stop : kill the PID in pidfile; a missing or empty pidfile is an error (exit 1), never a bare `kill`
#   alive: exit 0 if the PID in pidfile is running
set -u

bg_start() {
  local pidfile=$1; shift
  mkdir -p "$(dirname "$pidfile")" || { echo "bg_start: cannot create $(dirname "$pidfile")" >&2; return 1; }
  nohup "$@" </dev/null >/dev/null 2>&1 &
  local pid=$!
  if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
    echo "bg_start: '$*' did not start" >&2; return 1
  fi
  echo "$pid" > "$pidfile" || { echo "bg_start: cannot write $pidfile" >&2; kill "$pid"; return 1; }
  sleep 0.5
  kill -0 "$pid" 2>/dev/null || { echo "bg_start: '$*' exited right after start" >&2; return 1; }
}

bg_stop() {
  local pidfile=$1 pid
  [ -s "$pidfile" ] || { echo "bg_stop: no PID in $pidfile" >&2; return 1; }
  pid=$(cat "$pidfile")
  case $pid in ''|*[!0-9]*) echo "bg_stop: bad PID '$pid' in $pidfile" >&2; return 1 ;; esac
  kill "$pid" 2>/dev/null || { echo "bg_stop: PID $pid from $pidfile is not running" >&2; return 1; }
  rm -f "$pidfile"
}

bg_alive() {
  [ -s "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  cmd=$1; shift
  case $cmd in
    start) bg_start "$@" ;;
    stop) bg_stop "$@" ;;
    alive) bg_alive "$@" ;;
    *) echo "usage: bg.sh start|stop|alive <pidfile> [cmd...]" >&2; exit 2 ;;
  esac
fi
