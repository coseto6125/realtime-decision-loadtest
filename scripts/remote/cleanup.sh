#!/usr/bin/env bash
# Remove what the load test created on the host, then print the proof.
# Usage: [LT_REMOTE_DIR=<dir>] bash cleanup.sh "<UTC start of the work, e.g. 2026-10-02 14:55>" [--apply]
#   LT_REMOTE_DIR: the deploy dir, relative to $HOME or absolute; default lt-loadtest (same as deploy.sh).
#   Run it as: ssh <alias> 'LT_REMOTE_DIR=<dir> bash -s -- "<start>" --apply' < scripts/remote/cleanup.sh
# Without --apply it only prints what it would do. With --apply it:
#   1. stops the guard.sh and sampler.sh loops that run from the deploy dir (pkill anchored to that dir);
#   2. runs `docker compose -p lt down -v --remove-orphans` in the deploy dir;
#   3. removes containers, volumes and networks whose name STARTS with lt-;
#   4. removes the images listed in IMAGES that exist and are not in ~/.lt-preexisting-images, the
#      inventory deploy.sh writes before its first build. No inventory file: it removes no image;
#   5. removes dangling images and build-cache entries created at or after START (datetime compare);
#   6. deletes the deploy dir and the inventory file.
# It exits non-zero without touching anything when START is empty or not a date and time, or when docker fails.
set -u
APPLY=${2:-}
case $APPLY in ''|--apply) ;; *) echo "cleanup.sh: unknown argument '$APPLY' (only --apply is allowed)" >&2; exit 2 ;; esac

IMAGES="lt-app:1 lt-collector-rs:1 postgres:18.6 postgres:17-alpine valkey/valkey:8.1.4-alpine grafana/k6:2.3.0 redis:7.2-alpine docker.dragonflydb.io/dragonflydb/dragonfly:v1.38.0 python:3.13-slim rust:1.97.1-slim-bookworm gcr.io/distroless/cc-debian12:nonroot"
INV=$HOME/.lt-preexisting-images

# datetime helper. check: exit 1 unless argv[2] parses. after: keep JSON records on stdin whose CreatedAt >= argv[2].
# Accepts "2026-10-02 14:55[:ss]", RFC 3339 "2026-10-02T14:55:01.123456789Z" and docker's "2026-10-02 14:55:01 +0000 UTC".
# A time without an offset is UTC.
DT_PY='import json, re, sys
from datetime import datetime, timedelta, timezone
PAT = re.compile(r"\s*(\d{4})-(\d\d)-(\d\d)[T ](\d\d):(\d\d)(?::(\d\d)(?:\.(\d+))?)?\s*(Z|[+-]\d\d:?\d\d)?(?:\s+[A-Za-z]{2,5})?\s*")
def parse(s):
    m = PAT.fullmatch(s)
    if not m:
        raise ValueError(s)
    y, mo, d, h, mi, sec, frac, off = m.groups()
    tz = timezone.utc
    if off and off != "Z":
        o = off.replace(":", "")
        tz = timezone(timedelta(hours=int(o[1:3]), minutes=int(o[3:5])) * (-1 if o[0] == "-" else 1))
    return datetime(int(y), int(mo), int(d), int(h), int(mi), int(sec or 0), int((frac or "0")[:6].ljust(6, "0")), tzinfo=tz)
mode, start = sys.argv[1], sys.argv[2]
try:
    start_dt = parse(start)
except ValueError:
    sys.exit(1)
if mode == "after":
    for line in sys.stdin:
        if not line.strip():
            continue
        r = json.loads(line)
        try:
            keep = parse(r["CreatedAt"]) >= start_dt
        except (KeyError, ValueError):
            print("cleanup.sh: skipped a record with an unreadable CreatedAt:", r.get("ID"), file=sys.stderr)
            continue
        if keep:
            print(r["ID"], "|", r.get("Description", r.get("Repository", "")))'
dt() { python3 -c "$DT_PY" "$@"; }

START=${1:-}
dt check "$START" || { echo "cleanup.sh: START '$START' is empty or is not a UTC time like '2026-10-02 14:55'; nothing was touched" >&2; exit 2; }

case ${LT_REMOTE_DIR:-lt-loadtest} in /*) D=${LT_REMOTE_DIR:-lt-loadtest} ;; *) D=$HOME/${LT_REMOTE_DIR:-lt-loadtest} ;; esac
D=$(realpath -m -- "$D")
# refuse the root, $HOME and every parent of $HOME (LT_REMOTE_DIR=. or ..): rm -rf would take the home with it
case $D in /) D= ;; esac
case $HOME/ in "$D"/*) D= ;; esac
[ -n "$D" ] || { echo "cleanup.sh: refusing to remove LT_REMOTE_DIR='${LT_REMOTE_DIR:-}': it is \$HOME or a parent of it" >&2; exit 2; }

run() { echo "+ $*"; [ "$APPLY" = --apply ] && "$@" </dev/null; }
# docker output is read once, so a docker failure stops the script instead of looking like "nothing to remove"
d() { local out; out=$(docker "$@") || { echo "cleanup.sh: docker $* failed" >&2; exit 1; }; printf '%s\n' "$out"; }
lt_names() { d "$@" | grep '^lt-'; }
owned_images() {   # IMAGES that exist and were not on the host before the test; none without an inventory
  [ -r "$INV" ] || return 0
  for i in $IMAGES; do
    docker image inspect "$i" >/dev/null 2>&1 && ! grep -qxF -- "$i" "$INV" && echo "$i"
  done
}

d ps -a --format '{{.Names}}' >/dev/null   # fail early when docker is down

pat="^bash $(printf '%s' "$D" | sed 's/[][\.*^$+?(){}|/]/\\&/g')/scripts/remote/(guard|sampler)\.sh( |\$)"
run pkill -f "$pat"
[ -d "$D" ] && (cd "$D" && run docker compose -p lt --profile dual --profile tls down -v --remove-orphans)
cd "$HOME" || exit 1
for c in $(lt_names ps -a --format '{{.Names}}'); do run docker rm -f -v "$c"; done
for v in $(lt_names volume ls --format '{{.Name}}'); do run docker volume rm "$v"; done
for n in $(lt_names network ls --format '{{.Name}}'); do run docker network rm "$n"; done
if [ -r "$INV" ]; then
  for i in $(owned_images); do run docker image rm "$i"; done
else
  echo "## no image is removed: $INV is missing (deploy.sh writes it before the first build)"
fi
# dangling layers left by rebuilding lt-app:1 during the test
for i in $(docker images -f dangling=true --format json | dt after "$START" | cut -d' ' -f1); do run docker image rm "$i"; done
echo "## build-cache entries created after $START"
if raw=$(docker buildx du --format json); then
  cache=$(printf '%s\n' "$raw" | dt after "$START")
  echo "$cache"
  for id in $(echo "$cache" | cut -d' ' -f1); do run docker buildx prune -f --filter id="$id" >/dev/null; done
else
  echo "cleanup.sh: docker buildx du failed: build cache not checked" >&2
fi
if [ "$APPLY" = --apply ]; then run rm -rf "$D"; fi
echo "## proof"
echo "containers: $(lt_names ps -a --format '{{.Names}}' | grep -c .)"
echo "volumes: $(lt_names volume ls --format '{{.Name}}' | grep -c .)"
echo "networks: $(lt_names network ls --format '{{.Name}}' | grep -c .)"
if [ -r "$INV" ]; then echo "images: $(owned_images | grep -c .)"; else echo "images: not checked (no $INV)"; fi
echo "build-cache entries after start: $(docker buildx du --format json | dt after "$START" | grep -c .)"
ls "$D" 2>&1 | head -2
if [ "$APPLY" = --apply ]; then run rm -f "$INV"; fi
