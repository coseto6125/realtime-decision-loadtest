#!/usr/bin/env bash
# Sync sources to the load host, build lt-app:1, and (re)start the stack.
# Usage: LT_SSH=<ssh-alias> scripts/deploy.sh [compose args...]
# Before the first build it records the images already on the host in ~/.lt-preexisting-images (outside the
# rsync target; written once, so a second deploy never lists the test's own images). cleanup.sh reads it.
set -euo pipefail
: "${LT_SSH:?set LT_SSH to the ssh alias of the load host}"
DIR=${LT_REMOTE_DIR:-lt-loadtest}
cd "$(dirname "$0")/.."
rsync -az --delete --exclude .venv --exclude results --exclude __pycache__ --exclude .pytest_cache --exclude target ./ "$LT_SSH:$DIR/" || [ $? -eq 24 ]   # 24: files vanished mid-copy
# shellcheck disable=SC2016   # $HOME must expand on the remote host
INV='$HOME/.lt-preexisting-images'
ssh "$LT_SSH" "{ [ -e $INV ] || { docker image ls --format '{{.Repository}}:{{.Tag}}' > $INV.tmp && mv $INV.tmp $INV; }; } && cd $DIR && mkdir -p results && docker build -q -t lt-app:1 . && docker compose -p lt up -d ${*:-} && docker compose -p lt ps --format '{{.Name}} {{.State}} {{.Status}}'"
