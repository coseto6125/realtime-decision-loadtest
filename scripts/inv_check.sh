#!/usr/bin/env bash
# Print "<test>: N invariants, M non-zero" for each results/<test>/invariants.txt given.
for f in "$@"; do
  n=$(grep -cE '^ [a-z0-9_]+ +\| +[0-9]+$' "$f"); bad=$(grep -E '^ [a-z0-9_]+ +\| +[0-9]+$' "$f" | grep -vcE '\| +0$')
  echo "$(basename "$(dirname "$f")"): $n invariants, $bad non-zero"
done
