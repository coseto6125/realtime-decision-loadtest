#!/usr/bin/env bash
# Postgres A/B at T1 load, 3 minutes each, every run on a freshly initialised cluster.
#   pg_a_default : PG18 defaults (plus pg_stat_statements for measurement), libc locale
#   pg_b_libc    : tuned pg/postgresql.conf, libc locale            (isolates collation)
#   pg_c_tuned   : tuned config, builtin C.UTF-8 locale, io_method=worker
#   pg_d_iouring : as pg_c, io_method=io_uring with the lt-pg-only seccomp profile
set -uo pipefail
: "${LT_SSH:?set LT_SSH}"
cd "$(dirname "$0")/.."
run() { name=$1; shift; echo "== $name $(date -Is)"; scripts/restack.sh "$@"; sleep 5
        scripts/run_test.sh "$name" -e DURATION=3m -e RATE=5.3 | tail -1; }
run pg_a_default PG_INITDB_ARGS= 'PG_CMD=postgres -c shared_preload_libraries=pg_stat_statements -c track_io_timing=on'
run pg_b_libc PG_INITDB_ARGS=
run pg_c_tuned
run pg_d_iouring -- -f compose.yaml -f compose.iouring.yaml
