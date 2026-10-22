#!/usr/bin/env bash
# Ablation and repeats of the Postgres config A/B under load the T1 run cannot produce: pgbench replays the decision hot path
# (idempotency lookup + Decision/outbox/job insert) with 8 clients for 60 s, from container lt-pgbench
# on core 2. App services are stopped during the run. Per-transaction latencies -> p50/p95/p99.
set -uo pipefail
: "${LT_SSH:?set LT_SSH}"
cd "$(dirname "$0")/.."
R=${LT_REMOTE_DIR:-lt-loadtest}
# shellcheck source=/dev/null
. scripts/pg_guard_lib.sh
bench() { name=$1; shift; pg_may_run "$name" || return 0; echo "== $name $(date -Is)"; scripts/restack.sh "$@" >/dev/null
  pg_run_begin "$name" || return 0
  ssh "$LT_SSH" "cd $R && docker compose -p lt stop lt-collector lt-context lt-decision lt-payments lt-scheduler lt-model >/dev/null 2>&1
    mkdir -p results/$name && chmod 777 results/$name
    echo before; docker exec lt-pg psql -U lt -d lt -AtF, -c \"SELECT wal_bytes, wal_fpi FROM pg_stat_wal\" -c \"SELECT num_timed, num_requested FROM pg_stat_checkpointer\"
    cat /sys/fs/cgroup/system.slice/docker-\$(docker inspect -f '{{.Id}}' lt-pg).scope/cpu.stat | head -1
    docker run --rm --name lt-pgbench --cpuset-cpus 2 --network lt-net -e PGPASSWORD=lt-test-only \
      -v \$HOME/$R/bench:/bench:ro -v \$HOME/$R/results/$name:/out -w /out postgres:18.6 \
      pgbench -h lt-pg -U lt -n -c 8 -j 2 -T 60 -M prepared -r -l --log-prefix=/out/txn -f /bench/pgbench_decide.sql lt
    echo after
    docker exec lt-pg psql -U lt -d lt -AtF, -c \"SELECT wal_bytes, wal_fpi FROM pg_stat_wal\" -c \"SELECT num_timed, num_requested FROM pg_stat_checkpointer\"
    cat /sys/fs/cgroup/system.slice/docker-\$(docker inspect -f '{{.Id}}' lt-pg).scope/cpu.stat | head -1" > "results/$name.pgbench.txt" 2>&1
  pg_run_end "$name"
  tail -25 "results/$name.pgbench.txt"; }
T='postgres -c config_file=/etc/postgresql/postgresql.conf'
D='postgres -c shared_preload_libraries=pg_stat_statements -c track_io_timing=on'
bench pgb_a_default_r2 PG_INITDB_ARGS= "PG_CMD=$D"
bench pgb_c_tuned_r2
bench pgb_x_tuned_sb128 "PG_CMD=$T -c shared_buffers=128MB"
bench pgb_x_tuned_walcomp_off "PG_CMD=$T -c wal_compression=off"
bench pgb_x_tuned_av_default "PG_CMD=$T -c autovacuum_naptime=60s -c autovacuum_vacuum_cost_limit=-1"
bench pgb_x_default_sb512 PG_INITDB_ARGS= "PG_CMD=$D -c shared_buffers=512MB"
bench pgb_b_libc_r2 PG_INITDB_ARGS=
bench pgb_c_tuned_r3
pg_status
