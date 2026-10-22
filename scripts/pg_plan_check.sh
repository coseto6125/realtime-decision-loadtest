#!/usr/bin/env bash
# Why do the autovacuum settings decide throughput on fresh tables? pgbench the decide path for 30 s on a
# fresh cluster with tuned config but default autovacuum, then read scan counters and the cached plan shape.
set -uo pipefail
: "${LT_SSH:?set LT_SSH}"
cd "$(dirname "$0")/.."
R=${LT_REMOTE_DIR:-lt-loadtest}
# shellcheck source=/dev/null
. scripts/pg_guard_lib.sh
for v in av_default av_tuned; do
  case $v in av_default) extra=(-c autovacuum_naptime=60s -c autovacuum_vacuum_cost_limit=-1) ;; *) extra=() ;; esac
  pg_may_run "pg_plan_$v" || break
  echo "== $v"
  scripts/restack.sh "PG_CMD=postgres -c config_file=/etc/postgresql/postgresql.conf ${extra[*]}" >/dev/null || { echo "restack failed: remaining runs skipped" >&2; exit 1; }
  pg_run_begin "pg_plan_$v" || break
  ssh "$LT_SSH" "cd $R && docker compose -p lt stop lt-collector lt-context lt-decision lt-payments lt-scheduler lt-model >/dev/null 2>&1
    docker run --rm --name lt-pgbench --cpuset-cpus 2 --network lt-net -e PGPASSWORD=lt-test-only -v \$HOME/$R/bench:/bench:ro \
      postgres:18.6 pgbench -h lt-pg -U lt -n -c 8 -j 2 -T 30 -M prepared -f /bench/pgbench_decide.sql lt 2>&1 | grep -E '^tps'
    docker exec lt-pg psql -U lt -d lt -P pager=off -c \"SELECT relname, seq_scan, seq_tup_read, idx_scan, n_live_tup, last_autoanalyze IS NOT NULL AS analyzed, autoanalyze_count FROM pg_stat_user_tables WHERE relname IN ('decisions','outbox','next_day_jobs') ORDER BY 1\""
  pg_run_end "pg_plan_$v"
done
pg_status
