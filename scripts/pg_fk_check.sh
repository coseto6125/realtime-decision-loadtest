#!/usr/bin/env bash
# FK insert cost: the pgbench decide path (8 clients, 60 s) on the tuned config with the five FOREIGN KEYs
# dropped, twice. Compare with the tuned runs that keep them (pgb_c_tuned_r2 / _r3).
set -uo pipefail
: "${LT_SSH:?set LT_SSH}"
cd "$(dirname "$0")/.."
R=${LT_REMOTE_DIR:-lt-loadtest}
# shellcheck source=/dev/null
. scripts/pg_guard_lib.sh
for run in 1 2; do
  name=pgb_nofk_r$run; pg_may_run "$name" || break; echo "== $name $(date -Is)"
  scripts/restack.sh >/dev/null
  pg_run_begin "$name" || break
  ssh "$LT_SSH" "cd $R && docker compose -p lt stop lt-collector lt-context lt-decision lt-payments lt-scheduler lt-model >/dev/null 2>&1
    docker exec lt-pg psql -U lt -d lt -qc 'ALTER TABLE outbox DROP CONSTRAINT outbox_decision_id_fkey, DROP CONSTRAINT outbox_job_id_fkey' \
      -c 'ALTER TABLE next_day_jobs DROP CONSTRAINT next_day_jobs_decision_id_fkey' \
      -c 'ALTER TABLE entitlements DROP CONSTRAINT entitlements_job_id_fkey, DROP CONSTRAINT entitlements_payment_id_fkey'
    mkdir -p results/$name && chmod 777 results/$name
    docker run --rm --name lt-pgbench --cpuset-cpus 2 --network lt-net -e PGPASSWORD=lt-test-only \
      -v \$HOME/$R/bench:/bench:ro -v \$HOME/$R/results/$name:/out -w /out postgres:18.6 \
      pgbench -h lt-pg -U lt -n -c 8 -j 2 -T 60 -M prepared -r -l --log-prefix=/out/txn -f /bench/pgbench_decide.sql lt" \
    > "results/$name.pgbench.txt" 2>&1
  pg_run_end "$name"
  grep -E "^tps" "results/$name.pgbench.txt"
done
pg_status
