#!/usr/bin/env bash
# T4 error-burst root cause. Evidence goes to results/t4_rootcause/.
#   1. t4_diag      : T4 again with the original settings (keep-alive 75 s, non-blocking redis pool 64),
#                     k6 tags vu/iter, server-side error classes logged, socket and Valkey counters before/after
#   2. ka_repro_75  : 200 VUs idle about 75 s between two POSTs on one connection, server keep-alive 75 s
#      ka_repro_600 : same client, server keep-alive 600 s
#   3. t4_fixed     : keep-alive 600 s, blocking redis pool, ramp to 25x
set -uo pipefail
: "${LT_SSH:?set LT_SSH}"
cd "$(dirname "$0")/.."
R=${LT_REMOTE_DIR:-lt-loadtest}
E=results/t4_rootcause; mkdir -p $E
evidence() {  # $1 = label
  ssh "$LT_SSH" "set +e
    echo '## valkey'; docker exec lt-redis valkey-cli CONFIG GET maxclients | tail -1
    docker exec lt-redis valkey-cli INFO clients | grep -E 'connected_clients|maxclients|blocked_clients'
    docker exec lt-redis valkey-cli INFO stats | grep -E 'rejected_connections|total_connections_received|evicted|total_error_replies'
    for c in lt-collector lt-decision; do
      echo \"## \$c ulimit -n: \$(docker exec \$c sh -c 'ulimit -n')\"
      docker exec \$c cat /proc/net/sockstat | grep TCP
      docker exec \$c sh -c 'grep -A1 ^TcpExt /proc/net/netstat' | python3 -c 'import sys; h,v=[l.split() for l in sys.stdin.read().splitlines()[:2]]; print({k:x for k,x in zip(h,v) if k in (\"ListenOverflows\",\"ListenDrops\",\"TCPAbortOnClose\",\"TCPAbortOnTimeout\",\"TCPAbortOnData\")})' 2>/dev/null || true
    done" > "$E/evidence_$1.txt" 2>&1
}
echo "== t4_diag $(date -Is)"
scripts/restack.sh KEEP_ALIVE_S=75 REDIS_POOL_BLOCKING=0 >/dev/null; sleep 5
evidence before_diag
K6_MODE=host scripts/run_test.sh t4_diag -e PROFILE=stress -e PEAK_X=15 -e RAMP=270s -e MAX_VUS=3000 \
  --system-tags=proto,status,method,name,error_code,scenario,vu,iter | tail -1
evidence after_diag
ssh "$LT_SSH" "docker logs lt-collector 2>&1 | grep -E 'event store error' | head -20; echo '## decision log'; docker logs lt-decision 2>&1 | grep -vE 'Starting|ready' | tail -20" > "$E/logs_diag.txt" 2>&1
for ka in 75 600; do
  echo "== ka_repro_$ka $(date -Is)"
  scripts/restack.sh KEEP_ALIVE_S=$ka >/dev/null; sleep 5
  SKIP_INVARIANTS=1 K6_MODE=host K6_SCRIPT=k6/keepalive_repro.js scripts/run_test.sh ka_repro_$ka -e KA=75 -e VUS=200 | tail -1
done
echo "== t4_fixed $(date -Is)"
scripts/restack.sh KEEP_ALIVE_S=600 REDIS_POOL_BLOCKING=1 >/dev/null; sleep 5
evidence before_fixed
K6_MODE=host scripts/run_test.sh t4_fixed -e PROFILE=stress -e PEAK_X=25 -e RAMP=270s -e MAX_VUS=4000 \
  --system-tags=proto,status,method,name,error_code,scenario,vu,iter | tail -1
evidence after_fixed
echo "rootcause done $(date -Is)"
