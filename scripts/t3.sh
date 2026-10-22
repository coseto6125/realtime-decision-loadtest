#!/usr/bin/env bash
# T3 failure injection at baseline load, one at a time; every run ends with the invariant SQL.
# Usage: LT_SSH=<alias> scripts/t3.sh [case ...]   (default: all cases)
set -uo pipefail
: "${LT_SSH:?set LT_SSH}"
cd "$(dirname "$0")/.."
R=${LT_REMOTE_DIR:-lt-loadtest}
rs() { ssh -o BatchMode=yes "$LT_SSH" "$@"; }
faults() { curl -s -m 5 -XPOST 127.0.0.1:17104/admin/faults -d "$1"; echo; }
cases=("${@:-model_timeout context_stop redis_pause dup_events webhook_replay dual_scheduler forged_recharge}")
for c in ${cases[*]}; do
  echo "== t3_$c $(date -Is)"
  case $c in
    model_timeout)
      faults '{"timeout_rate":1.0,"error_rate":0.0}'
      scripts/run_test.sh t3_model_timeout -e DURATION=3m | tail -1
      faults '{"timeout_rate":0.0,"error_rate":0.0}' ;;
    context_stop)
      INJECT="sleep 60; echo stop \$(date -Is); docker stop lt-context; sleep 60; echo start \$(date -Is); docker start lt-context" \
        scripts/run_test.sh t3_context_stop -e DURATION=4m | tail -1 ;;
    redis_pause)
      INJECT="sleep 60; echo pause \$(date -Is); docker pause lt-redis; sleep 10; docker unpause lt-redis; echo unpause \$(date -Is)" \
        scripts/run_test.sh t3_redis_pause -e DURATION=3m | tail -1 ;;
    dup_events)
      scripts/run_test.sh t3_dup_events -e DURATION=3m -e DUP=1 | tail -1 ;;
    webhook_replay)
      scripts/run_test.sh t3_webhook_replay -e DURATION=3m -e WEBHOOK_DUP=1 -e BAD_SIG=0.01 | tail -1 ;;
    dual_scheduler)
      rs "cd $R && docker compose -p lt --profile dual up -d lt-scheduler-2 2>&1 | tail -1"
      scripts/run_test.sh t3_dual_scheduler -e DURATION=3m | tail -1
      rs "cd $R && docker compose -p lt --profile dual stop lt-scheduler-2 2>&1 | tail -1" ;;
    forged_recharge)
      scripts/run_test.sh t3_forged_recharge -e DURATION=3m -e FORGE=0.2 | tail -1 ;;
  esac
done
