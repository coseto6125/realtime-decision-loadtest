#!/usr/bin/env bash
# T7: resource cost of the model call (each run 3 minutes, red-line guard on).
#   t7_prof_1x / t7_prof_13x : py-spy --native on lt-decision at T1 load and at 13x (k6 on the load host)
#   t7_slow_mock             : mock p50 200 ms (most calls exceed the 150 ms budget); /stats polled every 5 s
#   t7_tls_{ka,noka}_{1x,5x} : TLS terminator in front of lt-model, keep-alive vs new connection per call.
#                              The noka runs were voided: rnet no_keepalive=True did not stop connection reuse
#                              (TIME_WAIT stayed 0). batch7.sh reran them as t7_tls_noka2_{1x,5x} with pool_max_idle_per_host=0.
#   t7_plain_ka_5x           : plain HTTP keep-alive at 5x, as the reference for the 5x TLS runs
#   t7_breaker_{off,on}      : model 100% timeouts, breaker off vs on
set -uo pipefail
: "${LT_SSH:?set LT_SSH}"
cd "$(dirname "$0")/.."
faults() { curl -s -m 5 -XPOST 127.0.0.1:17104/admin/faults -d "$1"; echo; }
TLS=(MODEL_URL=https://lt-model-tls:8443/judge MODEL_TLS_VERIFY=0)
step() { echo "== $1 $(date -Is)"; }

step t7_prof_1x; scripts/restack.sh >/dev/null; sleep 5
PROFILE_SECS=120 PROFILE_TARGETS=lt-decision POLL_SECS=5 scripts/run_test.sh t7_prof_1x -e DURATION=3m | tail -1
step t7_prof_13x; scripts/restack.sh >/dev/null; sleep 5
PROFILE_SECS=120 PROFILE_TARGETS=lt-decision POLL_SECS=5 K6_MODE=host scripts/run_test.sh t7_prof_13x \
  -e DURATION=3m -e RATE=69 -e MAX_VUS=3000 | tail -1
step t7_slow_mock; scripts/restack.sh >/dev/null; sleep 5
faults '{"median_ms":200,"p95_ms":600}'
POLL_SECS=5 scripts/run_test.sh t7_slow_mock -e DURATION=3m | tail -1
faults '{}'
for load in 1x 5x; do
  rate=$([ $load = 1x ] && echo 5.3 || echo 26.5); mode=$([ $load = 1x ] && echo local || echo host)
  for ka in 1 0; do
    name=t7_tls_$([ $ka = 1 ] && echo ka || echo noka)_$load; step $name
    scripts/restack.sh "${TLS[@]}" MODEL_KEEPALIVE=$ka -- -f compose.yaml --profile tls >/dev/null; sleep 5
    POLL_SECS=5 K6_MODE=$mode scripts/run_test.sh $name -e DURATION=3m -e RATE=$rate | tail -1
  done
done
step t7_plain_ka_5x; scripts/restack.sh >/dev/null; sleep 5
POLL_SECS=5 K6_MODE=host scripts/run_test.sh t7_plain_ka_5x -e DURATION=3m -e RATE=26.5 | tail -1
for b in 0 1; do
  name=t7_breaker_$([ $b = 1 ] && echo on || echo off); step $name
  scripts/restack.sh BREAKER=$b >/dev/null; sleep 5
  faults '{"timeout_rate":1.0}'
  POLL_SECS=5 scripts/run_test.sh $name -e DURATION=3m | tail -1
  faults '{}'
done
echo "t7 done $(date -Is)"
