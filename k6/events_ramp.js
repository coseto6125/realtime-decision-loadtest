// Ingest-only ramp for the Collector: batches of 10 valid events at a rising arrival rate.
// Used to find the Collector's own ceiling on its 0.5 vCPU cap (Python vs Rust, same script).
// The run aborts once p95 > 500 ms or errors > 1% (30 s grace), and the per-request CSV lets the
// knee be read per 10-second window.
import http from 'k6/http';
import exec from 'k6/execution';
import { Rate, Trend } from 'k6/metrics';

const E = __ENV;
const COLLECTOR = E.COLLECTOR || 'http://lt-collector:8000';
const START = +(E.START_RPS || 15);          // requests/s (x10 events)
const PEAK = +(E.PEAK_RPS || 1500);
const RAMP = E.RAMP || '240s';
const SUMMARY = E.SUMMARY || 'k6_summary.json';
const TYPES = ['watch_progress', 'chat', 'chat', 'send_gift', 'enter_room', 'click_banner'];

export const options = {
  scenarios: { ramp: { executor: 'ramping-arrival-rate', startRate: START, timeUnit: '1s',
    preAllocatedVUs: 200, maxVUs: +(E.MAX_VUS || 2000),
    stages: [{ target: START, duration: '20s' }, { target: PEAK, duration: RAMP }] } },
  summaryTrendStats: ['count', 'avg', 'med', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    t_events: [{ threshold: 'p(95)<500', abortOnFail: true, delayAbortEval: '30s' }],
    err_events: [{ threshold: 'rate<0.01', abortOnFail: true, delayAbortEval: '30s' }],
  },
};

const tEvents = new Trend('t_events', true);
const errEvents = new Rate('err_events');

export default function () {
  const id = `${E.RUN || 'ramp'}-${exec.vu.idInTest}-${exec.vu.iterationInScenario}`;
  const now = Date.now();
  const batch = [];
  for (let i = 0; i < 10; i++) {
    batch.push({ event_id: `${id}-${i}`, user_id: `u${(exec.vu.iterationInScenario * 7 + i) % 50000}`,
      session_id: id, type: TYPES[i % TYPES.length], client_ts: now, props: { room: 'room_1', minutes: 5 } });
  }
  const r = http.post(`${COLLECTOR}/events`, JSON.stringify(batch),
    { headers: { 'Content-Type': 'application/json' }, timeout: '10s', tags: { name: 'events' } });
  tEvents.add(r.timings.duration);
  errEvents.add(r.status !== 200);
}

export function handleSummary(d) {
  const t = d.metrics.t_events ? d.metrics.t_events.values : {};
  const out = { duration_s: d.state.testRunDurationMs / 1000, requests: t.count, events: (t.count || 0) * 10,
    p50: t.med, p95: t['p(95)'], p99: t['p(99)'], max: t.max,
    error_pct: d.metrics.err_events ? d.metrics.err_events.values.rate * 100 : 0,
    dropped_iterations: d.metrics.dropped_iterations ? d.metrics.dropped_iterations.values.count : 0,
    aborted_by_threshold: Object.entries(d.metrics).filter(([, m]) => m.thresholds && Object.values(m.thresholds).some((x) => !x.ok)).map(([k]) => k) };
  return { stdout: JSON.stringify(out, null, 1) + '\n', [SUMMARY]: JSON.stringify(out, null, 1) };
}
