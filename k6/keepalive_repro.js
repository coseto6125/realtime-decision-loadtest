// Keep-alive race repro: VUs open a connection, idle for about the server's keep-alive timeout, then POST
// again on the same connection. A POST is not retried by the client, so a close that crosses the reuse
// surfaces as an error (EOF / "server closed idle connection").
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter } from 'k6/metrics';
import exec from 'k6/execution';

const URL = __ENV.URL || 'http://lt-collector:8000/events';
const KA = +(__ENV.KA || 75);
const failed = new Counter('reuse_failed');
export const options = { scenarios: { r: { executor: 'per-vu-iterations', vus: +(__ENV.VUS || 200), iterations: 2,
  maxDuration: '6m' } } };

// EVENTS=1 sends one valid event per POST, so the collector also has to reach Valkey.
const body = () => (__ENV.EVENTS === '1' ? JSON.stringify([{ event_id: `ka-${exec.vu.idInTest}-${Date.now()}-${Math.random()}`,
  user_id: `u${exec.vu.idInTest}`, session_id: 's', type: 'chat', client_ts: Date.now(), props: {} }]) : '[]');

export default function () {
  const p = { headers: { 'Content-Type': 'application/json' }, timeout: '10s' };
  http.post(URL, body(), p);
  sleep(KA - 0.3 + Math.random() * 0.6);      // idle across the server's keep-alive deadline
  const r = http.post(URL, body(), p);
  if (r.status !== 200) { failed.add(1); console.warn(`status=${r.status} error=${r.error} code=${r.error_code}`); }
}
