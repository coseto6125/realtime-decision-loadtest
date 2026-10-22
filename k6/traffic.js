// Synthetic users of a live-streaming app. Each iteration is one App session that follows a realistic sequence
// (including the recharge-abandonment sequence) on a compressed clock.
// The App batches events (flush at 10, before an abandonment decision, and at session end) and calls
// /decide synchronously at login, watch_30min, leave_room and cancel_recharge.
import http from 'k6/http';
import crypto from 'k6/crypto';
import exec from 'k6/execution';
import { sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const E = __ENV;
const COLLECTOR = E.COLLECTOR || 'http://127.0.0.1:17101';
const DECISION = E.DECISION || 'http://127.0.0.1:17102';
const PAYMENTS = E.PAYMENTS || 'http://127.0.0.1:17103';
const RUN = E.RUN || `r${Date.now()}`;
const POOL = +(E.POOL || 50000);
const RATE = +(E.RATE || 5.3);              // sessions per second at baseline
const DURATION = E.DURATION || '10m';
const THINK = +(E.THINK || 0.5);            // mean seconds between App actions
const DAY_S = +(E.DAY_S || 30);
const DUP = E.DUP === '1';                  // send every event batch and decision twice, concurrently
const WEBHOOK_DUP = E.WEBHOOK_DUP === '1';  // deliver every payment webhook twice, concurrently
const BAD_SIG = +(E.BAD_SIG || 0);          // share of payments that also get a forged-signature webhook
const FORGE = +(E.FORGE || 0);              // share of sessions that forge a client recharge after an offer
const SECRET = E.WEBHOOK_SECRET || 'lt-test-secret';   // test-only default; pass -e WEBHOOK_SECRET=...
const PROFILE = E.PROFILE || 'constant';
const RETRIES = +(E.RETRIES || 6);          // App keeps unsent events and retries with backoff (max ~12 s)
const SUMMARY = E.SUMMARY || 'k6_summary.json';
const JSON_H = { 'Content-Type': 'application/json' };

const perTen = (sessionsPerSec) => Math.max(1, Math.round(sessionsPerSec * 10));

function scenarios() {
  const base = { exec: 'session', timeUnit: '10s', gracefulStop: `${DAY_S + 60}s` };
  switch (PROFILE) {
    case 'stress': {
      const peak = +(E.PEAK_X || 20);
      const ramp = E.RAMP || '270s';
      return { stress: { ...base, executor: 'ramping-arrival-rate', startRate: perTen(RATE),
        preAllocatedVUs: 300, maxVUs: +(E.MAX_VUS || 3000),
        stages: [{ target: perTen(RATE), duration: '30s' }, { target: perTen(RATE * peak), duration: ramp }] } };
    }
    case 'spike': {
      const x = +(E.SPIKE_X || 10);
      return { spike: { ...base, executor: 'ramping-arrival-rate', startRate: perTen(RATE),
        preAllocatedVUs: 300, maxVUs: +(E.MAX_VUS || 3000),
        stages: [{ target: perTen(RATE), duration: '60s' }, { target: perTen(RATE * x), duration: '5s' },
          { target: perTen(RATE * x), duration: '60s' }, { target: perTen(RATE), duration: '5s' },
          { target: perTen(RATE), duration: '110s' }] } };
    }
    default:
      return { baseline: { ...base, executor: 'constant-arrival-rate', rate: perTen(RATE), duration: DURATION,
        preAllocatedVUs: 200, maxVUs: +(E.MAX_VUS || 1000) } };
  }
}

const abort = PROFILE === 'stress' ? [{ threshold: 'p(95)<500', abortOnFail: true, delayAbortEval: '30s' }] : ['p(95)<500'];
export const options = {
  scenarios: scenarios(),
  summaryTrendStats: ['count', 'avg', 'med', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    t_events: abort, t_decide: abort,
    events_quarantined: ['count==0'],       // the generator sends only valid events: any quarantine is a collector regression
    err_any: PROFILE === 'stress' ? [{ threshold: 'rate<0.01', abortOnFail: true, delayAbortEval: '30s' }] : ['rate<0.01'],
  },
  discardResponseBodies: false,
  noConnectionReuse: false,
};

const tEvents = new Trend('t_events', true);
const tDecide = new Trend('t_decide', true);
const tWebhook = new Trend('t_webhook', true);
const errEvents = new Rate('err_events');
const errDecide = new Rate('err_decide');
const errWebhook = new Rate('err_webhook');
const errAny = new Rate('err_any');
const eventsSent = new Counter('events_sent');
const eventsLost = new Counter('events_lost');
const eventsQuarantined = new Counter('events_quarantined');
const eventsRetried = new Counter('events_retried');
const decisions = new Counter('decisions');
const webhooks = new Counter('webhooks');
const badSigSent = new Counter('bad_sig_sent');
const badSigRejected = new Counter('bad_sig_rejected');
const webhookDuplicates = new Counter('webhook_duplicate_replies');
const forgedRecharges = new Counter('forged_recharges');

const rnd = (lo, hi) => lo + Math.floor(Math.random() * (hi - lo + 1));
const think = () => sleep(THINK * (0.5 + Math.random()));

function record(res, trend, err) {
  const bad = res.status !== 200;
  trend.add(res.timings.duration);
  err.add(bad);
  errAny.add(bad);
  return !bad;
}

function send(url, body, name, trend, err) {
  const params = { headers: JSON_H, tags: { name }, timeout: '10s' };
  if (!DUP) {
    const r = http.post(url, body, params);
    record(r, trend, err);
    return [r];
  }
  const rs = http.batch([['POST', url, body, params], ['POST', url, body, params]]);
  rs.forEach((r) => record(r, trend, err));
  return rs;
}

// The collector answers 200 {"accepted": n, "quarantined": m}; anything else is not an acknowledgement.
function ackOf(res) {
  if (res.status !== 200) return null;
  try {
    const j = res.json();
    return Number.isInteger(j.accepted) && Number.isInteger(j.quarantined) ? j : null;
  } catch (e) {
    return null;
  }
}

class App {
  constructor(uid, platform) {
    this.uid = uid;
    this.platform = platform;
    this.sid = `${RUN}-s${exec.vu.idInTest}-${exec.vu.iterationInScenario}-${rnd(0, 1e6)}`;
    this.buf = [];
    this.seq = 0;
    this.recent = [];
  }

  ev(type, props = {}) {
    const id = `${this.sid}-${this.seq++}`;
    this.buf.push({ event_id: id, user_id: this.uid, session_id: this.sid, type, client_ts: Date.now(), props });
    this.recent.unshift(type);
    this.recent.length = Math.min(this.recent.length, 10);
    if (this.buf.length >= 10) this.flush();
    return id;
  }

  flush() {
    if (!this.buf.length) return;
    const body = JSON.stringify(this.buf);
    const n = this.buf.length;
    this.buf = [];
    for (let attempt = 0; attempt < RETRIES; attempt++) {
      if (attempt) eventsRetried.add(n);
      const rs = send(`${COLLECTOR}/events`, body, 'events', tEvents, errEvents);
      const ack = rs.map(ackOf).find((a) => a);
      if (ack) {
        // HTTP 200 alone proves nothing: count only what the collector says it accepted
        eventsSent.add(ack.accepted);
        if (ack.quarantined) { eventsQuarantined.add(ack.quarantined); errEvents.add(true); errAny.add(true); }
        if (n > ack.accepted) eventsLost.add(n - ack.accepted);
        return;
      }
      if (rs.every((r) => r.status >= 400 && r.status < 500)) break;
      sleep(Math.min(0.25 * 2 ** attempt, 4));
    }
    eventsLost.add(n);
  }

  decide(trigger, eventId) {
    const body = JSON.stringify({ event_id: eventId, user_id: this.uid, trigger, ts: Date.now(),
      session_id: this.sid, platform: this.platform, recent_events: this.recent });
    send(`${DECISION}/decide`, body, 'decide', tDecide, errDecide);
    decisions.add(1);
  }
}

function pay(uid) {
  const txn = `${RUN}-p${exec.vu.idInTest}-${exec.vu.iterationInScenario}-${rnd(0, 1e6)}`;
  const body = JSON.stringify({ provider: 'tappay', txn_id: txn, user_id: uid, amount: 300, currency: 'TWD',
    status: 'paid', paid_at: Date.now() });
  const params = { headers: { ...JSON_H, 'X-Signature': crypto.hmac('sha256', SECRET, body, 'hex') },
    tags: { name: 'webhook' }, timeout: '10s' };
  const rs = WEBHOOK_DUP ? http.batch([['POST', `${PAYMENTS}/webhook`, body, params], ['POST', `${PAYMENTS}/webhook`, body, params]])
    : [http.post(`${PAYMENTS}/webhook`, body, params)];
  rs.forEach((r) => {
    record(r, tWebhook, errWebhook);
    if (r.status === 200 && r.body && r.body.indexOf('"duplicate"') >= 0) webhookDuplicates.add(1);
  });
  webhooks.add(1);
  if (BAD_SIG && Math.random() < BAD_SIG) {
    const forged = JSON.stringify({ provider: 'tappay', txn_id: `bad-${txn}`, user_id: uid, amount: 300000,
      currency: 'TWD', status: 'paid', paid_at: Date.now() });
    const r = http.post(`${PAYMENTS}/webhook`, forged, { headers: { ...JSON_H, 'X-Signature': params.headers['X-Signature'] },
      tags: { name: 'webhook_bad' }, timeout: '10s' });
    badSigSent.add(1);
    if (r.status === 401) badSigRejected.add(1);
  }
}

function nextDay(uid, platform, payReally) {
  sleep(DAY_S + 5 + Math.random() * 20);
  const app = new App(uid, platform);
  app.decide('login', app.ev('login', { platform }));
  think();
  if (payReally) pay(uid);
  else forgedRecharges.add(1);
  app.ev('recharge', { amount: 300, source: 'client' });
  app.flush();
}

export function session() {
  const forge = FORGE > 0 && Math.random() < FORGE;
  const uid = forge ? `f${rnd(1, 5000)}` : `u${rnd(1, POOL)}`;
  const platform = Math.random() < 0.3 ? 'ios' : 'android';
  const app = new App(uid, platform);
  app.decide('login', app.ev('login', { platform }));
  think();
  app.ev('enter_room', { room: `room_${rnd(1, 300)}` });
  let lastWatch = '';
  for (let i = 1; i <= 6; i++) {
    think();
    lastWatch = app.ev('watch_progress', { minutes: 5 });
    for (let c = 0; c < 2; c++) { think(); app.ev('chat'); }
    if (i === 2) app.ev('send_gift', { amount: 10 });
    if (i === 3 && Math.random() < 0.3) app.ev('following');
    if (i === 4) app.ev('click_banner', { banner: 'b1' });
    if (i === 5) app.ev('watch_ad');
  }
  app.decide('watch_30min', lastWatch);
  think();
  app.decide('leave_room', app.ev('leave_room', { foreground: true }));
  think();
  app.ev('enter_room', { room: `room_${rnd(1, 300)}` });
  think();
  app.ev('watch_progress', { minutes: 5 });
  app.ev('chat');
  if (forge || Math.random() < 0.5) {
    think();
    app.ev('click_recharge', { plan: 300 });
    const r = Math.random();
    if (forge || r < 0.35) {
      think();
      const cancel = app.ev('cancel_recharge');
      app.flush();
      app.decide('abandonment', cancel);
      if (forge || Math.random() < 0.3) {
        app.ev('leave_room');
        app.flush();
        nextDay(uid, platform, !forge);
        return;
      }
    } else if (r < 0.55) {
      app.flush();        // App closed while paying: only the state-timeout trigger can catch this one
      return;
    } else {
      think();
      pay(uid);
      app.ev('recharge', { amount: 300, source: 'client' });
    }
  }
  think();
  app.ev('leave_room');
  app.flush();
}

function trendRow(d, name) {
  const m = d.metrics[name];
  if (!m) return null;
  const v = m.values;
  return { count: v.count, avg: +v.avg.toFixed(2), p50: +v.med.toFixed(2), p95: +v['p(95)'].toFixed(2),
    p99: +v['p(99)'].toFixed(2), max: +v.max.toFixed(2) };
}

export function handleSummary(d) {
  const sec = d.state.testRunDurationMs / 1000;
  const c = (n) => (d.metrics[n] ? d.metrics[n].values.count : 0);
  const r = (n) => (d.metrics[n] ? +(d.metrics[n].values.rate * 100).toFixed(3) : 0);
  const out = {
    run: RUN, profile: PROFILE, duration_s: +sec.toFixed(1),
    client_latency_ms: { events: trendRow(d, 't_events'), decide: trendRow(d, 't_decide'), webhook: trendRow(d, 't_webhook') },
    error_pct: { events: r('err_events'), decide: r('err_decide'), webhook: r('err_webhook'), any: r('err_any') },
    counts: { iterations: c('iterations'), dropped_iterations: c('dropped_iterations'), events_sent: c('events_sent'),
      events_lost: c('events_lost'), events_quarantined: c('events_quarantined'), events_retried: c('events_retried'), decisions: c('decisions'), webhooks: c('webhooks'),
      bad_sig_sent: c('bad_sig_sent'), bad_sig_rejected: c('bad_sig_rejected'),
      webhook_duplicate_replies: c('webhook_duplicate_replies'), forged_recharges: c('forged_recharges'),
      http_reqs: c('http_reqs') },
    rates_per_s: { events: +(c('events_sent') / sec).toFixed(1), decisions: +(c('decisions') / sec).toFixed(1),
      http_reqs: +(c('http_reqs') / sec).toFixed(1) },
    thresholds_failed: Object.entries(d.metrics).filter(([, m]) => m.thresholds && Object.values(m.thresholds).some((t) => !t.ok)).map(([k]) => k),
  };
  return { stdout: JSON.stringify(out, null, 1) + '\n', [SUMMARY]: JSON.stringify(out, null, 1) };
}
