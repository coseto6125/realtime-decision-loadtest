"""k6/traffic.js: an event flush counts events as sent only when the collector's JSON says it accepted them.

The real script runs in Node's `vm` with k6 modules stubbed (import and export lines stripped).
"""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

TRAFFIC = Path(os.environ.get("LT_TRAFFIC_JS") or Path(__file__).resolve().parents[1] / "k6" / "traffic.js")  # override to run against a backup

pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")

HARNESS = r"""
const vm = require('vm'), fs = require('fs');
const cfg = JSON.parse(process.env.CFG);
const metrics = {};
class Metric { constructor(name) { this.name = name; metrics[name] = metrics[name] || []; }
  add(v) { metrics[this.name].push(v); } }
const posts = [];
const reply = (r) => ({ status: r.status, body: r.body, timings: { duration: 1 },
  json() { return JSON.parse(r.body); } });
const queue = cfg.responses.slice();
const next = () => reply(queue.length > 1 ? queue.shift() : queue[0]);
const stubs = {
  http: { post: (...a) => { posts.push(a); return next(); }, batch: (reqs) => reqs.map(() => next()) },
  crypto: { hmac: () => 'sig' },
  exec: { vu: { idInTest: 1, iterationInScenario: 0 } },
  sleep: () => {},
  Counter: Metric, Rate: Metric, Trend: Metric,
};
let src = fs.readFileSync(cfg.path, 'utf8')
  .replace(/^import .*$/gm, '').replace(/^export /gm, '');
src += '\n;globalThis.__App = App; globalThis.__options = options;';
const ctx = vm.createContext({ ...stubs, __ENV: cfg.env, console, Math, JSON, Date, Object, Number });
vm.runInContext(src, ctx);
const app = new ctx.__App('u1', 'ios');
for (let i = 0; i < cfg.events; i++) app.buf.push({ event_id: `e${i}` });
app.flush();
const sum = (n) => (metrics[n] || []).reduce((a, b) => a + b, 0);
console.log(JSON.stringify({
  sent: sum('events_sent'), lost: sum('events_lost'), quarantined: sum('events_quarantined'),
  retried: sum('events_retried'), posts: posts.length,
  err_events_true: (metrics.err_events || []).filter(Boolean).length,
  err_any_true: (metrics.err_any || []).filter(Boolean).length,
  thresholds: Object.keys(ctx.__options.thresholds),
  quarantine_threshold: ctx.__options.thresholds.events_quarantined || null,
}));
"""


def flush(events, responses, **env):
    cfg = {"path": str(TRAFFIC), "events": events, "responses": responses, "env": {"RETRIES": "3", **env}}
    r = subprocess.run(
        ["node", "-e", HARNESS], capture_output=True, text=True, timeout=30, env={"CFG": json.dumps(cfg), "PATH": "/usr/bin:/bin:/usr/local/bin"}
    )
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


def ok(accepted, quarantined):
    return {"status": 200, "body": json.dumps({"accepted": accepted, "quarantined": quarantined})}


def test_flush_all_quarantined_200_counts_zero_sent_and_one_quarantined():
    out = flush(1, [ok(0, 1)])
    assert out["sent"] == 0
    assert out["quarantined"] == 1
    assert out["lost"] == 1
    assert out["err_events_true"] >= 1 and out["err_any_true"] >= 1
    assert out["posts"] == 1  # a deterministic reject is not retried


def test_flush_partial_quarantine_splits_sent_and_quarantined():
    out = flush(3, [ok(2, 1)])
    assert (out["sent"], out["quarantined"], out["lost"]) == (2, 1, 1)


def test_flush_all_accepted_counts_every_event_without_errors():
    out = flush(5, [ok(5, 0)])
    assert (out["sent"], out["quarantined"], out["lost"]) == (5, 0, 0)
    assert out["err_events_true"] == 0


@pytest.mark.parametrize("body", ["", "   ", "not json", "{}", '{"accepted":"1","quarantined":0}', "[]"])
def test_flush_200_without_valid_ack_counts_nothing_sent_and_retries(body):
    out = flush(2, [{"status": 200, "body": body}])
    assert out["sent"] == 0
    assert out["lost"] == 2
    assert out["posts"] == 3  # RETRIES=3: an unreadable 200 is not an acknowledgement


def test_flush_server_error_then_ack_counts_sent_once():
    out = flush(2, [{"status": 503, "body": ""}, ok(2, 0)])
    assert (out["sent"], out["lost"], out["retried"]) == (2, 0, 2)


def test_flush_duplicated_send_counts_the_batch_once():
    out = flush(4, [ok(4, 0)], DUP="1")
    assert out["sent"] == 4


def test_options_unexpected_quarantine_has_a_zero_threshold():
    out = flush(1, [ok(1, 0)])
    assert "events_quarantined" in out["thresholds"]
    assert out["quarantine_threshold"] == ["count==0"]
