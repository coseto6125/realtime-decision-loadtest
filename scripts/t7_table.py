"""T7 rows: decision CPU per 1,000 decisions and per 1,000 model calls, model-call latency, connections.

Usage: uv run python scripts/t7_table.py results/t7_* results/t1_baseline
"""

import sys
from pathlib import Path

import msgspec

print("| run | decisions | model calls | decision CPU s | CPU s / 1k decisions | CPU s / 1k model calls |"
      " model call p50 / p95 / p99 ms | /decide p95 / p99 ms | conns open max | new conns total | breaker opened |")
print("|---|---|---|---|---|---|---|---|---|---|---|")
for a in sys.argv[1:]:
    d = Path(a)
    p = d / "stats_decision.json"
    if not p.exists():
        continue
    s = msgspec.json.decode(p.read_bytes())
    c, g, lat = s["counters"], s["gauges"], s["latency_ms"]
    dec = lat.get("http POST /decide 2xx", {}).get("count", 0)
    calls = sum(v for k, v in c.items() if k in ("model.ok", "model.timeout", "model.error"))
    mc = lat.get("model_call_ms", {})
    de = lat.get("http POST /decide 2xx", {})
    cpu = s["cpu_s"]
    print(f"| {d.name} | {dec} | {calls} | {cpu:.2f} | {1000 * cpu / dec if dec else 0:.3f} |"
          f" {1000 * cpu / calls if calls else 0:.3f} | {mc.get('p50')} / {mc.get('p95')} / {mc.get('p99')} |"
          f" {de.get('p95')} / {de.get('p99')} | {g.get('model_conns_open_max_run', '')} |"
          f" {c.get('model_conns_new_total', '')} | {c.get('breaker.opened', 0)} |")
