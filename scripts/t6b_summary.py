"""T6b summary per design: baseline and ramp steps, break point, table bloat, HOT ratio, WAL.

Usage: uv run python scripts/t6b_summary.py results/t6b
"""

import sys
from pathlib import Path

import msgspec

for jl in sorted(Path(sys.argv[1]).glob("*.jsonl")):
    rows = []
    for raw in jl.read_bytes().splitlines():
        try:
            rows.append(msgspec.json.decode(raw))
        except msgspec.DecodeError:
            continue
    print(f"### {jl.stem}")
    print("| mode | events/s | achieved | consumed A / B | ingest p50/p95/p99 ms | read p50/p95/p99 ms | apply p95 ms |"
          " lag A / B s | WAL KB/s | autovacuum | HOT % (state) | driver cores |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|")
    mode = ""
    for r in rows:
        if r.get("kind") == "start":
            mode = r["mode"]
        if r.get("kind") != "step":
            continue
        lat = r["latency_ms"]

        def q(op: str, lat: dict = lat) -> str:
            h = lat.get(op, {})
            return f"{h.get('p50', '')}/{h.get('p95', '')}/{h.get('p99', '')}"
        tabs = r.get("tables", {})
        av = sum(t.get("autovacuum_count", 0) for t in tabs.values())
        hot = ""
        for name, t in tabs.items():
            if t.get("n_tup_upd"):
                hot += f"{name} {100 * t.get('n_tup_hot_upd', 0) / t['n_tup_upd']:.0f}% "
        lag = r.get("lag", {})
        print(f"| {mode} | {r['target_eps']:.0f} | {r['achieved_eps']:.0f} | {r['consumed_eps'].get('A', 0):.0f} /"
              f" {r['consumed_eps'].get('B', 0):.0f} | {q('ingest')} | {q('read')} | {lat.get('apply', {}).get('p95', '')} |"
              f" {lag.get('A', {}).get('s', '')} / {lag.get('B', {}).get('s', '')} | {r['wal_bytes_per_s'] / 1024:.0f} |"
              f" {av} | {hot.strip()} | {r['driver_cpu_s'] / r['duration_s']:.2f} |")
    for r in rows:
        if r.get("kind") == "summary":
            print(f"\n{r['mode']}: break_eps {r['break_eps']} first_fail_eps {r['first_fail_eps']}"
                  f" truncated {r.get('truncated')} audit consistent {r['audit'].get('consistent')}"
                  f" id/txid inversions {r['audit'].get('id_txid_inversions')}")
    last = next((r for r in reversed(rows) if r.get("kind") == "step"), None)
    if last:
        print("\nTables at the end of the ramp:")
        for name, t in last.get("tables", {}).items():
            print(f"- {name}: total {t.get('total_bytes', 0) / 1048576:.1f} MB, index {t.get('index_bytes', 0) / 1048576:.1f} MB,"
                  f" live {t.get('n_live_tup')}, dead {t.get('n_dead_tup')}")
    print()
