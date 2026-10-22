"""CPU use per container over time from a sampler CSV (cgroup usage_usec), in cores.

Usage: uv run python scripts/cpu_rates.py results/<test>/remote/sampler.csv [--every 30]
"""

import argparse
import csv
from collections import defaultdict

ap = argparse.ArgumentParser()
ap.add_argument("path")
ap.add_argument("--every", type=float, default=30)
a = ap.parse_args()
series: defaultdict[str, list[tuple[float, int, int]]] = defaultdict(list)
with open(a.path, newline="") as f:
    for r in csv.DictReader(f):
        series[r["container"]].append((float(r["ts"]), int(r["cpu_usec"]), int(r["mem_bytes"])))
t0 = min(s[0][0] for s in series.values())
names = sorted(series)
print("| t_s | " + " | ".join(n.removeprefix("lt-") for n in names) + " |")
print("|---" * (len(names) + 1) + "|")
rows: defaultdict[int, dict[str, str]] = defaultdict(dict)
for n in names:
    s = series[n]
    last = s[0]
    for p in s[1:]:
        if p[0] - last[0] >= a.every:
            rows[int((p[0] - t0) // a.every * a.every)][n] = f"{(p[1] - last[1]) / 1e6 / (p[0] - last[0]):.2f}"
            last = p
for t in sorted(rows):
    print(f"| {t} | " + " | ".join(rows[t].get(n, "") for n in names) + " |")
