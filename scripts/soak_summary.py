"""Soak memory and CPU trend per container from the sampler CSV.

Usage: uv run python scripts/soak_summary.py results/t2_soak/remote/sampler.csv
Memory is cgroup memory.current (includes page cache, e.g. Postgres and AOF files). Slope from a
least-squares fit over the run, in MB per hour.
"""

import csv
import statistics
import sys
from collections import defaultdict

series: defaultdict[str, list[tuple[float, int, int]]] = defaultdict(list)
with open(sys.argv[1], newline="") as f:
    for r in csv.DictReader(f):
        series[r["container"]].append((float(r["ts"]), int(r["cpu_usec"]), int(r["mem_bytes"])))
print("| container | mem at 5 min MB | mem at 30 min MB | mem at end MB | slope MB/h (5 min to end) | avg CPU cores |")
print("|---|---|---|---|---|---|")
for c in sorted(series):
    s = series[c]
    t0 = s[0][0]
    after5 = [x for x in s if x[0] - t0 >= 300]
    if len(after5) < 3:
        continue

    def mem_at(t: float, s: list = s, t0: float = t0) -> float:
        return min(s, key=lambda p: abs(p[0] - t0 - t))[2] / 1048576
    xs = [(x[0] - t0) / 3600 for x in after5]
    ys = [x[2] / 1048576 for x in after5]
    slope = statistics.linear_regression(xs, ys).slope
    cpu = (s[-1][1] - s[0][1]) / 1e6 / (s[-1][0] - s[0][0])
    print(f"| {c} | {mem_at(300):.1f} | {mem_at(1800):.1f} | {ys[-1]:.1f} | {slope:+.1f} | {cpu:.3f} |")
