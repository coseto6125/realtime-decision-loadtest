"""Per-window client latency from a k6 CSV output (--out csv=...csv.gz), for knee and recovery analysis.

Usage: uv run python scripts/windows.py results/<test>/remote/k6_points.csv.gz [--window 10]
Prints one row per window and endpoint tag: requests/s, p50, p95, p99 (ms) and error %.
"""

import argparse
import csv
import gzip
import statistics
from collections import defaultdict


def rows(f):
    """CSV rows; a k6 killed by the red-line guard leaves a truncated gzip, so stop at the cut."""
    reader = csv.DictReader(f)
    while True:
        try:
            yield next(reader)
        except (StopIteration, EOFError, ValueError):
            return


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--window", type=int, default=10)
    a = ap.parse_args()
    lat: defaultdict[tuple[int, str], list[float]] = defaultdict(list)
    fail: defaultdict[tuple[int, str], int] = defaultdict(int)
    t0 = None
    with gzip.open(a.path, "rt", newline="") as f:
        for r in rows(f):
            if r["metric_name"] not in ("http_req_duration", "http_req_failed"):
                continue
            ts = int(float(r["timestamp"]))
            t0 = ts if t0 is None else min(t0, ts)
            key = ((ts - t0) // a.window, r["name"])
            if r["metric_name"] == "http_req_duration":
                lat[key].append(float(r["metric_value"]))
            elif float(r["metric_value"]) > 0:
                fail[key] += 1
    print("| t_s | endpoint | req/s | p50 ms | p95 ms | p99 ms | err % |")
    print("|---|---|---|---|---|---|---|")
    for (w, name), xs in sorted(lat.items()):
        q = statistics.quantiles(xs, n=100) if len(xs) > 1 else [xs[0]] * 99
        print(f"| {w * a.window} | {name} | {len(xs) / a.window:.1f} | {q[49]:.1f} | {q[94]:.1f} | {q[98]:.1f} |"
              f" {100 * fail[(w, name)] / len(xs):.2f} |")


if __name__ == "__main__":
    main()
