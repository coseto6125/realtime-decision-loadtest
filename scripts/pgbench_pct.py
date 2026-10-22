"""p50/p95/p99 transaction latency from pgbench per-transaction logs (-l).

Usage: uv run python scripts/pgbench_pct.py results/<name>
Log columns (pgbench docs): client_id transaction_no time_us script_no time_epoch time_epoch_us.
"""

import csv
import statistics
import sys
from pathlib import Path

lat = []
for f in sorted(Path(sys.argv[1]).glob("txn*")):
    with f.open(newline="") as fh:
        lat.extend(int(r[2]) / 1000 for r in csv.reader(fh, delimiter=" "))
lat.sort()
q = statistics.quantiles(lat, n=100)
print(f"txns={len(lat)} tps={len(lat) / 60:.0f} p50={q[49]:.2f}ms p95={q[94]:.2f}ms p99={q[98]:.2f}ms max={lat[-1]:.2f}ms")
