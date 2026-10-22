"""T6 summary: per store, per-op latency at baseline, and store CPU per 1,000 events per ramp step.

Usage: uv run python scripts/t6_summary.py results/t6
The store CPU per step comes from the cgroup sampler (2 s): the driver container (lt-bench) marks the two
runs (baseline, then ramp); ramp steps are --step-s long from the start of the second run.
"""

import csv
import sys
from pathlib import Path

import msgspec

STEP_S = 20.0
OPS = ("xadd_pipeline", "xreadgroup_hit", "apply_pipeline", "apply_per_event", "read_pipeline")


def lines(p: Path) -> list[dict]:
    out = []
    for raw in p.read_bytes().splitlines():
        try:
            out.append(msgspec.json.decode(raw))
        except msgspec.DecodeError:
            continue
    return out


def samples(p: Path) -> dict[str, list[tuple[float, int, int]]]:
    s: dict[str, list[tuple[float, int, int]]] = {}
    with p.open(newline="") as f:
        for r in csv.DictReader(f):
            s.setdefault(r["container"], []).append((float(r["ts"]), int(r["cpu_usec"]), int(r["mem_bytes"])))
    return s


def cpu_between(series: list[tuple[float, int, int]], t0: float, t1: float) -> float:
    pts = [x for x in series if t0 <= x[0] <= t1]
    return (pts[-1][1] - pts[0][1]) / 1e6 / (pts[-1][0] - pts[0][0]) if len(pts) > 1 else float("nan")


def runs(bench: list[tuple[float, int, int]]) -> list[tuple[float, float]]:
    """Driver runs: each new lt-bench container starts a fresh cgroup, so its CPU counter drops."""
    segs, start = [], bench[0][0]
    for prev, cur in zip(bench, bench[1:], strict=False):
        if cur[1] < prev[1] or cur[0] - prev[0] > 5:
            segs.append((start, prev[0]))
            start = cur[0]
    segs.append((start, bench[-1][0]))
    return segs


def main(d: Path) -> None:
    for jl in sorted(d.glob("*.jsonl")):
        store = jl.stem
        rows = lines(jl)
        steps = [r for r in rows if r.get("kind") == "step"]
        start = next((r for r in rows if r.get("kind") == "start"), {})
        print(f"### {store} {start.get('server', {})}")
        if not steps:
            print("no steps\n")
            continue
        base = steps[0]
        print("| op (baseline 150 events/s) | p50 ms | p95 ms | p99 ms |\n|---|---|---|---|")
        for op in OPS:
            if h := base["latency_ms"].get(op):
                print(f"| {op} | {h['p50']} | {h['p95']} | {h['p99']} |")
        smp = samples(p) if (p := d / f"sampler_{store}.csv").exists() else {}
        store_c = next((k for k in smp if k.startswith("lt-cache-")), None)
        segs = runs(smp["lt-bench"]) if "lt-bench" in smp else []
        print("\n| step events/s | achieved | consumed | driver CPU (cores) | store CPU (cores) | store CPU s / 1k events |"
              " xadd p99 | apply p99 | read p99 | late ticks |\n|---|---|---|---|---|---|---|---|---|---|")
        ramp = steps[1:]
        for i, s in enumerate(ramp):
            store_cpu = float("nan")
            if store_c and len(segs) > 1:
                t0 = segs[1][0] + i * STEP_S + 2
                store_cpu = cpu_between(smp[store_c], t0, t0 + STEP_S - 4)
            per_k = 1000 * store_cpu / s["achieved_eps"] if s["achieved_eps"] else float("nan")
            lat = s["latency_ms"]
            p99 = {op: lat.get(op, {}).get("p99", "") for op in ("xadd_pipeline", "apply_pipeline", "read_pipeline")}
            print(f"| {s['target_eps']:.0f} | {s['achieved_eps']:.0f} | {s['consumed_eps']:.0f} |"
                  f" {s['driver_cpu_s'] / s['duration_s']:.2f} | {store_cpu:.2f} | {per_k:.3f} |"
                  f" {p99['xadd_pipeline']} | {p99['apply_pipeline']} | {p99['read_pipeline']} | {s['late_ticks']} |")
        if store_c:
            peak = max(x[2] for x in smp[store_c]) / 1048576
            print(f"\nstore peak memory.current {peak:.1f} MB")
        summ = next((r for r in rows if r.get("kind") == "summary" and r.get("mode") == "ramp"), None)
        if summ:
            for res in summ["results"]:
                print(f"saturation_eps {res['saturation_eps']} first_fail_eps {res['first_fail_eps']}"
                      f" incompatible {res['incompatible']}")
        print()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__.splitlines()[2])
    main(Path(sys.argv[1]))
