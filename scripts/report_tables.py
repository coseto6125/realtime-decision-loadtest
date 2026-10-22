"""Markdown tables for REPORT.md from one or more results/<test>/ folders.

Usage: uv run python scripts/report_tables.py results/t1_baseline [results/... ...]
Prints, per folder: client-side latency (k6), server-side latency, errors and counts, CPU and memory.
"""

import csv
import sys
from pathlib import Path

import msgspec

SERVER_OPS = [
    ("collector", "http POST /events 2xx", "collector /events"),
    ("decision", "http POST /decide 2xx", "decision /decide"),
    ("decision", "pg_write_ms", "decision PG write (1 statement, commit)"),
    ("decision", "ctx_load_ms", "decision Context load"),
    ("decision", "model_call_ms", "decision model call"),
    ("payments", "http POST /webhook 2xx", "payments /webhook"),
    ("payments", "pg_txn_ms", "payments PG transaction"),
    ("context", "ingest_to_context_ms", "event ingest to Context applied"),
    ("context", "apply_batch_ms", "context apply batch"),
    ("scheduler", "claim_batch_ms", "scheduler claim batch"),
]


def load(p: Path) -> dict:
    return msgspec.json.decode(p.read_bytes()) if p.exists() and p.stat().st_size else {}


def row(*cells: object) -> str:
    return "| " + " | ".join(str(c) for c in cells) + " |"


def fmt(x: object) -> str:
    return f"{x:.2f}" if isinstance(x, float) else str(x)


def cgroup(p: Path) -> dict[str, tuple[int, int]]:
    out = {}
    if p.exists():
        for line in p.read_text().splitlines():
            name, cpu, mem = line.split()
            out[name] = (int(cpu), int(mem))
    return out


def peak_mem(p: Path) -> dict[str, tuple[float, float]]:
    """container -> (first, peak) memory.current in MB from the sampler CSV."""
    first: dict[str, float] = {}
    peak: dict[str, float] = {}
    if p.exists():
        with p.open(newline="") as f:
            for r in csv.DictReader(f):
                mb = int(r["mem_bytes"]) / 1048576
                first.setdefault(r["container"], mb)
                peak[r["container"]] = max(peak.get(r["container"], 0.0), mb)
    return {c: (first[c], peak[c]) for c in peak}


def tables(d: Path) -> str:
    out = [f"### {d.name}", ""]
    k6 = load(d / "k6_summary.json")
    if lat := k6.get("client_latency_ms"):
        out += ["Client-side (k6, end to end)", "", row("endpoint", "count", "p50 ms", "p95 ms", "p99 ms", "max ms"),
                row(*["---"] * 6)]
        out += [row(k, v["count"], v["p50"], v["p95"], v["p99"], v["max"]) for k, v in lat.items() if v]
        out += ["", f"errors %: {k6.get('error_pct')}  counts: {k6.get('counts')}", ""]
    elif k6:
        out += [f"k6: {k6}", ""]
    stats = {p.stem.removeprefix("stats_"): load(p) for p in sorted(d.glob("stats_*.json"))}
    out += ["Server-side (handling time inside each service)", "",
            row("operation", "count", "p50 ms", "p95 ms", "p99 ms", "max ms"), row(*["---"] * 6)]
    for svc, op, label in SERVER_OPS:
        if h := stats.get(svc, {}).get("latency_ms", {}).get(op):
            out.append(row(label, h["count"], h["p50"], h["p95"], h["p99"], h["max"]))
    out.append("")
    acc = stats.get("collector", {}).get("counters", {}).get("accepted", 0)
    app = stats.get("context", {}).get("counters", {}).get("applied", 0)
    before, after = cgroup(d / "cgroup_before.txt"), cgroup(d / "cgroup_after.txt")
    mem = peak_mem(d / "remote" / "sampler.csv")
    out += [row("container", "process CPU s", "cgroup CPU s", "CPU s / 1k events", "mem first MB", "mem peak MB"),
            row(*["---"] * 6)]
    for c in sorted(after):
        svc = c.removeprefix("lt-")
        pcpu = stats.get(svc, {}).get("cpu_s", "")
        per = ""
        if svc == "collector" and acc and pcpu != "":
            per = f"{1000 * pcpu / acc:.4f}"
        elif svc == "context" and app and pcpu != "":
            per = f"{1000 * pcpu / app:.4f}"
        cg = (after[c][0] - before[c][0]) / 1e6 if c in before else ""
        f0, pk = mem.get(c, ("", ""))
        out.append(row(c, fmt(pcpu), fmt(cg), per, fmt(f0), fmt(pk)))
    out += ["", f"events accepted {acc}, applied {app}", ""]
    for name in ("host_before_aggregate.txt", "host_after_aggregate.txt"):
        if (p := d / name).exists():
            out.append(f"{name.removesuffix('_aggregate.txt')}: `{p.read_text().strip().removeprefix('AGGREGATE ')}`")
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    sys.stdout.write("\n".join(tables(Path(a)) for a in sys.argv[1:]))
