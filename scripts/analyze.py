"""Summarise one results/<test>/ folder: client and server latency, CPU per 1,000 events, profiles.

Usage: uv run python scripts/analyze.py results/<test> [--json]

Profile classification (py-spy --native --idle, raw collapsed stacks):
- idle: the innermost Python frame is the event loop's run_forever (epoll wait), no work in progress;
- python: the leaf is a Python frame or a CPython C-API function (Py*/_Py*): interpreter time;
- native: anything else (C/Rust extensions, libuv/uvloop, libc syscalls such as send/recv, malloc).
"""

import csv
import re
import sys
from collections import Counter
from pathlib import Path

import msgspec

LEAF_FILE = re.compile(r"\(([^()]*)\)$")
CPYTHON_FN = re.compile(r"^_?Py[A-Za-z_]")
IDLE_PARENTS = ("_run_server_forever (sanic", "run_forever (asyncio/base_events.py", "_run_once (asyncio")


def classify(frames: list[str]) -> str:
    leaf = frames[-1]
    py = [f for f in frames if ".py:" in f or "<frozen" in f]
    if py and py[-1].startswith(IDLE_PARENTS) and ".py:" not in leaf and "libc" in leaf:
        return "idle"
    if ".py:" in leaf or "<frozen" in leaf or CPYTHON_FN.match(leaf):
        return "python"
    return "native"


def profile(path: Path) -> dict:
    kinds: Counter[str] = Counter()
    leaves: Counter[str] = Counter()
    libs: Counter[str] = Counter()
    for line in path.read_text().splitlines():
        stack, n = line.rsplit(" ", 1)
        frames = stack.split(";")
        k = classify(frames)
        kinds[k] += int(n)
        if k == "idle":
            continue
        leaf = frames[-1]
        m = LEAF_FILE.search(leaf)
        where = m.group(1).split(":")[0] if m else leaf
        leaves[re.sub(r":\d+\)$", ")", leaf)] += int(n)
        libs[f"{k}:{where}"] += int(n)
    busy = kinds["python"] + kinds["native"]
    return {"samples": dict(kinds), "busy": busy,
            "python_share": round(kinds["python"] / busy, 3) if busy else None,
            "top_self": leaves.most_common(5), "top_files": libs.most_common(8)}


def load(path: Path) -> dict:
    return msgspec.json.decode(path.read_bytes()) if path.exists() else {}


def cgroup(path: Path) -> dict[str, tuple[int, int]]:
    out = {}
    if path.exists():
        for line in path.read_text().splitlines():
            name, cpu, mem = line.split()
            out[name] = (int(cpu), int(mem))
    return out


def sampler_peak_mem(path: Path) -> dict[str, float]:
    peak: dict[str, float] = {}
    if path.exists():
        with path.open(newline="") as f:
            for row in csv.DictReader(f):
                mb = int(row["mem_bytes"]) / 1048576
                peak[row["container"]] = max(peak.get(row["container"], 0.0), mb)
    return peak


def main() -> None:
    d = Path(sys.argv[1])
    k6 = load(d / "k6_summary.json")
    out: dict = {"test": d.name, "k6": k6}
    stats = {p.stem.removeprefix("stats_"): load(p) for p in sorted(d.glob("stats_*.json"))}
    out["server"] = {s: v.get("latency_ms", {}) for s, v in stats.items()}
    out["counters"] = {s: v.get("counters", {}) for s, v in stats.items()}
    accepted = stats.get("collector", {}).get("counters", {}).get("accepted", 0)
    applied = stats.get("context", {}).get("counters", {}).get("applied", 0)
    out["cpu_s_process"] = {s: v.get("cpu_s") for s, v in stats.items()}
    out["rss_mb"] = {s: v.get("rss_mb") for s, v in stats.items()}
    if accepted:
        out["cpu_s_per_1000_events"] = {
            "collector": round(1000 * stats["collector"]["cpu_s"] / accepted, 4),
            "context": round(1000 * stats["context"]["cpu_s"] / max(applied, 1), 4)}
    before, after = cgroup(d / "cgroup_before.txt"), cgroup(d / "cgroup_after.txt")
    out["cgroup_cpu_s"] = {c: round((after[c][0] - before[c][0]) / 1e6, 2) for c in after if c in before}
    out["peak_mem_mb"] = {k: round(v, 1) for k, v in sampler_peak_mem(d / "remote" / "sampler.csv").items()}
    out["profiles"] = {p.stem: profile(p) for p in sorted((d / "remote").glob("*.collapsed"))}
    sys.stdout.buffer.write(msgspec.json.format(msgspec.json.encode(out), indent=1) + b"\n")


if __name__ == "__main__":
    main()
