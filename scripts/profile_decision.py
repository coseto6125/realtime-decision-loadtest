"""Where lt-decision spends CPU: py-spy --native --idle collapsed stacks, attributed by component.

Usage: uv run python scripts/profile_decision.py results/<test>/remote/lt-decision.collapsed
Each busy sample goes to the innermost frame that belongs to a known component (rnet, msgspec JSON,
psqlpy, redis-py, policy logic, Sanic/httptools, event loop); idle epoll waits are excluded.
"""

import sys
from collections import Counter

sys.path.insert(0, "scripts")
from analyze import classify  # noqa: E402

RULES = [
    ("rnet HTTP client", ("rnet",)),
    ("msgspec JSON/msgpack", ("msgspec",)),
    ("Postgres (psqlpy)", ("psqlpy",)),
    ("Valkey (redis-py / glide)", ("redis/", "hiredis", "glide", "cffi")),
    ("policy logic (lt/logic.py)", ("lt/logic.py",)),
    ("T7 instrumentation (/proc/net/tcp gauge)", ("tcp_established",)),
    ("event loop + socket syscalls", ("serve (lt/common.py", "loop.c", "asyncio/", "core.c", "linux.c",
                                      "_asyncio.cpython")),
    ("service code (other lt/*.py)", ("lt/",)),
    ("Sanic + httptools (request/response)", ("sanic/http", "sanic/request", "sanic/response", "httptools",
                                              "http1_request_header", "handle_request")),
]


def component(frames: list[str]) -> str:
    for f in reversed(frames):
        for name, keys in RULES:
            if any(k in f for k in keys):
                return name
    return "other (libc, CPython, Sanic server loop)"


counts: Counter[str] = Counter()
kinds: Counter[str] = Counter()
with open(sys.argv[1]) as fh:
    lines = fh.read().splitlines()
for line in lines:
    stack, n = line.rsplit(" ", 1)
    frames = stack.split(";")
    k = classify(frames)
    kinds[k] += int(n)
    if k != "idle":
        counts[component(frames)] += int(n)
busy = sum(counts.values())
print(f"samples: {dict(kinds)}; busy {busy}; Python share {kinds['python'] / busy:.1%}")
print("| component | samples | share of busy |\n|---|---|---|")
for name, n in counts.most_common():
    print(f"| {name} | {n} | {n / busy:.1%} |")
