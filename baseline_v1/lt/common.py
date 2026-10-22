"""Shared runtime: settings, latency histograms, /metrics and /stats, client factories."""

import array
import math
import os
import resource
import time
from collections import defaultdict
from time import perf_counter

import msgspec
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from sanic import HTTPResponse, Request, Sanic, raw

STREAM = "events"
QUARANTINE = "events:quarantine"
GROUP = "ctx"
JSON = "application/json"


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def envf(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class Hist:
    """Log-bucketed latency histogram: 4% relative error, O(1) observe, fixed memory."""

    __slots__ = ("buckets", "count", "sum", "max")
    LO = 0.01            # ms
    R = math.log(1.04)
    N = 420              # covers 0.01 ms .. ~140 s

    def __init__(self) -> None:
        self.buckets = array.array("Q", bytes(8 * self.N))
        self.count = 0
        self.sum = 0.0
        self.max = 0.0

    def observe(self, ms: float) -> None:
        i = int(math.log(ms / self.LO) / self.R) + 1 if ms > self.LO else 0
        self.buckets[i if i < self.N else self.N - 1] += 1
        self.count += 1
        self.sum += ms
        if ms > self.max:
            self.max = ms

    def upper(self, i: int) -> float:
        return self.LO * math.exp(self.R * i)

    def quantile(self, q: float) -> float:
        if not self.count:
            return 0.0
        rank = q * self.count
        acc = 0
        for i, c in enumerate(self.buckets):
            acc += c
            if acc >= rank:
                return self.max if i == self.N - 1 else min(self.upper(i), self.max)
        return self.max

    def le(self, bound: float) -> int:
        """Cumulative count at or below bound (bucket-resolution)."""
        n = min(int(math.log(bound / self.LO) / self.R) + 1, self.N - 1)
        return sum(self.buckets[: n + 1])

    def summary(self) -> dict[str, float]:
        return {"count": self.count, "p50": round(self.quantile(0.5), 3), "p95": round(self.quantile(0.95), 3),
                "p99": round(self.quantile(0.99), 3), "max": round(self.max, 3),
                "mean": round(self.sum / self.count, 3) if self.count else 0.0}


class Metrics:
    __slots__ = ("service", "hists", "counters", "gauges", "started", "cpu0")
    PROM_LE = (1, 2, 5, 10, 25, 50, 100, 150, 250, 500, 1000, 2500, 5000)

    def __init__(self, service: str) -> None:
        self.service = service
        self.hists: defaultdict[str, Hist] = defaultdict(Hist)
        self.counters: defaultdict[str, int] = defaultdict(int)
        self.gauges: dict[str, float] = {}
        self.started = time.time()
        self.cpu0 = _cpu_s()

    def inc(self, name: str, n: int = 1) -> None:
        self.counters[name] += n

    def reset(self) -> None:
        self.hists.clear()
        self.counters.clear()
        self.started = time.time()
        self.cpu0 = _cpu_s()

    def stats(self) -> dict:
        ru = resource.getrusage(resource.RUSAGE_SELF)
        cpu = ru.ru_utime + ru.ru_stime
        return {"service": self.service, "since_s": round(time.time() - self.started, 1),
                "latency_ms": {k: h.summary() for k, h in sorted(self.hists.items())},
                "counters": dict(sorted(self.counters.items())), "gauges": self.gauges,
                "cpu_s": round(cpu - self.cpu0, 3), "cpu_s_total": round(cpu, 3), "maxrss_mb": round(ru.ru_maxrss / 1024, 1),
                "rss_mb": round(_rss_bytes() / 1048576, 1)}

    def prometheus(self) -> str:
        svc = self.service
        out = ["# TYPE lt_handle_ms histogram"]
        for name, h in sorted(self.hists.items()):
            lbl = f'service="{svc}",op="{name}"'
            out.extend(f'lt_handle_ms_bucket{{{lbl},le="{b}"}} {h.le(b)}' for b in self.PROM_LE)
            out.append(f'lt_handle_ms_bucket{{{lbl},le="+Inf"}} {h.count}')
            out.append(f"lt_handle_ms_sum{{{lbl}}} {h.sum:.3f}")
            out.append(f"lt_handle_ms_count{{{lbl}}} {h.count}")
        out.append("# TYPE lt_events_total counter")
        out.extend(f'lt_events_total{{service="{svc}",name="{k}"}} {v}' for k, v in sorted(self.counters.items()))
        out.append("# TYPE lt_gauge gauge")
        out.extend(f'lt_gauge{{service="{svc}",name="{k}"}} {v}' for k, v in sorted(self.gauges.items()))
        ru = resource.getrusage(resource.RUSAGE_SELF)
        out.append("# TYPE process_cpu_seconds_total counter")
        out.append(f"process_cpu_seconds_total {ru.ru_utime + ru.ru_stime:.3f}")
        out.append("# TYPE process_resident_memory_bytes gauge")
        out.append(f"process_resident_memory_bytes {_rss_bytes()}")
        return "\n".join(out) + "\n"


def _cpu_s() -> float:
    ru = resource.getrusage(resource.RUSAGE_SELF)
    return ru.ru_utime + ru.ru_stime


def _rss_bytes() -> int:
    with open("/proc/self/statm", "rb") as f:
        return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")


_enc = msgspec.json.Encoder()


def json_resp(obj: object, status: int = 200) -> HTTPResponse:
    return raw(_enc.encode(obj), status=status, content_type=JSON)


def make_app(service: str) -> tuple[Sanic, Metrics]:
    app = Sanic(service.replace("-", "_"))
    app.config.KEEP_ALIVE_TIMEOUT = 75
    app.config.REQUEST_MAX_SIZE = 1_000_000
    m = Metrics(service)
    skip = frozenset({"/metrics", "/stats", "/admin/reset", "/health"})

    @app.on_request
    async def _t0(request: Request) -> None:
        request.ctx.t0 = perf_counter()

    @app.on_response
    async def _t1(request: Request, response: HTTPResponse) -> None:
        if (t0 := getattr(request.ctx, "t0", None)) is not None and request.path not in skip:
            m.hists[f"http {request.method} {request.path} {response.status // 100}xx"].observe(
                (perf_counter() - t0) * 1000)

    @app.get("/metrics")
    async def _metrics(_: Request) -> HTTPResponse:
        return raw(m.prometheus().encode(), content_type="text/plain; version=0.0.4")

    @app.get("/stats")
    async def _stats(_: Request) -> HTTPResponse:
        return json_resp(m.stats())

    @app.post("/admin/reset")
    async def _reset(_: Request) -> HTTPResponse:
        m.reset()
        return json_resp({"reset": True})

    @app.get("/health")
    async def _health(_: Request) -> HTTPResponse:
        return json_resp({"ok": True})

    return app, m


def serve(app: Sanic) -> None:
    app.run(host="0.0.0.0", port=int(env("PORT", "8000")), single_process=True, access_log=False,
            motd=False, debug=False)


def redis_client(timeout_s: float, max_connections: int = 64) -> Redis:
    """hiredis-parsed client; no hidden retries, so a stalled Redis surfaces as a fast error."""
    return Redis(host=env("REDIS_HOST", "lt-redis"), port=int(env("REDIS_PORT", "6379")),
                 socket_timeout=timeout_s, socket_connect_timeout=1.0, retry=Retry(NoBackoff(), 0),
                 max_connections=max_connections, decode_responses=False, health_check_interval=0)


PG_DSN = env("PG_DSN", "postgres://lt:lt-test-only@lt-pg:5432/lt")   # test-only default; real DSN via env
PG_OPTIONS = "-c statement_timeout=3000 -c idle_in_transaction_session_timeout=10000"
