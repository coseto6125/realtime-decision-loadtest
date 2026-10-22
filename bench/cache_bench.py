"""
T6 cache bench: the services' per-event Redis-protocol op mix, open-loop, against one store.

Use it to compare Valkey 8.1, Redis 7.2 and Dragonfly (run once per store), and to compare two
clients on the Context Builder hot path (``--client both`` runs the same mix on each client in turn).

Op mix per event (taken from the services, not re-invented):

* ingest (lt/collector.py): every 10 events, ONE pipeline of 10
  ``XADD events MAXLEN ~ 500000 * d <msgpack> st <ms> src client``.
  ``d`` is msgpack of the 6-element array ``[event_id, user_id, session_id, type, client_ts, props]``.
* consume (lt/context.py ``consume()``/``apply()``): ``--consumers`` loops of
  ``XREADGROUP GROUP ctx bench-N COUNT 200 BLOCK 100 STREAMS events >``, then ONE pipeline of
  n x ``EVALSHA <APPLY_LUA sha> len(keys) <keys(uid)> <args as apply() sends them>``. The script and
  ``keys()`` are imported from lt.context, so the bench follows the service: the current script XACKs
  its own entry (ARGV[13] group, ARGV[14] entry id), and a separate ``XACK`` is sent only for entries
  without a ``d`` field. On NOSCRIPT the driver runs SCRIPT LOAD and re-sends the failed EVALSHAs;
  their entries are still pending, and the script dedups by event_id, so a re-send is safe.
* read (lt/decision.py ``load_context()``): per decision (``--decisions-per-event`` x events) ONE
  pipeline ``HGETALL ctx:{uid}`` + ``LRANGE ev:{uid} 0 4`` for the user of the latest event.

Each run deletes key ``events`` and creates group ``ctx`` fresh. Run it against a dedicated store,
never against the store of a running stack. Dragonfly needs every key a script touches declared in
KEYS[]; APPLY_LUA does that. A command a store rejects goes to ``"incompatible"`` and the run goes on.

Clients:

* ``redis`` (default): redis-py 8.1 asyncio with hiredis, BlockingConnectionPool, RESP2. Pipelines
  are ``pipeline(transaction=False)`` + ``execute_command``.
* ``glide``: valkey-glide 2.5 async GlideClient. Pipelines are ``Batch(is_atomic=False)`` +
  ``custom_command`` + ``exec(raise_on_error=False)``; XREADGROUP uses the typed ``xreadgroup``.
  Glide multiplexes one connection per client and the server parks a blocked XREADGROUP on its
  connection, so each consumer gets its own GlideClient; producers and reads share one more. Every
  command goes through ``custom_command``, so the comparison is pipeline transport, not typed wrappers.

Timing is open-loop: the scheduler wakes at the next due unit (at most 5 ms apart, at least 1 ms, the
loop's timer resolution), issues every ingest pipeline and read that is due as its own task, and
latency is measured from the intended (scheduled) time, so it includes queueing in the driver when
the store falls behind (no coordinated omission). ``late_ticks`` counts 5 ms slots by which a
wake-up was late; a non-zero count means the driver, not the store, is the limit. ``driver_cpu_s`` is
process CPU (all threads, so glide's Rust core is included) per step.

Latency op types: ``xadd_pipeline``, ``apply_pipeline`` (EVALSHA x n), ``apply_per_event``
(apply_pipeline / n, one sample per batch), ``read_pipeline``, ``xreadgroup_hit`` and
``xreadgroup_empty``, ``ingest_to_applied`` (XADD st to apply done, per event). XREADGROUP latency
includes BLOCK wait for data, so it is reported but kept out of the saturation test.
Saturation = the highest step where xadd_pipeline, apply_pipeline and read_pipeline all have successful
samples with p99 <= ``--p99-limit-ms``, none of them nor XREADGROUP recorded an error, no unit was
cancelled at drain, and both ingested and consumed events/s are >= 95% of target. An op with no
successful sample fails the step; it never counts as zero latency.

Output: one JSON object per line on stdout. ``{"kind": "start", ...}``, then one ``"step"`` line per
step, then ``"summary"``. Diagnostics go to stderr.

Usage (from the loadtest directory; the image has lt/ on the path)::

    python -m bench.cache_bench --host lt-redis --port 6379 --mode baseline --events-per-sec 150 --duration-s 60
    python -m bench.cache_bench --host lt-redis --port 6379 --mode ramp \
        --ramp-steps 150,500,1000,2000,4000,8000,16000,32000 --step-s 20 --p99-limit-ms 10
    python -m bench.cache_bench --host lt-redis --mode baseline --events-per-sec 2000 --client both
"""

import argparse
import asyncio
import math
import random
import resource
import sys
import time
from collections import defaultdict
from collections.abc import Callable, Coroutine, Iterable
from itertools import chain, repeat, starmap
from time import perf_counter
from typing import TYPE_CHECKING, Any, Protocol

import msgspec
import uvloop
from lt.common import GROUP, STREAM, Hist, now_ms
from lt.context import APPLY_LUA, keys
from lt.logic import StreamEvent
from redis._parsers.helpers import parse_info  # ruff: ignore[import-private-name] -- INFO's own reader; glide returns raw INFO text
from redis.asyncio import BlockingConnectionPool, Redis
from redis.exceptions import NoScriptError, RedisError

if TYPE_CHECKING:
    from glide import GlideClient, StreamReadGroupOptions

TICK_S = 0.005
MIN_SLEEP_S = 0.001
BATCH = 10
STREAM_MAXLEN = b"500000"
READ_COUNT = 200
BLOCK_MS = 100
CRITERIA_OPS = ("xadd_pipeline", "apply_pipeline", "read_pipeline")
FAIL_ERRORS = (*CRITERIA_OPS, "xreadgroup")  # noscript_reload is a recovered retry, not a failure
MIN_RATE_FRACTION = 0.95  # achieved and consumed events/s must reach this share of the target
TYPE_WEIGHTS = (("watch_progress", 50), ("chat", 30), ("enter_room", 6), ("leave_room", 4), ("send_gift", 4),
                ("click_recharge", 3), ("cancel_recharge", 2), ("recharge", 1))
TYPE_TABLE = tuple(chain.from_iterable(starmap(repeat, TYPE_WEIGHTS)))
PROPS: dict[str, dict[str, str | int]] = {
    "watch_progress": {"minutes": 1}, "enter_room": {"room": "r42"}, "click_recharge": {"plan": "p99"},
}
NO_PROPS: dict[str, str | int] = {}


EV_ENC = msgspec.msgpack.Encoder()
EV_DEC = msgspec.msgpack.Decoder(StreamEvent)
_out = msgspec.json.Encoder()


def emit(obj: object) -> None:
    """Write one JSON line to stdout and flush, so a parser sees each step as it ends."""
    sys.stdout.buffer.write(_out.encode(obj) + b"\n")
    sys.stdout.buffer.flush()


def cpu_s() -> float:
    ru = resource.getrusage(resource.RUSAGE_SELF)
    return ru.ru_utime + ru.ru_stime


def make_payload(event_id: str, uid_n: int, typ: str, client_ts: int) -> bytes:
    """Encode one event for user ``u<uid_n>``; the session is fixed per user so the script never resets it."""
    return EV_ENC.encode(StreamEvent(event_id, f"u{uid_n}", f"s{uid_n}", typ, client_ts, PROPS.get(typ, NO_PROPS)))


class Gen:
    """Event source: uniform users, weighted types, ids unique per run (the script dedups by event_id)."""

    __slots__ = ("last_uid", "prefix", "rnd", "seq", "users")

    def __init__(self, users: int, seed: int) -> None:
        self.users = users
        self.prefix = f"e{time.time_ns():x}-"
        self.seq = 0
        self.last_uid = "u0"
        self.rnd = random.Random(seed).random

    def next(self, client_ts: int) -> tuple[str, bytes]:
        rnd = self.rnd
        n = int(rnd() * self.users)
        self.seq += 1
        self.last_uid = uid = f"u{n}"
        return uid, make_payload(f"{self.prefix}{self.seq}", n, TYPE_TABLE[int(rnd() * 100)], client_ts)


class Pacer:
    """Open-loop schedule: unit k is due at k / rate seconds after start."""

    __slots__ = ("done", "rate")

    def __init__(self, rate: float) -> None:
        self.rate = rate
        self.done = 0

    def due(self, elapsed: float) -> int:
        """Units that became due since the last call; marks them issued."""
        n = int(elapsed * self.rate) - self.done
        if n <= 0:
            return 0
        self.done += n
        return n

    def at(self, k: int) -> float:
        """Offset in seconds at which unit k (1-based) was due."""
        return k / self.rate

    def next_at(self) -> float:
        """Offset at which the next unit falls due; infinity for a zero rate."""
        return (self.done + 1) / self.rate if self.rate > 0 else math.inf


class Ticker:
    """Wake-up plan: sleep to the next due unit, at most one tick; count tick slots a wake-up was late by."""

    __slots__ = ("late", "planned", "tick", "wakes")

    def __init__(self, tick: float) -> None:
        self.tick = tick
        self.planned = 0.0
        self.wakes = 0
        self.late = 0

    def wake(self, elapsed: float) -> None:
        self.wakes += 1
        if (behind := elapsed - self.planned) > self.tick:
            self.late += int(behind / self.tick)

    def plan(self, elapsed: float, *due_at: float) -> float:
        """
        Set the next wake-up to the earliest due time, capped at one tick; return the sleep in seconds.

        The sleep is at least MIN_SLEEP_S: the event loop's timers have 1 ms resolution, and a shorter
        sleep returns at once and spins. A unit can therefore start up to 1 ms after its due time.
        """
        self.planned = min((elapsed + self.tick, *due_at))
        return max(MIN_SLEEP_S, self.planned - elapsed)


async def open_loop(rate: float, decisions_per_event: float, duration_s: float, batch: int,
                    ingest: Callable[[float], Coroutine[Any, Any, None]],
                    read: Callable[[float], Coroutine[Any, Any, None]], tasks: set[asyncio.Task]) -> Ticker:
    """
    Issue ``rate`` events/s as batches of ``batch`` and ``rate * decisions_per_event`` reads/s by wall clock.

    Every unit runs as its own task and gets its intended start time (perf_counter seconds), so a late
    driver or a slow store shows up as latency, not as a lower offered rate.
    """
    loop = asyncio.get_running_loop()
    bp, rp, tk = Pacer(rate / batch), Pacer(rate * decisions_per_event), Ticker(TICK_S)
    t0 = perf_counter()
    while (el := perf_counter() - t0) < duration_s:
        tk.wake(el)
        for src, fn in ((bp, ingest), (rp, read)):
            if n := src.due(el):
                for k in range(src.done - n + 1, src.done + 1):
                    t = loop.create_task(fn(t0 + src.at(k)))
                    tasks.add(t)
                    t.add_done_callback(tasks.discard)
        await asyncio.sleep(tk.plan(perf_counter() - t0, bp.next_at(), rp.next_at()))
    return tk


async def drain(tasks: set[asyncio.Task], timeout_s: float) -> int:
    """Wait for in-flight units of the finished step; cancel and count what is still running at the timeout."""
    if not tasks:
        return 0
    _, pending = await asyncio.wait(set(tasks), timeout=timeout_s)
    for t in pending:
        t.cancel()
    return len(pending)


def saturation(steps: list[dict], p99_limit_ms: float,
               ops: tuple[str, ...] = CRITERIA_OPS) -> tuple[float | None, float | None]:
    """
    Return (highest passing target_eps, first failing target_eps); None where there is none.

    A step passes when every op in ``ops`` has successful samples with p99 <= p99_limit_ms, no op in
    FAIL_ERRORS recorded an error, no unit was cancelled at drain, and achieved and consumed events/s
    reach MIN_RATE_FRACTION of the target. An op without samples fails the step.
    """
    best = first_fail = None
    for s in steps:
        tgt = s["target_eps"]
        lat, errors = s["latency_ms"], s["errors"]
        ok = (all((h := lat.get(op)) and h["count"] > 0 and h["p99"] <= p99_limit_ms for op in ops)
              and not any(errors.get(op) for op in FAIL_ERRORS) and not s["cancelled_at_drain"]
              and min(s["achieved_eps"], s["consumed_eps"]) >= MIN_RATE_FRACTION * tgt)
        if ok:
            best = tgt if best is None else max(best, tgt)
        elif first_fail is None:
            first_fail = tgt
    return best, first_fail


def apply_args(sha: str | bytes, d: bytes, st: bytes, src: bytes, ev: StreamEvent, mid: bytes) -> tuple:
    """EVALSHA argv in the order lt/context.py apply() sends it: keys(uid), then ARGV[1..14] of APPLY_LUA."""
    p = ev.props
    k = keys(ev.user_id)
    return ("EVALSHA", sha, str(len(k)), *k, ev.event_id, ev.type, st, str(ev.client_ts), d,
            ev.session_id, ev.user_id, src, str(p.get("room", "")), str(p.get("minutes", 0)),
            str(p.get("platform", "android")), str(p.get("plan", "")), GROUP, mid)


def stream_entries(resp: object) -> list[tuple[bytes, dict]]:
    """
    Normalise an XREADGROUP reply of one stream to ``[(id, {field: value})]``.

    Accepts redis-py legacy ``[[stream, [(id, fields)]]]``, redis-py unified ``{stream: [(id, fields)]}``
    and glide ``{stream: {id: [[field, value], ...]}}``.
    """
    if not resp:
        return []
    if isinstance(resp, dict):
        body = next(iter(resp.values()))
        if isinstance(body, dict):
            return [(mid, dict(pairs) if pairs else {}) for mid, pairs in body.items()]
        return [(mid, f or {}) for mid, f in body]
    return [(mid, f or {}) for mid, f in resp[0][1]]


def is_noscript(x: object) -> bool:
    """redis-py raises NoScriptError; glide returns RequestError("... NoScriptError: No matching script ...")."""
    return isinstance(x, NoScriptError) or (isinstance(x, Exception) and "noscript" in str(x).lower())


class Store(Protocol):
    """What the bench needs from a client: non-atomic pipelines, single commands, one consumer read."""

    name: str
    errors: tuple[type[BaseException], ...]

    async def run(self, cmds: list[tuple]) -> list: ...
    async def call(self, *args: str | bytes) -> object: ...
    async def readgroup(self, consumer: str) -> list[tuple[bytes, dict]]: ...
    async def consumer_client(self) -> "Store": ...
    async def close(self) -> None: ...


class RedisPyStore:
    """redis-py asyncio + hiredis; one blocking pool shared by producers, readers and consumers."""

    __slots__ = ("r",)
    name = "redis-py"
    errors: tuple[type[BaseException], ...] = (RedisError, OSError)

    def __init__(self, host: str, port: int, max_connections: int) -> None:
        pool = BlockingConnectionPool(host=host, port=port, max_connections=max_connections, timeout=10,
                                      socket_timeout=5.0, socket_connect_timeout=2.0, health_check_interval=0)
        self.r = Redis(connection_pool=pool)

    async def run(self, cmds: list[tuple]) -> list:
        """Non-atomic pipeline; server errors come back in place."""
        pipe = self.r.pipeline(transaction=False)
        ex = pipe.execute_command
        for c in cmds:
            ex(*c)
        return await pipe.execute(raise_on_error=False)

    async def call(self, *args: str | bytes) -> object:
        return await self.r.execute_command(*args)

    async def readgroup(self, consumer: str) -> list[tuple[bytes, dict]]:
        return stream_entries(await self.r.xreadgroup(GROUP, consumer, {STREAM: ">"}, count=READ_COUNT,
                                                      block=BLOCK_MS))

    async def consumer_client(self) -> "RedisPyStore":
        return self

    async def close(self) -> None:
        await self.r.aclose()


class GlideStore:
    """valkey-glide async client; consumers get their own client (see module docstring)."""

    __slots__ = ("c", "errors", "extra", "host", "opts", "port")
    name = "glide"

    def __init__(self, c: "GlideClient", host: str, port: int, opts: "StreamReadGroupOptions",
                 errors: tuple[type[BaseException], ...]) -> None:
        self.c, self.host, self.port, self.opts, self.errors = c, host, port, opts, errors
        self.extra: list[GlideStore] = []

    @classmethod
    async def create(cls, host: str, port: int) -> "GlideStore":
        from glide import GlideClient, GlideClientConfiguration, GlideError, NodeAddress, StreamReadGroupOptions

        c = await GlideClient.create(GlideClientConfiguration(
            [NodeAddress(host, port)], request_timeout=5000, inflight_requests_limit=4000))
        return cls(c, host, port, StreamReadGroupOptions(block_ms=BLOCK_MS, count=READ_COUNT), (GlideError, OSError))

    async def run(self, cmds: list[tuple]) -> list:
        from glide import Batch

        b = Batch(is_atomic=False)
        add = b.custom_command
        for c in cmds:
            add(list(c))
        return await self.c.exec(b, raise_on_error=False) or []

    async def call(self, *args: str | bytes) -> object:
        return await self.c.custom_command(list(args))

    async def readgroup(self, consumer: str) -> list[tuple[bytes, dict]]:
        return stream_entries(await self.c.xreadgroup({STREAM: ">"}, GROUP, consumer, self.opts))

    async def consumer_client(self) -> "GlideStore":
        own = await GlideStore.create(self.host, self.port)
        self.extra.append(own)
        return own

    async def close(self) -> None:
        for s in self.extra:
            await s.c.close()
        await self.c.close()


class Step:
    """Counters of one measurement step; tasks record into the Step that was current when they were issued."""

    __slots__ = ("cmds", "consumed", "cpu0", "errors", "hists", "ingested", "t0")

    def __init__(self) -> None:
        self.hists: defaultdict[str, Hist] = defaultdict(Hist)
        self.cmds: defaultdict[str, int] = defaultdict(int)
        self.errors: defaultdict[str, int] = defaultdict(int)
        self.ingested = 0
        self.consumed = 0
        self.t0 = perf_counter()
        self.cpu0 = cpu_s()


class Bench:
    __slots__ = ("args", "gen", "incompatible", "running", "sem", "sha", "step", "store")

    def __init__(self, store: "Store", args: argparse.Namespace) -> None:
        self.store = store
        self.args = args
        self.gen = Gen(args.users, args.seed)
        self.sha: str | bytes = b""
        self.step = Step()
        self.incompatible: dict[tuple[str, str], str] = {}
        self.running = True
        self.sem = asyncio.Semaphore(args.max_inflight)

    def server_errors(self, op: str, pairs: Iterable[tuple[tuple, Any]]) -> int:
        """Count in-place errors among (command, reply) pairs; record each distinct one as an incompatibility."""
        n = 0
        for c, x in pairs:
            if isinstance(x, Exception):
                n += 1
                if len(self.incompatible) < 20:
                    self.incompatible.setdefault((op, str(c[0])), f"{type(x).__name__}: {x}"[:200])
        if n:
            self.step.errors[op] += n
        return n

    async def ingest(self, intended: float) -> None:
        st = self.step
        ms = now_ms()
        stb = str(ms).encode()
        nxt = self.gen.next
        cmds = [("XADD", STREAM, "MAXLEN", "~", STREAM_MAXLEN, "*", "d", nxt(ms - 3)[1], "st", stb, "src", "client")
                for _ in range(BATCH)]
        try:
            async with self.sem:
                res = await self.store.run(cmds)
        except self.store.errors:
            st.errors["xadd_pipeline"] += 1
            return
        st.hists["xadd_pipeline"].observe((perf_counter() - intended) * 1000)
        st.cmds["XADD"] += BATCH
        st.ingested += BATCH - self.server_errors("xadd_pipeline", zip(cmds, res, strict=False))

    async def read(self, intended: float) -> None:
        st = self.step
        k = keys(self.gen.last_uid)
        cmds = [("HGETALL", k[0]), ("LRANGE", k[1], "0", "4")]
        try:
            async with self.sem:
                res = await self.store.run(cmds)
        except self.store.errors:
            st.errors["read_pipeline"] += 1
            return
        st.hists["read_pipeline"].observe((perf_counter() - intended) * 1000)
        st.cmds["HGETALL"] += 1
        st.cmds["LRANGE"] += 1
        self.server_errors("read_pipeline", zip(cmds, res, strict=False))

    async def consume(self, store: "Store", name: str) -> None:
        while self.running:
            st = self.step
            t0 = perf_counter()
            try:
                entries = await store.readgroup(name)
            except store.errors as e:
                st.errors["xreadgroup"] += 1
                self.incompatible.setdefault(("consume", "XREADGROUP"), f"{type(e).__name__}: {e}"[:200])
                await asyncio.sleep(0.2)
                continue
            t1 = perf_counter()
            st.cmds["XREADGROUP"] += 1
            st.hists["xreadgroup_hit" if entries else "xreadgroup_empty"].observe((t1 - t0) * 1000)
            if entries:
                await self.apply(store, entries, t1)

    async def apply(self, store: "Store", entries: list[tuple[bytes, dict]], t1: float) -> None:
        st = self.step
        sha = self.sha
        evals, sts, skipped = [], [], []
        for mid, f in entries:
            if (d := f.get(b"d")) is None:
                skipped.append(mid)
                continue
            sts.append(int(stb := f[b"st"]))
            evals.append(apply_args(sha, d, stb, f.get(b"src", b"client"), EV_DEC.decode(d), mid))
        cmds = [*evals, ("XACK", STREAM, GROUP, *skipped)] if skipped else evals
        try:
            pairs = await self.run_apply(store, cmds)
        except store.errors:
            st.errors["apply_pipeline"] += 1
            return
        done = perf_counter()
        n = len(entries)
        st.hists["apply_pipeline"].observe((done - t1) * 1000)
        st.hists["apply_per_event"].observe((done - t1) * 1000 / n)
        st.cmds["EVALSHA"] += len(evals)
        st.cmds["XACK"] += bool(skipped)
        st.consumed += n - self.server_errors("apply_pipeline", pairs)
        h = st.hists["ingest_to_applied"]
        wall = now_ms()
        for s in sts:
            h.observe(max(wall - s, 0))

    async def run_apply(self, store: "Store", cmds: list[tuple]) -> list[tuple[tuple, object]]:
        """
        Send EVALSHA x n; on NOSCRIPT load the script and re-send only the failed EVALSHAs.

        The script acks its own entry, so an entry whose EVALSHA failed is still pending.
        """
        pairs = list(zip(cmds, await store.run(cmds), strict=False))
        if any(is_noscript(x) for _, x in pairs):
            self.sha = sha = await store.call("SCRIPT", "LOAD", APPLY_LUA)
            self.step.errors["noscript_reload"] += 1
            retry = [(c[0], sha, *c[2:]) for c, x in pairs if is_noscript(x)]
            pairs = [p for p in pairs if not is_noscript(p[1])]
            pairs += zip(retry, await store.run(retry), strict=False)
        return pairs

    async def info(self) -> dict[str, Any]:
        """Server identity from INFO server; empty when the store rejects it."""
        try:
            raw = await self.store.call("INFO", "server")
        except self.store.errors as e:
            self.incompatible.setdefault(("setup", "INFO"), str(e)[:200])
            return {}
        info = raw if isinstance(raw, dict) else parse_info(raw)
        return {k: info[k] for k in ("redis_version", "valkey_version", "dragonfly_version", "server_name",
                                     "redis_mode", "arch_bits") if k in info}

    async def group_state(self) -> dict[str, Any]:
        try:
            groups = await self.store.call("XINFO", "GROUPS", STREAM)
            mem = await self.store.call("INFO", "memory")
        except self.store.errors as e:
            self.incompatible.setdefault(("gauge", "XINFO"), str(e)[:200])
            return {}
        out: dict[str, Any] = {}
        for g in groups or []:
            g = g if isinstance(g, dict) else dict(zip(g[::2], g[1::2], strict=False))
            g = {(k.decode() if isinstance(k, bytes) else k): v for k, v in g.items()}
            if g.get("name") in {GROUP, GROUP.encode()}:
                out = {"pending": g.get("pending"), "lag": g.get("lag")}
        mem = mem if isinstance(mem, dict) else parse_info(mem)
        out["used_memory_mb"] = round(int(mem.get("used_memory", 0)) / 1048576, 1)
        return out

    async def prepare(self) -> None:
        call = self.store.call
        await call("DEL", STREAM)
        await call("XGROUP", "CREATE", STREAM, GROUP, "$", "MKSTREAM")
        self.sha = await call("SCRIPT", "LOAD", APPLY_LUA)

    def report(self, i: int, target: float, late: int, ticks: int, cancelled: int, gauges: dict) -> dict:
        st = self.step
        dur = perf_counter() - st.t0
        cpu = cpu_s() - st.cpu0
        return {
            "kind": "step", "bench": "cache", "client": self.store.name, "step": i, "target_eps": target,
            "duration_s": round(dur, 3), "achieved_eps": round(st.ingested / dur, 1),
            "consumed_eps": round(st.consumed / dur, 1),
            "ops_per_sec": {k: round(v / dur, 1) for k, v in sorted(st.cmds.items())},
            "latency_ms": {k: h.summary() for k, h in sorted(st.hists.items())},
            "late_ticks": late, "wakeups": ticks, "cancelled_at_drain": cancelled,
            "driver_cpu_s": round(cpu, 3),
            "cpu_s_per_1k_events": round(cpu * 1000 / st.ingested, 4) if st.ingested else None,
            "errors": dict(st.errors), "stream": gauges,
            "incompatible": [{"op": op, "cmd": c, "error": e} for (op, c), e in self.incompatible.items()],
        }


async def make_store(client: str, args: argparse.Namespace) -> "Store":
    if client == "glide":
        return await GlideStore.create(args.host, args.port)
    return RedisPyStore(args.host, args.port, args.max_inflight + args.consumers + 4)


async def run_client(client: str, args: argparse.Namespace, rates: list[float], step_s: float) -> dict:
    store = await make_store(client, args)
    b = Bench(store, args)
    emit({"kind": "start", "bench": "cache", "client": store.name, "host": args.host, "port": args.port,
          "mode": args.mode, "rates": rates, "step_s": step_s, "users": args.users,
          "decisions_per_event": args.decisions_per_event, "consumers": args.consumers, "server": await b.info()})
    await b.prepare()
    consumer_stores = [await store.consumer_client() for _ in range(args.consumers)]
    workers = [asyncio.create_task(b.consume(cs, f"bench-{i}")) for i, cs in enumerate(consumer_stores)]
    tasks: set[asyncio.Task] = set()
    steps = []
    try:
        for i, rate in enumerate(rates):
            b.step = Step()
            tk = await open_loop(rate, args.decisions_per_event, step_s, BATCH, b.ingest, b.read, tasks)
            cancelled = await drain(tasks, 5.0)
            line = b.report(i, rate, tk.late, tk.wakes, cancelled, await b.group_state())
            emit(line)
            steps.append(line)
    finally:
        b.running = False
        await asyncio.wait(workers, timeout=1.0 + BLOCK_MS / 1000)
        for w in workers:
            w.cancel()
        await store.close()
    sat, fail = saturation(steps, args.p99_limit_ms)
    return {"client": store.name, "saturation_eps": sat, "first_fail_eps": fail,
            "incompatible": steps[-1]["incompatible"] if steps else [],
            "per_step": [{"target_eps": s["target_eps"], "achieved_eps": s["achieved_eps"],
                          "consumed_eps": s["consumed_eps"], "cpu_s_per_1k_events": s["cpu_s_per_1k_events"],
                          "late_ticks": s["late_ticks"]} for s in steps]}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="bench.cache_bench",
                                description="T6 Redis-protocol store A/B (see module docstring)")
    p.add_argument("--host", default="lt-redis")
    p.add_argument("--port", type=int, default=6379)
    p.add_argument("--mode", choices=("baseline", "ramp"), default="baseline")
    p.add_argument("--events-per-sec", type=float, default=150.0, help="baseline rate")
    p.add_argument("--duration-s", type=float, default=60.0, help="baseline duration")
    p.add_argument("--ramp-steps", default="150,500,1000,2000,4000,8000,16000,32000")
    p.add_argument("--step-s", type=float, default=20.0)
    p.add_argument("--decisions-per-event", type=float, default=0.113)
    p.add_argument("--users", type=int, default=50_000)
    p.add_argument("--p99-limit-ms", type=float, default=10.0)
    p.add_argument("--client", choices=("redis", "glide", "both"), default="redis")
    p.add_argument("--consumers", type=int, default=2, help="XREADGROUP loops in group ctx")
    p.add_argument("--max-inflight", type=int, default=256, help="cap on concurrent ingest/read pipelines")
    p.add_argument("--seed", type=int, default=1)
    return p.parse_args(argv)


async def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.mode == "ramp":
        rates, step_s = [float(x) for x in args.ramp_steps.split(",") if x.strip()], args.step_s
    else:
        rates, step_s = [args.events_per_sec], args.duration_s
    clients = ("redis", "glide") if args.client == "both" else (args.client,)
    results = [await run_client(c, args, rates, step_s) for c in clients]
    emit({"kind": "summary", "bench": "cache", "mode": args.mode, "p99_limit_ms": args.p99_limit_ms,
          "results": results})


if __name__ == "__main__":
    uvloop.run(main())
