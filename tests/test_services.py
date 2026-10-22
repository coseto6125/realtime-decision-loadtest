"""Service code with its I/O replaced: fakeredis (with Lua) for Valkey, fakes for Postgres, the model and HTTP."""

import asyncio

import fakeredis
import msgspec
import pytest
from glide import ClosingError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from rnet import exceptions as rx

import lt.context as context
import lt.decision as decision
from lt.common import Metrics, make_app, now_ms
from lt.logic import Decision, DecisionIn, Policy, TimeoutMember

POL = Policy(trigger="abandonment", version=1, salt="s", arms=[("model", 100)], options=["price_sensitive", "x"])


def dreq(event_id: str = "e1", user_id: str = "u1", session_id: str = "") -> DecisionIn:
    return DecisionIn(event_id=event_id, user_id=user_id, trigger="abandonment", ts=1, session_id=session_id)


# ---------------------------------------------------------------- decision: frequency guard


@pytest.fixture
def dredis(monkeypatch):
    r = fakeredis.aioredis.FakeRedis()
    monkeypatch.setattr(decision, "r", r)
    return r


def test_frequency_guard_users_without_session_id_do_not_share_cap(dredis):
    async def run():
        return [await decision.frequency_guard(dreq(f"e-{u}", u)) for u in ("alice", "bob")]

    assert asyncio.run(run()) == [None, None]


def test_frequency_guard_retry_of_same_event_passes_other_event_capped(dredis):
    async def run():
        return [await decision.frequency_guard(dreq(e, session_id="s1")) for e in ("e1", "e1", "e2")]

    assert asyncio.run(run()) == [None, None, "frequency_cap"]


def test_frequency_guard_same_event_twice_in_flight_both_pass(dredis):
    async def run():
        return await asyncio.gather(decision.frequency_guard(dreq()), decision.frequency_guard(dreq()))

    assert asyncio.run(run()) == [None, None]


def test_frequency_guard_redis_down_returns_guard_unavailable(monkeypatch):
    class Down:
        async def set(self, *_a, **_k):
            raise RedisConnectionError("down")

    monkeypatch.setattr(decision, "r", Down())
    assert asyncio.run(decision.frequency_guard(dreq())) == "guard_unavailable"


# ---------------------------------------------------------------- decision: compute and the PG pool


class _Result:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    def result(self) -> list[dict]:
        return self.rows


class _Pool:
    """psqlpy pool double: counts held connections; SQL_GET answers from `stored`, SQL_WRITE from `write_id`."""

    def __init__(self, stored: bytes | None = None, write_id: int | None = 1) -> None:
        self.held = 0
        self.stored = stored
        self.write_id = write_id
        self.writes: list[list] = []

    def acquire(self):
        pool = self

        class _Conn:
            async def __aenter__(self):
                pool.held += 1
                return self

            async def __aexit__(self, *_exc):
                pool.held -= 1

            async def fetch(self, _sql, _args):
                return _Result([{"body": pool.stored}] if pool.stored is not None else [])

            async def fetch_val(self, _sql, args):
                pool.writes.append(args)
                if pool.write_id is None:       # another process won the insert
                    pool.stored = args[10]
                return pool.write_id

        return _Conn()


@pytest.fixture
def dcompute(monkeypatch, dredis):
    seen: dict[str, int] = {}

    async def policy(_trigger):
        return POL

    async def load_context(_uid):
        return {"updated_st": str(now_ms())}, []

    async def judge(_state, _pol):
        await asyncio.sleep(0)
        seen["held_during_model"] = pool.held
        return "skipped", None

    pool = _Pool()
    monkeypatch.setattr(decision, "pool", pool)
    monkeypatch.setattr(decision, "policy", policy)
    monkeypatch.setattr(decision, "load_context", load_context)
    monkeypatch.setattr(decision, "judge", judge)
    return pool, seen


def test_compute_model_wait_holds_no_pg_connection(dcompute):
    pool, seen = dcompute
    out = msgspec.json.decode(asyncio.run(decision.compute(dreq())), type=Decision)
    assert seen["held_during_model"] == 0
    assert (out.event_id, len(pool.writes), pool.held) == ("e1", 1, 0)


def test_compute_stored_decision_replays_without_model_or_write(dcompute):
    pool, seen = dcompute
    prior = Decision(event_id="e1", user_id="u1", trigger="abandonment", arm="model", action="none", reason="r",
                     policy_version=1, model_status="ok", intent=None, decided_at=1)
    pool.stored = msgspec.msgpack.encode(prior)
    assert msgspec.json.decode(asyncio.run(decision.compute(dreq())), type=Decision) == prior
    assert (seen, pool.writes, pool.held) == ({}, [], 0)


def test_compute_lost_insert_race_returns_the_stored_winner(dcompute):
    pool, _ = dcompute
    pool.write_id = None
    out = msgspec.json.decode(asyncio.run(decision.compute(dreq())), type=Decision)
    assert out == msgspec.msgpack.decode(pool.stored, type=Decision)
    assert pool.held == 0


# ---------------------------------------------------------------- context: the Lua scripts on fakeredis


def apply_lua(r, eid: str, typ: str = "watch_progress", minutes: str = "1", st: int = 10_000, cts: int = 10_000):
    keys = ["ctx:{u1}", "ev:{u1}", "dd:{u1}", context.PAYING, context.STREAM]
    argv = [eid, typ, str(st), str(cts), b"raw", "s1", "u1", "client", "", minutes, "android", "", context.GROUP, "1-0"]
    return r.eval(context.APPLY_LUA, 5, *keys, *argv)


def test_apply_lua_failed_write_leaves_no_dedupe_marker():
    r = fakeredis.FakeRedis()
    with pytest.raises(ResponseError):
        apply_lua(r, "e1", minutes="1.5")           # HINCRBY rejects a non-integer
    assert r.zscore("dd:{u1}", "e1") is None        # so the redelivery applies it instead of skipping it
    assert apply_lua(r, "e1", minutes="1") == 1
    assert r.hget("ctx:{u1}", "watch_minutes") == b"1"


def test_apply_lua_redelivered_event_is_duplicate_and_counted_once():
    r = fakeredis.FakeRedis()
    assert [apply_lua(r, "e1", minutes="5"), apply_lua(r, "e1", minutes="5")] == [1, 0]
    assert r.hget("ctx:{u1}", "watch_minutes") == b"5"


def test_apply_lua_late_event_marked_then_duplicate():
    r = fakeredis.FakeRedis()
    late = context.LATE_MS + 1
    assert [apply_lua(r, "e1", st=late + 10, cts=10), apply_lua(r, "e1", st=late + 10, cts=10)] == [2, 0]
    assert r.exists("ctx:{u1}") == 0


@pytest.mark.parametrize(("session", "platform"), [("a\x1fb", "ios"), ("", "android"), (" ", "web")])
def test_claim_timeout_lua_member_round_trips_any_session(session, platform):
    r = fakeredis.FakeRedis()
    r.hset("ctx:{u1}", mapping={"payment_state": "paying", "session_id": session, "platform": platform})
    r.zadd(context.PAYING, {"u1": 1_000})
    member = r.eval(context.CLAIM_TIMEOUT_LUA, 3, "ctx:{u1}", context.PAYING, context.TRIG_PENDING, "u1", 2_000, 3_000)
    assert msgspec.json.decode(member, type=TimeoutMember) == TimeoutMember("u1", "1000", session, platform)
    assert r.zrange(context.TRIG_PENDING, 0, -1) == [member]


# ---------------------------------------------------------------- context: post_trigger


class _Status:
    def __init__(self, code: int) -> None:
        self.code = code

    def as_int(self) -> int:
        return self.code


class _Http:
    def __init__(self, code: int | None) -> None:
        self.code = code
        self.bodies: list[bytes] = []

    async def post(self, _url, body, headers):
        self.bodies.append(body)
        if self.code is None:
            raise rx.ConnectionError("decision down")
        resp = type("Resp", (), {"status_code": _Status(self.code)})()

        async def read() -> bytes:
            return b"{}"

        resp.bytes = read
        return resp


@pytest.fixture
def ctx_env(monkeypatch):
    r = fakeredis.aioredis.FakeRedis()
    m = Metrics("lt-context-test")
    monkeypatch.setattr(context, "rq", r)
    monkeypatch.setattr(context, "m", m)
    return r, m


def post(r, http, member: bytes) -> list[bytes]:
    async def run():
        await r.zadd(context.TRIG_PENDING, {member: 1})
        await context.post_trigger(http, member)
        return await r.zrange(context.TRIG_PENDING, 0, -1)

    return asyncio.run(run())


GOOD = msgspec.json.encode(["u1", "1790959330981", "s1", "android"])


@pytest.mark.parametrize(("code", "left", "counter"), [
    (200, [], "timeout_triggers_sent"), (400, [], "timeout_trigger_rejected"),
    (422, [], "timeout_trigger_rejected"), (429, [GOOD], "timeout_trigger_post_failed"),
    (503, [GOOD], "timeout_trigger_post_failed"), (None, [GOOD], "timeout_trigger_post_failed"),
])
def test_post_trigger_status_decides_whether_member_stays(ctx_env, code, left, counter):
    r, m = ctx_env
    assert post(r, _Http(code), GOOD) == left
    assert m.counters[counter] == 1


@pytest.mark.parametrize("member", [b"timeout:u1:1\x1fu1\x1fs1\x1fandroid", b"", b"[1]", b"   ", b'"u1"'])
def test_post_trigger_bad_member_dropped_without_post(ctx_env, member):
    r, m = ctx_env
    http = _Http(200)
    assert post(r, http, member) == []
    assert (http.bodies, m.counters["timeout_trigger_bad_member"]) == ([], 1)


def test_post_trigger_longest_user_id_posts_valid_decision_request(ctx_env):
    r, _ = ctx_env
    http = _Http(200)
    post(r, http, msgspec.json.encode(["u" * 96, "1790959330981", "", "ios"]))
    req = msgspec.json.decode(http.bodies[0], type=DecisionIn)       # the decide decoder's own limits
    assert (req.user_id, req.session_id, req.source) == ("u" * 96, "unknown", "state_timeout")


# ---------------------------------------------------------------- context: startup


def test_start_glide_create_fails_once_then_starts_every_task(monkeypatch):
    calls: list[str] = []
    tasks: list = []

    async def noop() -> None:
        return None

    async def create(_cfg):
        calls.append("create")
        if len(calls) == 1:
            raise ClosingError("Received error for address `lt-redis:6379`: timed out")
        return "glide"

    async def fast_sleep(_s) -> None:
        return None

    class App:
        def add_task(self, coro) -> None:
            tasks.append(coro.__name__)
            coro.close()

    monkeypatch.setattr(context, "ensure_group", noop)
    monkeypatch.setattr(context, "load_scripts", noop)
    monkeypatch.setattr(context.GlideClient, "create", create)
    monkeypatch.setattr(context.asyncio, "sleep", fast_sleep)
    monkeypatch.setattr(context, "GL", [])
    asyncio.run(context.start(App()))
    assert (calls, context.GL, tasks) == (["create", "create"], ["glide"], ["consume", "autoclaim", "timeouts", "gauges"])


# ---------------------------------------------------------------- common: latency histogram keys


def test_make_app_unknown_paths_share_one_histogram_key():
    app, m = make_app("lt-hist-test")

    @app.get("/item/<n:int>")
    async def item(_req, n):
        from lt.common import json_resp
        return json_resp({"n": n})

    async def call(path: str) -> int:
        sent: list[dict] = []

        async def receive() -> dict:
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(msg: dict) -> None:
            sent.append(msg)

        scope = {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": "GET",
                 "scheme": "http", "path": path, "raw_path": path.encode(), "query_string": b"", "headers": [],
                 "server": ("t", 80), "client": ("c", 1)}
        await app(scope, receive, send)
        return sent[0]["status"]

    async def run() -> list[int]:
        q: asyncio.Queue = asyncio.Queue()
        await q.put({"type": "lifespan.startup"})
        started = asyncio.Event()

        async def lsend(msg: dict) -> None:
            if msg["type"] == "lifespan.startup.complete":
                started.set()

        life = asyncio.create_task(app({"type": "lifespan", "asgi": {"version": "3.0"}}, q.get, lsend))
        await started.wait()
        codes = [await call(p) for p in ("/item/1", "/item/2", "/nope/1", "/nope/2", "/nope/3")]
        life.cancel()
        return codes

    assert asyncio.run(run()) == [200, 200, 404, 404, 404]
    assert sorted(m.hists) == ["http GET /item/<n:int> 2xx", "http GET unmatched 4xx"]
