"""lt-decision: POST /decide, the runnable decide() of the architecture in README.md.

Idempotent by event_id: an in-process single-flight map covers a duplicate that arrives while the
first is in flight; the decisions unique key covers everything else. The Decision, its pending
Action (outbox) and the next-day job are written by one statement, so they commit together.
The stored Decision is msgpack (bytea); the idempotent path re-encodes it to JSON at the edge.
One worker process: the container has 0.5 vCPU.
"""

import asyncio
from collections import deque
from time import monotonic, perf_counter
from urllib.parse import urlsplit

import cachebox
import msgspec
import rnet
from loguru import logger
from psqlpy import ConnectionPool
from psqlpy.exceptions import Error as PGError
from redis.exceptions import RedisError
from rnet import exceptions as rx
from sanic import HTTPResponse, Request, Sanic, raw

from lt.common import (
    DAY_S, JSON, PG_DSN, PG_OPTIONS, env, envf, json_resp, make_app, now_ms, redis_client, serve, tcp_established,
)
from lt.logic import (
    Decision, DecisionIn, Intent, JudgeIn, JudgeOut, Policy, StreamEvent, assign_arm, choose_action,
    enforce_guardrails, top_intent,
)

MODEL_URL = env("MODEL_URL", "http://lt-model:8000/judge")
MODEL_TIMEOUT_S = envf("MODEL_TIMEOUT_S", 0.150)
MODEL_KEEPALIVE = env("MODEL_KEEPALIVE", "1") == "1"
_MODEL = urlsplit(MODEL_URL)
MODEL_PORT = _MODEL.port or (443 if _MODEL.scheme == "https" else 80)       # the gauge counts sockets to it
BREAKER = env("BREAKER", "1") == "1"
STALE_MS = int(envf("STALE_S", 20) * 1000)
WEEK_S = int(DAY_S * 7)

SQL_GET = "SELECT body FROM decisions WHERE event_id = $1"
SQL_WRITE = (
    "WITH d AS (INSERT INTO decisions (policy_version, top_p, event_id, user_id, trigger, arm, action, reason,"
    " model_status, top_intent, body) VALUES ($1, NULLIF($2::float8, -1), $3, $4, $5, $6, $7::text, $8, $9,"
    " NULLIF($10::text, ''), $11) ON CONFLICT (event_id) DO NOTHING RETURNING id, user_id),"
    " o AS (INSERT INTO outbox (decision_id, user_id, action, payload) SELECT id, user_id, $7::text, $11 FROM d"
    " WHERE $7::text <> 'none' RETURNING id),"
    " j AS (INSERT INTO next_day_jobs (decision_id, abandoned_at, due_at, user_id, arm)"
    " SELECT id, now(), now() + make_interval(secs => $12::float8), user_id, $6 FROM d"
    " WHERE $5::text = 'abandonment' RETURNING id)"
    " SELECT (SELECT id FROM d)"
)
SQL_POLICIES = (
    "SELECT DISTINCT ON (trigger) trigger, version, salt, arms, threshold, options, question"
    " FROM policies WHERE active ORDER BY trigger, version DESC"
)

app, m = make_app("lt-decision")
r = redis_client(float(env("REDIS_TIMEOUT_S", "0.3")))
pool = ConnectionPool(dsn=PG_DSN, max_db_pool_size=int(env("PG_POOL", "8")), options=PG_OPTIONS)
_in_dec = msgspec.json.Decoder(DecisionIn)
_judge_dec = msgspec.json.Decoder(JudgeOut)
_ev_dec = msgspec.msgpack.Decoder(StreamEvent)
_dec_mp = msgspec.msgpack.Decoder(Decision)
_enc = msgspec.json.Encoder()
_mp = msgspec.msgpack.Encoder()
POLICIES = cachebox.TTLCache(16, envf("POLICY_TTL_S", 5))     # trigger -> Policy, reloaded on expiry
INFLIGHT: dict[str, asyncio.Future] = {}
HTTP: list[rnet.Client] = []
MODEL_INFLIGHT = [0, 0]                    # current, max since the last gauge tick


class Breaker:
    """Skip the model while it is failing: open for OPEN_S when the timeout ratio over the last WINDOW_S
    exceeds RATIO (with at least MIN_CALLS calls); then half-open, letting one probe through."""

    __slots__ = ("calls", "open_until", "probing")
    WINDOW_S, RATIO, MIN_CALLS, OPEN_S = 10.0, 0.2, 10, 30.0

    def __init__(self) -> None:
        self.calls: deque[tuple[float, bool]] = deque()
        self.open_until = 0.0
        self.probing = False

    def allow(self, now: float) -> bool:
        if now < self.open_until:
            return False
        if self.open_until and not self.probing:      # half-open: one probe at a time
            self.probing = True
            return True
        return not self.open_until

    def record(self, now: float, timed_out: bool) -> None:
        if self.probing:
            self.probing = False
            self.open_until = now + self.OPEN_S if timed_out else 0.0
            self.calls.clear()
            m.inc("breaker.probe_failed" if timed_out else "breaker.closed")
            return
        self.calls.append((now, timed_out))
        while self.calls and self.calls[0][0] < now - self.WINDOW_S:
            self.calls.popleft()
        bad = sum(t for _, t in self.calls)
        if len(self.calls) >= self.MIN_CALLS and bad / len(self.calls) > self.RATIO:
            self.open_until = now + self.OPEN_S
            self.calls.clear()
            m.inc("breaker.opened")


BRK = Breaker()
PG_ERRORS = (PGError,)
HTTP_ERRORS = (rx.ConnectionError, rx.ConnectionResetError, rx.TimeoutError, rx.RequestError, rx.BodyError,
               rx.DecodingError, msgspec.DecodeError)


async def policy(trigger: str) -> Policy | None:
    if (p := POLICIES.get(trigger)) is not None:
        return p
    async with pool.acquire() as conn:
        rows = (await conn.execute(SQL_POLICIES)).result()
    for row in rows:
        POLICIES[row["trigger"]] = msgspec.convert(row, Policy)
    m.inc("policy_reload")
    return POLICIES.get(trigger)


async def load_context(uid: str) -> tuple[dict[str, str] | None, list[bytes]]:
    t0 = perf_counter()
    try:
        pipe = r.pipeline(transaction=False)
        pipe.hgetall(f"ctx:{{{uid}}}")
        pipe.lrange(f"ev:{{{uid}}}", 0, 4)
        h, evs = await pipe.execute()
    except (RedisError, OSError):
        m.inc("ctx_unavailable")
        return None, []
    finally:
        m.hists["ctx_load_ms"].observe((perf_counter() - t0) * 1000)
    if not h:
        return None, []
    return {k.decode(): v.decode() for k, v in h.items()}, evs


def state_text(ctx: dict[str, str] | None, recent: list[bytes], app_recent: list[str]) -> str:
    if ctx is None:
        return "no context"
    types = ",".join(_ev_dec.decode(e).type for e in recent) if recent else ",".join(app_recent[:5])
    return (f"watched {ctx.get('watch_minutes', '0')} min; rooms {ctx.get('rooms_visited', '0')};"
            f" payment {ctx.get('payment_state', 'none')}; plan {ctx.get('plan', '')}; recent {types}")


async def judge(state: str, pol: Policy) -> tuple[str, Intent | None]:
    now = monotonic()
    if BREAKER and not BRK.allow(now):
        return "breaker_open", None
    body = _enc.encode(JudgeIn(state=state, question=pol.question, options=pol.options))
    t0 = perf_counter()
    MODEL_INFLIGHT[0] += 1
    MODEL_INFLIGHT[1] = max(MODEL_INFLIGHT[1], MODEL_INFLIGHT[0])
    status, out = "timeout", None          # stays "timeout" on TimeoutError and on a cancel
    try:
        async with asyncio.timeout(MODEL_TIMEOUT_S):
            resp = await HTTP[0].post(MODEL_URL, body=body, headers={"content-type": JSON})
            payload = await resp.bytes()
        if resp.status_code.is_success():
            out = _judge_dec.decode(payload)
            status = "ok"
        else:
            status = "error"
    except TimeoutError:
        pass
    except HTTP_ERRORS:
        status = "error"
    finally:
        MODEL_INFLIGHT[0] -= 1
        m.hists["model_call_ms"].observe((perf_counter() - t0) * 1000)
        # in finally: a cancelled call (Sanic cancels on client disconnect) still ends a half-open probe
        if BREAKER:
            BRK.record(monotonic(), status != "ok")
    if out is None:
        return status, None
    if len(out.probs) != len(pol.options):
        return "error", None
    top, p = top_intent(pol.options, out.probs)
    return "ok", Intent(top=top, p=p, probs=out.probs, latency_ms=out.latency_ms)


async def model_gauges(_: Sanic) -> None:
    """In-flight model calls (max per second) and the TCP connections to the model endpoint."""
    seen: set[tuple[str, str]] = set()
    while True:
        await asyncio.sleep(1)
        conns = tcp_established(remote_port=MODEL_PORT)
        g = m.gauges
        g["model_inflight_max_1s"] = MODEL_INFLIGHT[1]
        g["model_inflight_max_run"] = max(g.get("model_inflight_max_run", 0), MODEL_INFLIGHT[1])
        MODEL_INFLIGHT[1] = MODEL_INFLIGHT[0]
        g["model_conns_open"] = len(conns)
        g["model_conns_open_max_run"] = max(g.get("model_conns_open_max_run", 0), len(conns))
        g["model_conns_new_1s"] = len(conns - seen)
        g["model_conns_new_max_1s"] = max(g.get("model_conns_new_max_1s", 0), len(conns - seen))
        m.inc("model_conns_new_total", len(conns - seen))
        g["breaker_open"] = int(BRK.open_until > monotonic())
        seen = conns


async def frequency_guard(req: DecisionIn) -> str | None:
    """Per-session and per-week caps for in-app prompts; a Redis failure blocks the action."""
    match req.trigger:
        case "abandonment":
            key, ttl = f"cap:hint:{{{req.user_id}}}:{req.session_id}", 86400
        case "watch_30min":
            key, ttl = f"cap:follow:{{{req.user_id}}}", WEEK_S
        case _:
            return None
    try:
        # NX GET returns the holder's event_id, so a retry of the event that set the cap still passes it
        prev = await r.set(key, req.event_id, nx=True, get=True, ex=ttl)
    except (RedisError, OSError):
        return "guard_unavailable"
    return None if prev is None or prev == req.event_id.encode() else "frequency_cap"


def replay(stored: bytes) -> bytes:
    m.inc("idempotent_hit")
    return _enc.encode(_dec_mp.decode(stored))


async def stored(event_id: str) -> bytes | None:
    async with pool.acquire() as conn:
        rows = (await conn.fetch(SQL_GET, [event_id])).result()
    return rows[0]["body"] if rows else None


async def compute(req: DecisionIn) -> bytes:
    if (pol := await policy(req.trigger)) is None:
        raise LookupError(f"no active policy for {req.trigger}")
    # A pooled PG connection is held only for its own statement, never across the model or Redis waits.
    prior, (ctx, recent) = await asyncio.gather(stored(req.event_id), load_context(req.user_id))
    if prior is not None:
        return replay(prior)
    arm = assign_arm(req.user_id, pol)
    status, intent = "skipped", None
    if pol.options:    # every arm, holdout and rule too: the decision row logs the model's intent for all arms
        status, intent = await judge(state_text(ctx, recent, req.recent_events), pol)
    action, reason = choose_action(req.trigger, arm, (intent.top, intent.p) if intent else None, pol,
                                   ctx or {}, req.event_id)
    now = now_ms()
    action, reason = enforce_guardrails(action, reason, req, ctx, now, STALE_MS)
    if action != "none" and (blocked := await frequency_guard(req)):
        action, reason = "none", blocked
    decision = Decision(event_id=req.event_id, user_id=req.user_id, trigger=req.trigger, arm=arm, action=action,
                        reason=reason, policy_version=pol.version, model_status=status, intent=intent,
                        decided_at=now)
    async with pool.acquire() as conn:
        t0 = perf_counter()
        new_id = await conn.fetch_val(SQL_WRITE, [
            pol.version, intent.p if intent else -1.0, req.event_id, req.user_id, req.trigger, arm, action, reason,
            status, intent.top if intent else "", _mp.encode(decision), DAY_S])
        m.hists["pg_write_ms"].observe((perf_counter() - t0) * 1000)
    if new_id is None:                 # lost a race with another process: return the winner
        m.inc("idempotent_race")
        return replay(await stored(req.event_id))
    m.inc(f"arm.{arm}")
    m.inc(f"action.{action}")
    m.inc(f"reason.{reason}")
    m.inc(f"model.{status}")
    return _enc.encode(decision)


@app.post("/decide")
async def decide(request: Request) -> HTTPResponse:
    try:
        req = _in_dec.decode(request.body)
    except msgspec.DecodeError as e:
        return json_resp({"error": str(e)}, 400)
    if (fut := INFLIGHT.get(req.event_id)) is not None:
        m.inc("inflight_dedup")
        if (body := await asyncio.shield(fut)) is not None:
            return raw(body, content_type=JSON)
    fut = asyncio.get_running_loop().create_future()
    INFLIGHT[req.event_id] = fut
    body = None
    try:
        body = await compute(req)
        return raw(body, content_type=JSON)
    except PG_ERRORS as e:
        m.inc("pg_unavailable")
        return json_resp({"error": "decision store unavailable", "detail": type(e).__name__}, 503)
    except LookupError as e:
        return json_resp({"error": str(e)}, 422)
    finally:
        fut.set_result(body)
        INFLIGHT.pop(req.event_id, None)


@app.before_server_start
async def setup(_: Sanic) -> None:
    # MODEL_KEEPALIVE=0 (T7 A/B): keep no idle connection, so every model call opens a new TCP (+TLS)
    # connection. rnet's no_keepalive alone did not stop pooled reuse (measured: TIME_WAIT stayed near 0).
    HTTP.append(rnet.Client(timeout=2, http1_only=True, tcp_nodelay=True,
                            pool_max_idle_per_host=64 if MODEL_KEEPALIVE else 0, no_keepalive=not MODEL_KEEPALIVE,
                            verify=env("MODEL_TLS_VERIFY", "1") == "1"))
    while True:
        try:
            await policy("abandonment")
            break
        except PG_ERRORS as e:
            logger.warning("waiting for postgres: {}", e)
            await asyncio.sleep(1)


@app.after_server_start
async def start(app_: Sanic) -> None:
    app_.add_task(model_gauges(app_))


if __name__ == "__main__":
    serve(app)
