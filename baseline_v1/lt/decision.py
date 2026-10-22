"""lt-decision: POST /decide, the decide() skeleton from stack.py made runnable.

Idempotent by event_id: an in-process single-flight map covers a duplicate that arrives while the
first is in flight; the decisions primary key covers everything else. The Decision, its pending
Action (outbox) and the next-day job are written by one statement, so they commit together.
"""

import asyncio
from time import perf_counter

import aiohttp
import msgspec
from loguru import logger
from psqlpy import ConnectionPool
from psqlpy.exceptions import Error as PGError
from redis.exceptions import RedisError
from sanic import HTTPResponse, Request, Sanic, raw

from lt.common import JSON, PG_DSN, PG_OPTIONS, env, envf, json_resp, make_app, now_ms, redis_client, serve
from lt.logic import (
    Decision, DecisionIn, Intent, JudgeIn, JudgeOut, Policy, assign_arm, choose_action, enforce_guardrails,
    top_intent,
)

MODEL_URL = env("MODEL_URL", "http://lt-model:8000/judge")
MODEL_TIMEOUT_S = envf("MODEL_TIMEOUT_S", 0.150)
STALE_MS = int(envf("STALE_S", 20) * 1000)
DAY_S = envf("DAY_S", 30)
WEEK_S = int(DAY_S * 7)

SQL_GET = "SELECT body FROM decisions WHERE event_id = $1"
SQL_WRITE = (
    "WITH d AS (INSERT INTO decisions (event_id, user_id, trigger, arm, action, reason, policy_version,"
    " model_status, top_intent, top_p, body) VALUES ($1, $2, $3, $4, $5::text, $6, $7, $8,"
    " NULLIF($9::text, ''), NULLIF($10::float8, -1), $11) ON CONFLICT (event_id) DO NOTHING RETURNING id, user_id),"
    " o AS (INSERT INTO outbox (decision_id, user_id, action) SELECT id, user_id, $5::text FROM d"
    " WHERE $5::text <> 'none' RETURNING id),"
    " j AS (INSERT INTO next_day_jobs (decision_id, user_id, arm, abandoned_at, due_at)"
    " SELECT id, user_id, $4, now(), now() + make_interval(secs => $12::float8) FROM d"
    " WHERE $3::text = 'abandonment' RETURNING id)"
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
_enc = msgspec.json.Encoder()
POLICIES: dict[str, Policy] = {}
INFLIGHT: dict[str, asyncio.Future] = {}
HTTP: list[aiohttp.ClientSession] = []
PG_ERRORS = (PGError,)


async def refresh_policies() -> None:
    async with pool.acquire() as conn:
        rows = (await conn.execute(SQL_POLICIES)).result()
    for row in rows:
        POLICIES[row["trigger"]] = msgspec.convert(row, Policy)


async def policy_loop(_: Sanic) -> None:
    while True:
        await asyncio.sleep(5)
        try:
            await refresh_policies()
        except PG_ERRORS as e:
            m.inc("policy_refresh_failed")
            logger.warning("policy refresh: {}", e)


async def load_context(uid: str) -> tuple[dict[str, str] | None, list[str]]:
    t0 = perf_counter()
    try:
        pipe = r.pipeline(transaction=False)
        pipe.hgetall(f"ctx:{{{uid}}}")
        pipe.lrange(f"ev:{{{uid}}}", 0, 9)
        h, evs = await pipe.execute()
    except (RedisError, OSError):
        m.inc("ctx_unavailable")
        return None, []
    finally:
        m.hists["ctx_load_ms"].observe((perf_counter() - t0) * 1000)
    if not h:
        return None, []
    return {k.decode(): v.decode() for k, v in h.items()}, [e.decode() for e in evs]


def state_text(ctx: dict[str, str] | None, recent: list[str]) -> str:
    if ctx is None:
        return "no context"
    types = ",".join(e.split('"type":"', 1)[-1].split('"', 1)[0] for e in recent[:5])
    return (f"watched {ctx.get('watch_minutes', '0')} min; rooms {ctx.get('rooms_visited', '0')};"
            f" payment {ctx.get('payment_state', 'none')}; plan {ctx.get('plan', '')}; recent {types}")


async def judge(state: str, policy: Policy) -> tuple[str, Intent | None]:
    body = _enc.encode(JudgeIn(state=state, question=policy.question, options=policy.options))
    t0 = perf_counter()
    try:
        async with asyncio.timeout(MODEL_TIMEOUT_S):
            async with HTTP[0].post(MODEL_URL, data=body, headers={"content-type": JSON}) as resp:
                if resp.status != 200:
                    return "error", None
                out = _judge_dec.decode(await resp.read())
    except TimeoutError:
        return "timeout", None
    except (aiohttp.ClientError, msgspec.DecodeError):
        return "error", None
    finally:
        m.hists["model_call_ms"].observe((perf_counter() - t0) * 1000)
    if len(out.probs) != len(policy.options):
        return "error", None
    top, p = top_intent(policy.options, out.probs)
    return "ok", Intent(top=top, p=p, probs=out.probs, latency_ms=out.latency_ms)


async def frequency_guard(req: DecisionIn, action: str) -> str | None:
    """Per-session and per-week caps for in-app prompts; a Redis failure blocks the action."""
    match req.trigger:
        case "abandonment":
            key, ttl = f"cap:hint:{req.session_id}", 86400
        case "watch_30min":
            key, ttl = f"cap:follow:{{{req.user_id}}}", WEEK_S
        case _:
            return None
    try:
        if not await r.set(key, req.event_id, nx=True, ex=ttl):
            return "frequency_cap"
    except (RedisError, OSError):
        return "guard_unavailable"
    return None


async def compute(req: DecisionIn) -> bytes:
    if (policy := POLICIES.get(req.trigger)) is None:
        raise LookupError(f"no active policy for {req.trigger}")
    async with pool.acquire() as conn:
        (prior, (ctx, recent)) = await asyncio.gather(conn.fetch(SQL_GET, [req.event_id]),
                                                     load_context(req.user_id))
        if rows := prior.result():
            m.inc("idempotent_hit")
            return rows[0]["body"].encode()
        arm = assign_arm(req.user_id, policy)
        status, intent = ("skipped", None)
        if policy.options:
            status, intent = await judge(state_text(ctx, recent or req.recent_events), policy)
        action, reason = choose_action(req.trigger, arm, (intent.top, intent.p) if intent else None, policy,
                                       ctx or {}, req.event_id)
        now = now_ms()
        action, reason = enforce_guardrails(action, reason, req, ctx, now, STALE_MS)
        if action != "none" and (blocked := await frequency_guard(req, action)):
            action, reason = "none", blocked
        body = _enc.encode(Decision(event_id=req.event_id, user_id=req.user_id, trigger=req.trigger, arm=arm,
                                    action=action, reason=reason, policy_version=policy.version,
                                    model_status=status, intent=intent, decided_at=now))
        t0 = perf_counter()
        new_id = await conn.fetch_val(SQL_WRITE, [
            req.event_id, req.user_id, req.trigger, arm, action, reason, policy.version, status,
            intent.top if intent else "", intent.p if intent else -1.0, body.decode(), DAY_S])
        m.hists["pg_write_ms"].observe((perf_counter() - t0) * 1000)
        if new_id is None:             # lost a race with another process: return the winner
            m.inc("idempotent_race")
            return (await conn.fetch(SQL_GET, [req.event_id])).result()[0]["body"].encode()
    m.inc(f"arm.{arm}")
    m.inc(f"action.{action}")
    m.inc(f"reason.{reason}")
    m.inc(f"model.{status}")
    return body


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
    HTTP.append(aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=256, keepalive_timeout=30)))
    while True:
        try:
            await refresh_policies()
            break
        except PG_ERRORS as e:
            logger.warning("waiting for postgres: {}", e)
            await asyncio.sleep(1)


@app.after_server_start
async def start(app_: Sanic) -> None:
    app_.add_task(policy_loop(app_))


if __name__ == "__main__":
    serve(app)
