"""lt-context: stream consumer that builds per-user Context and fires the state-timeout trigger.

Dedup and the state update run in one Lua script, so a redelivered event can never apply twice.
"""

import asyncio
import socket
from time import perf_counter

import aiohttp
import msgspec
from loguru import logger
from redis.exceptions import NoScriptError, RedisError, ResponseError
from sanic import Sanic

from lt.common import GROUP, QUARANTINE, STREAM, env, envf, make_app, now_ms, redis_client, serve
from lt.logic import DecisionIn, Event

CONSUMER = env("CONSUMER", socket.gethostname())
BATCH = int(env("BATCH", "500"))
TTL_MS = int(envf("CTX_TTL_S", 7 * 86400) * 1000)
DD_CAP = int(env("DEDUP_PER_USER", "128"))
LATE_MS = int(envf("LATE_S", 300) * 1000)
ABANDON_MS = int(envf("ABANDON_TIMEOUT_S", 10) * 1000)
DECISION_URL = env("DECISION_URL", "http://lt-decision:8000/decide")
PAYING = "paying"
TRIG_PENDING = "trig:pending"

APPLY_LUA = f"""
local ctx, ev, dd, paying = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local eid, typ, st, cts, src, uid = ARGV[1], ARGV[2], tonumber(ARGV[3]), tonumber(ARGV[4]), ARGV[8], ARGV[7]
if redis.call('ZSCORE', dd, eid) then return 0 end
redis.call('ZADD', dd, st, eid)
if redis.call('ZCARD', dd) > {DD_CAP} then redis.call('ZREMRANGEBYRANK', dd, 0, -{DD_CAP + 1}) end
redis.call('PEXPIRE', dd, {TTL_MS})
if src == 'client' and st - cts > {LATE_MS} then return 2 end
redis.call('LPUSH', ev, ARGV[5])
redis.call('LTRIM', ev, 0, 49)
redis.call('PEXPIRE', ev, {TTL_MS})
if src == 'client' and redis.call('HGET', ctx, 'session_id') ~= ARGV[6] then
  redis.call('HSET', ctx, 'session_id', ARGV[6], 'watch_minutes', 0, 'rooms_visited', 0, 'chats', 0, 'gifts', 0, 'current_room', '')
end
if typ == 'enter_room' then
  redis.call('HSET', ctx, 'current_room', ARGV[9])
  redis.call('HSETNX', ctx, 'fav_streamer', ARGV[9])
  redis.call('HINCRBY', ctx, 'rooms_visited', 1)
elseif typ == 'watch_progress' then redis.call('HINCRBY', ctx, 'watch_minutes', tonumber(ARGV[10]) or 0)
elseif typ == 'chat' then redis.call('HINCRBY', ctx, 'chats', 1)
elseif typ == 'send_gift' then redis.call('HINCRBY', ctx, 'gifts', 1)
elseif typ == 'following' then redis.call('HSET', ctx, 'followed', 1)
elseif typ == 'login' then redis.call('HSET', ctx, 'platform', ARGV[11])
elseif typ == 'leave_room' then redis.call('HSET', ctx, 'current_room', '')
elseif typ == 'click_recharge' then
  redis.call('HSET', ctx, 'payment_state', 'paying', 'pay_since', st, 'plan', ARGV[12])
  redis.call('ZADD', paying, st, uid)
elseif typ == 'cancel_recharge' then
  if redis.call('HGET', ctx, 'payment_state') == 'paying' then
    redis.call('HSET', ctx, 'payment_state', 'abandoned', 'abandoned_at', st, 'abandon_via', 'cancel')
    redis.call('ZREM', paying, uid)
  end
elseif typ == 'recharge' then
  if src == 'server' then
    redis.call('HSET', ctx, 'payment_state', 'completed', 'paid', 1, 'paid_at', st)
    redis.call('ZREM', paying, uid)
  else
    redis.call('HSET', ctx, 'client_recharge_at', st)
    if redis.call('HGET', ctx, 'payment_state') == 'paying' then
      redis.call('HSET', ctx, 'payment_state', 'client_reported')
      redis.call('ZREM', paying, uid)
    end
  end
end
if src == 'client' then redis.call('HSET', ctx, 'last_cts', cts) end
redis.call('HSET', ctx, 'updated_st', st)
redis.call('PEXPIRE', ctx, {TTL_MS})
return 1
"""

CLAIM_TIMEOUT_LUA = """
local ctx, paying, pending = KEYS[1], KEYS[2], KEYS[3]
local since = redis.call('ZSCORE', paying, ARGV[1])
if not since or tonumber(since) > tonumber(ARGV[2]) then return false end
redis.call('ZREM', paying, ARGV[1])
if redis.call('HGET', ctx, 'payment_state') ~= 'paying' then return false end
redis.call('HSET', ctx, 'payment_state', 'abandoned', 'abandoned_at', ARGV[3], 'abandon_via', 'timeout')
local f = redis.call('HMGET', ctx, 'session_id', 'platform')
local member = 'timeout:' .. ARGV[1] .. ':' .. since .. '\\31' .. ARGV[1] .. '\\31' .. (f[1] or '') .. '\\31' .. (f[2] or 'android')
redis.call('ZADD', pending, ARGV[3], member)
return member
"""

app, m = make_app("lt-context")
rb = redis_client(5.0)          # consumer: blocking reads need a timeout above BLOCK
rq = redis_client(1.0)          # short ops
_ev_dec = msgspec.json.Decoder(Event)
_enc = msgspec.json.Encoder()
SHA: dict[str, bytes] = {}


def keys(uid: str) -> list[str]:
    return [f"ctx:{{{uid}}}", f"ev:{{{uid}}}", f"dd:{{{uid}}}", PAYING]


async def load_scripts() -> None:
    SHA["apply"] = await rq.script_load(APPLY_LUA)
    SHA["claim"] = await rq.script_load(CLAIM_TIMEOUT_LUA)


async def ensure_group() -> None:
    try:
        await rq.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


async def apply(msgs: list) -> None:
    """Apply one batch in a single round trip: N x EVALSHA then one XACK."""
    t0 = perf_counter()
    pipe = rb.pipeline(transaction=False)
    ids, sts = [], []
    for mid, f in msgs:
        ids.append(mid)
        if not f:                      # entry trimmed away while pending: just ack it
            continue
        try:
            ev = _ev_dec.decode(f[b"d"])
        except msgspec.DecodeError as e:
            pipe.xadd(QUARANTINE, {"d": f[b"d"], "err": f"context: {e}", "st": now_ms()}, maxlen=100_000)
            continue
        st = int(f[b"st"])
        sts.append(st)
        p = ev.props
        pipe.evalsha(SHA["apply"], 4, *keys(ev.user_id), ev.event_id, ev.type, st, ev.client_ts, f[b"d"],
                     ev.session_id, ev.user_id, f.get(b"src", b"client"), str(p.get("room", "")),
                     str(p.get("minutes", 0)), str(p.get("platform", "android")), str(p.get("plan", "")))
    pipe.xack(STREAM, GROUP, *ids)
    res = await pipe.execute(raise_on_error=False)
    errs = [x for x in res if isinstance(x, Exception)]
    if errs:
        if any(isinstance(x, NoScriptError) for x in errs):
            await load_scripts()       # script cache lost (store restart); entries stay pending
        raise errs[0]
    done = now_ms()
    for x in res[:-1]:
        m.inc({0: "duplicates", 1: "applied", 2: "late"}.get(x, "other"))
    h = m.hists["ingest_to_context_ms"]
    for st in sts:
        h.observe(max(done - st, 0))
    m.hists["apply_batch_ms"].observe((perf_counter() - t0) * 1000)
    m.inc("batches")


async def consume(_: Sanic) -> None:
    cursor = "0"                       # drain this consumer's own pending list first
    while True:
        try:
            resp = await rb.xreadgroup(GROUP, CONSUMER, {STREAM: cursor}, count=BATCH, block=1000)
            msgs = resp[0][1] if resp else []
            if cursor == "0" and not msgs:
                cursor = ">"
                continue
            if msgs:
                await apply(msgs)
        except (RedisError, OSError) as e:
            m.inc("redis_errors")
            logger.warning("consume: {}: {}", type(e).__name__, e)
            await asyncio.sleep(0.5)
            if "NOGROUP" in str(e):
                await ensure_group()


async def autoclaim(_: Sanic) -> None:
    """Reclaim entries idle in any consumer's pending list (crash between read and ack)."""
    while True:
        await asyncio.sleep(5)
        try:
            start = "0-0"
            while True:
                nxt, msgs, _deleted = await rb.xautoclaim(STREAM, GROUP, CONSUMER, min_idle_time=30_000,
                                                          start_id=start, count=BATCH)
                if msgs:
                    m.inc("reclaimed", len(msgs))
                    await apply(msgs)
                if nxt in (b"0-0", "0-0"):
                    break
                start = nxt
        except (RedisError, OSError) as e:
            m.inc("redis_errors")
            logger.warning("autoclaim: {}", e)


async def post_trigger(http: aiohttp.ClientSession, member: bytes) -> None:
    tid, uid, sid, platform = member.decode().split("\x1f")
    body = _enc.encode(DecisionIn(event_id=tid, user_id=uid, trigger="abandonment", ts=now_ms(),
                                  session_id=sid or "unknown", platform=platform, source="state_timeout"))
    try:
        async with http.post(DECISION_URL, data=body, headers={"content-type": "application/json"}) as resp:
            await resp.read()
            ok = resp.status == 200
    except (aiohttp.ClientError, TimeoutError):
        ok = False
    if ok:
        await rq.zrem(TRIG_PENDING, member)
        m.inc("timeout_triggers_sent")
    else:
        m.inc("timeout_trigger_post_failed")


async def timeouts(_: Sanic) -> None:
    """State-timeout trigger: payment_state stuck at paying for ABANDON_TIMEOUT_S."""
    http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2.0),
                                 connector=aiohttp.TCPConnector(limit=32, keepalive_timeout=30))
    while True:
        await asyncio.sleep(1)
        try:
            now = now_ms()
            uids = await rq.zrangebyscore(PAYING, "-inf", now - ABANDON_MS, start=0, num=500)
            if uids:
                pipe = rq.pipeline(transaction=False)
                for u in uids:
                    uid = u.decode()
                    pipe.evalsha(SHA["claim"], 3, f"ctx:{{{uid}}}", PAYING, TRIG_PENDING, uid, now - ABANDON_MS, now)
                claimed = [x for x in await pipe.execute(raise_on_error=False) if isinstance(x, bytes)]
                m.inc("timeout_detected", len(claimed))
            # everything still pending (new claims plus earlier failed posts) is (re)sent; decide is idempotent
            if pending := await rq.zrangebyscore(TRIG_PENDING, "-inf", "+inf", start=0, num=200):
                await asyncio.gather(*(post_trigger(http, p) for p in pending))
            gauges = m.gauges
            gauges["paying_users"] = await rq.zcard(PAYING)
            gauges["trigger_pending"] = await rq.zcard(TRIG_PENDING)
        except (RedisError, OSError) as e:
            m.inc("redis_errors")
            logger.warning("timeouts: {}", e)
            if isinstance(e, NoScriptError):
                await load_scripts()


async def gauges(_: Sanic) -> None:
    while True:
        await asyncio.sleep(5)
        try:
            pipe = rq.pipeline(transaction=False)
            pipe.xinfo_groups(STREAM)
            pipe.xlen(STREAM)
            pipe.info("memory")
            groups, xlen, mem = await pipe.execute()
            g = next((x for x in groups if x["name"] in (GROUP.encode(), GROUP)), None)
            if g:
                m.gauges["stream_pending"] = g["pending"]
                m.gauges["stream_lag"] = g.get("lag") or 0
            m.gauges["stream_len"] = xlen
            m.gauges["redis_used_memory_mb"] = round(mem["used_memory"] / 1048576, 1)
        except (RedisError, OSError, KeyError):
            m.inc("gauge_errors")


@app.after_server_start
async def start(app_: Sanic) -> None:
    while True:
        try:
            await ensure_group()
            await load_scripts()
            break
        except (RedisError, OSError) as e:
            logger.warning("startup waiting for redis: {}", e)
            await asyncio.sleep(1)
    for fn in (consume, autoclaim, timeouts, gauges):
        app_.add_task(fn(app_))


if __name__ == "__main__":
    serve(app)
