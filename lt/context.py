"""lt-context: stream consumer that builds per-user Context and fires the state-timeout trigger.

Dedup and the state update run in one Lua script, so a redelivered event can never apply twice.
"""

import asyncio
import socket
from time import perf_counter

import msgspec
import rnet
from loguru import logger
from redis.exceptions import NoScriptError, RedisError, ResponseError
from rnet import exceptions as rx
from sanic import Sanic

from glide import Batch, GlideClient, GlideClientConfiguration, GlideError, NodeAddress, StreamReadGroupOptions

from lt.common import GROUP, QUARANTINE, QUARANTINE_MAXLEN, STREAM, env, envf, make_app, now_ms, redis_client, serve
from lt.logic import DecisionIn, StreamEvent, TimeoutMember, timeout_event_id, watch_minutes

CONSUMER = env("CONSUMER", socket.gethostname())
BATCH = int(env("BATCH", "500"))
TTL_MS = int(envf("CTX_TTL_S", 7 * 86400) * 1000)
DD_CAP = int(env("DEDUP_PER_USER", "128"))
LATE_MS = int(envf("LATE_S", 300) * 1000)
ABANDON_MS = int(envf("ABANDON_TIMEOUT_S", 10) * 1000)
DECISION_URL = env("DECISION_URL", "http://lt-decision:8000/decide")
PAYING = "paying"
TRIG_PENDING = "trig:pending"

# Dedup, state update and XACK of the stream entry are one script: an entry is acknowledged only by the
# script run that applied (or deliberately skipped) it. The dedupe marker and XACK come after every state
# write, so a run that fails part-way leaves no marker and the entry stays pending for redelivery. A script
# error does not roll back the writes before it, so a redelivery can repeat them (LPUSH, HINCRBY): the trade
# is a possible double count, not a lost event. apply() passes only validated arguments (watch_minutes), so
# no event can make the script fail on its data.
APPLY_LUA = f"""
local ctx, ev, dd, paying, stream = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local eid, typ, st, cts, src, uid = ARGV[1], ARGV[2], tonumber(ARGV[3]), tonumber(ARGV[4]), ARGV[8], ARGV[7]
local function done(code)
  redis.call('XACK', stream, ARGV[13], ARGV[14])
  return code
end
local function applied(code)
  redis.call('ZADD', dd, st, eid)
  if redis.call('ZCARD', dd) > {DD_CAP} then redis.call('ZREMRANGEBYRANK', dd, 0, -{DD_CAP + 1}) end
  redis.call('PEXPIRE', dd, {TTL_MS})
  return done(code)
end
if redis.call('ZSCORE', dd, eid) then return done(0) end
if src == 'client' and st - cts > {LATE_MS} then return applied(2) end
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
return applied(1)
"""
APPLY_RESULT = {0: "duplicates", 1: "applied", 2: "late"}     # the codes of done() and applied() above

CLAIM_TIMEOUT_LUA = """
local ctx, paying, pending = KEYS[1], KEYS[2], KEYS[3]
local since = redis.call('ZSCORE', paying, ARGV[1])
if not since or tonumber(since) > tonumber(ARGV[2]) then return false end
redis.call('ZREM', paying, ARGV[1])
if redis.call('HGET', ctx, 'payment_state') ~= 'paying' then return false end
redis.call('HSET', ctx, 'payment_state', 'abandoned', 'abandoned_at', ARGV[3], 'abandon_via', 'timeout')
local f = redis.call('HMGET', ctx, 'session_id', 'platform')
local member = cjson.encode({ARGV[1], since, f[1] or '', f[2] or 'android'})   -- lt.logic.TimeoutMember
redis.call('ZADD', pending, ARGV[3], member)
return member
"""

app, m = make_app("lt-context")
rb = redis_client(5.0)          # autoclaim: reads up to BATCH entries
rq = redis_client(1.0)          # short ops (timeout trigger, gauges, script load)
# Hot path (XREADGROUP + EVALSHA batch) runs on valkey-glide: its Rust core packs and parses commands.
# The T6 client A/B measured it against redis-py + hiredis on the same op mix (REPORT.md, 實作優化).
GL: list[GlideClient] = []
READ_OPTS = StreamReadGroupOptions(block_ms=1000, count=BATCH)
HOT_ERRORS = (GlideError, RedisError, OSError)
_ev_dec = msgspec.msgpack.Decoder(StreamEvent)
_trig_dec = msgspec.json.Decoder(TimeoutMember)
_enc = msgspec.json.Encoder()
SHA: dict[str, str] = {}       # script name -> sha; refilled on NOSCRIPT (store restart)
HTTP_ERRORS = (rx.ConnectionError, rx.ConnectionResetError, rx.TimeoutError, rx.RequestError, rx.BodyError,
               TimeoutError)


def keys(uid: str) -> list[str]:
    return [f"ctx:{{{uid}}}", f"ev:{{{uid}}}", f"dd:{{{uid}}}", PAYING, STREAM]


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
    """Apply one batch in a single round trip: one EVALSHA per entry, each acking its own entry."""
    t0 = perf_counter()
    batch = Batch(is_atomic=False)
    cmd = batch.custom_command
    sts, skipped, quarantine = [], [], []
    for mid, f in msgs:
        if not f:                      # entry trimmed away while pending: nothing to apply
            skipped.append(mid)
            continue
        try:
            ev = _ev_dec.decode(f[b"d"])
        except msgspec.DecodeError as e:
            quarantine.append({"d": f[b"d"], "err": f"context: {e}", "st": now_ms()})
            skipped.append(mid)
            continue
        st = int(f[b"st"])
        sts.append(st)
        p = ev.props
        cmd(["EVALSHA", SHA["apply"], "5", *keys(ev.user_id), ev.event_id, ev.type, str(st), str(ev.client_ts),
             f[b"d"], ev.session_id, ev.user_id, f.get(b"src", b"client"), str(p.get("room", "")),
             str(watch_minutes(p.get("minutes", 0))), str(p.get("platform", "android")), str(p.get("plan", "")), GROUP, mid])
    for q in quarantine:               # after the EVALSHAs, so res[:len(sts)] are their results
        cmd(["XADD", QUARANTINE, "MAXLEN", "~", str(QUARANTINE_MAXLEN), "*", "d", q["d"], "err", q["err"], "st", str(q["st"])])
    if skipped:
        cmd(["XACK", STREAM, GROUP, *skipped])
    res = await GL[0].exec(batch, raise_on_error=False) or []
    if errs := [x for x in res if isinstance(x, Exception)]:
        m.inc("apply_errors", len(errs))  # failed entries stay pending and are re-read
        raise next((x for x in errs if "noscript" in str(x).lower()), errs[0])
    done = now_ms()
    for x in res[: len(sts)]:
        m.inc(APPLY_RESULT.get(x, "other"))
    h = m.hists["ingest_to_context_ms"]
    for st in sts:
        h.observe(max(done - st, 0))
    m.hists["apply_batch_ms"].observe((perf_counter() - t0) * 1000)
    m.inc("batches")


async def consume(_: Sanic) -> None:
    cursor = "0"                       # drain this consumer's own pending list first
    while True:
        try:
            resp = await GL[0].xreadgroup({STREAM: cursor}, GROUP, CONSUMER, READ_OPTS)
            # glide reply: {stream: {id: [[field, value], ...] or None (entry trimmed)}}
            msgs = [(mid, dict(pairs) if pairs else {}) for mid, pairs in next(iter(resp.values())).items()] if resp else []
            if cursor == "0" and not msgs:
                cursor = ">"
                continue
            if msgs:
                await apply(msgs)
        except HOT_ERRORS as e:
            m.inc("redis_errors")
            logger.warning("consume: {}: {}", type(e).__name__, e)
            cursor = "0"                   # the failed batch is still pending: re-read it first
            await asyncio.sleep(0.5)
            try:
                if "NOGROUP" in str(e):
                    await ensure_group()
                if "noscript" in str(e).lower() or not SHA.get("apply"):
                    await load_scripts()
            except HOT_ERRORS:
                pass


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
        except HOT_ERRORS as e:
            m.inc("redis_errors")
            logger.warning("autoclaim: {}", e)


async def post_trigger(http: rnet.Client, member: bytes) -> None:
    """Post one pending trigger. It leaves trig:pending once decide answers, or once it can never succeed."""
    try:
        t = _trig_dec.decode(member)
    except msgspec.DecodeError:        # not a claim-script record: drop it, so it cannot block the queue
        await rq.zrem(TRIG_PENDING, member)
        m.inc("timeout_trigger_bad_member")
        return
    body = _enc.encode(DecisionIn(event_id=timeout_event_id(t.user_id, t.since), user_id=t.user_id,
                                  trigger="abandonment", ts=now_ms(), session_id=t.session_id or "unknown",
                                  platform=t.platform, source="state_timeout"))
    try:
        resp = await http.post(DECISION_URL, body=body, headers={"content-type": "application/json"})
        await resp.bytes()
        code = resp.status_code.as_int()
    except HTTP_ERRORS:
        code = 0
    match code:
        case _ if 200 <= code < 300:
            await rq.zrem(TRIG_PENDING, member)
            m.inc("timeout_triggers_sent")
        case _ if 400 <= code < 500 and code != 429:   # decide rejects this record: a retry cannot succeed
            await rq.zrem(TRIG_PENDING, member)
            m.inc("timeout_trigger_rejected")
        case _:                                         # 429, 5xx or no answer: retry next tick
            m.inc("timeout_trigger_post_failed")


async def timeouts(_: Sanic) -> None:
    """State-timeout trigger: payment_state stuck at paying for ABANDON_TIMEOUT_S."""
    http = rnet.Client(timeout=2, http1_only=True, tcp_nodelay=True, pool_max_idle_per_host=32)
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
                res = await pipe.execute(raise_on_error=False)
                if any(isinstance(x, NoScriptError) for x in res):
                    await load_scripts()
                m.inc("timeout_detected", sum(isinstance(x, bytes) for x in res))
            # everything still pending (new claims plus earlier failed posts) is (re)sent; decide is idempotent
            if pending := await rq.zrangebyscore(TRIG_PENDING, "-inf", "+inf", start=0, num=200):
                await asyncio.gather(*(post_trigger(http, p) for p in pending))
            gauges = m.gauges
            gauges["paying_users"] = await rq.zcard(PAYING)
            gauges["trigger_pending"] = await rq.zcard(TRIG_PENDING)
        except (RedisError, OSError) as e:
            m.inc("redis_errors")
            logger.warning("timeouts: {}", e)


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
            GL.append(await GlideClient.create(GlideClientConfiguration(
                [NodeAddress(env("REDIS_HOST", "lt-redis"), int(env("REDIS_PORT", "6379")))], request_timeout=5000)))
            break
        except HOT_ERRORS as e:        # GlideClient.create raises GlideError (ClosingError), not RedisError
            logger.warning("startup waiting for redis: {}", e)
            await asyncio.sleep(1)
    for fn in (consume, autoclaim, timeouts, gauges):
        app_.add_task(fn(app_))


if __name__ == "__main__":
    serve(app)
