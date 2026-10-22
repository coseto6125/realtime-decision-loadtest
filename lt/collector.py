"""lt-collector: POST /events validates a batch and appends it to the Redis Stream in one round trip.

Stream entries carry the event as a msgpack array (lt.logic.StreamEvent); lt-context decodes it straight
into a typed Struct. One worker process: the container has 0.5 vCPU.
"""

import asyncio

import msgspec
from loguru import logger
from redis.exceptions import NoScriptError, RedisError
from sanic import HTTPResponse, Request, Sanic

from lt.common import (
    QUARANTINE, QUARANTINE_MAXLEN, STREAM, STREAM_MAXLEN, env, json_resp, make_app, now_ms, redis_client, serve,
)
from lt.logic import partition_batch, stream_entry

MAX_BATCH = int(env("MAX_BATCH", "500"))

XADD_MODE = env("XADD_MODE", "lua")      # lua: one EVALSHA per request; pipeline: one XADD per event

# One command per request instead of one per event: the client packs a single EVALSHA, the server runs
# the same XADDs. events and events:quarantine sit in different hash slots: this script needs a single-node store.
XADD_BATCH_LUA = f"""
local st, maxlen, n = ARGV[1], ARGV[2], tonumber(ARGV[3])
for i = 4, 3 + n do
  redis.call('XADD', KEYS[1], 'MAXLEN', '~', maxlen, '*', 'd', ARGV[i], 'st', st, 'src', 'client')
end
for i = 4 + n, #ARGV, 2 do
  redis.call('XADD', KEYS[2], 'MAXLEN', '~', {QUARANTINE_MAXLEN}, '*', 'd', ARGV[i], 'err', ARGV[i + 1], 'st', st)
end
return n
"""

app, m = make_app("lt-collector")
r = redis_client(float(env("REDIS_TIMEOUT_S", "1.0")))
SHA: list[bytes] = []


async def append(good: list, bad: list[tuple[bytes, str]], st: int) -> None:
    if XADD_MODE == "pipeline":
        pipe = r.pipeline(transaction=False)
        for ev in good:
            pipe.xadd(STREAM, {"d": stream_entry(ev), "st": st, "src": "client"}, maxlen=STREAM_MAXLEN,
                      approximate=True)
        for d, err in bad:
            pipe.xadd(QUARANTINE, {"d": d, "err": err, "st": st}, maxlen=QUARANTINE_MAXLEN, approximate=True)
        await pipe.execute()
        return
    args = [st, STREAM_MAXLEN, len(good), *map(stream_entry, good)]
    for d, err in bad:
        args += (d, err)
    try:
        await r.execute_command("EVALSHA", SHA[0], 2, STREAM, QUARANTINE, *args)
    except NoScriptError:              # script cache lost (store restart): reload once and retry
        SHA[0] = await r.script_load(XADD_BATCH_LUA)
        await r.execute_command("EVALSHA", SHA[0], 2, STREAM, QUARANTINE, *args)


@app.post("/events")
async def events(request: Request) -> HTTPResponse:
    try:
        good, bad = partition_batch(request.body)
    except msgspec.DecodeError as e:
        m.inc("batch_rejected")
        return json_resp({"error": "body must be a JSON array of events", "detail": str(e)}, 400)
    n = len(good) + len(bad)
    if n > MAX_BATCH:
        m.inc("batch_too_large")
        return json_resp({"error": f"batch exceeds {MAX_BATCH} events"}, 413)
    if n:
        try:
            await append(good, bad, now_ms())
        except (RedisError, OSError) as e:
            m.inc("redis_unavailable")
            m.inc(f"redis_unavailable.{type(e).__name__}")
            if m.counters["redis_unavailable"] <= 20:
                logger.warning("event store error: {}: {}", type(e).__name__, e)
            return json_resp({"error": "event store unavailable, retry", "detail": type(e).__name__}, 503)
    m.inc("accepted", len(good))
    m.inc("quarantined", len(bad))
    m.inc("batches")
    return json_resp({"accepted": len(good), "quarantined": len(bad)})


@app.before_server_start
async def setup(_: Sanic) -> None:
    while True:
        try:
            SHA.append(await r.script_load(XADD_BATCH_LUA))
            return
        except (RedisError, OSError) as e:
            logger.warning("waiting for redis: {}", e)
            await asyncio.sleep(1)


if __name__ == "__main__":
    serve(app)
