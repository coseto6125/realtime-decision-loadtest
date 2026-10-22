"""lt-collector: POST /events validates a batch and appends it to the Redis Stream in one round trip."""

import msgspec
from redis.exceptions import RedisError
from sanic import HTTPResponse, Request

from lt.common import QUARANTINE, STREAM, env, json_resp, make_app, now_ms, redis_client, serve
from lt.logic import partition_batch

MAX_BATCH = int(env("MAX_BATCH", "500"))
STREAM_MAXLEN = int(env("STREAM_MAXLEN", "500000"))

app, m = make_app("lt-collector")
r = redis_client(float(env("REDIS_TIMEOUT_S", "1.0")))


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
        st = now_ms()
        pipe = r.pipeline(transaction=False)
        for d in good:
            pipe.xadd(STREAM, {"d": d, "st": st, "src": "client"}, maxlen=STREAM_MAXLEN, approximate=True)
        for d, err in bad:
            pipe.xadd(QUARANTINE, {"d": d, "err": err, "st": st}, maxlen=100_000, approximate=True)
        try:
            await pipe.execute()
        except (RedisError, OSError) as e:
            m.inc("redis_unavailable")
            return json_resp({"error": "event store unavailable, retry", "detail": type(e).__name__}, 503)
    m.inc("accepted", len(good))
    m.inc("quarantined", len(bad))
    m.inc("batches")
    return json_resp({"accepted": len(good), "quarantined": len(bad)})


if __name__ == "__main__":
    serve(app)
