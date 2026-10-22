"""lt-payments: POST /webhook, the authoritative source of payment results.

Verify HMAC, then in one transaction: insert the ledger row (idempotent by provider txn id) and redeem
the bonus when this is the user's first-ever payment and the entitlement is valid and unredeemed.
After commit, emit the server-side recharge event; a sweeper re-emits anything a failed XADD missed.
"""

import asyncio
from time import perf_counter

import msgspec
from loguru import logger
from psqlpy import ConnectionPool
from psqlpy.exceptions import Error as PGError
from redis.exceptions import RedisError
from sanic import HTTPResponse, Request, Sanic

from lt.common import PG_DSN, PG_OPTIONS, STREAM, env, json_resp, make_app, now_ms, redis_client, serve
from lt.logic import Webhook, verify

SECRET = env("WEBHOOK_SECRET", "lt-test-secret").encode()
CAMPAIGN = env("CAMPAIGN", "first_recharge_bonus")
STREAM_MAXLEN = int(env("STREAM_MAXLEN", "500000"))

SQL_LOCK = "SELECT pg_advisory_xact_lock(hashtext($1::text))"
SQL_PAY = (
    "WITH ins AS (INSERT INTO payments (provider, txn_id, user_id, amount, currency, paid_at, verified)"
    " VALUES ($1, $2, $3, $4::int, $5, to_timestamp($6::float8 / 1000), true)"
    " ON CONFLICT (provider, txn_id) DO NOTHING RETURNING id, user_id, amount),"
    " red AS (UPDATE entitlements e SET redeemed_at = now(), payment_id = ins.id,"
    " redeemed_points = ins.amount * e.bonus_pct / 100 FROM ins"
    " WHERE e.user_id = ins.user_id AND e.campaign = $7 AND e.redeemed_at IS NULL AND e.expires_at > now()"
    " AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.user_id = ins.user_id) RETURNING e.redeemed_points)"
    " SELECT (SELECT id FROM ins) AS payment_id, (SELECT redeemed_points FROM red) AS bonus"
)
SQL_EMITTED = "UPDATE payments SET emitted = true WHERE id = ANY($1::bigint[])"
SQL_UNEMITTED = (
    "SELECT id, provider, txn_id, user_id, amount FROM payments WHERE NOT emitted"
    " AND created_at < now() - interval '3 seconds' ORDER BY id LIMIT 200"
)

app, m = make_app("lt-payments")
r = redis_client(float(env("REDIS_TIMEOUT_S", "1.0")))
pool = ConnectionPool(dsn=PG_DSN, max_db_pool_size=int(env("PG_POOL", "8")), options=PG_OPTIONS)
_dec = msgspec.json.Decoder(Webhook)
_enc = msgspec.json.Encoder()
PG_ERRORS = (PGError,)


def server_event(pid: int, provider: str, txn_id: str, uid: str, amount: int) -> dict:
    d = _enc.encode({"event_id": f"pay:{provider}:{txn_id}", "user_id": uid, "session_id": "server",
                     "type": "recharge", "client_ts": now_ms(),
                     "props": {"txn_id": txn_id, "amount": amount, "payment_id": pid}})
    return {"d": d, "st": now_ms(), "src": "server"}


async def emit(rows: list[tuple[int, str, str, str, int]]) -> None:
    """XADD server recharge events, then mark them emitted. Failure leaves them for the sweeper."""
    try:
        pipe = r.pipeline(transaction=False)
        for row in rows:
            pipe.xadd(STREAM, server_event(*row), maxlen=STREAM_MAXLEN, approximate=True)
        await pipe.execute()
    except (RedisError, OSError):
        m.inc("emit_deferred", len(rows))
        return
    async with pool.acquire() as conn:
        await conn.execute(SQL_EMITTED, [[row[0] for row in rows]])
    m.inc("emitted", len(rows))


@app.post("/webhook")
async def webhook(request: Request) -> HTTPResponse:
    if not verify(SECRET, request.body, request.headers.get("x-signature", "")):
        m.inc("bad_signature")
        return json_resp({"status": "rejected", "error": "bad signature"}, 401)
    try:
        w = _dec.decode(request.body)
    except msgspec.DecodeError as e:
        m.inc("bad_body")
        return json_resp({"status": "rejected", "error": str(e)}, 400)
    t0 = perf_counter()
    try:
        async with pool.acquire() as conn, conn.transaction() as tx:
            await tx.execute(SQL_LOCK, [w.user_id])
            row = await tx.fetch_row(SQL_PAY, [w.provider, w.txn_id, w.user_id, w.amount, w.currency,
                                               w.paid_at, CAMPAIGN])
            res = row.result()
    except PG_ERRORS as e:
        m.inc("pg_unavailable")
        return json_resp({"status": "error", "detail": type(e).__name__}, 503)
    m.hists["pg_txn_ms"].observe((perf_counter() - t0) * 1000)
    if (pid := res["payment_id"]) is None:
        m.inc("duplicate")
        return json_resp({"status": "duplicate", "txn_id": w.txn_id})
    m.inc("recorded")
    if res["bonus"] is not None:
        m.inc("redeemed")
    try:
        await emit([(pid, w.provider, w.txn_id, w.user_id, w.amount)])
    except PG_ERRORS:
        m.inc("emit_mark_failed")
    return json_resp({"status": "ok", "payment_id": pid, "redeemed": res["bonus"] is not None,
                      "bonus_points": res["bonus"] or 0})


async def sweeper(_: Sanic) -> None:
    while True:
        await asyncio.sleep(3)
        try:
            async with pool.acquire() as conn:
                rows = (await conn.execute(SQL_UNEMITTED)).result()
            if rows:
                m.inc("sweeper_reemit", len(rows))
                await emit([(x["id"], x["provider"], x["txn_id"], x["user_id"], x["amount"]) for x in rows])
        except PG_ERRORS as e:
            logger.warning("sweeper: {}", e)


@app.after_server_start
async def start(app_: Sanic) -> None:
    app_.add_task(sweeper(app_))


if __name__ == "__main__":
    serve(app)
