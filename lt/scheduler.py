"""lt-scheduler: next-day decisions on a compressed clock ("next day" = DAY_S seconds).

Claims due jobs with FOR UPDATE SKIP LOCKED, so replicas never take the same job. Each check reads the
source of truth (payments, entitlements) under a per-user advisory lock that the payment webhook also
takes, and the unique constraints on entitlements and contact_log are the second line of defence.
"""

import asyncio
import socket
import time
from time import perf_counter

import msgspec
from loguru import logger
from psqlpy import ConnectionPool
from psqlpy.exceptions import Error as PGError
from sanic import Sanic

from lt.common import CAMPAIGN, DAY_S, PG_DSN, PG_OPTIONS, SQL_LOCK, env, envf, make_app, serve
from lt.logic import bucket

TICK_S = envf("TICK_S", 2)
CLAIM = int(env("CLAIM_BATCH", "50"))
BONUS_PCT = 20
VALID_S = DAY_S * 3                     # 72 hours on the compressed clock
REPLICA = env("REPLICA", socket.gethostname())

SQL_CLAIM = (
    "SELECT id, user_id, arm, abandoned_at FROM next_day_jobs WHERE status = 'due' AND due_at <= now()"
    " ORDER BY due_at LIMIT $1 FOR UPDATE SKIP LOCKED"
)
SQL_FACTS = (
    "SELECT (SELECT count(*) FROM payments WHERE user_id = $1 AND created_at >= $2) AS paid_after,"
    " (SELECT count(*) FROM payments WHERE user_id = $1 AND created_at < $2) AS paid_before,"
    " EXISTS (SELECT 1 FROM entitlements WHERE user_id = $1 AND campaign = $3) AS granted"
)
SQL_GRANT = (
    "WITH c AS (INSERT INTO contact_log (user_id, day, channel) VALUES ($1, $2, 'push')"
    " ON CONFLICT (user_id, day) DO NOTHING RETURNING id),"
    " e AS (INSERT INTO entitlements (user_id, campaign, bonus_pct, expires_at, job_id)"
    " SELECT $1, $3, $4, now() + make_interval(secs => $5::float8), $6 WHERE EXISTS (SELECT 1 FROM c)"
    " ON CONFLICT (user_id, campaign) DO NOTHING RETURNING id),"
    " o AS (INSERT INTO outbox (job_id, day, user_id, action, payload)"
    " SELECT $6, $2, $1, 'push_bonus_offer', $7 FROM e RETURNING id)"
    " SELECT (SELECT id FROM c) AS contact, (SELECT id FROM e) AS ent"
)
SQL_DONE = (
    "UPDATE next_day_jobs SET status = 'done', result = $2, processed_by = $3, processed_at = now(),"
    " attempts = attempts + 1 WHERE id = $1"
)
SQL_RELAY = (
    "WITH s AS (SELECT id FROM outbox WHERE status = 'pending' ORDER BY id LIMIT 1000 FOR UPDATE SKIP LOCKED)"
    " UPDATE outbox o SET status = 'sent', sent_at = now() FROM s WHERE o.id = s.id"
)
SQL_BACKLOG = (
    "SELECT (SELECT count(*) FROM next_day_jobs WHERE status = 'due' AND due_at <= now()) AS due,"
    " (SELECT count(*) FROM outbox WHERE status = 'pending') AS outbox_pending"
)

app, m = make_app("lt-scheduler")
pool = ConnectionPool(dsn=PG_DSN, max_db_pool_size=4, options=PG_OPTIONS)
PG_ERRORS = (PGError,)
_mp = msgspec.msgpack.Encoder()


class PushOffer(msgspec.Struct, array_like=True):
    """Outbox payload for the push relay (machine-only, msgpack)."""

    user_id: str
    campaign: str
    bonus_pct: int
    valid_s: float


def push_consent(uid: str) -> bool:
    """Stand-in for the push service's live consent lookup (90% consent)."""
    return bucket("consent", uid) >= 10


async def run_job(tx, job: dict, day: int) -> str:
    uid = job["user_id"]
    if job["arm"] == "holdout":
        return "holdout_no_action"
    await tx.execute(SQL_LOCK, [uid])
    f = (await tx.fetch_row(SQL_FACTS, [uid, job["abandoned_at"], CAMPAIGN])).result()
    if f["paid_after"]:
        return "paid_after_abandonment"
    if f["paid_before"]:
        return "prior_payer_no_offer"
    if f["granted"]:
        return "lifetime_once"
    if not push_consent(uid):
        return "no_push_consent"
    payload = _mp.encode(PushOffer(user_id=uid, campaign=CAMPAIGN, bonus_pct=BONUS_PCT, valid_s=VALID_S))
    g = (await tx.fetch_row(SQL_GRANT, [uid, day, CAMPAIGN, BONUS_PCT, VALID_S, job["id"], payload])).result()
    if g["contact"] is None:
        return "daily_push_cap"
    if g["ent"] is None:
        return "lifetime_once_race"
    return "pushed_offer"


async def tick() -> int:
    t0 = perf_counter()
    async with pool.acquire() as conn, conn.transaction() as tx:
        jobs = (await tx.execute(SQL_CLAIM, [CLAIM])).result()
        jobs.sort(key=lambda j: j["user_id"])          # one lock order across replicas: no deadlock
        day = int(time.time() // DAY_S)
        for job in jobs:
            result = await run_job(tx, job, day)
            await tx.execute(SQL_DONE, [job["id"], result, REPLICA])
            m.inc(f"job.{result}")
    if jobs:
        m.hists["claim_batch_ms"].observe((perf_counter() - t0) * 1000)
        m.inc("jobs", len(jobs))
    return len(jobs)


async def loop(_: Sanic) -> None:
    while True:
        try:
            while await tick() == CLAIM:               # drain a backlog without waiting a tick
                pass
            async with pool.acquire() as conn:
                await conn.execute(SQL_RELAY)
                b = (await conn.fetch_row(SQL_BACKLOG)).result()
            m.gauges["jobs_due_backlog"] = b["due"]
            m.gauges["outbox_pending"] = b["outbox_pending"]
        except PG_ERRORS as e:
            m.inc("pg_errors")
            logger.warning("scheduler: {}", e)
        await asyncio.sleep(TICK_S)


@app.after_server_start
async def start(app_: Sanic) -> None:
    app_.add_task(loop(app_))


if __name__ == "__main__":
    serve(app)
