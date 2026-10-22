"""End-to-end behaviour against a running stack (compose up, ports tunnelled to 127.0.0.1).

Run: LT_IT=1 uv run pytest tests/test_integration.py -q
The DAY_S and ABANDON_TIMEOUT_S values must match the stack (defaults 30 s and 10 s).
"""

import asyncio
import itertools
import os
import time
import uuid

import aiohttp
import msgspec
import pytest
from psqlpy import ConnectionPool
from redis.asyncio import Redis

from lt.logic import Policy, assign_arm, sign

pytestmark = pytest.mark.skipif(not os.environ.get("LT_IT"), reason="needs a running stack (LT_IT=1)")

BASE = {name: f"http://127.0.0.1:{port}" for name, port in
        [("collector", 17101), ("decision", 17102), ("payments", 17103)]}
SECRET = os.environ.get("WEBHOOK_SECRET", "lt-test-secret").encode()   # test-only default
DAY_S = float(os.environ.get("DAY_S", "30"))
ABANDON_S = float(os.environ.get("ABANDON_TIMEOUT_S", "10"))
PG = "postgres://lt:lt-test-only@127.0.0.1:17110/lt"
ABANDON = Policy(trigger="abandonment", version=1, salt="exp-abandon-2026-10",
                 arms=[("holdout", 34), ("rule", 33), ("model", 33)])
RUN = uuid.uuid4().hex[:8]
_SEQ = itertools.count()


def now() -> int:
    return time.time_ns() // 1_000_000


def user(arm: str, prefix: str = "it") -> str:
    """A fresh never-paid user id whose abandonment arm is `arm`; never the same id twice."""
    for i in _SEQ:
        uid = f"{prefix}{RUN}x{i}"
        if assign_arm(uid, ABANDON) == arm:
            return uid


def ev(uid: str, sid: str, typ: str, **props: object) -> dict:
    return {"event_id": f"{sid}-{uuid.uuid4().hex[:10]}", "user_id": uid, "session_id": sid, "type": typ,
            "client_ts": now(), "props": props}


async def post(s: aiohttp.ClientSession, svc: str, path: str, payload: object, headers: dict | None = None):
    body = payload if isinstance(payload, bytes) else msgspec.json.encode(payload)
    async with s.post(BASE[svc] + path, data=body, headers={"content-type": "application/json", **(headers or {})}) as r:
        return r.status, msgspec.json.decode(await r.read())


async def webhook(s: aiohttp.ClientSession, uid: str, txn: str, secret: bytes = SECRET, amount: int = 300):
    body = msgspec.json.encode({"provider": "tappay", "txn_id": txn, "user_id": uid, "amount": amount,
                                "currency": "TWD", "status": "paid", "paid_at": now()})
    return await post(s, "payments", "/webhook", body, {"x-signature": sign(secret, body)})


async def abandon(s: aiohttp.ClientSession, uid: str, platform: str = "android") -> tuple[str, dict]:
    sid = f"s{uuid.uuid4().hex[:10]}"
    cancel = ev(uid, sid, "cancel_recharge")
    batch = [ev(uid, sid, "login", platform=platform), ev(uid, sid, "enter_room", room="room_1"),
             ev(uid, sid, "watch_progress", minutes=30), ev(uid, sid, "click_recharge", plan=300), cancel]
    assert (await post(s, "collector", "/events", batch))[0] == 200
    await asyncio.sleep(0.3)
    status, body = await post(s, "decision", "/decide", {"event_id": cancel["event_id"], "user_id": uid,
                                                         "trigger": "abandonment", "ts": now(), "session_id": sid,
                                                         "platform": platform})
    assert status == 200
    return sid, body


async def rows(sql: str, params: list | None = None) -> list[dict]:
    pool = ConnectionPool(dsn=PG, max_db_pool_size=2)
    try:
        async with pool.acquire() as conn:
            return (await conn.execute(sql, params or [])).result()
    finally:
        pool.close()


def run(coro):
    return asyncio.run(coro)


def test_decide_same_event_twice_in_flight_returns_one_decision():
    async def go():
        async with aiohttp.ClientSession() as s:
            uid = user("model")
            sid = f"s{uuid.uuid4().hex[:8]}"
            e = ev(uid, sid, "cancel_recharge")
            await post(s, "collector", "/events", [ev(uid, sid, "click_recharge", plan=300), e])
            await asyncio.sleep(0.3)
            req = {"event_id": e["event_id"], "user_id": uid, "trigger": "abandonment", "ts": now(), "session_id": sid}
            a, b = await asyncio.gather(post(s, "decision", "/decide", req), post(s, "decision", "/decide", req))
            later = await post(s, "decision", "/decide", req)
            assert a == b == later
            n = await rows("SELECT count(*) AS n FROM decisions WHERE event_id = $1", [e["event_id"]])
            assert n[0]["n"] == 1
    run(go())


def test_holdout_abandonment_gets_no_action_and_no_offer_next_day():
    async def go():
        async with aiohttp.ClientSession() as s:
            uid = user("holdout")
            _, body = await abandon(s, uid)
            assert (body["arm"], body["action"]) == ("holdout", "none")
            await asyncio.sleep(DAY_S + 6)
            jobs = await rows("SELECT result FROM next_day_jobs WHERE user_id = $1", [uid])
            assert [j["result"] for j in jobs] == ["holdout_no_action"]
            assert not await rows("SELECT 1 FROM entitlements WHERE user_id = $1", [uid])
    run(go())


def test_next_day_offer_then_webhook_redeems_once_and_replay_is_duplicate():
    async def go():
        async with aiohttp.ClientSession() as s:
            uid = user("model")
            await abandon(s, uid)
            await asyncio.sleep(DAY_S + 6)
            jobs = await rows("SELECT result FROM next_day_jobs WHERE user_id = $1", [uid])
            if jobs[0]["result"] == "no_push_consent":
                pytest.skip("user drew no push consent")
            assert jobs[0]["result"] == "pushed_offer"
            txn = f"it-{uuid.uuid4().hex}"
            (s1, b1), (s2, b2) = await asyncio.gather(webhook(s, uid, txn), webhook(s, uid, txn))
            assert {b1["status"], b2["status"]} == {"ok", "duplicate"}
            assert (await webhook(s, uid, txn))[1]["status"] == "duplicate"
            ent = await rows("SELECT redeemed_at IS NOT NULL AS r, redeemed_points FROM entitlements WHERE user_id = $1", [uid])
            assert ent == [{"r": True, "redeemed_points": 60}]
            assert len(await rows("SELECT 1 FROM payments WHERE txn_id = $1", [txn])) == 1
            second = await webhook(s, uid, f"it-{uuid.uuid4().hex}")
            assert second[1]["redeemed"] is False
    run(go())


def test_prior_payer_gets_no_offer():
    async def go():
        async with aiohttp.ClientSession() as s:
            uid = next(f"u{i}" for i in range(1, 10_001) if assign_arm(f"u{i}", ABANDON) == "rule")
            _, body = await abandon(s, uid)
            await asyncio.sleep(DAY_S + 6)
            res = await rows("SELECT result FROM next_day_jobs WHERE decision_id = (SELECT id FROM decisions WHERE event_id = $1)",
                             [body["event_id"]])
            assert res[0]["result"] in ("prior_payer_no_offer", "lifetime_once", "paid_after_abandonment")
    run(go())


def test_bad_signature_rejected_and_not_written():
    async def go():
        async with aiohttp.ClientSession() as s:
            txn = f"bad-it-{uuid.uuid4().hex}"
            status, body = await webhook(s, "u42", txn, secret=b"wrong")
            assert status == 401
            assert not await rows("SELECT 1 FROM payments WHERE txn_id = $1", [txn])
    run(go())


def test_forged_client_recharge_never_redeems():
    async def go():
        async with aiohttp.ClientSession() as s:
            uid = user("rule", prefix="f")
            sid, _ = await abandon(s, uid)
            await asyncio.sleep(DAY_S + 6)
            await post(s, "collector", "/events", [ev(uid, sid, "recharge", amount=300, source="client")])
            await asyncio.sleep(2)
            assert not await rows("SELECT 1 FROM payments WHERE user_id = $1", [uid])
            assert not await rows("SELECT 1 FROM entitlements WHERE user_id = $1 AND redeemed_at IS NOT NULL", [uid])
    run(go())


def test_state_timeout_trigger_fires_without_cancel_and_not_after_payment():
    async def go():
        async with aiohttp.ClientSession() as s:
            quitter, payer = user("rule"), user("model")
            for uid in (quitter, payer):
                sid = f"s{uuid.uuid4().hex[:8]}"
                await post(s, "collector", "/events", [ev(uid, sid, "login", platform="android"),
                                                       ev(uid, sid, "click_recharge", plan=300)])
            await webhook(s, payer, f"it-{uuid.uuid4().hex}")
            await asyncio.sleep(ABANDON_S + 4)
            q = await rows("SELECT event_id FROM decisions WHERE user_id = $1", [quitter])
            p = await rows("SELECT event_id FROM decisions WHERE user_id = $1", [payer])
            assert [r["event_id"].split(":")[0] for r in q] == ["timeout"]
            assert p == []
    run(go())


def test_duplicate_events_apply_once_and_malformed_is_quarantined():
    async def go():
        async with aiohttp.ClientSession() as s:
            r = Redis(host="127.0.0.1", port=17111)
            uid = f"dup{RUN}"
            sid = f"s{uuid.uuid4().hex[:8]}"
            w = ev(uid, sid, "watch_progress", minutes=5)
            q0 = await r.xlen("events:quarantine")
            st, body = await post(s, "collector", "/events", [ev(uid, sid, "login"), w, {"type": "nope"}, w])
            assert (st, body) == (200, {"accepted": 3, "quarantined": 1})
            await asyncio.gather(post(s, "collector", "/events", [w]), post(s, "collector", "/events", [w]))
            await asyncio.sleep(0.5)
            assert await r.hget(f"ctx:{{{uid}}}", "watch_minutes") == b"5"
            assert await r.xlen("events:quarantine") == q0 + 1
            await r.aclose()
    run(go())


def test_empty_batch_and_invalid_body():
    async def go():
        async with aiohttp.ClientSession() as s:
            assert await post(s, "collector", "/events", []) == (200, {"accepted": 0, "quarantined": 0})
            assert (await post(s, "collector", "/events", b"{oops"))[0] == 400
            assert (await post(s, "decision", "/decide", b'{"event_id":"x"}'))[0] == 400
    run(go())
