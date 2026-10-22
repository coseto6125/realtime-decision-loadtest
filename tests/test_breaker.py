import asyncio

import pytest

import lt.decision as decision
from lt.decision import Breaker
from lt.logic import Policy


def fed(outcomes: list[bool], t0: float = 100.0) -> Breaker:
    b = Breaker()
    for i, timed_out in enumerate(outcomes):
        b.record(t0 + i * 0.1, timed_out)
    return b


def test_breaker_closed_at_exactly_20_percent_stays_closed():
    assert fed([True] * 2 + [False] * 8).allow(101.0)


def test_breaker_above_20_percent_with_min_calls_opens():
    b = fed([True] * 3 + [False] * 7)
    assert not b.allow(101.0)
    assert not b.allow(100.9 + Breaker.OPEN_S - 0.1)


def test_breaker_too_few_calls_never_opens():
    assert fed([True] * 9).allow(101.0)


def test_breaker_half_open_allows_one_probe_then_closes_on_success():
    b = fed([True] * 10)
    t = 101.0 + Breaker.OPEN_S
    assert b.allow(t)
    assert not b.allow(t)              # second caller while the probe is in flight
    b.record(t + 0.05, False)
    assert b.allow(t + 0.1)
    assert b.allow(t + 0.2)


def test_breaker_failed_probe_reopens():
    b = fed([True] * 10)
    t = 101.0 + Breaker.OPEN_S
    assert b.allow(t)
    b.record(t + 0.15, True)
    assert not b.allow(t + 1)
    assert b.allow(t + 0.15 + Breaker.OPEN_S)


def test_breaker_old_calls_leave_the_window():
    b = fed([True] * 2 + [False] * 8, t0=0.0)
    b.record(50.0, True)               # earlier calls are now outside the 10 s window
    assert b.allow(50.1)


class _Hang:
    """An rnet client whose model call never answers, so the caller can cancel it mid-flight."""

    async def post(self, *_a, **_k):
        await asyncio.Event().wait()


def test_judge_cancelled_half_open_probe_reopens_breaker(monkeypatch):
    brk = Breaker()
    brk.open_until = 1.0                 # expired: the next allow() is the half-open probe
    monkeypatch.setattr(decision, "BRK", brk)
    monkeypatch.setattr(decision, "BREAKER", True)
    monkeypatch.setattr(decision, "HTTP", [_Hang()])
    pol = Policy(trigger="abandonment", version=1, salt="s", arms=[("model", 100)], options=["a", "b"])

    async def run() -> None:
        task = asyncio.create_task(decision.judge("state", pol))
        await asyncio.sleep(0.01)
        assert brk.probing
        task.cancel()                    # Sanic cancels the handler when the client disconnects
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert not brk.probing               # the cancelled probe counted as failed: open again, not stuck
    assert not brk.allow(brk.open_until - 1)
    assert brk.allow(brk.open_until)     # and half-open again after OPEN_S
