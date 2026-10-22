import argparse
import asyncio
import math

import msgspec
import pytest
from bench.cache_bench import (
    EV_DEC,
    Bench,
    Gen,
    Pacer,
    Ticker,
    apply_args,
    drain,
    is_noscript,
    make_payload,
    open_loop,
    saturation,
    stream_entries,
)
from bench.pg_bench import (
    PAY_ABANDONED,
    PAY_CANCEL_IF_PAYING,
    PAY_COMPLETED,
    PAY_KEEP,
    PAY_PAYING,
    break_point,
    cursor_updates,
    event_delta,
    fold_events,
    lag_seconds,
    numeric_delta,
    pack,
    parse_args,
    shard_of,
)
from lt.context import keys
from lt.logic import StreamEvent
from redis.exceptions import NoScriptError, ResponseError


def ev(typ: str, uid: str = "u1", **props: str | int) -> StreamEvent:
    return StreamEvent("e1", uid, "s1", typ, 1, props)


def step(target: float, p99: float = 1.0, achieved: float | None = None, consumed: float | None = None,
         errors: dict[str, int] | None = None, cancelled: int = 0) -> dict:
    lat = {op: {"count": 100, "p99": p99} for op in ("xadd_pipeline", "apply_pipeline", "read_pipeline")}
    return {"target_eps": target, "latency_ms": lat, "achieved_eps": target if achieved is None else achieved,
            "consumed_eps": target if consumed is None else consumed, "errors": errors or {},
            "cancelled_at_drain": cancelled}


def pg_step(target: float, read_p95: float = 1.0, lag_a: float = 0.0, lag_b: float = 0.0,
            achieved: float | None = None, consumed_a: float | None = None,
            errors: dict[str, int] | None = None, cancelled: int = 0) -> dict:
    return {"target_eps": target,
            "latency_ms": {"ingest": {"count": 100, "p95": 1.0}, "read": {"count": 100, "p95": read_p95}},
            "achieved_eps": target if achieved is None else achieved,
            "consumed_eps": {"A": target if consumed_a is None else consumed_a, "B": target},
            "lag": {"A": {"s": lag_a}, "B": {"s": lag_b}}, "errors": errors or {}, "cancelled_at_drain": cancelled}


# Pacer


def test_pacer_due_after_one_second_returns_rate():
    p = Pacer(150)
    assert p.due(1.0) == 150
    assert p.due(1.0) == 0


def test_pacer_due_fractional_rate_accumulates_without_loss():
    p = Pacer(0.5)
    assert [p.due(t) for t in (0.5, 1.9, 2.0, 4.0)] == [0, 0, 1, 1]


def test_pacer_due_zero_rate_returns_zero():
    p = Pacer(0.0)
    assert p.due(100.0) == 0
    assert p.next_at() == math.inf


def test_pacer_at_and_next_at_give_due_times():
    p = Pacer(10)
    assert p.at(1) == pytest.approx(0.1)
    p.due(0.35)
    assert p.next_at() == pytest.approx(0.4)


# Ticker


def test_ticker_wake_on_time_counts_no_late():
    t = Ticker(0.005)
    for el in (0.0, 0.005, 0.010):
        t.wake(el)
        t.plan(el)
    assert t.late == 0
    assert t.wakes == 3


def test_ticker_wake_three_ticks_behind_counts_late_slots():
    t = Ticker(0.005)
    t.plan(0.0)
    t.wake(0.0201)
    assert t.late == 3


def test_ticker_plan_due_sooner_than_tick_sleeps_until_due():
    t = Ticker(0.005)
    assert t.plan(0.0, 0.003) == pytest.approx(0.003)


def test_ticker_plan_due_in_past_sleeps_minimum():
    t = Ticker(0.005)
    assert t.plan(1.0, 0.5) == pytest.approx(0.001)


# open_loop / drain


def test_open_loop_rate_and_reads_issue_expected_counts():
    seen: dict[str, list[float]] = {"ingest": [], "read": []}

    async def ingest(t: float) -> None:
        seen["ingest"].append(t)

    async def read(t: float) -> None:
        seen["read"].append(t)

    async def go() -> Ticker:
        tasks: set[asyncio.Task] = set()
        tk = await open_loop(1000, 0.1, 0.3, 10, ingest, read, tasks)
        await drain(tasks, 1.0)
        return tk

    asyncio.run(go())
    assert 25 <= len(seen["ingest"]) <= 30
    assert 25 <= len(seen["read"]) <= 30
    assert seen["ingest"] == sorted(seen["ingest"])


def test_drain_task_still_running_is_cancelled_and_counted():
    async def go() -> int:
        tasks = {asyncio.create_task(asyncio.sleep(10)), asyncio.create_task(asyncio.sleep(0))}
        return await drain(tasks, 0.05)

    assert asyncio.run(go()) == 1


def test_drain_empty_set_returns_zero():
    assert asyncio.run(drain(set(), 0.01)) == 0


# payload builder / generator


def test_make_payload_is_six_element_msgpack_array():
    raw = msgspec.msgpack.decode(make_payload("e9", 7, "watch_progress", 123))
    assert raw == ["e9", "u7", "s7", "watch_progress", 123, {"minutes": 1}]


def test_make_payload_type_without_props_has_empty_props():
    assert EV_DEC.decode(make_payload("e1", 1, "chat", 1)).props == {}


def test_gen_next_ids_unique_and_session_fixed_per_user():
    g = Gen(3, seed=1)
    evs = [EV_DEC.decode(g.next(1)[1]) for _ in range(50)]
    assert len({e.event_id for e in evs}) == 50
    assert all(e.session_id == "s" + e.user_id.removeprefix("u") for e in evs)
    assert {e.user_id for e in evs} <= {"u0", "u1", "u2"}
    assert g.last_uid == evs[-1].user_id


# apply_args / stream_entries / is_noscript


def test_apply_args_positions_match_apply_lua_argv():
    d = make_payload("e5", 4, "enter_room", 99)
    args = apply_args("sha1", d, b"1000", b"client", EV_DEC.decode(d), b"7-0")
    k = keys("u4")
    assert args[:3] == ("EVALSHA", "sha1", str(len(k)))
    assert list(args[3:3 + len(k)]) == k
    argv = args[3 + len(k):]
    assert len(argv) == 14
    assert (argv[0], argv[1], argv[2], argv[3], argv[4]) == ("e5", "enter_room", b"1000", "99", d)
    assert (argv[5], argv[6], argv[7], argv[8], argv[9]) == ("s4", "u4", b"client", "r42", "0")
    assert (argv[12], argv[13]) == ("ctx", b"7-0")


def test_stream_entries_none_returns_empty():
    assert stream_entries(None) == []
    assert stream_entries([]) == []


def test_stream_entries_redis_legacy_shape_returns_pairs():
    resp = [[b"events", [(b"1-0", {b"d": b"x"})]]]
    assert stream_entries(resp) == [(b"1-0", {b"d": b"x"})]


def test_stream_entries_redis_unified_shape_returns_pairs():
    assert stream_entries({b"events": [(b"1-0", {b"d": b"x"})]}) == [(b"1-0", {b"d": b"x"})]


def test_stream_entries_glide_shape_with_deleted_entry_returns_empty_fields():
    resp = {b"events": {b"1-0": [[b"d", b"x"], [b"st", b"5"]], b"2-0": None}}
    assert stream_entries(resp) == [(b"1-0", {b"d": b"x", b"st": b"5"}), (b"2-0", {})]


def test_is_noscript_redis_and_glide_errors_detected():
    assert is_noscript(NoScriptError("No matching script"))
    assert is_noscript(RuntimeError("NOSCRIPT No matching script. Please use EVAL."))
    assert not is_noscript(ResponseError("ERR unknown command"))


def test_is_noscript_glide_request_error_text_detected():
    from glide import RequestError

    assert is_noscript(RequestError("An error was signalled by the server: - NoScriptError: No matching script."))


def test_is_noscript_non_error_reply_ignored():
    assert not is_noscript(b"noscript-looking value")


def test_bench_read_lrange_matches_decision_window_returns_five_entries():
    class FakeStore:
        errors = (OSError,)

        def __init__(self) -> None:
            self.sent: list[tuple] = []

        async def run(self, cmds):
            self.sent.extend(cmds)
            return [{}, []]

    store = FakeStore()
    b = Bench(store, argparse.Namespace(users=10, seed=1, max_inflight=4))
    asyncio.run(b.read(0.0))
    k = keys(b.gen.last_uid)
    assert store.sent == [("HGETALL", k[0]), ("LRANGE", k[1], "0", "4")]


# saturation


def test_saturation_all_steps_pass_returns_highest():
    assert saturation([step(150), step(500), step(1000)], 10) == (1000, None)


def test_saturation_p99_over_limit_fails_step():
    assert saturation([step(150), step(500, p99=12), step(1000)], 10) == (1000, 500)


def test_saturation_achieved_below_95_percent_fails_step():
    assert saturation([step(150), step(500, achieved=470)], 10) == (150, 500)


def test_saturation_consumed_below_95_percent_fails_step():
    assert saturation([step(1000, consumed=900)], 10) == (None, 1000)


def test_saturation_no_steps_returns_none():
    assert saturation([], 10) == (None, None)


def test_saturation_reads_all_failed_without_samples_fails_step():
    # codex F9 reproduction: every Context read failed, so read_pipeline has no histogram at all
    cache = {"target_eps": 150, "achieved_eps": 150, "consumed_eps": 150,
             "latency_ms": {"xadd_pipeline": {"count": 150, "p99": 1}, "apply_pipeline": {"count": 150, "p99": 1}},
             "errors": {"read_pipeline": 100}, "cancelled_at_drain": 0}
    assert saturation([cache], 10) == (None, 150)


def test_saturation_op_missing_from_latency_fails_step():
    s = step(150)
    del s["latency_ms"]["apply_pipeline"]
    assert saturation([s], 10) == (None, 150)


def test_saturation_op_count_zero_fails_step():
    s = step(150)
    s["latency_ms"]["read_pipeline"] = {"count": 0, "p99": 0.0}
    assert saturation([s], 10) == (None, 150)


def test_saturation_errors_with_samples_present_fails_step():
    assert saturation([step(150), step(500, errors={"xadd_pipeline": 1})], 10) == (150, 500)


def test_saturation_xreadgroup_errors_fails_step():
    assert saturation([step(150, errors={"xreadgroup": 2})], 10) == (None, 150)


def test_saturation_cancelled_at_drain_fails_step():
    assert saturation([step(150), step(500, cancelled=3)], 10) == (150, 500)


def test_saturation_noscript_reload_alone_passes():
    assert saturation([step(150, errors={"noscript_reload": 1})], 10) == (150, None)


# pg_bench pure helpers


def test_pack_offsets_are_one_based_for_substr():
    blob, offs, lens = pack([b"ab", b"", b"cde"])
    assert blob == b"abcde"
    assert [blob[o - 1:o - 1 + n] for o, n in zip(offs, lens, strict=True)] == [b"ab", b"", b"cde"]


def test_pack_empty_list_returns_empty_arrays():
    assert pack([]) == (b"", [], [])


def test_cursor_updates_takes_last_row_and_count_per_shard():
    rows = [{"shard": 0, "tx": "5", "id": 1, "ts": 10}, {"shard": 0, "tx": "7", "id": 3, "ts": 11},
            {"shard": 2, "tx": "6", "id": 2, "ts": 12}]
    assert cursor_updates(rows) == ([0, 2], ["7", "6"], [3, 2], [11, 12], [2, 1])


def test_cursor_updates_empty_batch_returns_empty_arrays():
    assert cursor_updates([]) == ([], [], [], [], [])


@pytest.mark.parametrize(("typ", "expected"), [
    ("watch_progress", (1, 0, 0, 0, PAY_KEEP)), ("enter_room", (0, 1, 0, 0, PAY_KEEP)),
    ("chat", (0, 0, 1, 0, PAY_KEEP)), ("send_gift", (0, 0, 0, 1, PAY_KEEP)),
    ("click_recharge", (0, 0, 0, 0, PAY_PAYING)), ("cancel_recharge", (0, 0, 0, 0, PAY_CANCEL_IF_PAYING)),
    ("recharge", (0, 0, 0, 0, PAY_COMPLETED)), ("leave_room", (0, 0, 0, 0, PAY_KEEP)),
])
def test_event_delta_each_type_returns_its_counters(typ, expected):
    props = {"minutes": 1} if typ == "watch_progress" else {}
    assert event_delta(ev(typ, **props)) == expected


def test_event_delta_minutes_not_int_counts_zero():
    assert event_delta(ev("watch_progress", minutes="3"))[0] == 0


def test_fold_events_sums_counters_per_user():
    out = fold_events([ev("chat"), ev("chat"), ev("watch_progress", minutes=2), ev("chat", uid="u2")])
    assert out == {"u1": [2, 0, 2, 0, PAY_KEEP], "u2": [0, 0, 1, 0, PAY_KEEP]}


def test_fold_events_click_then_cancel_in_batch_is_abandoned():
    assert fold_events([ev("click_recharge"), ev("cancel_recharge")])["u1"][4] == PAY_ABANDONED


def test_fold_events_cancel_without_click_defers_to_stored_state():
    assert fold_events([ev("chat"), ev("cancel_recharge")])["u1"][4] == PAY_CANCEL_IF_PAYING


def test_fold_events_cancel_after_completed_keeps_completed():
    assert fold_events([ev("recharge"), ev("cancel_recharge")])["u1"][4] == PAY_COMPLETED


def test_fold_events_empty_returns_empty():
    assert fold_events([]) == {}


def test_lag_seconds_nothing_unread_is_zero():
    assert lag_seconds(None, 5000) == 0.0


def test_lag_seconds_oldest_unread_gives_age():
    assert lag_seconds(1000, 3500) == 2.5


def test_lag_seconds_clock_skew_never_negative():
    assert lag_seconds(4000, 3500) == 0.0


def test_break_point_read_p95_over_budget_fails_step():
    assert break_point([pg_step(150), pg_step(300, read_p95=25)], 20, 1.0) == (150, 300)


def test_break_point_lag_growing_more_than_limit_fails_step():
    steps = [pg_step(150), pg_step(300, lag_a=0.5), pg_step(600, lag_a=2.0)]
    assert break_point(steps, 20, 1.0) == (300, 600)


def test_break_point_lag_high_but_stable_passes():
    steps = [pg_step(150, lag_b=0.9), pg_step(300, lag_b=1.5), pg_step(600, lag_b=1.6)]
    assert break_point(steps, 20, 1.0) == (600, None)


def test_break_point_no_steps_returns_none():
    assert break_point([], 20, 1.0) == (None, None)


def test_break_point_ingest_and_read_failed_zero_throughput_fails_step():
    # codex F9 reproduction: ingest and reads all failed, stats queries succeeded, the stream stayed empty
    pg = {"target_eps": 9600, "achieved_eps": 0, "consumed_eps": {"A": 0, "B": 0}, "latency_ms": {},
          "lag": {"A": {"s": 0}, "B": {"s": 0}}, "errors": {"ingest": 100, "read": 100}, "cancelled_at_drain": 0}
    assert break_point([pg], 20, 1) == (None, 9600)


def test_break_point_read_missing_samples_fails_step():
    s = pg_step(150)
    del s["latency_ms"]["read"]
    assert break_point([s], 20, 1.0) == (None, 150)


def test_break_point_ingest_count_zero_fails_step():
    s = pg_step(150)
    s["latency_ms"]["ingest"] = {"count": 0, "p95": 0.0}
    assert break_point([s], 20, 1.0) == (None, 150)


def test_break_point_achieved_below_95_percent_fails_step():
    assert break_point([pg_step(150), pg_step(300, achieved=280)], 20, 1.0) == (150, 300)


def test_break_point_group_consumed_below_95_percent_fails_step():
    assert break_point([pg_step(150), pg_step(300, consumed_a=280)], 20, 1.0) == (150, 300)


def test_break_point_errors_with_samples_present_fails_step():
    assert break_point([pg_step(150), pg_step(300, errors={"read": 1})], 20, 1.0) == (150, 300)


def test_break_point_worker_error_fails_step():
    assert break_point([pg_step(150, errors={"worker_B": 1})], 20, 1.0) == (None, 150)


def test_break_point_cancelled_at_drain_fails_step():
    assert break_point([pg_step(150), pg_step(300, cancelled=5)], 20, 1.0) == (150, 300)


def test_break_point_simulated_crash_and_lease_lost_alone_pass():
    errs = {"simulated_crash_holding_lease": 1, "lease_lost_A": 2}
    assert break_point([pg_step(150, errors=errs)], 20, 1.0) == (150, None)


def test_numeric_delta_skips_non_numeric_and_bool():
    assert numeric_delta({"a": 1, "c": 1.0}, {"a": 4, "b": "x", "c": 2.5, "d": True}) == {"a": 3, "c": 1.5}


def test_shard_of_stable_and_in_range():
    assert shard_of("u42", 8) == shard_of("u42", 8)
    assert {shard_of(f"u{i}", 8) for i in range(200)} == set(range(8))


def test_parse_args_no_action_exits():
    with pytest.raises(SystemExit):
        parse_args([])


def test_parse_args_pool_not_above_workers_exits():
    with pytest.raises(SystemExit):
        parse_args(["--mode", "baseline", "--pool", "3", "--workers-a", "2", "--workers-b", "1"])


def test_parse_args_teardown_alone_is_valid():
    assert parse_args(["--teardown"]).teardown
