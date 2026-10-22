import random
import statistics

import msgspec
import pytest

from lt.common import Hist
from lt.logic import (
    MAX_WATCH_MINUTES, DecisionIn, Policy, TimeoutMember, assign_arm, choose_action, enforce_guardrails,
    mock_latency_ms, mock_probs, partition_batch, sign, stream_entry, timeout_event_id, verify, watch_minutes,
)

ABANDON = Policy(trigger="abandonment", version=1, salt="exp-a", arms=[("holdout", 34), ("rule", 33), ("model", 33)],
                 options=["price_sensitive", "payment_friction", "just_browsing", "about_to_leave"])
EV = {"event_id": "e1", "user_id": "u1", "session_id": "s1", "type": "chat", "client_ts": 1, "props": {}}


def body(*events: object) -> bytes:
    return msgspec.json.encode(list(events))


def req(trigger: str = "abandonment", source: str = "app", platform: str = "android") -> DecisionIn:
    return DecisionIn(event_id="e1", user_id="u1", trigger=trigger, ts=0, platform=platform, source=source)


def test_partition_batch_empty_array_accepts_nothing():
    assert partition_batch(b"[]") == ([], [])


def test_partition_batch_malformed_among_valid_quarantines_only_bad():
    good, bad = partition_batch(body(EV, {**EV, "type": "teleport"}, {**EV, "event_id": ""}, {"x": 1}, EV))
    assert len(good) == 2
    assert len(bad) == 3
    assert all(err for _, err in bad)


@pytest.mark.parametrize("payload", [b"", b"   ", b"{}", b'"x"', b"[1,", b"null"])
def test_partition_batch_not_an_array_raises(payload):
    with pytest.raises(msgspec.DecodeError):
        partition_batch(payload)


def test_partition_batch_scalar_where_object_expected_is_quarantined():
    good, bad = partition_batch(b'[1, "x", null]')
    assert good == []
    assert len(bad) == 3


def test_partition_batch_props_list_value_is_quarantined():
    _, bad = partition_batch(body({**EV, "props": {"a": [1, 2]}}))
    assert len(bad) == 1


def test_assign_arm_stable_across_calls_and_follows_weights():
    users = [f"u{i}" for i in range(30000)]
    arms = [assign_arm(u, ABANDON) for u in users]
    assert arms == [assign_arm(u, ABANDON) for u in users]
    share = {a: arms.count(a) / len(arms) for a in ("holdout", "rule", "model")}
    assert abs(share["holdout"] - 0.34) < 0.02
    assert abs(share["rule"] - 0.33) < 0.02


def test_assign_arm_independent_between_salts():
    other = msgspec.structs.replace(ABANDON, salt="exp-b")
    users = [f"u{i}" for i in range(20000)]
    both = sum(assign_arm(u, ABANDON) == "holdout" and assign_arm(u, other) == "holdout" for u in users)
    assert abs(both / len(users) - 0.34 * 0.34) < 0.02


@pytest.mark.parametrize("trigger", ["abandonment", "leave_room", "watch_30min", "login"])
def test_choose_action_holdout_never_acts(trigger):
    assert choose_action(trigger, "holdout", ("price_sensitive", 0.9), ABANDON, {}, "e1") == ("none", "holdout")


@pytest.mark.parametrize(("intent", "expected"), [
    (("price_sensitive", 0.5), "small_plan_hint"),
    (("payment_friction", 0.41), "alt_payment_hint"),
    (("just_browsing", 0.6), "none"),
    (("about_to_leave", 0.6), "none"),
    (("payment_friction", 0.39), "small_plan_hint"),
    (None, "small_plan_hint"),
])
def test_choose_action_model_arm_follows_intent_threshold_and_default(intent, expected):
    assert choose_action("abandonment", "model", intent, ABANDON, {}, "e1")[0] == expected


def test_choose_action_rule_arm_is_one_of_two_hints_and_deterministic():
    picks = {choose_action("abandonment", "rule", None, ABANDON, {}, f"e{i}")[0] for i in range(200)}
    assert picks == {"small_plan_hint", "alt_payment_hint"}
    assert choose_action("abandonment", "rule", None, ABANDON, {}, "e7") == choose_action(
        "abandonment", "rule", None, ABANDON, {}, "e7")


def test_choose_action_watch_30min_skips_followers():
    assert choose_action("watch_30min", "rule", None, ABANDON, {"followed": "1"}, "e")[0] == "none"
    assert choose_action("watch_30min", "rule", None, ABANDON, {}, "e")[0] == "follow_prompt"


def test_guardrails_missing_context_blocks_app_trigger():
    assert enforce_guardrails("small_plan_hint", "r", req(), None, 10_000, 20_000) == ("none", "context_unavailable")


def test_guardrails_stale_context_blocks_app_trigger():
    assert enforce_guardrails("small_plan_hint", "r", req(), {"updated_st": "0"}, 30_001, 30_000)[0] == "none"


def test_guardrails_state_timeout_trigger_allows_old_context():
    out = enforce_guardrails("small_plan_hint", "r", req(source="state_timeout"), {"updated_st": "0"}, 99_000, 1)
    assert out == ("small_plan_hint", "r")


def test_guardrails_ios_gets_app_store_plan_only():
    ctx = {"updated_st": "100"}
    assert enforce_guardrails("alt_payment_hint", "r", req(platform="ios"), ctx, 100, 10)[0] == "small_plan_hint"


def test_guardrails_already_paid_blocks_abandonment_action():
    ctx = {"updated_st": "100", "payment_state": "completed"}
    assert enforce_guardrails("small_plan_hint", "r", req(), ctx, 100, 10) == ("none", "already_paid")


@pytest.mark.parametrize("last_cts", ["0", "999999999"])
def test_enforce_guardrails_client_clock_skew_uses_server_time_allows(last_cts):
    # a device clock far behind (or ahead): only the server-side updated_st decides staleness
    ctx = {"last_cts": last_cts, "updated_st": "100"}
    assert enforce_guardrails("small_plan_hint", "r", req(), ctx, 100, 10) == ("small_plan_hint", "r")


def test_guardrails_login_needs_no_context():
    assert enforce_guardrails("home_rank", "r", req("login"), None, 0, 1) == ("home_rank", "r")


@pytest.mark.parametrize("signature", ["", "00", "x" * 64])
def test_verify_rejects_bad_signature(signature):
    assert not verify(b"k", b"{}", signature)


@pytest.mark.parametrize("signature", ["é", "\udcff", "é" * 64, " ", "\x00"])
def test_verify_non_ascii_signature_returns_false(signature):
    # Sanic decodes headers with surrogateescape; a non-ASCII value must be a 401, not a TypeError (500)
    assert verify(b"k", b"{}", signature) is False


def test_verify_accepts_good_and_rejects_tampered_body():
    sig = sign(b"k", b'{"amount":300}')
    assert verify(b"k", b'{"amount":300}', sig)
    assert not verify(b"k", b'{"amount":300000}', sig)


def test_mock_probs_deterministic_and_normalised():
    a = mock_probs(b"same request", 4)
    assert a == mock_probs(b"same request", 4)
    assert len(a) == 4
    assert abs(sum(a) - 1) < 1e-3


def test_mock_latency_median_40_p95_120():
    rng = random.Random(7)
    xs = sorted(mock_latency_ms(rng.gauss()) for _ in range(200_000))
    assert abs(statistics.median(xs) - 40) < 1.5
    assert abs(xs[int(0.95 * len(xs))] - 120) < 5


def test_hist_quantiles_within_bucket_error():
    h = Hist()
    rng = random.Random(1)
    xs = [rng.expovariate(1 / 20) for _ in range(50_000)]
    for x in xs:
        h.observe(x)
    xs.sort()
    for q in (0.5, 0.95, 0.99):
        exact = xs[int(q * len(xs))]
        assert abs(h.quantile(q) - exact) / exact < 0.05


def test_hist_handles_zero_and_huge_values():
    h = Hist()
    h.observe(0.0)
    h.observe(10**9)
    assert h.count == 2
    assert h.quantile(1.0) == 10**9


def test_stream_entry_roundtrips_as_msgpack_array():
    from lt.logic import StreamEvent
    good, _ = partition_batch(body({**EV, "props": {"room": "r1", "minutes": 5, "x": 1.5, "ok": True}}))
    raw = stream_entry(good[0])
    assert msgspec.msgpack.decode(raw)[:5] == ["e1", "u1", "s1", "chat", 1]
    ev = msgspec.msgpack.Decoder(StreamEvent).decode(raw)
    assert ev.props == {"room": "r1", "minutes": 5, "x": 1.5, "ok": True}


def test_partition_batch_fast_path_returns_typed_events():
    good, bad = partition_batch(body(EV, EV))
    assert bad == []
    assert [e.event_id for e in good] == ["e1", "e1"]


@pytest.mark.parametrize("field", [
    {"client_ts": 2**63}, {"client_ts": 2**64}, {"props": {"a": 2**63}}, {"props": {"a": 2**64}},
    {"props": {"a": -(2**63) - 1}}, {"props": {"a": 1e19}}, {"props": {"a": -1e19}},
])
def test_partition_batch_number_outside_int64_quarantined(field):
    # the same bounds as rust-collector/src/batch.rs; before, 2**64 passed and stream_entry raised OverflowError
    good, bad = partition_batch(body({**EV, **field}))
    assert (len(good), len(bad)) == (0, 1)


def test_partition_batch_int64_bounds_accepted_and_encodable():
    edge = {**EV, "client_ts": 2**63 - 1, "props": {"max": 2**63 - 1, "min": -(2**63), "f": 9.2e18}}
    good, bad = partition_batch(body(edge))
    assert bad == []
    assert msgspec.msgpack.decode(stream_entry(good[0]))[4] == 2**63 - 1


@pytest.mark.parametrize(("value", "expected"), [
    (30, 30), (1.5, 1), (0, 0), (MAX_WATCH_MINUTES, MAX_WATCH_MINUTES), (MAX_WATCH_MINUTES + 1, 0), (-5, 0),
    ("1.5", 0), (" ", 0), ("", 0), (True, 0), (float("inf"), 0), (float("nan"), 0), (2**63 - 1, 0), ([1], 0),
])
def test_watch_minutes_any_value_returns_hincrby_safe_int(value, expected):
    out = watch_minutes(value)
    assert out == expected
    assert type(out) is int


@pytest.mark.parametrize("uid", ["u", "x" * 96, "é" * 96])
def test_timeout_event_id_longest_user_id_fits_decision_id(uid):
    tid = timeout_event_id(uid, "1790959330981")
    assert len(tid) <= 96
    assert tid == timeout_event_id(uid, "1790959330981")       # idempotent across retries
    msgspec.json.decode(msgspec.json.encode(
        DecisionIn(event_id=tid, user_id=uid, trigger="abandonment", ts=1, source="state_timeout")), type=DecisionIn)


def test_timeout_member_separator_in_session_round_trips():
    raw = msgspec.json.encode(["u1", "1790959330981", "s\x1fx", "ios"])
    assert msgspec.json.decode(raw, type=TimeoutMember) == TimeoutMember("u1", "1790959330981", "s\x1fx", "ios")
