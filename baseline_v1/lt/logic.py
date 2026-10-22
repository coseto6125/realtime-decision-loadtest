"""Pure decision logic shared by the services: schemas, arms, actions, guardrails, signatures.

Nothing here does I/O, so the unit tests exercise the exact code the services run.
"""

import hashlib
import hmac
import math
from typing import Annotated, Literal

import msgspec
from msgspec import Meta

EventType = Literal[
    "login", "enter_room", "watch_progress", "chat", "send_gift", "following", "click_banner",
    "watch_ad", "leave_room", "click_recharge", "cancel_recharge", "recharge", "purchase",
]
TriggerType = Literal["login", "watch_30min", "leave_room", "abandonment"]
Id = Annotated[str, Meta(min_length=1, max_length=96)]
PropVal = str | int | float | bool


class Event(msgspec.Struct, frozen=True):
    event_id: Id
    user_id: Id
    session_id: Id
    type: EventType
    client_ts: Annotated[int, Meta(ge=0)]
    props: dict[str, PropVal] = {}


class DecisionIn(msgspec.Struct, frozen=True):
    event_id: Id
    user_id: Id
    trigger: TriggerType
    ts: Annotated[int, Meta(ge=0)]
    session_id: str = ""
    platform: Literal["ios", "android", "web"] = "android"
    source: Literal["app", "state_timeout"] = "app"
    recent_events: list[str] = []


class Intent(msgspec.Struct, frozen=True):
    top: str
    p: float
    probs: list[float]
    latency_ms: float


class Decision(msgspec.Struct, frozen=True):
    event_id: str
    user_id: str
    trigger: str
    arm: str
    action: str
    reason: str
    policy_version: int
    model_status: str
    intent: Intent | None
    decided_at: int


class JudgeIn(msgspec.Struct, frozen=True):
    state: str
    question: str
    options: list[str]


class JudgeOut(msgspec.Struct, frozen=True):
    probs: list[float]
    latency_ms: float


class Webhook(msgspec.Struct, frozen=True):
    provider: Id
    txn_id: Id
    user_id: Id
    amount: Annotated[int, Meta(gt=0)]
    currency: Literal["TWD", "USD"]
    status: Literal["paid"]
    paid_at: Annotated[int, Meta(ge=0)]


class Policy(msgspec.Struct, frozen=True):
    trigger: str
    version: int
    salt: str
    arms: list[tuple[str, int]]   # (arm, weight); weights sum to 100
    threshold: float = 0.4
    options: list[str] = []
    question: str = ""


_batch_dec = msgspec.json.Decoder(list[msgspec.Raw])
_event_dec = msgspec.json.Decoder(Event)


def partition_batch(body: bytes) -> tuple[list[bytes], list[tuple[bytes, str]]]:
    """Split a JSON array of events into valid raw events and (raw, error) rejects.

    Raises msgspec.DecodeError when the body is not a JSON array at all.
    """
    good: list[bytes] = []
    bad: list[tuple[bytes, str]] = []
    for raw in _batch_dec.decode(body):
        try:
            _event_dec.decode(raw)
        except msgspec.DecodeError as e:   # ValidationError subclasses DecodeError
            bad.append((bytes(raw), str(e)))
        else:
            good.append(bytes(raw))
    return good, bad


def bucket(*parts: str) -> int:
    """Stable 0..99 bucket from a keyed hash; identical across processes and restarts."""
    h = hashlib.blake2b("\x1f".join(parts).encode(), digest_size=8).digest()
    return int.from_bytes(h) % 100


def assign_arm(user_id: str, policy: Policy) -> str:
    b = bucket(policy.salt, user_id)
    acc = 0
    for arm, weight in policy.arms:
        acc += weight
        if b < acc:
            return arm
    return policy.arms[-1][0]


def top_intent(options: list[str], probs: list[float]) -> tuple[str, float]:
    i = max(range(len(probs)), key=probs.__getitem__)
    return options[i], probs[i]


def choose_action(trigger: str, arm: str, intent: tuple[str, float] | None, policy: Policy,
                  ctx: dict[str, str], event_id: str) -> tuple[str, str]:
    """Policy choice before guardrails. Returns (action, reason)."""
    if arm == "holdout":
        return "none", "holdout"
    match trigger:
        case "abandonment":
            if arm == "rule":
                hint = ("small_plan_hint", "alt_payment_hint")[bucket("rule", event_id) % 2]
                return hint, "rule_random"
            if intent is None or intent[1] < policy.threshold:
                return "small_plan_hint", "model_default"
            match intent[0]:
                case "price_sensitive":
                    return "small_plan_hint", "model_price"
                case "payment_friction":
                    return "alt_payment_hint", "model_friction"
                case _:
                    return "none", "model_no_intent"
        case "leave_room":
            if intent is not None and intent[0] == "switch_room" and intent[1] >= policy.threshold:
                return "recommend_next_room", "model_switch"
            return "none", "leaving_or_unknown"
        case "watch_30min":
            if ctx.get("followed") == "1":
                return "none", "already_following"
            return "follow_prompt", "not_following"
        case "login":
            if bucket("live", ctx.get("fav_streamer", "")) < 40:
                return "home_rank", "fav_live"
            return "none", "no_fav_live"
    return "none", "unknown_trigger"


NEEDS_FRESH_CTX = frozenset({"abandonment", "watch_30min", "leave_room"})


def enforce_guardrails(action: str, reason: str, req: DecisionIn, ctx: dict[str, str] | None,
                       now_ms: int, stale_ms: int) -> tuple[str, str]:
    """Code-enforced limits that hold whatever the policy says. Failing a check means doing less."""
    if action == "none":
        return action, reason
    if req.trigger in NEEDS_FRESH_CTX and req.source == "app":
        if ctx is None:
            return "none", "context_unavailable"
        if now_ms - int(ctx.get("last_cts", "0")) > stale_ms:
            return "none", "context_stale"
    if req.trigger == "abandonment" and ctx is not None and ctx.get("payment_state") == "completed":
        return "none", "already_paid"
    if action == "alt_payment_hint" and req.platform == "ios":
        return "small_plan_hint", "ios_app_store_only"
    return action, reason


def sign(secret: bytes, body: bytes) -> str:
    return hmac.new(secret, body, hashlib.sha256).hexdigest()


def verify(secret: bytes, body: bytes, signature: str) -> bool:
    return hmac.compare_digest(sign(secret, body), signature)


def mock_probs(body: bytes, n: int) -> list[float]:
    """Deterministic softmax over n options from a hash of the request body."""
    h = hashlib.blake2b(body, digest_size=2 * n).digest()
    logits = [int.from_bytes(h[2 * i:2 * i + 2]) / 65535 * 3.0 for i in range(n)]
    m = max(logits)
    ex = [math.exp(x - m) for x in logits]
    s = sum(ex)
    return [round(x / s, 4) for x in ex]


LOGNORM_MU = math.log(40.0)                 # median 40 ms
LOGNORM_SIGMA = math.log(120.0 / 40.0) / 1.6448536269514722   # p95 = 120 ms


def mock_latency_ms(u: float) -> float:
    """Lognormal latency from a standard normal draw u."""
    return math.exp(LOGNORM_MU + LOGNORM_SIGMA * u)
