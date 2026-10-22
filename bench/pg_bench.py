"""
T6b Postgres bench: can PostgreSQL 18 replace Valkey as both the User Context store and the event stream?

Everything lives in the dedicated database ``lt_t6b``; the driver refuses any other database. Create
it once (``psql -c 'CREATE DATABASE lt_t6b'``), then ``--setup`` builds the objects from
bench/pg_t6b/*.sql (teardown.sql, common.sql, then the design file) and ``--teardown`` drops them.

Context role, three designs (``--setup naive|tuned|tuned_unlogged``):

* naive: ``ctx_naive(user_id, context jsonb)`` holds counters plus the last 50 events; every event
  rewrites the row with ``INSERT ... ON CONFLICT DO UPDATE SET context = <jsonb expression>`` that
  adds the counters and prepends the event, trimmed to 50 with ``jsonb - 50``.
* tuned: ``ctx_state`` is a fixed-width hot row per user (fillfactor 70, no index on an updated
  column, so updates stay HOT); ``recent_events(user_id, ts, payload bytea)`` is append-only,
  PARTITIONED BY RANGE (ts) per UTC day, index (user_id, ts DESC), read with LIMIT 50. Retention is
  DROP PARTITION (``t6b_retention()``), never row-by-row DELETE. State rows are seeded at setup for
  ``--users`` users, so the apply is a plain UPDATE (production would upsert on first sight).
* tuned_unlogged: tuned with ctx_state and the recent_events partitions UNLOGGED. Unlogged tables
  write no WAL, are truncated after a crash, and are not replicated to standbys (so not to an RDS
  Multi-AZ standby either); the data must be rebuildable from the stream.

Context data is rebuildable, so every context write transaction runs ``SET LOCAL synchronous_commit
= off``: a crash can lose the last few hundred ms of commits, but the cursor advance commits in the
same transaction, so what is lost is a consistent tail that the consumer reads again. Ingest, group
B's cursor and the lease updates keep synchronous_commit on.

Stream role: ``events(id identity, txid xid8 DEFAULT pg_current_xact_id(), shard, user_id, ts,
payload)``, day-partitioned on ts like recent_events; an events partition is dropped only after
every group's cursor has passed it. Two consumer groups read the same rows through their own
cursors in ``stream_cursors(group_name, shard, last_txid, last_id, lease_owner, lease_until, ...)``
with shard = xxh3(user_id) % ``--shards``. Each worker leases its fair share of its group's shards
(FOR UPDATE SKIP LOCKED), renews the leases every visibility timeout / 3 and with every cursor
advance, and also takes over any shard whose lease expired while owned (a crashed or stuck
consumer). Every cursor advance is fenced on ``lease_owner = me AND lease_until > clock_timestamp()``;
if any shard of a batch fails the fence, the whole transaction rolls back, so a shard is never
applied twice. ``--crash-after-s`` stops one group-A worker without releasing its leases, to
exercise the takeover. Delivery is by polling (``--poll-ms`` after a pass with no full batch): one
query per worker reads all its shards; there is no LISTEN/NOTIFY.

Out-of-order commits: identity values are taken at insert time, so a transaction holding id 100 can
commit after one holding id 101 that a reader has already passed, and an id cursor would skip 100
for ever. Readers here only return rows whose txid is below ``pg_snapshot_xmin(pg_current_snapshot())``:
every transaction below that horizon has finished, so no row with such a txid can still appear. The
cursor is the pair (txid, id), rows are read in (txid, id) order, and all rows of one txid become
visible at the same instant, so a row that commits late is read when the horizon passes its txid
instead of being skipped. The price is head-of-line delay: one long transaction anywhere in the
cluster holds the horizon back and stalls delivery until it ends. The final audit checks the
claim: per group, events consumed must equal events at or below the cursors.

Driver: one process, uvloop, one psqlpy pool (``--pool``, default 8: each group worker holds one
connection, ingest/read/stats share the rest). Open-loop ingest of 10-event batches (one INSERT,
one transaction each) and context reads (``--decisions-per-event`` x rate, state row + last 50
events) use the wall-clock scheduler of bench.cache_bench; their latency counts from the intended
start. psqlpy 0.12 cannot bind list[bytes] to bytea[] nor decode bytea[], and it overflows on a
jsonb dict holding an int above 2**31-1, so payload batches travel as one concatenated bytea with
int4 offsets and lengths (split by ``substr()``), and the naive event jsonb is built in SQL.

Per step: latency (ingest, read, apply = one group-A transaction, apply_per_event, b_advance,
heartbeat, ingest_to_applied), consumed events/s per group, lag per group (``ids`` = max ingested
id minus the lowest cursor id, ``events`` = this run's ingested minus consumed, ``s`` = age of the
oldest unread event), WAL bytes/s from pg_stat_wal, pg_stat_user_tables deltas and sizes per table,
pg_stat_checkpointer deltas, driver CPU, late ticks. Table counters reach pg_stat with up to ~1 s
delay. Break point = the highest step with successful ingest and read samples, read p95 <=
``--read-p95-ms`` (20), no ingest, read or worker error, no unit cancelled at drain, achieved and
per-group consumed events/s >= 95% of target, and bounded lag: no group's lag in seconds grew by
more than ``--lag-growth-s`` (1.0) over the step.

Output: JSON lines on stdout (``setup``/``teardown``, ``start``, ``step`` per step, ``summary``);
diagnostics on stderr.

Usage (from the loadtest directory)::

    python -m bench.pg_bench --dsn postgres://lt:PW@lt-pg:5432/lt_t6b --setup tuned
    python -m bench.pg_bench --dsn ... --mode baseline --events-per-sec 150 --duration-s 120
    python -m bench.pg_bench --dsn ... --mode ramp --ramp-steps 150,300,600,1200,2400,4800 --step-s 30 --max-s 180
    python -m bench.pg_bench --dsn ... --setup tuned_unlogged --mode ramp --crash-after-s 20
    python -m bench.pg_bench --dsn ... --teardown
"""

import argparse
import asyncio
import os
import sys
from collections import defaultdict
from collections.abc import Iterable
from contextlib import suppress
from itertools import accumulate
from pathlib import Path
from time import perf_counter

import uvloop
import xxhash
from lt.common import Hist, now_ms
from lt.logic import StreamEvent
from psqlpy import Connection, ConnectionPool
from psqlpy.exceptions import Error as PGError

from bench.cache_bench import EV_DEC, MIN_RATE_FRACTION, Gen, cpu_s, drain, emit, open_loop

SQL_DIR = Path(__file__).with_name("pg_t6b")
DESIGN_FILES = {"naive": "naive.sql", "tuned": "tuned.sql", "tuned_unlogged": "tuned_unlogged.sql"}
DATABASE = "lt_t6b"
BATCH = 10
GROUPS = ("A", "B")
REQUIRED_OPS = ("ingest", "read")
# lease_lost_* and simulated_crash_holding_lease are the takeover path under test; lag measures their cost
FAIL_ERRORS = ("ingest", "read", *(f"worker_{g}" for g in GROUPS))

PAY_KEEP, PAY_PAYING, PAY_ABANDONED, PAY_COMPLETED, PAY_CANCEL_IF_PAYING = -1, 1, 2, 3, 5

SQL_INGEST = (
    "WITH i AS (INSERT INTO events (shard, user_id, ts, payload)"
    " SELECT sh, u, $3, substr($6::bytea, o, l)"
    " FROM unnest($1::smallint[], $2::text[], $4::int[], $5::int[]) AS t(sh, u, o, l) RETURNING id)"
    " SELECT max(id) FROM i"
)
SQL_CLAIM = (
    "WITH p AS MATERIALIZED (SELECT group_name, shard, lease_owner FROM stream_cursors WHERE group_name = $3"
    " AND lease_until < clock_timestamp() AND (lease_owner IS NOT NULL OR $4::boolean)"
    " ORDER BY lease_until LIMIT 1 FOR UPDATE SKIP LOCKED)"
    " UPDATE stream_cursors c SET lease_owner = $1, lease_until = clock_timestamp() + make_interval(secs => $2::float8)"
    " FROM p WHERE c.group_name = p.group_name AND c.shard = p.shard RETURNING c.shard, p.lease_owner AS prev"
)
SQL_RENEW = (
    "UPDATE stream_cursors SET lease_until = clock_timestamp() + make_interval(secs => $3::float8)"
    " WHERE group_name = $1 AND lease_owner = $2 AND lease_until > clock_timestamp() RETURNING shard"
)
SQL_RELEASE = (
    "UPDATE stream_cursors SET lease_owner = NULL, lease_until = clock_timestamp()"
    " WHERE group_name = $1 AND lease_owner = $2"
)
_OWNED_BATCHES = (
    " FROM stream_cursors c CROSS JOIN LATERAL (SELECT id, txid, user_id, ts, payload FROM events"
    " WHERE events.shard = c.shard AND (events.txid, events.id) > (c.last_txid, c.last_id)"
    " AND events.txid < pg_snapshot_xmin(pg_current_snapshot()) ORDER BY events.txid, events.id LIMIT $3) e"
    " WHERE c.group_name = $1 AND c.lease_owner = $2"
)
SQL_READ_OWNED = (
    "SELECT c.shard, e.id, e.txid::text AS tx, e.user_id, e.ts, e.payload"
    + _OWNED_BATCHES + " ORDER BY c.shard, e.txid, e.id"
)
_FENCE = (
    "UPDATE stream_cursors c SET last_txid = u.tx::xid8, last_id = u.id, last_ts = u.ts, consumed = c.consumed + u.n,"
    " lease_until = clock_timestamp() + make_interval(secs => $6::float8)"
    " FROM unnest($1::smallint[], $2::text[], $3::bigint[], $4::bigint[], $5::int[]) AS u(shard, tx, id, ts, n)"
    " WHERE c.group_name = $7 AND c.shard = u.shard AND c.lease_owner = $8 AND c.lease_until > clock_timestamp()"
)
SQL_ADVANCE = _FENCE + " RETURNING c.shard"
SQL_TUNED_APPLY = (
    "WITH d AS (SELECT * FROM unnest($9::text[], $10::int[], $11::int[], $12::int[], $13::int[], $14::smallint[],"  # ruff: ignore[hardcoded-sql-expression] -- constants only
    " $15::bigint[]) AS d(u, wm, rv, ch, gi, ps, upd)),"
    " s AS (UPDATE ctx_state t SET watch_minutes = t.watch_minutes + d.wm, rooms_visited = t.rooms_visited + d.rv,"
    " chats = t.chats + d.ch, gifts = t.gifts + d.gi,"
    " payment_state = CASE WHEN d.ps < 0 THEN t.payment_state"
    " WHEN d.ps = 5 THEN CASE WHEN t.payment_state = 1 THEN 2 ELSE t.payment_state END ELSE d.ps END,"
    " updated_ms = d.upd FROM d WHERE t.user_id = d.u RETURNING 1),"
    " r AS (INSERT INTO recent_events (user_id, ts, payload) SELECT u, ts, substr($20::bytea, o, l)"
    " FROM unnest($16::text[], $17::bigint[], $18::int[], $19::int[]) AS t(u, ts, o, l) RETURNING 1) "
    + _FENCE + " RETURNING c.shard, (SELECT count(*) FROM s) AS updated, (SELECT count(*) FROM r) AS inserted"
)
SQL_NAIVE_APPLY = (
    "INSERT INTO ctx_naive AS c (user_id, context) VALUES ($1, jsonb_build_object('watch_minutes', $2::int,"
    " 'rooms_visited', $3::int, 'chats', $4::int, 'gifts', $5::int,"
    " 'payment_state', CASE WHEN $6::int BETWEEN 1 AND 3 THEN $6::int ELSE 0 END, 'updated_ms', $7::bigint,"
    " 'events', jsonb_build_array(jsonb_build_object('id', $8::text, 'type', $9::text, 'ts', $7::bigint,"
    " 'props', $10::jsonb))))"
    " ON CONFLICT (user_id) DO UPDATE SET context = c.context || jsonb_build_object("
    "'watch_minutes', coalesce((c.context->>'watch_minutes')::int, 0) + $2::int,"
    " 'rooms_visited', coalesce((c.context->>'rooms_visited')::int, 0) + $3::int,"
    " 'chats', coalesce((c.context->>'chats')::int, 0) + $4::int,"
    " 'gifts', coalesce((c.context->>'gifts')::int, 0) + $5::int,"
    " 'payment_state', CASE WHEN $6::int < 0 THEN coalesce(c.context->'payment_state', '0'::jsonb)"
    " WHEN $6::int = 5 THEN CASE WHEN c.context->>'payment_state' = '1' THEN '2'::jsonb"
    " ELSE coalesce(c.context->'payment_state', '0'::jsonb) END ELSE to_jsonb($6::int) END,"
    " 'updated_ms', $7::bigint,"
    " 'events', ((EXCLUDED.context->'events') || coalesce(c.context->'events', '[]'::jsonb)) - 50)"
)
SQL_B_ADVANCE = (
    "WITH b AS (SELECT c.shard, e.txid, e.id, e.ts" + _OWNED_BATCHES + " AND c.lease_until > clock_timestamp()),"  # ruff: ignore[hardcoded-sql-expression] -- constants only
    " l AS (SELECT DISTINCT ON (shard) shard, txid, id, ts, count(*) OVER (PARTITION BY shard) AS n FROM b"
    " ORDER BY shard, txid DESC, id DESC)"
    " UPDATE stream_cursors c SET last_txid = l.txid, last_id = l.id, last_ts = l.ts, consumed = c.consumed + l.n,"
    " lease_until = clock_timestamp() + make_interval(secs => $4::float8)"
    " FROM l WHERE c.group_name = $1 AND c.shard = l.shard AND c.lease_owner = $2 RETURNING l.n"
)
SQL_READ = {
    "tuned": (
        "SELECT s.watch_minutes, s.rooms_visited, s.chats, s.gifts, s.payment_state, s.updated_ms, r.cat, r.lens"
        " FROM ctx_state s CROSS JOIN LATERAL (SELECT string_agg(x.payload, ''::bytea ORDER BY x.ts DESC) AS cat,"
        " array_agg(length(x.payload) ORDER BY x.ts DESC) AS lens FROM (SELECT ts, payload FROM recent_events"
        " WHERE user_id = $1 ORDER BY ts DESC LIMIT 50) x) r WHERE s.user_id = $1"
    ),
    "naive": "SELECT context::text AS context FROM ctx_naive WHERE user_id = $1",
}
SQL_LAG = (
    "SELECT c.group_name, min(c.last_id) AS min_last_id, sum(c.consumed)::bigint AS consumed,"
    " min(f.ts) AS oldest_unread_ts FROM stream_cursors c LEFT JOIN LATERAL (SELECT e.ts FROM events e"
    " WHERE e.shard = c.shard AND (e.txid, e.id) > (c.last_txid, c.last_id) ORDER BY e.txid, e.id LIMIT 1) f ON true"
    " GROUP BY c.group_name ORDER BY 1"
)
SQL_TABLES = (
    "SELECT relname, n_tup_ins, n_tup_upd, n_tup_hot_upd, n_dead_tup, n_live_tup, autovacuum_count,"
    " pg_total_relation_size(relid) AS total_bytes, pg_indexes_size(relid) AS index_bytes"
    " FROM pg_stat_user_tables ORDER BY relname"
)
SQL_WAL = "SELECT wal_bytes::bigint AS wal_bytes, wal_records, wal_fpi FROM pg_stat_wal"
SQL_CKPT = "SELECT * FROM pg_stat_checkpointer"
SQL_AUDIT = (
    "SELECT c.group_name, sum(c.consumed)::bigint AS consumed, sum((SELECT count(*) FROM events e"
    " WHERE e.shard = c.shard AND (e.txid, e.id) <= (c.last_txid, c.last_id)))::bigint AS at_or_below_cursor"
    " FROM stream_cursors c GROUP BY 1 ORDER BY 1"
)
SQL_INVERSIONS = (
    "SELECT count(*) FILTER (WHERE txid < prev) AS id_txid_inversions, count(*) AS events"
    " FROM (SELECT txid, lag(txid) OVER (ORDER BY id) AS prev FROM events) x"
)
TABLE_DELTAS = ("n_tup_ins", "n_tup_upd", "n_tup_hot_upd", "autovacuum_count")
TABLE_LEVELS = ("n_dead_tup", "n_live_tup", "total_bytes", "index_bytes")


class LeaseLostError(Exception):
    """A shard's lease expired and another worker may own it; the transaction rolls back."""


def shard_of(uid: str, shards: int) -> int:
    return xxhash.xxh3_64_intdigest(uid.encode()) % shards


def pack(payloads: list[bytes]) -> tuple[bytes, list[int], list[int]]:
    """Concatenate payloads for one bytea parameter; return (blob, 1-based offsets, lengths) for substr()."""
    lens = [len(p) for p in payloads]
    return b"".join(payloads), [o + 1 for o in accumulate(lens, initial=0)][:-1], lens


def cursor_updates(rows: list[dict]) -> tuple[list[int], list[str], list[int], list[int], list[int]]:
    """
    Per shard of a (shard, txid, id)-ordered batch: the last row's (txid, id, ts) and the row count.

    Returns parallel arrays (shards, txids, ids, ts, counts) for the multi-shard cursor fence.
    """
    last: dict[int, dict] = {}
    counts: defaultdict[int, int] = defaultdict(int)
    for r in rows:
        last[r["shard"]] = r
        counts[r["shard"]] += 1
    shards = list(last)
    return (shards, [last[s]["tx"] for s in shards], [last[s]["id"] for s in shards],
            [last[s]["ts"] for s in shards], [counts[s] for s in shards])


def event_delta(ev: StreamEvent) -> tuple[int, int, int, int, int]:
    """(watch_minutes, rooms_visited, chats, gifts, payment code) that one event adds, per APPLY_LUA's rules."""
    match ev.type:
        case "watch_progress":
            m = ev.props.get("minutes", 0)
            return (m if isinstance(m, int) else 0), 0, 0, 0, PAY_KEEP
        case "enter_room":
            return 0, 1, 0, 0, PAY_KEEP
        case "chat":
            return 0, 0, 1, 0, PAY_KEEP
        case "send_gift":
            return 0, 0, 0, 1, PAY_KEEP
        case "click_recharge":
            return 0, 0, 0, 0, PAY_PAYING
        case "cancel_recharge":
            return 0, 0, 0, 0, PAY_CANCEL_IF_PAYING
        case "recharge":
            return 0, 0, 0, 0, PAY_COMPLETED
    return 0, 0, 0, 0, PAY_KEEP


def fold_events(evs: Iterable[StreamEvent]) -> dict[str, list[int]]:
    """One row per user for a batch (an UPDATE ... FROM needs unique keys): summed counters, final payment code."""
    out: dict[str, list[int]] = {}
    for ev in evs:
        wm, rv, ch, gi, ps = event_delta(ev)
        if (a := out.get(ev.user_id)) is None:
            out[ev.user_id] = [wm, rv, ch, gi, ps]
            continue
        a[0] += wm
        a[1] += rv
        a[2] += ch
        a[3] += gi
        if ps == PAY_CANCEL_IF_PAYING:
            if a[4] == PAY_PAYING:
                a[4] = PAY_ABANDONED
            elif a[4] == PAY_KEEP:
                a[4] = PAY_CANCEL_IF_PAYING
        elif ps != PAY_KEEP:
            a[4] = ps
    return out


def lag_seconds(oldest_unread_ts: int | None, now: int) -> float:
    return round(max(now - oldest_unread_ts, 0) / 1000, 3) if oldest_unread_ts else 0.0


def break_point(steps: list[dict], read_p95_ms: float, lag_growth_s: float) -> tuple[float | None, float | None]:
    """
    Return (highest passing target_eps, first failing target_eps).

    A step passes when every op in REQUIRED_OPS has successful samples, read p95 <= read_p95_ms, no op
    in FAIL_ERRORS recorded an error, no unit was cancelled at drain, achieved and every group's consumed
    events/s reach MIN_RATE_FRACTION of the target, and no group's lag (seconds) grew by more than
    lag_growth_s since the end of the previous step (the first step compares with zero lag).
    An op without samples fails the step; it never counts as zero latency.
    """
    best = first_fail = None
    prev: dict[str, float] = {}
    for s in steps:
        lat, errors, floor = s["latency_ms"], s["errors"], MIN_RATE_FRACTION * s["target_eps"]
        lag = {g: v["s"] for g, v in s["lag"].items()}
        ok = (all((h := lat.get(op)) and h["count"] > 0 for op in REQUIRED_OPS)
              and lat["read"]["p95"] <= read_p95_ms
              and not any(errors.get(op) for op in FAIL_ERRORS) and not s["cancelled_at_drain"]
              and min(s["achieved_eps"], *s["consumed_eps"].values()) >= floor
              and all(v - prev.get(g, 0.0) <= lag_growth_s for g, v in lag.items()))
        prev = lag
        if ok:
            best = s["target_eps"] if best is None else max(best, s["target_eps"])
        elif first_fail is None:
            first_fail = s["target_eps"]
    return best, first_fail


def numeric_delta(before: dict, after: dict) -> dict[str, float]:
    return {k: round(v - before.get(k, 0), 3) for k, v in after.items()
            if isinstance(v, int | float) and not isinstance(v, bool)}


class PgStep:
    __slots__ = ("consumed", "cpu0", "errors", "hists", "ingested", "reads", "reclaimed", "t0")

    def __init__(self) -> None:
        self.hists: defaultdict[str, Hist] = defaultdict(Hist)
        self.errors: defaultdict[str, int] = defaultdict(int)
        self.consumed = dict.fromkeys(GROUPS, 0)
        self.ingested = 0
        self.reads = 0
        self.reclaimed = 0
        self.t0 = perf_counter()
        self.cpu0 = cpu_s()


class PgBench:
    __slots__ = ("args", "consumed_run", "crash_at", "design", "gen", "ingested_run", "max_id", "pool", "running",
                 "sem", "shards", "sql_read", "step")

    def __init__(self, pool: ConnectionPool, args: argparse.Namespace, meta: dict[str, str]) -> None:
        self.pool = pool
        self.args = args
        self.design = meta["design"]
        self.shards = int(meta["shards"])
        self.gen = Gen(int(meta["users"]), args.seed)
        self.sql_read = SQL_READ[self.design]
        self.step = PgStep()
        self.max_id = 0
        self.ingested_run = 0
        self.consumed_run = dict.fromkeys(GROUPS, 0)
        self.running = True
        self.sem = asyncio.Semaphore(args.max_inflight)
        self.crash_at = perf_counter() + args.crash_after_s if args.crash_after_s > 0 else 0.0

    async def ingest(self, intended: float) -> None:
        st = self.step
        ms = now_ms()
        nxt, n = self.gen.next, self.shards
        evs = [nxt(ms - 3) for _ in range(BATCH)]
        uids = [u for u, _ in evs]
        blob, offs, lens = pack([p for _, p in evs])
        params = [[shard_of(u, n) for u in uids], uids, ms, offs, lens, blob]
        try:
            async with self.sem, self.pool.acquire() as conn:
                mx = await conn.fetch_val(SQL_INGEST, params)
        except PGError:
            st.errors["ingest"] += 1
            return
        st.hists["ingest"].observe((perf_counter() - intended) * 1000)
        st.ingested += BATCH
        self.ingested_run += BATCH
        self.max_id = max(self.max_id, mx or 0)

    async def read(self, intended: float) -> None:
        st = self.step
        try:
            async with self.sem, self.pool.acquire() as conn:
                (await conn.execute(self.sql_read, [self.gen.last_uid])).result()
        except PGError:
            st.errors["read"] += 1
            return
        st.hists["read"].observe((perf_counter() - intended) * 1000)
        st.reads += 1

    def consumed(self, group: str, n: int) -> None:
        self.step.consumed[group] += n
        self.consumed_run[group] += n

    async def apply_a(self, conn: Connection, me: str) -> bool:
        """
        Group A: read a batch from every leased shard, apply it to the context design, advance the cursors.

        One transaction; returns True when some shard had a full batch (more is waiting).
        """
        a = self.args
        rows = (await conn.execute(SQL_READ_OWNED, ["A", me, a.batch])).result()
        if not rows:
            return False
        t0 = perf_counter()
        fence = [*cursor_updates(rows), a.visibility_timeout_s, "A", me]
        evs = [EV_DEC.decode(r["payload"]) for r in rows]
        async with conn.transaction() as tx:
            await tx.execute("SET LOCAL synchronous_commit = off")
            if self.design == "naive":
                await tx.execute_many(SQL_NAIVE_APPLY, [
                    [ev.user_id, *event_delta(ev), r["ts"], ev.event_id, ev.type, ev.props]
                    for ev, r in zip(evs, rows, strict=True)])
                ok = (await tx.execute(SQL_ADVANCE, fence)).result()
            else:
                folded = fold_events(evs)
                blob, offs, lens = pack([r["payload"] for r in rows])
                ok = (await tx.execute(SQL_TUNED_APPLY, [
                    *fence, list(folded), *map(list, zip(*folded.values(), strict=True)),
                    [rows[-1]["ts"]] * len(folded), [r["user_id"] for r in rows], [r["ts"] for r in rows],
                    offs, lens, blob])).result()
            if len(ok) != len(fence[0]):
                raise LeaseLostError
        done = perf_counter()
        st = self.step
        st.hists["apply"].observe((done - t0) * 1000)
        st.hists["apply_per_event"].observe((done - t0) * 1000 / len(rows))
        h, wall = st.hists["ingest_to_applied"], now_ms()
        for r in rows:
            h.observe(max(wall - r["ts"], 0))
        self.consumed("A", len(rows))
        return max(fence[4]) >= a.batch

    async def advance_b(self, conn: Connection, me: str) -> bool:
        """Group B: only move the cursors of every leased shard, in one statement."""
        a = self.args
        t0 = perf_counter()
        res = (await conn.execute(SQL_B_ADVANCE, ["B", me, a.batch, a.visibility_timeout_s])).result()
        if not res:
            return False
        self.step.hists["b_advance"].observe((perf_counter() - t0) * 1000)
        self.consumed("B", sum(r["n"] for r in res))
        return max(r["n"] for r in res) >= a.batch

    async def heartbeat(self, conn: Connection, group: str, me: str, share: int) -> set[int]:
        """Renew this worker's leases, lease free shards up to its fair share, and take over orphaned shards."""
        vt = self.args.visibility_timeout_s
        t0 = perf_counter()
        owned = {r["shard"] for r in (await conn.execute(SQL_RENEW, [group, me, vt])).result()}
        while got := (await conn.execute(SQL_CLAIM, [me, vt, group, len(owned) < share])).result():
            owned.add(got[0]["shard"])
            if (prev := got[0]["prev"]) is not None and prev != me:
                self.step.reclaimed += 1
        self.step.hists["heartbeat"].observe((perf_counter() - t0) * 1000)
        return owned

    async def worker(self, group: str, idx: int) -> None:
        """Hold leased shards and consume them; sleep --poll-ms after a pass without a full batch."""
        a = self.args
        me = f"{group}-{idx}-{os.getpid()}"
        share = -(-self.shards // (a.workers_a if group == "A" else a.workers_b))
        consume = self.apply_a if group == "A" else self.advance_b
        crashes = group == "A" and idx == 0 and self.crash_at > 0
        poll_s, hb_s = a.poll_ms / 1000, a.visibility_timeout_s / 3
        owned: set[int] = set()
        next_hb = 0.0
        async with self.pool.acquire() as conn:
            while self.running:
                full = False
                try:
                    if perf_counter() >= next_hb:
                        owned, next_hb = await self.heartbeat(conn, group, me, share), perf_counter() + hb_s
                    if crashes and owned and perf_counter() >= self.crash_at:
                        self.step.errors["simulated_crash_holding_lease"] += 1
                        return
                    full = bool(owned) and await consume(conn, me)
                except LeaseLostError:
                    self.step.errors[f"lease_lost_{group}"] += 1
                    next_hb = 0.0
                except PGError as e:
                    self.step.errors[f"worker_{group}"] += 1
                    print(f"worker {me}: {type(e).__name__}: {e}", file=sys.stderr)
                    next_hb = 0.0
                if not full:
                    await asyncio.sleep(poll_s)
            with suppress(PGError):
                await conn.execute(SQL_RELEASE, [group, me])

    async def snapshot(self) -> dict:
        async with self.pool.acquire() as conn:
            tables = {r.pop("relname"): r for r in (await conn.execute(SQL_TABLES)).result()}
            wal = (await conn.execute(SQL_WAL)).result()[0]
            ckpt = (await conn.execute(SQL_CKPT)).result()[0]
            lag = (await conn.execute(SQL_LAG)).result()
        return {"tables": tables, "wal": wal, "ckpt": ckpt, "lag": lag, "t": perf_counter()}

    def report(self, i: int, target: float, tk_late: int, wakes: int, cancelled: int, s0: dict, s1: dict) -> dict:
        st = self.step
        dur = perf_counter() - st.t0
        wall = now_ms()
        lag = {r["group_name"]: {"ids": max(self.max_id - r["min_last_id"], 0),
                                 "events": self.ingested_run - self.consumed_run.get(r["group_name"], 0),
                                 "s": lag_seconds(r["oldest_unread_ts"], wall)} for r in s1["lag"]}
        t0s = s0["tables"]
        tables = {name: {**{k: t[k] - t0s.get(name, {}).get(k, 0) for k in TABLE_DELTAS},
                         **{k: t[k] for k in TABLE_LEVELS}} for name, t in s1["tables"].items()}
        return {
            "kind": "step", "bench": "pg", "design": self.design, "step": i, "target_eps": target,
            "duration_s": round(dur, 3), "achieved_eps": round(st.ingested / dur, 1),
            "consumed_eps": {g: round(n / dur, 1) for g, n in st.consumed.items()},
            "reads_per_sec": round(st.reads / dur, 1),
            "latency_ms": {k: h.summary() for k, h in sorted(st.hists.items())},
            "lag": lag, "max_id": self.max_id, "reclaimed": st.reclaimed,
            "wal_bytes_per_s": round((s1["wal"]["wal_bytes"] - s0["wal"]["wal_bytes"]) / (s1["t"] - s0["t"]), 1),
            "wal": numeric_delta(s0["wal"], s1["wal"]), "checkpointer": numeric_delta(s0["ckpt"], s1["ckpt"]),
            "tables": tables, "late_ticks": tk_late, "wakeups": wakes, "cancelled_at_drain": cancelled,
            "driver_cpu_s": round(cpu_s() - st.cpu0, 3), "errors": dict(st.errors),
        }


async def check_database(pool: ConnectionPool) -> None:
    async with pool.acquire() as conn:
        if (db := await conn.fetch_val("SELECT current_database()")) != DATABASE:
            sys.exit(f"pg_bench: connected to database {db!r}; this driver only runs in {DATABASE!r}")


async def teardown(pool: ConnectionPool) -> None:
    async with pool.acquire() as conn:
        await conn.execute_batch((SQL_DIR / "teardown.sql").read_text())


async def setup(pool: ConnectionPool, design: str, args: argparse.Namespace) -> None:
    sql = "\n".join((SQL_DIR / f).read_text() for f in ("teardown.sql", "common.sql", DESIGN_FILES[design]))
    async with pool.acquire() as conn:
        await conn.execute_batch(sql)
        await conn.execute("INSERT INTO t6b_meta (k, v) VALUES ('shards', $1), ('users', $2)",
                           [str(args.shards), str(args.users)])
        await conn.execute("INSERT INTO stream_cursors (group_name, shard)"
                           " SELECT g, s FROM unnest($1::text[]) g, generate_series(0, $2 - 1) s",
                           [list(GROUPS), args.shards])
        if design == "naive":
            await conn.execute(
                "INSERT INTO ctx_naive (user_id, context) SELECT 'u' || g, jsonb_build_object('watch_minutes', 0,"
                " 'rooms_visited', 0, 'chats', 0, 'gifts', 0, 'payment_state', 0, 'updated_ms', 0,"
                " 'events', '[]'::jsonb) FROM generate_series(0, $1 - 1) g", [args.users])
        else:
            await conn.execute("INSERT INTO ctx_state (user_id) SELECT 'u' || g FROM generate_series(0, $1 - 1) g",
                               [args.users])
        await conn.execute_batch("VACUUM ANALYZE")


async def prepare_run(pool: ConnectionPool) -> dict[str, str]:
    """Read the design from t6b_meta, make sure today's and tomorrow's partitions exist, clear stale leases."""
    async with pool.acquire() as conn:
        meta = {r["k"]: r["v"] for r in (await conn.execute("SELECT k, v FROM t6b_meta")).result()}
        await conn.execute("SELECT t6b_ensure_partitions('events', 2, false)")
        if meta["design"] == "tuned":
            await conn.execute("SELECT t6b_ensure_partitions('recent_events', 2, $1)", [meta["unlogged"] == "true"])
        await conn.execute("UPDATE stream_cursors SET lease_owner = NULL, lease_until = '-infinity'")
        meta["server_version"] = await conn.fetch_val("SHOW server_version")
    return meta


async def audit(pool: ConnectionPool) -> dict:
    """Per group, events consumed must equal events at or below the cursors (no skip, no double apply)."""
    async with pool.acquire() as conn:
        out = {"groups": (await conn.execute(SQL_AUDIT)).result(), **(await conn.execute(SQL_INVERSIONS)).result()[0]}
    out["consistent"] = all(g["consumed"] == g["at_or_below_cursor"] for g in out["groups"])
    return out


async def run(pool: ConnectionPool, args: argparse.Namespace) -> None:
    deadline = perf_counter() + args.max_s
    meta = await prepare_run(pool)
    if args.mode == "ramp":
        rates, step_s = [float(x) for x in args.ramp_steps.split(",") if x.strip()], args.step_s
    else:
        rates, step_s = [args.events_per_sec], args.duration_s
    b = PgBench(pool, args, meta)
    emit({"kind": "start", "bench": "pg", "mode": args.mode, "design": meta["design"],
          "unlogged": meta["unlogged"] == "true", "shards": b.shards, "users": int(meta["users"]),
          "server_version": meta["server_version"], "rates": rates, "step_s": step_s, "pool": args.pool,
          "workers": {"A": args.workers_a, "B": args.workers_b}, "max_s": args.max_s})
    workers = [asyncio.create_task(b.worker(g, i)) for g, n in (("A", args.workers_a), ("B", args.workers_b))
               for i in range(n)]
    tasks: set[asyncio.Task] = set()
    steps: list[dict] = []
    truncated = False
    try:
        for i, rate in enumerate(rates):
            budget = min(step_s, deadline - perf_counter() - args.reserve_s)
            truncated = budget < step_s
            if budget < 1.0:
                break
            b.step = PgStep()
            s0 = await b.snapshot()
            tk = await open_loop(rate, args.decisions_per_event, budget, BATCH, b.ingest, b.read, tasks)
            cancelled = await drain(tasks, 2.0)
            line = b.report(i, rate, tk.late, tk.wakes, cancelled, s0, await b.snapshot())
            emit(line)
            steps.append(line)
    finally:
        b.running = False
        await asyncio.wait(workers, timeout=2.0)
        for w in workers:
            w.cancel()
    checked = await audit(pool) if args.audit and deadline - perf_counter() > 2.0 else {}
    best, fail = break_point(steps, args.read_p95_ms, args.lag_growth_s)
    emit({"kind": "summary", "bench": "pg", "mode": args.mode, "design": meta["design"],
          "unlogged": meta["unlogged"] == "true", "break_eps": best, "first_fail_eps": fail,
          "read_p95_ms": args.read_p95_ms, "lag_growth_s": args.lag_growth_s, "truncated": truncated,
          "audit": checked, "per_step": [
              {"target_eps": s["target_eps"], "achieved_eps": s["achieved_eps"], "consumed_eps": s["consumed_eps"],
               "read_p95": s["latency_ms"].get("read", {}).get("p95"),
               "lag_s": {g: v["s"] for g, v in s["lag"].items()},
               "wal_bytes_per_s": s["wal_bytes_per_s"], "driver_cpu_s": s["driver_cpu_s"],
               "late_ticks": s["late_ticks"]} for s in steps]})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="bench.pg_bench",
                                description="T6b Postgres as context store + event stream (see module docstring)")
    p.add_argument("--dsn", default=os.environ.get("PG_DSN_T6B", "postgres://lt:lt-test-only@lt-pg:5432/lt_t6b"))
    p.add_argument("--setup", choices=tuple(DESIGN_FILES), help="drop and recreate the T6b objects for a design")
    p.add_argument("--teardown", action="store_true", help="drop the T6b objects and exit")
    p.add_argument("--mode", choices=("baseline", "ramp"), help="run the load (after --setup when both are given)")
    p.add_argument("--events-per-sec", type=float, default=150.0, help="baseline rate")
    p.add_argument("--duration-s", type=float, default=60.0, help="baseline duration")
    p.add_argument("--ramp-steps", default="150,300,600,1200,2400,4800")
    p.add_argument("--step-s", type=float, default=30.0)
    p.add_argument("--max-s", type=float, default=180.0, help="hard cap on the whole run")
    p.add_argument("--reserve-s", type=float, default=8.0, help="part of --max-s kept for drain, stats and audit")
    p.add_argument("--decisions-per-event", type=float, default=0.113)
    p.add_argument("--users", type=int, default=50_000, help="users seeded by --setup")
    p.add_argument("--shards", type=int, default=8, help="cursor shards per group, fixed at --setup")
    p.add_argument("--pool", type=int, default=8)
    p.add_argument("--workers-a", type=int, default=2)
    p.add_argument("--workers-b", type=int, default=1)
    p.add_argument("--batch", type=int, default=200, help="max events per shard per consumer pass")
    p.add_argument("--visibility-timeout-s", type=float, default=10.0, help="lease length")
    p.add_argument("--poll-ms", type=float, default=20.0)
    p.add_argument("--crash-after-s", type=float, default=0.0, help="stop one group-A worker holding its leases")
    p.add_argument("--max-inflight", type=int, default=64, help="cap on concurrent ingest/read tasks")
    p.add_argument("--read-p95-ms", type=float, default=20.0)
    p.add_argument("--lag-growth-s", type=float, default=1.0)
    p.add_argument("--no-audit", dest="audit", action="store_false")
    p.add_argument("--seed", type=int, default=1)
    args = p.parse_args(argv)
    if not (args.setup or args.teardown or args.mode):
        p.error("give --setup, --teardown or --mode")
    if args.pool <= args.workers_a + args.workers_b:
        p.error("--pool must exceed --workers-a + --workers-b (each worker holds one connection)")
    return args


async def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    pool = ConnectionPool(dsn=args.dsn, max_db_pool_size=args.pool, application_name="t6b-bench",
                          options="-c statement_timeout=10000 -c idle_in_transaction_session_timeout=30000")
    try:
        await check_database(pool)
        if args.teardown:
            await teardown(pool)
            emit({"kind": "teardown", "bench": "pg", "database": DATABASE})
            return
        if args.setup:
            await setup(pool, args.setup, args)
            emit({"kind": "setup", "bench": "pg", "design": args.setup, "users": args.users, "shards": args.shards})
        if args.mode:
            await run(pool, args)
    finally:
        pool.close()


if __name__ == "__main__":
    uvloop.run(main())
