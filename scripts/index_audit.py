"""Index audit: EXPLAIN (ANALYZE, BUFFERS) for every SQL statement the services issue.

Writes results/index_audit.md. Run right after a T1 run (before stats are reset), with lt-pg reachable:
    uv run python scripts/index_audit.py [--dsn postgres://lt:...@127.0.0.1:17110/lt]
Write statements run inside a transaction that is rolled back, so the audit changes nothing.
"""

import argparse
import asyncio
import re
import time
from datetime import UTC, datetime
from pathlib import Path

import msgspec
from psqlpy import ConnectionPool

from lt import decision, payments, scheduler

STATEMENTS: list[tuple[str, str, str]] = [
    ("decision.SQL_GET", decision.SQL_GET, "event_id"),
    ("decision.SQL_WRITE", decision.SQL_WRITE, "write_new"),
    ("decision.SQL_POLICIES", decision.SQL_POLICIES, "none"),
    ("payments.SQL_LOCK", payments.SQL_LOCK, "user"),
    ("payments.SQL_PAY", payments.SQL_PAY, "pay_new"),
    ("payments.SQL_EMITTED", payments.SQL_EMITTED, "payment_ids"),
    ("payments.SQL_UNEMITTED", payments.SQL_UNEMITTED, "none"),
    ("scheduler.SQL_CLAIM", scheduler.SQL_CLAIM, "claim"),
    ("scheduler.SQL_FACTS", scheduler.SQL_FACTS, "facts"),
    ("scheduler.SQL_GRANT", scheduler.SQL_GRANT, "grant_new"),
    ("scheduler.SQL_DONE", scheduler.SQL_DONE, "done"),
    ("scheduler.SQL_RELAY", scheduler.SQL_RELAY, "none"),
    ("scheduler.SQL_BACKLOG", scheduler.SQL_BACKLOG, "none"),
]


async def sample_params(conn) -> dict[str, list]:
    ev = await conn.fetch_val("SELECT event_id FROM decisions ORDER BY id DESC LIMIT 1")
    uid = await conn.fetch_val("SELECT user_id FROM payments WHERE provider <> 'seed' ORDER BY id DESC LIMIT 1")
    job = await conn.fetch_val("SELECT id FROM next_day_jobs ORDER BY id DESC LIMIT 1")
    pids = [r["id"] for r in (await conn.execute("SELECT id FROM payments ORDER BY id DESC LIMIT 5")).result()]
    tag = f"audit-{time.time_ns()}"
    body = msgspec.msgpack.encode({"audit": True})
    return {
        "event_id": [ev], "none": [], "user": [uid], "payment_ids": [pids], "claim": [50],
        "write_new": [1, 0.5, tag, "u-audit", "abandonment", "rule", "small_plan_hint", "audit", "ok",
                      "price_sensitive", body, 30.0],
        "pay_new": ["tappay", tag, uid, 300, "TWD", time.time() * 1000, "first_recharge_bonus"],
        "facts": [uid, datetime(2026, 1, 1, tzinfo=UTC), "first_recharge_bonus"],
        "grant_new": [f"u-{tag}", 1, "first_recharge_bonus", 20, 90.0, job, body],
        "done": [job, "pushed_offer", "audit"],
    }


def norm(q: str) -> str:
    """pg_stat_statements replaces literals with $n: compare with literals and parameters both erased."""
    return re.sub(r"\s+", " ", re.sub(r"\$\d+|'[^']*'", "?", q)).strip()


def walk(node: dict, out: list[str]) -> None:
    label = node["Node Type"]
    if idx := node.get("Index Name"):
        label += f" using {idx}"
    elif rel := node.get("Relation Name"):
        label += f" on {rel}"
    out.append(f"{label} (rows {node.get('Actual Rows')}, hit {node.get('Shared Hit Blocks', 0)},"
               f" read {node.get('Shared Read Blocks', 0)})")
    for child in node.get("Plans", []):
        walk(child, out)


async def main(dsn: str) -> None:
    pool = ConnectionPool(dsn=dsn, max_db_pool_size=2)
    rows_md = ["| statement | plan nodes (rows, buffers hit/read) | exec ms | pg_stat_statements calls / mean ms |",
               "|---|---|---|---|"]
    async with pool.acquire() as conn:
        params = await sample_params(conn)
        stats = {norm(r["query"]): r for r in (await conn.execute(
            "SELECT query, calls, mean_exec_time FROM pg_stat_statements")).result()}
        for name, sql, kind in STATEMENTS:
            tx = conn.transaction()
            await tx.begin()
            try:
                plan = await tx.fetch_val(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}", params[kind],
                                          prepared=False)
            finally:
                await tx.rollback()
            top = plan[0] if isinstance(plan, list) else msgspec.json.decode(plan)[0]
            nodes: list[str] = []
            walk(top["Plan"], nodes)
            st = stats.get(norm(sql))
            pss = f"{st['calls']} / {st['mean_exec_time']:.3f}" if st else "not called in this run"
            rows_md.append(f"| `{name}` | {'<br>'.join(nodes)} | {top['Execution Time']:.3f} | {pss} |")
        tables = (await conn.execute(
            "SELECT relname, seq_scan, idx_scan, n_tup_upd, n_tup_hot_upd FROM pg_stat_user_tables ORDER BY relname"
        )).result()
        indexes = (await conn.execute(
            "SELECT s.relname, s.indexrelname, s.idx_scan, i.indisunique OR i.indisprimary AS constraint_index"
            " FROM pg_stat_user_indexes s JOIN pg_index i ON i.indexrelid = s.indexrelid ORDER BY 1, 2")).result()
    pool.close()
    md = ["# Index audit", "", f"Generated {time.strftime('%Y-%m-%d %H:%M:%S %Z')} against a populated stack right after"
          " a baseline-load run; pg_stat_statements and the table/index counters cover that run. Write statements ran inside a rolled-back transaction.", "", *rows_md, "",
          "## pg_stat_user_tables (since the run's stats reset)", "",
          "| table | seq_scan | idx_scan | n_tup_upd | n_tup_hot_upd |", "|---|---|---|---|---|",
          *[f"| {t['relname']} | {t['seq_scan']} | {t['idx_scan']} | {t['n_tup_upd']} | {t['n_tup_hot_upd']} |"
            for t in tables], "",
          "## pg_stat_user_indexes", "", "| table | index | idx_scan | constraint index |", "|---|---|---|---|",
          *[f"| {i['relname']} | {i['indexrelname']} | {i['idx_scan']} | {i['constraint_index']} |" for i in indexes]]
    Path("results/index_audit.md").write_text("\n".join(md) + "\n")
    print("\n".join(md))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default="postgres://lt:lt-test-only@127.0.0.1:17110/lt")   # test-only default
    asyncio.run(main(ap.parse_args().dsn))
