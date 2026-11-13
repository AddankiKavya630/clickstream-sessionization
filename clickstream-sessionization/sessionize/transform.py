"""Sessionization step: recompute sessions for a set of users using sql/sessionize.sql."""
from __future__ import annotations

import sqlite3
from typing import Iterable

from .db import load_named_sql


ALL_USERS = "all"
PENDING = "pending"


def recompute_sessions(conn: sqlite3.Connection, user_ids: Iterable[str] | str,
                       gap_seconds: int, run_id: str, now: str) -> tuple[int, int]:
    """Delete and rebuild sessions for ``user_ids``: an iterable of ids, ALL_USERS, or PENDING
    (the pending_session_users queue, which is drained in the same transaction).

    Must be called inside the caller's transaction so events and sessions
    always change together. Returns (users_recomputed, sessions_written).
    """
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS affected_users (user_id TEXT PRIMARY KEY)")
    conn.execute("DELETE FROM affected_users")
    if user_ids == ALL_USERS:
        conn.execute("INSERT INTO affected_users SELECT DISTINCT user_id FROM events")
    elif user_ids == PENDING:
        conn.execute("INSERT INTO affected_users SELECT user_id FROM pending_session_users")
    else:
        conn.executemany("INSERT OR IGNORE INTO affected_users VALUES (?)", ((u,) for u in user_ids))
    n_users = conn.execute("SELECT COUNT(*) FROM affected_users").fetchone()[0]
    if n_users == 0:
        return 0, 0
    conn.execute("DELETE FROM pending_session_users WHERE user_id IN (SELECT user_id FROM affected_users)")

    params = {"gap_ms": gap_seconds * 1000, "gap_seconds": gap_seconds, "run_id": run_id, "now": now}
    sessions_written = 0
    for name, _desc, stmt in load_named_sql("sessionize.sql"):
        needed = {k: v for k, v in params.items() if f":{k}" in stmt}
        cur = conn.execute(stmt, needed)
        if name == "insert_sessions":
            sessions_written = cur.rowcount
    conn.execute("DROP TABLE IF EXISTS temp.tmp_session_events")
    return n_users, sessions_written
