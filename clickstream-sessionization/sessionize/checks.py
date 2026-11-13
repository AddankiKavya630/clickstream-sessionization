"""Reconciliation / data-quality checks (sql/reconciliation.sql)."""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from .db import load_named_sql, transaction
from .timeutil import now_iso


@dataclass(frozen=True)
class CheckResult:
    name: str
    description: str
    observed: int
    expected: int = 0

    @property
    def passed(self) -> bool:
        return self.observed == self.expected

    @property
    def status(self) -> str:
        return "PASS" if self.passed else "FAIL"


def run_checks(conn: sqlite3.Connection, gap_seconds: int, run_id: str) -> list[CheckResult]:
    params = {"gap_ms": gap_seconds * 1000, "run_id": run_id}
    results = []
    for name, desc, stmt in load_named_sql("reconciliation.sql"):
        needed = {k: v for k, v in params.items() if f":{k}" in stmt}
        observed = conn.execute(stmt, needed).fetchone()[0] or 0
        results.append(CheckResult(name, desc, int(observed)))
    return results


def persist_checks(conn: sqlite3.Connection, run_id: str, results: list[CheckResult]) -> None:
    ts = now_iso()
    with transaction(conn):
        conn.executemany(
            "INSERT OR REPLACE INTO dq_check_results VALUES (?,?,?,?,?,?,?)",
            [(run_id, r.name, r.status, r.observed, r.expected, r.description, ts) for r in results],
        )
