"""Shared fixtures: tiny hand-written inputs so every test states its own expectations."""
from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import pytest

from sessionize.config import PipelineConfig
from sessionize.pipeline import run_pipeline
from sessionize.schema import SOURCE_COLUMNS
from sessionize.timeutil import parse_iso_utc

REFERENCE_TIME = parse_iso_utc("2026-12-31T00:00:00Z")
USERS = ["u1", "u2", "u3"]


def write_events(path: Path, rows: list[dict | list | str], columns=SOURCE_COLUMNS) -> Path:
    """rows: dicts (missing keys -> ''), lists (written as-is) or raw strings (written verbatim)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(columns)
        for r in rows:
            if isinstance(r, str):
                fh.write(r + "\n")
            elif isinstance(r, dict):
                w.writerow([r.get(c, "") for c in columns])
            else:
                w.writerow(r)
    return path


def ev(event_id: str, ts: str, user: str = "u1", etype: str = "page_view", page: str = "/home",
       device: str = "desktop") -> dict:
    return {"event_id": event_id, "user_id": user, "event_type": etype, "event_ts": ts,
            "page_url": page, "device": device}


class Env:
    def __init__(self, tmp: Path):
        self.tmp = tmp
        self.inbox = tmp / "inbox"
        self.inbox.mkdir()
        self.db = tmp / "wh.db"
        self.out = tmp / "out"
        self.users = tmp / "users.csv"
        with self.users.open("w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(["user_id", "signup_ts", "country"])
            for u in USERS:
                w.writerow([u, "2026-01-01T00:00:00Z", "in"])
        self.cfg = PipelineConfig(reference_time=REFERENCE_TIME)

    def file(self, name: str, rows, columns=SOURCE_COLUMNS) -> Path:
        return write_events(self.inbox / name, rows, columns)

    def run(self, inputs=None, **kw):
        cfg = kw.pop("cfg", self.cfg)
        return run_pipeline(inputs or [self.inbox], self.db, self.out, cfg, users_path=self.users, **kw)

    def q(self, sql: str, params=()) -> list[tuple]:
        conn = sqlite3.connect(self.db)
        try:
            return [tuple(r) for r in conn.execute(sql, params)]
        finally:
            conn.close()

    def scalar(self, sql: str, params=()):
        return self.q(sql, params)[0][0]

    def sessions(self) -> list[tuple]:
        """(user_id, start, end, event_count) ordered — the business result."""
        return self.q("SELECT user_id, session_start_ts, session_end_ts, event_count FROM sessions "
                      "ORDER BY user_id, session_start_epoch_ms")

    def reasons(self) -> dict[int, str]:
        return dict(self.q("SELECT source_line, reason_code FROM rejected_events ORDER BY source_line"))


@pytest.fixture
def env(tmp_path) -> Env:
    return Env(tmp_path)
