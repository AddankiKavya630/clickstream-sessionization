"""Deterministic identifiers."""
from __future__ import annotations

import hashlib
import secrets
from datetime import datetime
from pathlib import Path

_SEP = "\x1f"  # unit separator: cannot appear in normal ids, avoids 'a|b' + 'c' == 'a' + 'b|c'


def stable_hash(*parts: object, length: int = 20) -> str:
    joined = _SEP.join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:length]


def make_session_id(user_id: str, first_event_id: str) -> str:
    """A session is identified by its user and its first event (ordered by ts, event_id)."""
    return "ses_" + stable_hash(user_id, first_event_id)


def make_reject_id(source_file_hash: str, source_line: int) -> str:
    return "rej_" + stable_hash(source_file_hash, source_line)


def new_run_id(started_at: datetime) -> str:
    """Unique per execution (a run is an event in time, not a function of the data)."""
    return f"run_{started_at:%Y%m%dT%H%M%SZ}_{secrets.token_hex(3)}"


def file_sha256(path: Path, chunk_size: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as fh:
        while chunk := fh.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()
