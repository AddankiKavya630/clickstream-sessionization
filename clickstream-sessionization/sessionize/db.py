"""SQLite connection, schema management and named-SQL loading."""
from __future__ import annotations

import re
import sqlite3
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path

from .ids import make_session_id

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
_NAME_RE = re.compile(r"^--\s*name:\s*([A-Za-z0-9_]+)\s*(?:\|\s*(.*))?$")


def connect(db_path: str | Path) -> sqlite3.Connection:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    # isolation_level=None: we issue BEGIN/COMMIT ourselves so transaction
    # boundaries are explicit (one transaction per source file).
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.create_function("make_session_id", 2, make_session_id, deterministic=True)
    apply_schema(conn)
    return conn


def apply_schema(conn: sqlite3.Connection) -> None:
    conn.executescript((SQL_DIR / "schema.sql").read_text(encoding="utf-8"))


@lru_cache(maxsize=None)
def load_named_sql(filename: str) -> tuple[tuple[str, str, str], ...]:
    """Parse a .sql file into (name, description, statement) blocks, in file order."""
    blocks: list[list] = []
    for line in (SQL_DIR / filename).read_text(encoding="utf-8").splitlines():
        m = _NAME_RE.match(line.strip())
        if m:
            blocks.append([m.group(1), (m.group(2) or "").strip(), []])
        elif blocks:
            blocks[-1][2].append(line)
    out = []
    for name, desc, lines in blocks:
        stmt = "\n".join(l for l in lines if not l.strip().startswith("--")).strip().rstrip(";").strip()
        if stmt:
            out.append((name, desc, stmt))
    return tuple(out)


@contextmanager
def transaction(conn: sqlite3.Connection):
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")
