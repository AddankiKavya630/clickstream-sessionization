"""Target publishing (CSV snapshots), run reports and per-run log files."""
from __future__ import annotations

import csv
import json
import logging
import os
import sqlite3
import tempfile
import time
from dataclasses import asdict
from pathlib import Path

SESSION_EXPORT_COLUMNS = (
    "session_id", "user_id", "user_session_seq", "session_start_ts", "session_end_ts",
    "duration_seconds", "event_count", "distinct_event_types", "first_event_id", "last_event_id",
    "landing_page", "exit_page", "device", "is_bounce", "gap_seconds_used",
)
REJECT_EXPORT_COLUMNS = (
    "reject_id", "record_class", "reason_code", "reason_detail", "event_id",
    "source_file", "source_line", "raw_record", "run_id",
)


class _RunIdFilter(logging.Filter):
    def __init__(self, run_id: str):
        super().__init__()
        self.run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = self.run_id
        return True


def attach_run_log(out_dir: Path, run_id: str) -> logging.Handler:
    """Write a JSON-lines log for this run to <out>/logs/<run_id>.log."""
    log_dir = Path(out_dir) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(log_dir / f"{run_id}.log", encoding="utf-8")
    handler.addFilter(_RunIdFilter(run_id))
    fmt = logging.Formatter(
        '{"ts":"%(asctime)s.%(msecs)03dZ","level":"%(levelname)s","run_id":"%(run_id)s","msg":%(message_json)s}',
        datefmt="%Y-%m-%dT%H:%M:%S")
    fmt.converter = time.gmtime          # log timestamps in UTC, like everything else
    handler.setFormatter(fmt)
    handler.setLevel(logging.INFO)

    class _JsonMsg(logging.Filter):
        def filter(self, record):
            record.message_json = json.dumps(record.getMessage())
            return True

    handler.addFilter(_JsonMsg())
    logger = logging.getLogger("sessionize")
    logger.addHandler(handler)
    if logger.level == logging.NOTSET or logger.level > logging.INFO:
        logger.setLevel(logging.INFO)
    return handler


def detach_run_log(handler: logging.Handler) -> None:
    logging.getLogger("sessionize").removeHandler(handler)
    handler.close()


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
            fh.write(text)
        os.replace(tmp, path)        # atomic on POSIX and Windows: readers never see half a file
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def _atomic_write_csv(path: Path, columns: tuple[str, ...], rows) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    n = 0
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(columns)
            for row in rows:
                w.writerow([row[c] for c in columns])
                n += 1
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise
    return n


def publish_targets(conn: sqlite3.Connection, out_dir: Path) -> dict:
    """Full-snapshot export for the analytical consumer. Deterministic ordering, atomic replace,
    so re-publishing the same warehouse state produces byte-identical files."""
    out_dir = Path(out_dir)
    n_sessions = _atomic_write_csv(
        out_dir / "sessions.csv", SESSION_EXPORT_COLUMNS,
        conn.execute(f"SELECT {', '.join(SESSION_EXPORT_COLUMNS)} FROM sessions "
                     "ORDER BY user_id, user_session_seq"))
    n_rejects = _atomic_write_csv(
        out_dir / "rejected_events.csv", REJECT_EXPORT_COLUMNS,
        conn.execute(f"SELECT {', '.join(REJECT_EXPORT_COLUMNS)} FROM rejected_events "
                     "ORDER BY source_file, source_line"))
    return {"sessions": n_sessions, "rejected_events": n_rejects}


def _query_dicts(conn: sqlite3.Connection, sql: str, params=()) -> list[dict]:
    return [dict(r) for r in conn.execute(sql, params)]


def write_run_report(conn: sqlite3.Connection, out_dir: Path, result) -> Path:
    out_dir = Path(out_dir)
    report = {
        "run_id": result.run_id,
        "status": result.status,
        "exit_code": result.exit_code,
        "started_at": result.started_at,
        "ended_at": result.ended_at,
        "failure_reason": result.failure_reason,
        "metrics": result.metrics,
        "files": [asdict(f) for f in result.files],
        "dq_checks": [{"name": c.name, "status": c.status, "observed": c.observed,
                       "expected": c.expected, "description": c.description} for c in result.checks],
        "warehouse_totals": {
            "dim_users": conn.execute("SELECT COUNT(*) FROM dim_users").fetchone()[0],
            "events": conn.execute("SELECT COUNT(*) FROM events").fetchone()[0],
            "sessions": conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0],
            "rejected_events": conn.execute("SELECT COUNT(*) FROM rejected_events").fetchone()[0],
        },
        "rejects_by_reason_this_run": _query_dicts(conn,
            "SELECT record_class, reason_code, COUNT(*) AS n FROM rejected_events WHERE run_id = ? "
            "GROUP BY record_class, reason_code ORDER BY record_class, reason_code", (result.run_id,)),
        "session_kpis": _query_dicts(conn,
            "SELECT COUNT(*) AS sessions, COUNT(DISTINCT user_id) AS users, "
            "ROUND(AVG(event_count), 3) AS avg_events_per_session, "
            "ROUND(AVG(duration_seconds), 3) AS avg_duration_seconds, "
            "ROUND(AVG(is_bounce), 4) AS bounce_rate FROM sessions")[0],
    }
    reports = out_dir / "run_reports"
    path = reports / f"{result.run_id}.json"
    text = json.dumps(report, indent=2, default=str)
    _atomic_write_text(path, text)
    _atomic_write_text(reports / "latest.json", text)
    _atomic_write_text(out_dir / "reconciliation_report.md", render_markdown(report))
    return path


def render_markdown(report: dict) -> str:
    m = report["metrics"]
    lines = [
        f"# Reconciliation report — `{report['run_id']}`",
        "",
        f"- **Status:** {report['status']} (exit code {report['exit_code']})",
        f"- **Started / ended (UTC):** {report['started_at']} / {report['ended_at']}",
        f"- **Duration:** {m['duration_seconds']} s, throughput {m['throughput_rows_per_second']} rows/s",
    ]
    if report["failure_reason"]:
        lines.append(f"- **Failure reason:** {report['failure_reason']}")
    lines += [
        "",
        "## Source-to-target counts (this run)",
        "",
        "| file | status | read | inserted | duplicate | already loaded | rejected | balanced |",
        "|---|---|---:|---:|---:|---:|---:|:---:|",
    ]
    for f in report["files"]:
        bal = f["rows_read"] == f["rows_inserted"] + f["rows_duplicate"] + f["rows_already_loaded"] + f["rows_rejected"]
        lines.append(f"| {f['source_file']} | {f['status']} | {f['rows_read']} | {f['rows_inserted']} | "
                     f"{f['rows_duplicate']} | {f['rows_already_loaded']} | {f['rows_rejected']} | "
                     f"{'yes' if bal or f['status'] != 'SUCCESS' else '**NO**'} |")
    lines += [
        f"| **total** | | {m['input_rows']} | {m['events_inserted']} | {m['duplicate_rows']} | "
        f"{m['already_loaded_rows']} | {m['rejected_rows']} | |",
        "",
        "## Quarantine by reason (this run)",
        "",
        "| class | reason | rows |",
        "|---|---|---:|",
    ]
    for r in report["rejects_by_reason_this_run"]:
        lines.append(f"| {r['record_class']} | {r['reason_code']} | {r['n']} |")
    lines += ["", "## Data-quality checks", "", "| check | status | violations | rule |", "|---|---|---:|---|"]
    for c in report["dq_checks"]:
        lines.append(f"| {c['name']} | {c['status']} | {c['observed']} | {c['description']} |")
    t = report["warehouse_totals"]
    k = report["session_kpis"]
    lines += [
        "",
        "## Warehouse totals (all runs)",
        "",
        f"- users: {t['dim_users']}, events: {t['events']}, sessions: {t['sessions']}, "
        f"quarantined rows: {t['rejected_events']}",
        f"- avg events/session: {k['avg_events_per_session']}, avg duration: {k['avg_duration_seconds']} s, "
        f"bounce rate: {k['bounce_rate']}",
        "",
    ]
    return "\n".join(lines)
