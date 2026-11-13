"""Pipeline orchestration: ingest -> validate -> de-duplicate -> load -> sessionize -> check -> publish.

Unit of atomicity: ONE SOURCE FILE. Its events, quarantined rows, recomputed
sessions and file-log entry are committed in a single transaction, so a crash
leaves either all of the file or none of it in the warehouse.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from . import publish
from .checks import CheckResult, persist_checks, run_checks
from .config import PipelineConfig
from .db import connect, transaction
from .ids import file_sha256, make_reject_id, new_run_id
from .readers import open_source, read_users
from .schema import PAYLOAD_FIELDS, USERS_REQUIRED_COLUMNS, Reason
from .timeutil import TimestampError, format_utc, parse_iso_utc, utc_now
from .transform import ALL_USERS, PENDING, recompute_sessions
from .validation import RecordValidator, RejectedRecord, ValidEvent, check_header

log = logging.getLogger("sessionize")

EXIT_CODES = {"SUCCESS": 0, "FAILED": 1, "PARTIAL_SUCCESS": 2, "DQ_FAILED": 3}
FAULT_ENV = "SESSIONIZE_FAULT_INJECT"   # "<stage>[:<file-name-substring>]", for retry demos/tests


class FaultInjected(RuntimeError):
    pass


class UsersFileError(RuntimeError):
    pass


@dataclass
class FileResult:
    source_file: str
    source_file_hash: str
    status: str                       # SUCCESS | SKIPPED | FAILED | REJECTED
    rows_read: int = 0
    rows_inserted: int = 0
    rows_duplicate: int = 0
    rows_already_loaded: int = 0
    rows_rejected: int = 0
    users_recomputed: int = 0
    sessions_written: int = 0
    schema_warnings: list[str] = field(default_factory=list)
    error: str | None = None


@dataclass
class RunResult:
    run_id: str
    status: str
    started_at: str
    ended_at: str
    files: list[FileResult]
    checks: list[CheckResult]
    metrics: dict
    report_path: str | None
    failure_reason: str | None = None

    @property
    def exit_code(self) -> int:
        return EXIT_CODES[self.status]


# --------------------------------------------------------------------------- helpers

def _maybe_fault(stage: str, path: Path, fault_inject: str | None) -> None:
    spec = fault_inject or os.environ.get(FAULT_ENV)
    if not spec:
        return
    want_stage, _, name_part = spec.partition(":")
    if want_stage == stage and (not name_part or name_part in path.name):
        raise FaultInjected(f"injected fault at stage '{stage}' for {path.name}")


def discover_inputs(inputs: list[str | Path], extensions: tuple[str, ...]) -> list[Path]:
    """Expand files/directories into a deterministic, de-duplicated, sorted file list."""
    found: dict[Path, None] = {}
    for item in inputs:
        p = Path(item)
        if p.is_dir():
            for child in sorted(p.iterdir()):
                if child.is_file() and child.suffix.lower() in extensions:
                    found[child.resolve()] = None
        elif p.is_file():
            found[p.resolve()] = None
        else:
            raise FileNotFoundError(f"input not found: {p}")
    return sorted(found, key=lambda x: (x.name, str(x)))


def _display_path(p: Path) -> str:
    try:
        return p.resolve().relative_to(Path.cwd()).as_posix()
    except ValueError:
        return p.name if p.is_absolute() else p.as_posix()


# --------------------------------------------------------------------------- users

def load_users(conn: sqlite3.Connection, users_path: Path, run_id: str) -> dict:
    columns, records = read_users(users_path)
    missing = [c for c in USERS_REQUIRED_COLUMNS if c not in columns]
    if not columns or missing:
        raise UsersFileError(f"users file {users_path.name} is missing column(s): {missing or list(USERS_REQUIRED_COLUMNS)}")
    stats = {"rows": 0, "loaded": 0, "invalid": 0, "duplicates": 0, "bad_signup_ts": 0}
    rows: dict[str, tuple] = {}
    now = format_utc(utc_now())
    shown = _display_path(users_path)
    for rec in records:
        stats["rows"] += 1
        if rec.fields is None:
            stats["invalid"] += 1
            continue
        uid = (rec.fields.get("user_id") or "").strip()
        if not uid:
            stats["invalid"] += 1
            continue
        if uid in rows:
            stats["duplicates"] += 1      # first occurrence wins
            continue
        signup = (rec.fields.get("signup_ts") or "").strip() or None
        if signup:
            try:
                signup = format_utc(parse_iso_utc(signup))
            except TimestampError:
                stats["bad_signup_ts"] += 1
                signup = None
        country = (rec.fields.get("country") or "").strip().upper() or None
        rows[uid] = (uid, signup, country, shown, run_id, now)
    with transaction(conn):
        conn.executemany(
            """INSERT INTO dim_users (user_id, signup_ts, country, source_file, loaded_run_id, updated_at)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(user_id) DO UPDATE SET
                 signup_ts = excluded.signup_ts, country = excluded.country,
                 source_file = excluded.source_file, loaded_run_id = excluded.loaded_run_id,
                 updated_at = excluded.updated_at""",
            list(rows.values()),
        )
    stats["loaded"] = len(rows)
    return stats


# --------------------------------------------------------------------------- one file

def _diff_fields(a: tuple, b: tuple) -> str:
    return ",".join(n for n, x, y in zip(PAYLOAD_FIELDS, a, b) if x != y)


def _classify_duplicate(ev: ValidEvent, raw: str, ref: tuple[tuple, str]) -> RejectedRecord:
    ref_payload, where = ref
    if ref_payload == ev.payload():
        return RejectedRecord(ev.source_line, "duplicate", Reason.DUPLICATE_EXACT,
                              f"duplicate of {where}", ev.event_id, raw)
    return RejectedRecord(ev.source_line, "duplicate", Reason.DUPLICATE_CONFLICT,
                          f"conflicts with {where}; reference version kept; differs in: "
                          f"{_diff_fields(ref_payload, ev.payload())}", ev.event_id, raw)


def _upsert_file_log(conn, fr: FileResult, size: int, run_id: str) -> None:
    conn.execute(
        """INSERT INTO file_ingestion_log (source_file_hash, source_file, file_size_bytes, status, attempts,
               first_run_id, last_run_id, rows_read, rows_inserted, rows_duplicate, rows_already_loaded,
               rows_rejected, schema_warnings, error_message, updated_at)
           VALUES (?,?,?,?,1,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(source_file_hash) DO UPDATE SET
               source_file = excluded.source_file, status = excluded.status,
               attempts = file_ingestion_log.attempts + 1, last_run_id = excluded.last_run_id,
               rows_read = excluded.rows_read, rows_inserted = excluded.rows_inserted,
               rows_duplicate = excluded.rows_duplicate, rows_already_loaded = excluded.rows_already_loaded,
               rows_rejected = excluded.rows_rejected, schema_warnings = excluded.schema_warnings,
               error_message = excluded.error_message, updated_at = excluded.updated_at""",
        (fr.source_file_hash, fr.source_file, size, fr.status, run_id, run_id, fr.rows_read,
         fr.rows_inserted, fr.rows_duplicate, fr.rows_already_loaded, fr.rows_rejected,
         json.dumps(fr.schema_warnings) if fr.schema_warnings else None, fr.error, format_utc(utc_now())),
    )


def _insert_rejects(conn, rejects: list[RejectedRecord], fr: FileResult, run_id: str) -> None:
    now = format_utc(utc_now())
    conn.executemany(
        """INSERT OR IGNORE INTO rejected_events (reject_id, record_class, reason_code, reason_detail, event_id,
               source_file, source_file_hash, source_line, raw_record, run_id, rejected_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        [(make_reject_id(fr.source_file_hash, r.source_line), r.record_class, r.reason_code, r.reason_detail,
          r.event_id, fr.source_file, fr.source_file_hash, r.source_line, r.raw, run_id, now)
         for r in rejects],
    )


def process_file(conn: sqlite3.Connection, path: Path, cfg: PipelineConfig, run_id: str,
                 reference_time: datetime, known_users: set[str], force: bool = False,
                 fault_inject: str | None = None) -> FileResult:
    file_hash = file_sha256(path)
    size = path.stat().st_size
    fr = FileResult(_display_path(path), file_hash, "SUCCESS")

    prev = conn.execute("SELECT status, last_run_id FROM file_ingestion_log WHERE source_file_hash = ?",
                        (file_hash,)).fetchone()
    if prev and prev["status"] in ("SUCCESS", "REJECTED") and not force:
        fr.status = "SKIPPED"
        log.info("skip %s: identical content already %s in %s", fr.source_file, prev["status"], prev["last_run_id"])
        return fr

    src = open_source(path)
    try:
        # ---- file-level schema check (CSV header)
        if src.fmt == "csv" and src.columns:
            hc = check_header(src.columns)
            if hc.extra:
                fr.schema_warnings.append(f"SCHEMA_DRIFT: unexpected column(s) ignored: {', '.join(hc.extra)}")
            if not hc.ok:
                detail = []
                if hc.missing:
                    detail.append(f"missing required column(s): {', '.join(hc.missing)}")
                if hc.duplicated:
                    detail.append(f"duplicated column(s): {', '.join(hc.duplicated)}")
                fr.status, fr.error = "REJECTED", "; ".join(detail)
                with transaction(conn):
                    _insert_rejects(conn, [RejectedRecord(0, "file", Reason.FILE_SCHEMA_MISMATCH, fr.error,
                                                          None, json.dumps(src.columns))], fr, run_id)
                    _upsert_file_log(conn, fr, size, run_id)
                log.error("file %s rejected: %s", fr.source_file, fr.error)
                return fr

        # ---- record validation + in-file de-duplication (first occurrence wins)
        validator = RecordValidator(cfg, known_users, reference_time)
        valid: dict[str, ValidEvent] = {}
        raw_of: dict[str, str] = {}                        # event_id -> raw text of its first occurrence
        invalid: list[RejectedRecord] = []
        later_copies: list[tuple[ValidEvent, str]] = []   # 2nd+ occurrence of an event_id in this file
        for rec in src.records:
            fr.rows_read += 1
            res = validator.validate(rec)
            if isinstance(res, RejectedRecord):
                invalid.append(res)
            elif res.event_id in valid:
                later_copies.append((res, rec.raw))
            else:
                valid[res.event_id] = res
                raw_of[res.event_id] = rec.raw
        if src.fmt == "jsonl" and validator.extra_keys_seen:
            fr.schema_warnings.append(
                f"SCHEMA_DRIFT: unexpected key(s) ignored: {', '.join(sorted(validator.extra_keys_seen))}")
        _maybe_fault("after_validate", path, fault_inject)

        # ---- load: one transaction per file
        with transaction(conn):
            conn.execute("CREATE TEMP TABLE IF NOT EXISTS batch_ids (event_id TEXT PRIMARY KEY)")
            conn.execute("DELETE FROM batch_ids")
            conn.executemany("INSERT INTO batch_ids VALUES (?)", ((k,) for k in valid))
            stored = conn.execute(
                f"""SELECT e.event_id, e.source_file, e.source_file_hash, e.source_line, {", ".join("e." + c for c in PAYLOAD_FIELDS)}
                    FROM events e JOIN batch_ids b ON b.event_id = e.event_id""").fetchall()
            # The reference version of an event_id is the STORED one if it exists, otherwise the
            # first occurrence in this file. Every other copy is quarantined as a duplicate.
            reference: dict[str, tuple[tuple, str]] = {
                k: (v.payload(), f"line {v.source_line} in the same file") for k, v in valid.items()}
            dups: list[RejectedRecord] = []
            for row in stored:
                ev = valid.pop(row["event_id"])
                stored_payload = tuple(row[c] for c in PAYLOAD_FIELDS)
                reference[ev.event_id] = (stored_payload, f"{row['source_file']} line {row['source_line']} (already loaded)")
                if row["source_file_hash"] == file_hash and row["source_line"] == ev.source_line:
                    fr.rows_already_loaded += 1      # same file re-processed (--force): no-op
                    continue
                dups.append(_classify_duplicate(ev, raw_of[ev.event_id], reference[ev.event_id]))
            for ev, raw in later_copies:
                dups.append(_classify_duplicate(ev, raw, reference[ev.event_id]))
            now = format_utc(utc_now())
            new_events = list(valid.values())
            conn.executemany(
                """INSERT INTO events (event_id, user_id, event_type, event_ts, event_ts_epoch_ms, page_url, device,
                       source_file, source_file_hash, source_line, loaded_run_id, ingested_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                [(e.event_id, e.user_id, e.event_type, e.event_ts, e.event_ts_epoch_ms, e.page_url, e.device,
                  fr.source_file, file_hash, e.source_line, run_id, now) for e in new_events],
            )
            fr.rows_inserted = len(new_events)
            fr.rows_rejected = len(invalid)
            fr.rows_duplicate = len(dups)
            _maybe_fault("after_insert_events", path, fault_inject)
            _insert_rejects(conn, invalid + dups, fr, run_id)

            conn.executemany("INSERT OR IGNORE INTO pending_session_users VALUES (?,?,?)",
                             ((u, run_id, now) for u in {e.user_id for e in new_events}))
            if cfg.sessionize_mode == "per_file":
                fr.users_recomputed, fr.sessions_written = recompute_sessions(
                    conn, PENDING, cfg.session_gap_seconds, run_id, now)
            _maybe_fault("after_sessionize", path, fault_inject)
            _upsert_file_log(conn, fr, size, run_id)

        log.info("loaded %s: read=%d inserted=%d duplicate=%d already_loaded=%d rejected=%d "
                 "users_recomputed=%d sessions_written=%d",
                 fr.source_file, fr.rows_read, fr.rows_inserted, fr.rows_duplicate,
                 fr.rows_already_loaded, fr.rows_rejected, fr.users_recomputed, fr.sessions_written)
        for w in fr.schema_warnings:
            log.warning("%s: %s", fr.source_file, w)
        return fr
    finally:
        src.close()


def _record_file_failure(conn, path: Path, fr: FileResult, run_id: str, exc: BaseException) -> None:
    fr.status = "FAILED"
    fr.error = f"{type(exc).__name__}: {exc}"
    fr.rows_inserted = fr.rows_duplicate = fr.rows_already_loaded = fr.rows_rejected = 0
    fr.users_recomputed = fr.sessions_written = 0
    try:
        with transaction(conn):
            _upsert_file_log(conn, fr, path.stat().st_size, run_id)
    except Exception:  # pragma: no cover - logging must never mask the original error
        log.exception("could not record failure for %s", fr.source_file)


# --------------------------------------------------------------------------- run

def run_pipeline(inputs: list[str | Path], db_path: str | Path, out_dir: str | Path,
                 cfg: PipelineConfig, users_path: str | Path | None = None, force: bool = False,
                 rebuild_sessions: bool = False, fault_inject: str | None = None,
                 publish_outputs: bool = True) -> RunResult:
    started = utc_now()
    t0 = time.perf_counter()
    run_id = new_run_id(started)
    reference_time = cfg.reference_time or started
    out_dir = Path(out_dir)
    timings: dict[str, float] = {}

    log_handler = publish.attach_run_log(out_dir, run_id)
    conn = connect(db_path)
    files: list[FileResult] = []
    checks: list[CheckResult] = []
    status, failure_reason = "SUCCESS", None
    users_stats: dict = {}
    run_level = {"users_recomputed": 0, "sessions_written": 0}
    try:
        with transaction(conn):
            conn.execute("INSERT INTO pipeline_runs (run_id, status, started_at, config_json) VALUES (?,?,?,?)",
                         (run_id, "RUNNING", format_utc(started), cfg.to_json()))
        log.info("run %s started (gap=%ss, mode=%s, reference_time=%s, force=%s)", run_id,
                 cfg.session_gap_seconds, cfg.sessionize_mode, format_utc(reference_time), force)

        t = time.perf_counter()
        if users_path:
            users_stats = load_users(conn, Path(users_path), run_id)
            log.info("users loaded from %s: %s", Path(users_path).name, users_stats)
        known_users = {r[0] for r in conn.execute("SELECT user_id FROM dim_users")}
        if not known_users:
            log.warning("dim_users is empty: every event will be rejected as INVALID_USER_REF")
        timings["load_users"] = time.perf_counter() - t

        paths = discover_inputs(inputs, cfg.input_extensions)
        if not paths:
            log.warning("empty batch: no input files found in %s", [str(i) for i in inputs])

        t = time.perf_counter()
        for path in paths:
            try:
                fr = process_file(conn, path, cfg, run_id, reference_time, known_users, force, fault_inject)
            except Exception as exc:
                fr = FileResult(_display_path(path), file_sha256(path), "FAILED")
                _record_file_failure(conn, path, fr, run_id, exc)
                log.error("file %s FAILED (rolled back, will be retried on next run): %s", fr.source_file, fr.error)
            files.append(fr)
        timings["ingest_validate_load_sessionize"] = time.perf_counter() - t

        # Drain the queue: users queued by per_run mode, or left behind by a crashed earlier run.
        t = time.perf_counter()
        _maybe_fault("before_queued_sessionize", Path("run"), fault_inject)
        with transaction(conn):
            target = ALL_USERS if rebuild_sessions else PENDING
            run_level["users_recomputed"], run_level["sessions_written"] = recompute_sessions(
                conn, target, cfg.session_gap_seconds, run_id, format_utc(utc_now()))
        if rebuild_sessions or run_level["users_recomputed"]:
            log.info("%s session recompute: users=%d sessions=%d", "full" if rebuild_sessions else "queued",
                     run_level["users_recomputed"], run_level["sessions_written"])
        timings["sessionize_queued_or_rebuild"] = time.perf_counter() - t

        t = time.perf_counter()
        checks = run_checks(conn, cfg.session_gap_seconds, run_id)
        persist_checks(conn, run_id, checks)
        timings["reconciliation_checks"] = time.perf_counter() - t
        for c in checks:
            (log.info if c.passed else log.error)("check %-34s %s (observed=%s)", c.name, c.status, c.observed)

        failed_checks = [c for c in checks if not c.passed]
        if failed_checks:
            status = "DQ_FAILED"
            failure_reason = "data-quality checks failed: " + ", ".join(c.name for c in failed_checks)
        elif any(f.status in ("FAILED", "REJECTED") for f in files):
            status = "PARTIAL_SUCCESS"
            failure_reason = "; ".join(f"{f.source_file}: {f.status} ({f.error})"
                                       for f in files if f.status in ("FAILED", "REJECTED"))
    except Exception as exc:
        status, failure_reason = "FAILED", f"{type(exc).__name__}: {exc}"
        log.exception("run %s FAILED", run_id)

    ended = utc_now()
    metrics = _metrics(files, checks, users_stats, time.perf_counter() - t0, run_level)
    report_path = None
    try:
        if publish_outputs and status in ("SUCCESS", "PARTIAL_SUCCESS"):
            t = time.perf_counter()
            publish.publish_targets(conn, out_dir)
            timings["publish"] = time.perf_counter() - t
        metrics["stage_timings_seconds"] = {k: round(v, 4) for k, v in timings.items()}
        with transaction(conn):
            conn.execute(
                """UPDATE pipeline_runs SET status=?, ended_at=?, duration_seconds=?, files_discovered=?,
                       files_processed=?, files_skipped=?, files_failed=?, files_rejected=?, input_rows=?,
                       events_inserted=?, duplicate_rows=?, already_loaded_rows=?, rejected_rows=?,
                       users_recomputed=?, sessions_written=?, dq_checks_failed=?, failure_reason=?,
                       stage_timings_json=?
                   WHERE run_id=?""",
                (status, format_utc(ended), metrics["duration_seconds"], len(files),
                 metrics["files_processed"], metrics["files_skipped"], metrics["files_failed"],
                 metrics["files_rejected"], metrics["input_rows"], metrics["events_inserted"],
                 metrics["duplicate_rows"], metrics["already_loaded_rows"], metrics["rejected_rows"],
                 metrics["users_recomputed"], metrics["sessions_written"], metrics["dq_checks_failed"],
                 failure_reason, json.dumps(metrics["stage_timings_seconds"]), run_id))
        result = RunResult(run_id, status, format_utc(started), format_utc(ended), files, checks, metrics,
                           None, failure_reason)
        report_path = publish.write_run_report(conn, out_dir, result)
        result.report_path = str(report_path)
        log.info("run %s finished: %s in %.2fs (%s)", run_id, status, metrics["duration_seconds"],
                 failure_reason or "no failures")
        return result
    finally:
        conn.close()
        publish.detach_run_log(log_handler)


def _metrics(files: list[FileResult], checks: list[CheckResult], users_stats: dict, duration: float,
             run_level: dict) -> dict:
    s = lambda attr: sum(getattr(f, attr) for f in files)
    input_rows = s("rows_read")
    return {
        "duration_seconds": round(duration, 4),
        "files_processed": sum(f.status == "SUCCESS" for f in files),
        "files_skipped": sum(f.status == "SKIPPED" for f in files),
        "files_failed": sum(f.status == "FAILED" for f in files),
        "files_rejected": sum(f.status == "REJECTED" for f in files),
        "input_rows": input_rows,
        "events_inserted": s("rows_inserted"),
        "duplicate_rows": s("rows_duplicate"),
        "already_loaded_rows": s("rows_already_loaded"),
        "rejected_rows": s("rows_rejected"),
        "users_recomputed": s("users_recomputed") + run_level["users_recomputed"],
        "sessions_written": s("sessions_written") + run_level["sessions_written"],
        "dq_checks_failed": sum(not c.passed for c in checks),
        "throughput_rows_per_second": round(input_rows / duration, 1) if duration > 0 else None,
        "users_file": users_stats,
    }
