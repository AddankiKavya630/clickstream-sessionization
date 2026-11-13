"""Schema validation and business-rule checks for single records and file headers."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta

from .config import PipelineConfig
from .readers import RawRecord
from .schema import SOURCE_COLUMNS, SOURCE_REQUIRED_COLUMNS, Reason
from .timeutil import TimestampError, format_utc, parse_iso_utc, to_epoch_ms


@dataclass(frozen=True)
class ValidEvent:
    event_id: str
    user_id: str
    event_type: str
    event_ts: str            # canonical ISO-8601 UTC
    event_ts_epoch_ms: int
    page_url: str | None
    device: str | None
    source_line: int

    def payload(self) -> tuple:
        return (self.user_id, self.event_type, self.event_ts_epoch_ms, self.page_url, self.device)


@dataclass(frozen=True)
class RejectedRecord:
    source_line: int
    record_class: str        # 'invalid' | 'duplicate'
    reason_code: str
    reason_detail: str
    event_id: str | None
    raw: str


@dataclass(frozen=True)
class HeaderCheck:
    ok: bool
    missing: tuple[str, ...]
    extra: tuple[str, ...]
    duplicated: tuple[str, ...]


def check_header(columns: list[str]) -> HeaderCheck:
    """File-level schema check (CSV). Missing required columns => whole file rejected.
    Extra columns => accepted, ignored, and reported as schema drift."""
    seen, dup = set(), []
    for c in columns:
        if c in seen:
            dup.append(c)
        seen.add(c)
    missing = tuple(c for c in SOURCE_REQUIRED_COLUMNS if c not in seen)
    extra = tuple(c for c in columns if c not in SOURCE_COLUMNS)
    return HeaderCheck(ok=not missing and not dup, missing=missing, extra=extra, duplicated=tuple(dup))


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value or None


class RecordValidator:
    def __init__(self, cfg: PipelineConfig, known_users: set[str], reference_time: datetime):
        self.cfg = cfg
        self.known_users = known_users
        self.max_ts = reference_time + timedelta(seconds=cfg.max_future_skew_seconds)
        self.allowed_types = set(cfg.allowed_event_types)
        self.id_re = re.compile(cfg.event_id_pattern)
        self.extra_keys_seen: set[str] = set()

    def validate(self, rec: RawRecord) -> ValidEvent | RejectedRecord:
        if rec.fields is None:
            return RejectedRecord(rec.line_no, "invalid", Reason.MALFORMED_ROW,
                                  rec.error or "malformed record", None, rec.raw)

        f = rec.fields
        for k in f:
            if k not in SOURCE_COLUMNS:
                self.extra_keys_seen.add(k)

        event_id = _clean(f.get("event_id"))
        user_id = _clean(f.get("user_id"))
        event_type = _clean(f.get("event_type"))
        event_type = event_type.lower() if event_type else None
        raw_ts = f.get("event_ts")

        errors: list[tuple[str, str]] = []
        missing = [name for name, val in (("event_id", event_id), ("user_id", user_id),
                                          ("event_type", event_type)) if val is None]
        if missing:
            errors.append((Reason.MISSING_REQUIRED_FIELD, f"missing required field(s): {', '.join(missing)}"))

        ts = None
        try:
            ts = parse_iso_utc(raw_ts, self.cfg.naive_timestamp_policy)
        except TimestampError as exc:
            errors.append((exc.code, exc.detail))
        if ts is not None:
            if ts < self.cfg.min_event_ts:
                errors.append((Reason.EVENT_TS_OUT_OF_RANGE, f"event_ts {format_utc(ts)} is before {format_utc(self.cfg.min_event_ts)}"))
            elif ts > self.max_ts:
                errors.append((Reason.FUTURE_EVENT_TS, f"event_ts {format_utc(ts)} is after allowed max {format_utc(self.max_ts)}"))

        if event_id is not None and not self.id_re.match(event_id):
            errors.append((Reason.INVALID_EVENT_ID, f"event_id {event_id[:80]!r} violates pattern"))
        if event_type is not None and event_type not in self.allowed_types:
            errors.append((Reason.UNKNOWN_EVENT_TYPE, f"event_type {event_type[:80]!r} not allowed"))
        if user_id is not None and user_id not in self.known_users:
            errors.append((Reason.INVALID_USER_REF, f"user_id {user_id[:80]!r} not found in dim_users"))

        if errors:
            errors.sort(key=lambda e: Reason.PRIORITY.index(e[0]))
            return RejectedRecord(rec.line_no, "invalid", errors[0][0],
                                  "; ".join(d for _, d in errors), event_id, rec.raw)

        return ValidEvent(
            event_id=event_id,
            user_id=user_id,
            event_type=event_type,
            event_ts=format_utc(ts),
            event_ts_epoch_ms=to_epoch_ms(ts),
            page_url=_clean(f.get("page_url")),
            device=(_clean(f.get("device")) or "").lower() or None,
            source_line=rec.line_no,
        )
