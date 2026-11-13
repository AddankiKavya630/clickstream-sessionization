"""Timestamp parsing and canonical ISO-8601 UTC formatting."""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# Must contain a date AND a time; date-only values are rejected as ambiguous.
_HAS_TIME = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}")


class TimestampError(ValueError):
    """Raised with a reason code when a timestamp cannot be accepted."""

    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code = code
        self.detail = detail


def parse_iso_utc(value: str, naive_policy: str = "assume_utc") -> datetime:
    """Parse an ISO-8601 date-time string into an aware UTC datetime.

    Accepts 'Z' or numeric offsets (e.g. +05:30) and 'T' or space separators.
    Naive values are treated per ``naive_policy`` ("assume_utc" | "reject").
    """
    if value is None:
        raise TimestampError("MISSING_EVENT_TS", "timestamp is missing")
    text = value.strip()
    if not text:
        raise TimestampError("MISSING_EVENT_TS", "timestamp is empty")
    if not _HAS_TIME.match(text):
        raise TimestampError("INVALID_EVENT_TS", f"not an ISO-8601 date-time: {text!r}")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise TimestampError("INVALID_EVENT_TS", f"unparseable timestamp {text!r}: {exc}") from None
    if dt.tzinfo is None:
        if naive_policy == "reject":
            raise TimestampError("INVALID_EVENT_TS", f"timestamp has no UTC offset: {text!r}")
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def to_epoch_ms(dt: datetime) -> int:
    """Exact integer milliseconds since epoch (sub-millisecond precision is truncated)."""
    return (dt - EPOCH) // timedelta(milliseconds=1)


def from_epoch_ms(ms: int) -> datetime:
    return EPOCH + timedelta(milliseconds=ms)


def format_utc(dt: datetime) -> str:
    """Canonical form used everywhere in the target: 2026-11-01T10:15:30.123Z"""
    dt = dt.astimezone(UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def utc_now() -> datetime:
    return datetime.now(UTC)


def now_iso() -> str:
    return format_utc(utc_now())
