"""Source schema contract and reject reason codes."""
from __future__ import annotations

# ---- Source contract: clickstream event files (CSV with header, or JSON Lines) ----
SOURCE_REQUIRED_COLUMNS: tuple[str, ...] = ("event_id", "user_id", "event_type", "event_ts")
SOURCE_OPTIONAL_COLUMNS: tuple[str, ...] = ("page_url", "device")
SOURCE_COLUMNS: tuple[str, ...] = SOURCE_REQUIRED_COLUMNS + SOURCE_OPTIONAL_COLUMNS

# ---- Reference contract: users file (CSV with header) ----
USERS_REQUIRED_COLUMNS: tuple[str, ...] = ("user_id",)
USERS_OPTIONAL_COLUMNS: tuple[str, ...] = ("signup_ts", "country")

# Fields compared to decide whether two records with the same event_id are
# exact duplicates or conflicting duplicates.
PAYLOAD_FIELDS: tuple[str, ...] = ("user_id", "event_type", "event_ts_epoch_ms", "page_url", "device")


class Reason:
    """Reject / quarantine reason codes (stable strings, used in reports and tests)."""

    # record_class = 'invalid'
    MALFORMED_ROW = "MALFORMED_ROW"                    # wrong field count / bad JSON / not an object
    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"  # event_id, user_id or event_type missing
    MISSING_EVENT_TS = "MISSING_EVENT_TS"              # event_ts missing or empty
    INVALID_EVENT_TS = "INVALID_EVENT_TS"              # not an ISO-8601 date-time
    EVENT_TS_OUT_OF_RANGE = "EVENT_TS_OUT_OF_RANGE"    # earlier than min_event_ts
    FUTURE_EVENT_TS = "FUTURE_EVENT_TS"                # later than reference_time + skew
    INVALID_EVENT_ID = "INVALID_EVENT_ID"              # violates event_id_pattern
    UNKNOWN_EVENT_TYPE = "UNKNOWN_EVENT_TYPE"          # not in allowed_event_types
    INVALID_USER_REF = "INVALID_USER_REF"              # user_id not in dim_users
    # record_class = 'duplicate'
    DUPLICATE_EXACT = "DUPLICATE_EXACT"                # same event_id, same payload
    DUPLICATE_CONFLICT = "DUPLICATE_CONFLICT"          # same event_id, different payload
    # record_class = 'file'
    FILE_SCHEMA_MISMATCH = "FILE_SCHEMA_MISMATCH"      # required column(s) missing from header

    # When a record fails several rules, the first in this list is the primary reason.
    PRIORITY = (
        MALFORMED_ROW, MISSING_REQUIRED_FIELD, MISSING_EVENT_TS, INVALID_EVENT_TS,
        EVENT_TS_OUT_OF_RANGE, FUTURE_EVENT_TS, INVALID_EVENT_ID, UNKNOWN_EVENT_TYPE,
        INVALID_USER_REF,
    )
