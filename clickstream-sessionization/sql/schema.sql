-- =====================================================================
-- Target schema (SQLite). Applied idempotently on every run.
-- Layers:
--   dim_users           reference data (referential-integrity target)
--   events              silver: validated, de-duplicated, append-only event history
--   rejected_events     quarantine: invalid + duplicate records, traceable to source line
--   sessions            gold: one row per user session (derived, recomputable)
--   session_events      gold bridge: event -> session assignment (derived, recomputable)
--   pending_session_users ops: users whose sessions must be recomputed (crash-safe work queue)
--   pipeline_runs       ops: one row per run with status and metrics
--   file_ingestion_log  ops: one row per distinct source file (by content hash)
--   dq_check_results    ops: reconciliation / data-quality check results per run
-- All timestamps are ISO-8601 UTC strings: YYYY-MM-DDTHH:MM:SS.mmmZ
-- =====================================================================

CREATE TABLE IF NOT EXISTS dim_users (
    user_id          TEXT PRIMARY KEY,
    signup_ts        TEXT,
    country          TEXT,
    source_file      TEXT NOT NULL,
    loaded_run_id    TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    event_id          TEXT PRIMARY KEY,
    user_id           TEXT NOT NULL REFERENCES dim_users(user_id),
    event_type        TEXT NOT NULL,
    event_ts          TEXT NOT NULL,
    event_ts_epoch_ms INTEGER NOT NULL,
    page_url          TEXT,
    device            TEXT,
    source_file       TEXT NOT NULL,
    source_file_hash  TEXT NOT NULL,
    source_line       INTEGER NOT NULL,
    loaded_run_id     TEXT NOT NULL,
    ingested_at       TEXT NOT NULL
);

-- Covering index for the sessionization window: PARTITION BY user ORDER BY ts, id
CREATE INDEX IF NOT EXISTS ix_events_user_ts ON events (user_id, event_ts_epoch_ms, event_id);

CREATE TABLE IF NOT EXISTS rejected_events (
    reject_id         TEXT PRIMARY KEY,           -- sha256(source_file_hash, source_line)
    record_class      TEXT NOT NULL CHECK (record_class IN ('invalid', 'duplicate', 'file')),
    reason_code       TEXT NOT NULL,
    reason_detail     TEXT,
    event_id          TEXT,                       -- as supplied (may be NULL/garbage)
    source_file       TEXT NOT NULL,
    source_file_hash  TEXT NOT NULL,
    source_line       INTEGER NOT NULL,           -- 0 = whole-file rejection
    raw_record        TEXT,
    run_id            TEXT NOT NULL,
    rejected_at       TEXT NOT NULL,
    UNIQUE (source_file_hash, source_line)
);
CREATE INDEX IF NOT EXISTS ix_rejected_reason ON rejected_events (reason_code);

CREATE TABLE IF NOT EXISTS sessions (
    session_id             TEXT PRIMARY KEY,      -- 'ses_' + sha256(user_id, first_event_id)[:20]
    user_id                TEXT NOT NULL REFERENCES dim_users(user_id),
    user_session_seq       INTEGER NOT NULL,      -- 1..n per user, ordered by start time
    session_start_ts       TEXT NOT NULL,
    session_end_ts         TEXT NOT NULL,
    session_start_epoch_ms INTEGER NOT NULL,
    session_end_epoch_ms   INTEGER NOT NULL,
    duration_seconds       REAL NOT NULL,
    event_count            INTEGER NOT NULL,
    distinct_event_types   INTEGER NOT NULL,
    first_event_id         TEXT NOT NULL,
    last_event_id          TEXT NOT NULL,
    landing_page           TEXT,
    exit_page              TEXT,
    device                 TEXT,
    is_bounce              INTEGER NOT NULL,      -- 1 when event_count = 1
    gap_seconds_used       INTEGER NOT NULL,      -- inactivity gap that produced this session
    computed_run_id        TEXT NOT NULL,
    computed_at            TEXT NOT NULL,
    UNIQUE (user_id, user_session_seq)
);

CREATE TABLE IF NOT EXISTS session_events (
    event_id               TEXT PRIMARY KEY REFERENCES events(event_id),
    session_id             TEXT NOT NULL REFERENCES sessions(session_id),
    user_id                TEXT NOT NULL,
    event_seq_in_session   INTEGER NOT NULL,
    gap_from_prev_seconds  REAL                   -- NULL for the first event of a session
);
CREATE INDEX IF NOT EXISTS ix_session_events_session ON session_events (session_id);
CREATE INDEX IF NOT EXISTS ix_session_events_user ON session_events (user_id);

-- Users whose events changed but whose sessions are not yet recomputed. Rows are queued in the
-- SAME transaction that inserts the events and removed in the SAME transaction that rebuilds the
-- sessions, so a crash in between is recovered automatically by the next run.
CREATE TABLE IF NOT EXISTS pending_session_users (
    user_id          TEXT PRIMARY KEY,
    queued_run_id    TEXT NOT NULL,
    queued_at        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id               TEXT PRIMARY KEY,
    status               TEXT NOT NULL,          -- RUNNING | SUCCESS | PARTIAL_SUCCESS | FAILED | DQ_FAILED
    started_at           TEXT NOT NULL,
    ended_at             TEXT,
    duration_seconds     REAL,
    files_discovered     INTEGER DEFAULT 0,
    files_processed      INTEGER DEFAULT 0,
    files_skipped        INTEGER DEFAULT 0,
    files_failed         INTEGER DEFAULT 0,
    files_rejected       INTEGER DEFAULT 0,
    input_rows           INTEGER DEFAULT 0,
    events_inserted      INTEGER DEFAULT 0,
    duplicate_rows       INTEGER DEFAULT 0,
    already_loaded_rows  INTEGER DEFAULT 0,
    rejected_rows        INTEGER DEFAULT 0,
    users_recomputed     INTEGER DEFAULT 0,
    sessions_written     INTEGER DEFAULT 0,
    dq_checks_failed     INTEGER DEFAULT 0,
    failure_reason       TEXT,
    config_json          TEXT,
    stage_timings_json   TEXT
);

CREATE TABLE IF NOT EXISTS file_ingestion_log (
    source_file_hash     TEXT PRIMARY KEY,       -- sha256 of file bytes: the idempotency key
    source_file          TEXT NOT NULL,
    file_size_bytes      INTEGER NOT NULL,
    status               TEXT NOT NULL,          -- SUCCESS | FAILED | REJECTED
    attempts             INTEGER NOT NULL DEFAULT 0,
    first_run_id         TEXT NOT NULL,
    last_run_id          TEXT NOT NULL,
    rows_read            INTEGER DEFAULT 0,
    rows_inserted        INTEGER DEFAULT 0,
    rows_duplicate       INTEGER DEFAULT 0,
    rows_already_loaded  INTEGER DEFAULT 0,
    rows_rejected        INTEGER DEFAULT 0,
    schema_warnings      TEXT,
    error_message        TEXT,
    updated_at           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dq_check_results (
    run_id          TEXT NOT NULL,
    check_name      TEXT NOT NULL,
    status          TEXT NOT NULL,               -- PASS | FAIL
    observed_value  INTEGER,
    expected_value  INTEGER,
    description     TEXT,
    checked_at      TEXT NOT NULL,
    PRIMARY KEY (run_id, check_name)
);
