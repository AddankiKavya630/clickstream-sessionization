-- =====================================================================
-- Reconciliation and data-quality checks.
-- Every check returns ONE integer: the number of violations.
-- PASS when the value is 0. Each "-- name:" line may carry a description
-- after a "|" which is copied into dq_check_results.description.
-- Parameters: :gap_ms, :run_id
-- =====================================================================

-- name: pk_unique_events | events.event_id is unique
SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM events;

-- name: pk_unique_sessions | sessions.session_id is unique
SELECT COUNT(*) - COUNT(DISTINCT session_id) FROM sessions;

-- name: required_fields_not_null | no NULL required fields in events
SELECT COUNT(*) FROM events
WHERE event_id IS NULL OR user_id IS NULL OR event_type IS NULL
   OR event_ts IS NULL OR event_ts_epoch_ms IS NULL;

-- name: ri_events_users | every event.user_id exists in dim_users
SELECT COUNT(*) FROM events e
LEFT JOIN dim_users u ON u.user_id = e.user_id
WHERE u.user_id IS NULL;

-- name: ri_sessions_users | every session.user_id exists in dim_users
SELECT COUNT(*) FROM sessions s
LEFT JOIN dim_users u ON u.user_id = s.user_id
WHERE u.user_id IS NULL;

-- name: every_event_sessionized | every valid event is assigned to exactly one session
SELECT COUNT(*) FROM events e
LEFT JOIN session_events se ON se.event_id = e.event_id
WHERE se.event_id IS NULL;

-- name: no_orphan_session_events | every session_events row points to an existing session and event
SELECT
  (SELECT COUNT(*) FROM session_events se LEFT JOIN sessions s ON s.session_id = se.session_id WHERE s.session_id IS NULL)
+ (SELECT COUNT(*) FROM session_events se LEFT JOIN events e ON e.event_id = se.event_id WHERE e.event_id IS NULL);

-- name: session_event_count_matches | sessions.event_count equals the number of mapped events
SELECT COUNT(*) FROM sessions s
LEFT JOIN (SELECT session_id, COUNT(*) AS n FROM session_events GROUP BY session_id) c
       ON c.session_id = s.session_id
WHERE COALESCE(c.n, 0) <> s.event_count;

-- name: total_events_reconcile | SUM(sessions.event_count) equals COUNT(events)
SELECT ABS((SELECT COALESCE(SUM(event_count), 0) FROM sessions) - (SELECT COUNT(*) FROM events));

-- name: session_bounds_match_events | session start/end equal min/max event time of its events
SELECT COUNT(*) FROM sessions s
JOIN (SELECT se.session_id, MIN(e.event_ts_epoch_ms) AS mn, MAX(e.event_ts_epoch_ms) AS mx
      FROM session_events se JOIN events e ON e.event_id = se.event_id
      GROUP BY se.session_id) b ON b.session_id = s.session_id
WHERE b.mn <> s.session_start_epoch_ms OR b.mx <> s.session_end_epoch_ms;

-- name: duration_consistent | duration_seconds = (end - start) and is non-negative
SELECT COUNT(*) FROM sessions
WHERE duration_seconds < 0
   OR ABS(duration_seconds - (session_end_epoch_ms - session_start_epoch_ms) / 1000.0) > 0.0005;

-- name: intra_session_gap_within_limit | no gap inside a session exceeds the inactivity gap
SELECT COUNT(*) FROM (
  SELECT e.event_ts_epoch_ms - LAG(e.event_ts_epoch_ms) OVER (
           PARTITION BY se.session_id ORDER BY e.event_ts_epoch_ms, e.event_id) AS gap_ms
  FROM session_events se JOIN events e ON e.event_id = se.event_id
) WHERE gap_ms > :gap_ms;

-- name: inter_session_gap_exceeds_limit | consecutive sessions of a user are separated by more than the gap
SELECT COUNT(*) FROM (
  SELECT session_start_epoch_ms - LAG(session_end_epoch_ms) OVER (
           PARTITION BY user_id ORDER BY session_start_epoch_ms) AS gap_ms
  FROM sessions
) WHERE gap_ms IS NOT NULL AND gap_ms <= :gap_ms;

-- name: sessions_use_current_gap | all sessions were computed with the configured gap (else run --rebuild-sessions)
SELECT COUNT(*) FROM sessions WHERE gap_seconds_used * 1000 <> :gap_ms;

-- name: no_pending_sessionization | no user is waiting for session recomputation
SELECT COUNT(*) FROM pending_session_users;

-- name: user_session_seq_contiguous | user_session_seq runs 1..n without gaps per user
SELECT COUNT(*) FROM (
  SELECT user_id, MAX(user_session_seq) AS mx, COUNT(*) AS n, MIN(user_session_seq) AS mn
  FROM sessions GROUP BY user_id
) WHERE mx <> n OR mn <> 1;

-- name: rejects_traceable | every rejected record has source file, hash, line and reason
SELECT COUNT(*) FROM rejected_events
WHERE source_file IS NULL OR source_file_hash IS NULL OR source_line IS NULL OR reason_code IS NULL;

-- name: file_counts_reconcile | per file in this run: rows_read = inserted + duplicate + already_loaded + rejected
SELECT COUNT(*) FROM file_ingestion_log
WHERE last_run_id = :run_id AND status = 'SUCCESS'
  AND rows_read <> rows_inserted + rows_duplicate + rows_already_loaded + rows_rejected;

-- name: file_rejects_persisted | per successful file: rows_rejected + rows_duplicate equals quarantined rows for that file
SELECT COUNT(*) FROM file_ingestion_log f
LEFT JOIN (SELECT source_file_hash, COUNT(*) AS n FROM rejected_events GROUP BY source_file_hash) r
       ON r.source_file_hash = f.source_file_hash
WHERE f.status = 'SUCCESS' AND COALESCE(r.n, 0) <> f.rows_rejected + f.rows_duplicate;

-- name: file_inserts_persisted | per successful file: rows_inserted + rows_already_loaded equals events stored from that file
SELECT COUNT(*) FROM file_ingestion_log f
LEFT JOIN (SELECT source_file_hash, COUNT(*) AS n FROM events GROUP BY source_file_hash) e
       ON e.source_file_hash = f.source_file_hash
WHERE f.status = 'SUCCESS' AND COALESCE(e.n, 0) <> f.rows_inserted + f.rows_already_loaded;
