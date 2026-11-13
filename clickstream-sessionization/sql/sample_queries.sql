-- =====================================================================
-- Sample analytical / operational queries. Run with:
--   python -m sessionize report --db output/warehouse.db
-- =====================================================================

-- name: latest_runs | Last 5 pipeline runs
SELECT run_id, status, started_at, ROUND(duration_seconds, 2) AS secs,
       input_rows, events_inserted, duplicate_rows, rejected_rows, sessions_written, dq_checks_failed
FROM pipeline_runs ORDER BY started_at DESC LIMIT 5;

-- name: source_to_target | Source-to-target reconciliation per file
SELECT source_file, status, attempts, rows_read, rows_inserted, rows_duplicate,
       rows_already_loaded, rows_rejected,
       rows_read - (rows_inserted + rows_duplicate + rows_already_loaded + rows_rejected) AS unexplained
FROM file_ingestion_log ORDER BY source_file;

-- name: rejects_by_reason | Quarantined records by class and reason
SELECT record_class, reason_code, COUNT(*) AS n
FROM rejected_events GROUP BY record_class, reason_code ORDER BY record_class, n DESC;

-- name: session_kpis | Headline session KPIs
SELECT COUNT(*) AS sessions,
       COUNT(DISTINCT user_id) AS users,
       SUM(event_count) AS events,
       ROUND(AVG(event_count), 2) AS avg_events_per_session,
       ROUND(AVG(duration_seconds) / 60.0, 2) AS avg_duration_min,
       ROUND(100.0 * AVG(is_bounce), 2) AS bounce_rate_pct
FROM sessions;

-- name: sessions_per_day | Sessions by UTC start date
SELECT substr(session_start_ts, 1, 10) AS session_date, COUNT(*) AS sessions,
       SUM(event_count) AS events
FROM sessions GROUP BY session_date ORDER BY session_date;

-- name: sessions_by_device | Sessions and conversion by device
SELECT COALESCE(s.device, '(unknown)') AS device, COUNT(*) AS sessions,
       SUM(CASE WHEN EXISTS (
             SELECT 1 FROM session_events se JOIN events e ON e.event_id = se.event_id
             WHERE se.session_id = s.session_id AND e.event_type = 'purchase') THEN 1 ELSE 0 END) AS sessions_with_purchase
FROM sessions s GROUP BY device ORDER BY sessions DESC;

-- name: longest_sessions | Top 5 longest sessions
SELECT session_id, user_id, session_start_ts, session_end_ts,
       ROUND(duration_seconds / 60.0, 1) AS minutes, event_count
FROM sessions ORDER BY duration_seconds DESC, session_id LIMIT 5;

-- name: one_user_timeline | Example: the event timeline of one user with session boundaries
SELECT se.session_id, se.event_seq_in_session, e.event_ts, e.event_type,
       se.gap_from_prev_seconds
FROM session_events se JOIN events e ON e.event_id = se.event_id
WHERE se.user_id = (SELECT user_id FROM sessions ORDER BY user_id LIMIT 1)
ORDER BY e.event_ts_epoch_ms, e.event_id LIMIT 25;

-- name: dq_latest | Data-quality checks of the latest run
SELECT check_name, status, observed_value
FROM dq_check_results
WHERE run_id = (SELECT run_id FROM pipeline_runs ORDER BY started_at DESC LIMIT 1)
ORDER BY check_name;
