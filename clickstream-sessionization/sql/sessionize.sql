-- =====================================================================
-- Sessionization (event-time gap based), executed for the users listed in
-- the temp table `affected_users`. Sessions for those users are fully
-- recomputed from the complete event history, so late / out-of-order
-- events are handled the same way as on-time ones.
--
-- Rule: events of a user are ordered by (event_ts_epoch_ms, event_id).
--       A new session starts at the user's first event, or when the gap to
--       the previous event is STRICTLY GREATER than :gap_ms.
--       A gap exactly equal to :gap_ms stays in the same session.
-- Parameters: :gap_ms, :gap_seconds, :run_id, :now
-- Each block is executed in order inside the caller's transaction.
-- =====================================================================

-- name: clear_session_events
DELETE FROM session_events
WHERE user_id IN (SELECT user_id FROM affected_users);

-- name: clear_sessions
DELETE FROM sessions
WHERE user_id IN (SELECT user_id FROM affected_users);

-- name: drop_tmp
DROP TABLE IF EXISTS temp.tmp_session_events;

-- name: build_tmp
CREATE TEMP TABLE tmp_session_events AS
WITH ordered AS (
    SELECT e.event_id, e.user_id, e.event_type, e.event_ts, e.event_ts_epoch_ms,
           e.page_url, e.device,
           LAG(e.event_ts_epoch_ms) OVER (
               PARTITION BY e.user_id ORDER BY e.event_ts_epoch_ms, e.event_id
           ) AS prev_ts_ms
    FROM events e
    JOIN affected_users a ON a.user_id = e.user_id
),
flagged AS (
    SELECT o.*,
           CASE WHEN o.prev_ts_ms IS NULL
                  OR o.event_ts_epoch_ms - o.prev_ts_ms > :gap_ms
                THEN 1 ELSE 0 END AS is_new_session
    FROM ordered o
),
numbered AS (
    SELECT f.*,
           SUM(f.is_new_session) OVER (
               PARTITION BY f.user_id ORDER BY f.event_ts_epoch_ms, f.event_id
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS user_session_seq
    FROM flagged f
)
SELECT n.*,
       ROW_NUMBER() OVER s AS event_seq_in_session,
       COUNT(*)     OVER (PARTITION BY n.user_id, n.user_session_seq) AS session_event_count,
       FIRST_VALUE(n.event_id) OVER s AS first_event_id
FROM numbered n
WINDOW s AS (PARTITION BY n.user_id, n.user_session_seq ORDER BY n.event_ts_epoch_ms, n.event_id);

-- name: assign_session_ids
ALTER TABLE temp.tmp_session_events ADD COLUMN session_id TEXT;

-- name: fill_session_ids
UPDATE temp.tmp_session_events SET session_id = make_session_id(user_id, first_event_id);

-- name: insert_sessions
INSERT INTO sessions (
    session_id, user_id, user_session_seq,
    session_start_ts, session_end_ts, session_start_epoch_ms, session_end_epoch_ms,
    duration_seconds, event_count, distinct_event_types,
    first_event_id, last_event_id, landing_page, exit_page, device, is_bounce,
    gap_seconds_used, computed_run_id, computed_at
)
SELECT
    session_id,
    user_id,
    user_session_seq,
    MAX(CASE WHEN event_seq_in_session = 1 THEN event_ts END),
    MAX(CASE WHEN event_seq_in_session = session_event_count THEN event_ts END),
    MIN(event_ts_epoch_ms),
    MAX(event_ts_epoch_ms),
    (MAX(event_ts_epoch_ms) - MIN(event_ts_epoch_ms)) / 1000.0,
    COUNT(*),
    COUNT(DISTINCT event_type),
    MAX(first_event_id),
    MAX(CASE WHEN event_seq_in_session = session_event_count THEN event_id END),
    MAX(CASE WHEN event_seq_in_session = 1 THEN page_url END),
    MAX(CASE WHEN event_seq_in_session = session_event_count THEN page_url END),
    MAX(CASE WHEN event_seq_in_session = 1 THEN device END),
    CASE WHEN COUNT(*) = 1 THEN 1 ELSE 0 END,
    :gap_seconds,
    :run_id,
    :now
FROM temp.tmp_session_events
GROUP BY session_id, user_id, user_session_seq;

-- name: insert_session_events
INSERT INTO session_events (event_id, session_id, user_id, event_seq_in_session, gap_from_prev_seconds)
SELECT event_id, session_id, user_id, event_seq_in_session,
       CASE WHEN is_new_session = 1 THEN NULL
            ELSE (event_ts_epoch_ms - prev_ts_ms) / 1000.0 END
FROM temp.tmp_session_events;
