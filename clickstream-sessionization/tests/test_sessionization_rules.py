"""Domain edge cases of event-time sessionization."""
from sessionize.config import PipelineConfig
from tests.conftest import REFERENCE_TIME, ev


def test_gap_exactly_equal_to_threshold_stays_in_session(env):
    env.file("b.csv", [ev("e1", "2026-11-01T10:00:00Z"), ev("e2", "2026-11-01T10:30:00Z")])
    env.run()
    assert [s[3] for s in env.sessions()] == [2]


def test_gap_one_millisecond_over_threshold_starts_new_session(env):
    env.file("b.csv", [ev("e1", "2026-11-01T10:00:00Z"), ev("e2", "2026-11-01T10:30:00.001Z")])
    env.run()
    assert [s[3] for s in env.sessions()] == [1, 1]
    assert env.q("SELECT gap_from_prev_seconds FROM session_events ORDER BY event_id") == [(None,), (None,)]


def test_out_of_order_events_in_one_file(env):
    env.file("b.csv", [ev("e3", "2026-11-01T10:20:00Z"), ev("e1", "2026-11-01T10:00:00Z"),
                       ev("e4", "2026-11-01T11:30:00Z"), ev("e2", "2026-11-01T10:10:00Z")])
    env.run()
    assert env.sessions() == [("u1", "2026-11-01T10:00:00.000Z", "2026-11-01T10:20:00.000Z", 3),
                              ("u1", "2026-11-01T11:30:00.000Z", "2026-11-01T11:30:00.000Z", 1)]
    assert env.q("SELECT event_id, event_seq_in_session FROM session_events ORDER BY event_id") == \
        [("e1", 1), ("e2", 2), ("e3", 3), ("e4", 1)]


def test_late_event_in_later_batch_merges_two_sessions(env):
    env.file("batch_001.csv", [ev("e1", "2026-11-01T10:00:00Z"), ev("e3", "2026-11-01T10:50:00Z")])
    env.run()
    assert len(env.sessions()) == 2
    env.file("batch_002.csv", [ev("e2", "2026-11-01T10:25:00Z")])   # arrives a day late, bridges the gap
    res = env.run()
    assert env.sessions() == [("u1", "2026-11-01T10:00:00.000Z", "2026-11-01T10:50:00.000Z", 3)]
    assert all(c.passed for c in res.checks)


def test_late_event_before_first_event_extends_session_and_changes_its_id(env):
    env.file("batch_001.csv", [ev("e5", "2026-11-01T10:00:00Z")])
    env.run()
    old_id = env.scalar("SELECT session_id FROM sessions")
    env.file("batch_002.csv", [ev("e4", "2026-11-01T09:45:00Z")])
    env.run()
    assert env.sessions() == [("u1", "2026-11-01T09:45:00.000Z", "2026-11-01T10:00:00.000Z", 2)]
    assert env.scalar("SELECT session_id FROM sessions") != old_id      # id = f(user, first event)
    assert env.scalar("SELECT COUNT(*) FROM sessions") == 1             # old row replaced, not duplicated


def test_late_event_only_recomputes_affected_users(env):
    env.file("batch_001.csv", [ev("a1", "2026-11-01T10:00:00Z", user="u1"),
                               ev("b1", "2026-11-01T10:00:00Z", user="u2")])
    first = env.run()
    env.file("batch_002.csv", [ev("a2", "2026-11-01T10:05:00Z", user="u1")])
    second = env.run()
    assert second.files[-1].users_recomputed == 1
    assert env.q("SELECT user_id, computed_run_id = ? FROM sessions ORDER BY user_id", (first.run_id,)) == \
        [("u1", 0), ("u2", 1)]


def test_timezone_offsets_are_normalized_before_gap_calculation(env):
    # 10:00Z and 16:00+05:30 (=10:30Z) are 30 minutes apart, not 6.5 hours.
    env.file("b.csv", [ev("e1", "2026-11-01T10:00:00Z"), ev("e2", "2026-11-01T16:00:00+05:30"),
                       ev("e3", "2026-11-01 10:45:00-00:15")])   # space separator, odd offset = 11:00Z
    env.run()
    assert env.sessions() == [("u1", "2026-11-01T10:00:00.000Z", "2026-11-01T11:00:00.000Z", 3)]
    assert env.q("SELECT event_ts FROM events ORDER BY event_ts") == [
        ("2026-11-01T10:00:00.000Z",), ("2026-11-01T10:30:00.000Z",), ("2026-11-01T11:00:00.000Z",)]


def test_identical_timestamps_are_ordered_deterministically_by_event_id(env):
    env.file("b.csv", [ev("e_b", "2026-11-01T10:00:00Z", page="/b"), ev("e_a", "2026-11-01T10:00:00Z", page="/a")])
    env.run()
    assert env.q("SELECT first_event_id, landing_page, exit_page, duration_seconds FROM sessions") == \
        [("e_a", "/a", "/b", 0.0)]


def test_session_crossing_midnight_is_not_split(env):
    env.file("b.csv", [ev("e1", "2026-11-01T23:50:00Z"), ev("e2", "2026-11-02T00:10:00Z")])
    env.run()
    assert [s[3] for s in env.sessions()] == [2]


def test_users_are_sessionized_independently(env):
    env.file("b.csv", [ev("a1", "2026-11-01T10:00:00Z", user="u1"), ev("b1", "2026-11-01T10:10:00Z", user="u2"),
                       ev("a2", "2026-11-01T10:20:00Z", user="u1")])
    env.run()
    assert [(s[0], s[3]) for s in env.sessions()] == [("u1", 2), ("u2", 1)]


def test_naive_timestamps_follow_policy(env):
    env.file("b.csv", [ev("e1", "2026-11-01T10:00:00")])
    env.run()
    assert env.scalar("SELECT event_ts FROM events") == "2026-11-01T10:00:00.000Z"

    env2_cfg = PipelineConfig(reference_time=REFERENCE_TIME, naive_timestamp_policy="reject")
    env.file("b2.csv", [ev("e2", "2026-11-01T10:00:00")])
    env.run(cfg=env2_cfg)
    assert env.reasons() == {2: "INVALID_EVENT_TS"}


def test_configurable_gap(env):
    env.file("b.csv", [ev("e1", "2026-11-01T10:00:00Z"), ev("e2", "2026-11-01T10:20:00Z")])
    env.run(cfg=env.cfg.with_overrides(session_gap_seconds=600))
    assert [s[3] for s in env.sessions()] == [1, 1]
