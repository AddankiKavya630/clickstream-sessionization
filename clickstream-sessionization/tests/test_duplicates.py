"""Duplicate handling: in-file, across files, exact vs conflicting."""
from tests.conftest import ev


def test_exact_duplicate_in_same_file(env):
    env.file("batch_001.csv", [ev("e1", "2026-11-01T10:00:00Z"), ev("e2", "2026-11-01T10:05:00Z"),
                               ev("e1", "2026-11-01T10:00:00Z")])
    res = env.run()
    assert env.reasons() == {4: "DUPLICATE_EXACT"}
    assert env.scalar("SELECT reason_detail FROM rejected_events") == "duplicate of line 2 in the same file"
    assert env.scalar("SELECT event_count FROM sessions") == 2       # duplicate did not inflate the session
    assert res.metrics["duplicate_rows"] == 1 and res.metrics["events_inserted"] == 2


def test_same_instant_in_different_timezone_is_an_exact_duplicate(env):
    env.file("batch_001.csv", [ev("e1", "2026-11-01T10:00:00Z"), ev("e1", "2026-11-01T15:30:00+05:30")])
    env.run()
    assert env.reasons() == {3: "DUPLICATE_EXACT"}


def test_conflicting_duplicate_keeps_first_and_quarantines_second(env):
    env.file("batch_001.csv", [ev("e1", "2026-11-01T10:00:00Z", page="/a"),
                               ev("e1", "2026-11-01T10:00:00Z", page="/b")])
    env.run()
    code, detail = env.q("SELECT reason_code, reason_detail FROM rejected_events")[0]
    assert code == "DUPLICATE_CONFLICT" and "page_url" in detail
    assert env.scalar("SELECT page_url FROM events WHERE event_id='e1'") == "/a"


def test_duplicate_across_files_and_runs(env):
    env.file("batch_001.csv", [ev("e1", "2026-11-01T10:00:00Z")])
    env.run()
    env.file("batch_002.csv", [ev("e1", "2026-11-01T10:00:00Z"),                       # resent
                               ev("e1", "2026-11-01T10:00:00Z", etype="click"),        # conflicting
                               ev("e2", "2026-11-01T10:01:00Z")])
    res = env.run()
    rows = env.q("SELECT source_line, reason_code, reason_detail FROM rejected_events ORDER BY source_line")
    assert [(r[0], r[1]) for r in rows] == [(2, "DUPLICATE_EXACT"), (3, "DUPLICATE_CONFLICT")]
    assert "batch_001.csv line 2 (already loaded)" in rows[0][2]
    assert "event_type" in rows[1][2]
    assert env.scalar("SELECT event_type FROM events WHERE event_id='e1'") == "page_view"   # stored version wins
    assert env.scalar("SELECT COUNT(*) FROM events") == 2
    f = res.files[-1]
    assert (f.rows_read, f.rows_inserted, f.rows_duplicate) == (3, 1, 2)


def test_copies_compare_against_stored_version_not_each_other(env):
    """Regression: a file holding a conflicting copy then an exact copy of an already stored event.
    The exact copy must be DUPLICATE_EXACT (it equals the stored version)."""
    env.file("batch_001.csv", [ev("e1", "2026-11-01T10:00:00Z", page="/a")])
    env.run()
    env.file("batch_002.csv", [ev("e1", "2026-11-01T10:00:00Z", page="/zzz"),
                               ev("e1", "2026-11-01T10:00:00Z", page="/a")])
    env.run()
    assert env.reasons() == {2: "DUPLICATE_CONFLICT", 3: "DUPLICATE_EXACT"}


def test_same_event_id_for_different_users_is_a_conflict_not_two_events(env):
    env.file("batch_001.csv", [ev("e1", "2026-11-01T10:00:00Z", user="u1"),
                               ev("e1", "2026-11-01T10:00:00Z", user="u2")])
    env.run()
    assert env.reasons() == {3: "DUPLICATE_CONFLICT"}
    assert env.q("SELECT user_id FROM sessions") == [("u1",)]
