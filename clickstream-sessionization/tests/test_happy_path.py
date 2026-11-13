"""Happy path: clean events in, correct sessions out, everything reconciles."""
from tests.conftest import ev


def test_basic_sessionization(env):
    env.file("batch_001.csv", [
        ev("e1", "2026-11-01T10:00:00Z"),
        ev("e2", "2026-11-01T10:10:00Z", etype="click"),
        ev("e3", "2026-11-01T10:25:00Z", etype="purchase", page="/checkout"),
        ev("e4", "2026-11-01T12:00:00Z"),                 # 95 min gap -> new session
        ev("e5", "2026-11-01T09:00:00Z", user="u2", device="mobile"),
    ])
    res = env.run()

    assert res.status == "SUCCESS" and res.exit_code == 0
    assert env.sessions() == [
        ("u1", "2026-11-01T10:00:00.000Z", "2026-11-01T10:25:00.000Z", 3),
        ("u1", "2026-11-01T12:00:00.000Z", "2026-11-01T12:00:00.000Z", 1),
        ("u2", "2026-11-01T09:00:00.000Z", "2026-11-01T09:00:00.000Z", 1),
    ]
    first = env.q("SELECT duration_seconds, distinct_event_types, landing_page, exit_page, is_bounce, "
                  "user_session_seq, first_event_id, last_event_id FROM sessions WHERE first_event_id='e1'")[0]
    assert first == (1500.0, 3, "/home", "/checkout", 0, 1, "e1", "e3")
    assert env.scalar("SELECT is_bounce FROM sessions WHERE first_event_id='e4'") == 1
    assert all(c.passed for c in res.checks)
    m = res.metrics
    assert (m["input_rows"], m["events_inserted"], m["rejected_rows"], m["duplicate_rows"]) == (5, 5, 0, 0)


def test_outputs_published(env):
    env.file("batch_001.csv", [ev("e1", "2026-11-01T10:00:00Z"), ev("e2", "2026-11-01T10:05:00Z")])
    res = env.run()
    sessions_csv = (env.out / "sessions.csv").read_text(encoding="utf-8").splitlines()
    assert sessions_csv[0].startswith("session_id,user_id,user_session_seq")
    assert len(sessions_csv) == 2
    assert (env.out / "rejected_events.csv").exists()
    assert (env.out / "reconciliation_report.md").read_text(encoding="utf-8").count("| PASS |") == len(res.checks)
    assert (env.out / "run_reports" / f"{res.run_id}.json").exists()
    assert (env.out / "logs" / f"{res.run_id}.log").read_text(encoding="utf-8").strip()


def test_run_metadata_recorded(env):
    env.file("batch_001.csv", [ev("e1", "2026-11-01T10:00:00Z")])
    res = env.run()
    row = env.q("SELECT status, started_at, ended_at, input_rows, events_inserted, rejected_rows, "
                "failure_reason FROM pipeline_runs WHERE run_id=?", (res.run_id,))[0]
    assert row[0] == "SUCCESS" and row[1] and row[2] and row[1] <= row[2]
    assert row[3:] == (1, 1, 0, None)
    assert env.scalar("SELECT loaded_run_id FROM events WHERE event_id='e1'") == res.run_id
