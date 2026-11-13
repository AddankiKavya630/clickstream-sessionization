"""Reruns, forced reprocessing, partial failure + retry, order independence, backfill."""
import pytest

from tests.conftest import ev

SNAPSHOT_SQL = {
    "events": "SELECT event_id, user_id, event_type, event_ts, page_url, device FROM events ORDER BY event_id",
    "sessions": "SELECT session_id, user_id, user_session_seq, session_start_ts, session_end_ts, event_count, "
                "landing_page, exit_page FROM sessions ORDER BY session_id",
    "session_events": "SELECT event_id, session_id, event_seq_in_session FROM session_events ORDER BY event_id",
    "rejects": "SELECT reject_id, reason_code, source_line FROM rejected_events ORDER BY reject_id",
}


def snapshot(env):
    return {k: env.q(v) for k, v in SNAPSHOT_SQL.items()}


def _sample_files(env):
    env.file("batch_001.csv", [ev("e1", "2026-11-01T10:00:00Z"), ev("e2", "2026-11-01T10:20:00Z"),
                               ev("e9", "garbage"), ev("e1", "2026-11-01T10:00:00Z")])
    env.file("batch_002.csv", [ev("e3", "2026-11-01T10:40:00Z"), ev("e4", "2026-11-01T09:00:00Z", user="u2")])


def test_rerun_of_same_input_is_a_no_op(env):
    _sample_files(env)
    env.run()
    before = snapshot(env)
    res = env.run()
    assert [f.status for f in res.files] == ["SKIPPED", "SKIPPED"]
    assert res.status == "SUCCESS" and res.metrics["input_rows"] == 0
    assert snapshot(env) == before
    assert env.scalar("SELECT COUNT(*) FROM pipeline_runs") == 2


def test_forced_rerun_creates_no_duplicates(env):
    _sample_files(env)
    env.run()
    before = snapshot(env)
    sessions_csv = (env.out / "sessions.csv").read_bytes()
    res = env.run(force=True)
    assert snapshot(env) == before
    assert (env.out / "sessions.csv").read_bytes() == sessions_csv      # republish is byte-identical
    f1 = res.files[0]
    assert (f1.rows_read, f1.rows_inserted, f1.rows_already_loaded, f1.rows_duplicate, f1.rows_rejected) == (4, 0, 2, 1, 1)
    assert all(c.passed for c in res.checks)
    assert env.scalar("SELECT attempts FROM file_ingestion_log WHERE source_file LIKE '%batch_001.csv'") == 2


def test_renamed_copy_of_loaded_file_is_detected_by_content_hash(env):
    _sample_files(env)
    env.run()
    (env.inbox / "batch_001_resent.csv").write_bytes((env.inbox / "batch_001.csv").read_bytes())
    res = env.run()
    assert [f.status for f in res.files] == ["SKIPPED", "SKIPPED", "SKIPPED"]


@pytest.mark.parametrize("stage", ["after_validate", "after_insert_events", "after_sessionize"])
def test_failure_mid_file_rolls_back_and_retry_succeeds(env, stage):
    _sample_files(env)
    res = env.run(fault_inject=f"{stage}:batch_002")
    assert res.status == "PARTIAL_SUCCESS" and res.exit_code == 2
    assert [f.status for f in res.files] == ["SUCCESS", "FAILED"]
    assert "injected fault" in res.files[1].error
    # nothing from the failed file is visible: no events, no sessions, no rejects
    assert env.scalar("SELECT COUNT(*) FROM events WHERE source_file LIKE '%batch_002.csv'") == 0
    assert env.scalar("SELECT COUNT(*) FROM sessions WHERE user_id = 'u2'") == 0
    assert all(c.passed for c in res.checks)            # warehouse stays consistent after a failure
    log_row = env.q("SELECT status, error_message FROM file_ingestion_log WHERE source_file LIKE '%batch_002.csv'")[0]
    assert log_row[0] == "FAILED" and "injected" in log_row[1]

    retry = env.run()                                   # operator simply re-runs
    assert [f.status for f in retry.files] == ["SKIPPED", "SUCCESS"]
    assert retry.status == "SUCCESS"
    assert env.sessions() == [("u1", "2026-11-01T10:00:00.000Z", "2026-11-01T10:40:00.000Z", 3),
                              ("u2", "2026-11-01T09:00:00.000Z", "2026-11-01T09:00:00.000Z", 1)]
    assert env.scalar("SELECT attempts FROM file_ingestion_log WHERE source_file LIKE '%batch_002.csv'") == 2


def test_result_is_independent_of_file_arrival_order(env, tmp_path):
    from tests.conftest import Env
    _sample_files(env)
    env.run(inputs=[env.inbox / "batch_002.csv"])
    env.run(inputs=[env.inbox / "batch_001.csv"])
    (tmp_path / "other").mkdir()
    other = Env(tmp_path / "other")
    _sample_files(other)
    other.run()
    a, b = snapshot(env), snapshot(other)
    assert a["sessions"] == b["sessions"] and a["session_events"] == b["session_events"]
    assert a["events"] == b["events"]


def test_gap_change_requires_rebuild_and_rebuild_backfills_all_users(env):
    _sample_files(env)
    env.run()
    new_cfg = env.cfg.with_overrides(session_gap_seconds=15 * 60)
    stale = env.run(cfg=new_cfg)
    assert stale.status == "DQ_FAILED" and stale.exit_code == 3
    assert "sessions_use_current_gap" in stale.failure_reason
    rebuilt = env.run(cfg=new_cfg, rebuild_sessions=True)
    assert rebuilt.status == "SUCCESS"
    assert [s[3] for s in env.sessions()] == [1, 1, 1, 1]     # 20-min gaps now split u1's sessions


def test_dq_failure_does_not_overwrite_published_snapshot(env):
    _sample_files(env)
    env.run()
    good = (env.out / "sessions.csv").read_bytes()
    env.run(cfg=env.cfg.with_overrides(session_gap_seconds=60))   # stale sessions -> DQ_FAILED
    assert (env.out / "sessions.csv").read_bytes() == good


def test_per_run_mode_gives_identical_result(env, tmp_path):
    from tests.conftest import Env
    _sample_files(env)
    res = env.run()
    (tmp_path / "other").mkdir()
    other = Env(tmp_path / "other")
    _sample_files(other)
    res2 = other.run(cfg=other.cfg.with_overrides(sessionize_mode="per_run"))
    assert res.status == res2.status == "SUCCESS"
    assert snapshot(env)["sessions"] == snapshot(other)["sessions"]
    assert all(f.users_recomputed == 0 for f in res2.files) and res2.metrics["users_recomputed"] == 2


def test_crash_after_events_committed_but_before_sessionization_is_recovered(env):
    _sample_files(env)
    per_run = env.cfg.with_overrides(sessionize_mode="per_run")
    crashed = env.run(cfg=per_run, fault_inject="before_queued_sessionize")
    assert crashed.status == "FAILED" and crashed.exit_code == 1
    assert env.scalar("SELECT COUNT(*) FROM events") == 4          # file transactions committed
    assert env.scalar("SELECT COUNT(*) FROM sessions") == 0         # ... sessions not yet built
    assert env.scalar("SELECT COUNT(*) FROM pending_session_users") == 2   # but the work is queued

    recovered = env.run(cfg=per_run)                                # files skipped, queue drained
    assert [f.status for f in recovered.files] == ["SKIPPED", "SKIPPED"]
    assert recovered.status == "SUCCESS" and recovered.metrics["users_recomputed"] == 2
    assert env.scalar("SELECT COUNT(*) FROM pending_session_users") == 0
    assert len(env.sessions()) == 2
