"""Reconciliation checks catch corruption; generator is reproducible and the pipeline matches its labels."""
import hashlib
import sqlite3

import pytest

from sessionize.checks import run_checks
from sessionize.config import PipelineConfig
from sessionize.db import connect
from sessionize.expected import verify_against_expected
from sessionize.generator import GeneratorConfig, generate
from sessionize.pipeline import run_pipeline
from tests.conftest import REFERENCE_TIME, ev


def _failed(db):
    conn = connect(db)
    try:
        return {c.name for c in run_checks(conn, 1800, "none") if not c.passed}
    finally:
        conn.close()


@pytest.mark.parametrize("corruption, expected_check", [
    ("UPDATE sessions SET event_count = event_count + 1", "session_event_count_matches"),
    ("DELETE FROM session_events WHERE event_id = 'e2'", "every_event_sessionized"),
    ("UPDATE sessions SET duration_seconds = 1", "duration_consistent"),
    ("UPDATE sessions SET session_end_epoch_ms = session_end_epoch_ms + 5", "session_bounds_match_events"),
    ("UPDATE file_ingestion_log SET rows_rejected = rows_rejected + 1", "file_rejects_persisted"),
    ("UPDATE file_ingestion_log SET rows_inserted = 0", "file_inserts_persisted"),
    ("UPDATE events SET user_id = 'ghost' WHERE event_id = 'e1'", "ri_events_users"),
])
def test_checks_detect_corruption(env, corruption, expected_check):
    env.file("b.csv", [ev("e1", "2026-11-01T10:00:00Z"), ev("e2", "2026-11-01T10:10:00Z"), ev("e3", "x")])
    env.run()
    assert _failed(env.db) == set()
    conn = sqlite3.connect(env.db)
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute(corruption)
    conn.commit()
    conn.close()
    assert expected_check in _failed(env.db)


def test_checks_detect_wrong_sessionization(env):
    env.file("b.csv", [ev("e1", "2026-11-01T10:00:00Z"), ev("e2", "2026-11-01T12:00:00Z")])
    env.run()
    conn = sqlite3.connect(env.db)
    conn.execute("PRAGMA foreign_keys = OFF")
    keep = conn.execute("SELECT session_id FROM session_events WHERE event_id='e1'").fetchone()[0]
    conn.execute("UPDATE session_events SET session_id=? WHERE event_id='e2'", (keep,))   # wrongly merged
    conn.commit()
    conn.close()
    assert {"intra_session_gap_within_limit", "session_event_count_matches"} <= _failed(env.db)


def _digest(folder):
    h = hashlib.sha256()
    for p in sorted(folder.rglob("*")):
        if p.is_file() and p.name != "expected_summary.json":
            h.update(p.relative_to(folder).as_posix().encode())
            h.update(p.read_bytes())
    return h.hexdigest()


def test_generator_is_deterministic_for_a_seed(tmp_path):
    generate(tmp_path / "a", GeneratorConfig(seed=7))
    generate(tmp_path / "b", GeneratorConfig(seed=7))
    generate(tmp_path / "c", GeneratorConfig(seed=8))
    assert _digest(tmp_path / "a") == _digest(tmp_path / "b")
    assert _digest(tmp_path / "a") != _digest(tmp_path / "c")


def test_expected_labels_are_kept_out_of_the_input(tmp_path):
    generate(tmp_path, GeneratorConfig())
    header = (tmp_path / "input" / "events" / "events_batch_001.csv").read_text().splitlines()[0]
    assert "session" not in header and "expected" not in header


@pytest.mark.parametrize("seed, n_events, batches", [(42, 400, 2), (1, 3000, 3), (2024, 1500, 5)])
def test_end_to_end_matches_generator_ground_truth(tmp_path, seed, n_events, batches):
    gen = tmp_path / "gen"
    summary = generate(gen, GeneratorConfig(seed=seed, n_users=max(10, n_events // 20),
                                            n_events=n_events, n_batches=batches))
    res = run_pipeline([gen / "input" / "events"], tmp_path / "wh.db", tmp_path / "out",
                       PipelineConfig(reference_time=REFERENCE_TIME), users_path=gen / "input" / "users.csv")
    assert res.status == "SUCCESS", res.failure_reason
    assert res.metrics["input_rows"] == summary["total_input_rows"]
    conn = connect(tmp_path / "wh.db")
    try:
        v = verify_against_expected(conn, gen / "expected")
    finally:
        conn.close()
    assert v["passed"], v
