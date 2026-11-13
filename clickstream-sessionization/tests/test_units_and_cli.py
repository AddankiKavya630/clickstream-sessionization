"""Small unit tests (timestamps, ids, config) and a CLI smoke test."""
from datetime import datetime, timezone

import pytest

from sessionize.cli import main
from sessionize.config import load_config
from sessionize.ids import make_session_id, stable_hash
from sessionize.timeutil import TimestampError, format_utc, parse_iso_utc, to_epoch_ms


@pytest.mark.parametrize("text, expected", [
    ("2026-11-01T10:00:00Z", "2026-11-01T10:00:00.000Z"),
    ("2026-11-01T15:30:00.250+05:30", "2026-11-01T10:00:00.250Z"),
    ("2026-11-01 10:00:00+00:00", "2026-11-01T10:00:00.000Z"),
    ("2026-11-01T10:00:00.123456Z", "2026-11-01T10:00:00.123Z"),     # sub-ms truncated
    ("2026-03-29T02:30:00+01:00", "2026-03-29T01:30:00.000Z"),       # DST-day offsets are explicit
])
def test_parse_and_format(text, expected):
    assert format_utc(parse_iso_utc(text)) == expected


@pytest.mark.parametrize("text, code", [
    (None, "MISSING_EVENT_TS"), ("", "MISSING_EVENT_TS"), ("x", "INVALID_EVENT_TS"),
    ("2026-11-01", "INVALID_EVENT_TS"), ("2026-02-30T00:00:00Z", "INVALID_EVENT_TS"),
    ("1730000000", "INVALID_EVENT_TS"),
])
def test_parse_errors(text, code):
    with pytest.raises(TimestampError) as e:
        parse_iso_utc(text)
    assert e.value.code == code


def test_epoch_ms_exact():
    assert to_epoch_ms(datetime(1970, 1, 1, 0, 0, 1, 999999, tzinfo=timezone.utc)) == 1999


def test_ids_are_deterministic_and_separator_safe():
    assert make_session_id("u1", "e1") == make_session_id("u1", "e1")
    assert make_session_id("u1", "e1").startswith("ses_") and len(make_session_id("u1", "e1")) == 24
    assert stable_hash("a|b", "c") != stable_hash("a", "b|c")


def test_default_config_file_loads():
    cfg = load_config()
    assert cfg.session_gap_seconds == 1800
    assert "page_view" in cfg.allowed_event_types


def test_cli_generate_run_verify_report(tmp_path, capsys):
    gen, out = tmp_path / "gen", tmp_path / "out"
    assert main(["generate", "--out", str(gen), "--events", "200", "--users", "10", "--seed", "3"]) == 0
    args = ["run", "--input", str(gen / "input" / "events"), "--users", str(gen / "input" / "users.csv"),
            "--db", str(out / "wh.db"), "--out", str(out), "--reference-time", "2026-12-31T00:00:00Z"]
    assert main(args) == 0
    assert main(args) == 0                                   # rerun: still success, files skipped
    assert main(["verify", "--db", str(out / "wh.db"), "--expected", str(gen / "expected"),
                 "--report", str(out / "validation.md")]) == 0
    assert main(["report", "--db", str(out / "wh.db")]) == 0
    text = capsys.readouterr().out
    assert "Overall: PASS" in text and "session_kpis" in text
