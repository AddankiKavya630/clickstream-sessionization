"""Pipeline configuration: loaded from TOML, overridable from the CLI."""
from __future__ import annotations

import json
import tomllib
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
from pathlib import Path

from .timeutil import parse_iso_utc

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "default.toml"


@dataclass(frozen=True)
class PipelineConfig:
    session_gap_seconds: int = 30 * 60
    # "per_file": sessions rebuilt inside each file's transaction (sessions never lag events).
    # "per_run":  affected users are queued per file and rebuilt ONCE at the end of the run
    #             (faster for multi-file backlogs; sessions lag events only during the run).
    sessionize_mode: str = "per_file"
    allowed_event_types: tuple[str, ...] = (
        "page_view", "click", "search", "add_to_cart", "remove_from_cart",
        "purchase", "login", "logout",
    )
    max_future_skew_seconds: int = 10 * 60
    min_event_ts: datetime = field(default_factory=lambda: parse_iso_utc("2000-01-01T00:00:00Z"))
    naive_timestamp_policy: str = "assume_utc"
    event_id_pattern: str = r"^[A-Za-z0-9_\-:.]{1,64}$"
    input_extensions: tuple[str, ...] = (".csv", ".jsonl")
    # None -> use the run start time. Set it for fully reproducible runs/tests.
    reference_time: datetime | None = None

    @property
    def session_gap_ms(self) -> int:
        return self.session_gap_seconds * 1000

    def with_overrides(self, **kwargs) -> "PipelineConfig":
        clean = {k: v for k, v in kwargs.items() if v is not None}
        return replace(self, **clean)

    def to_json(self) -> str:
        d = asdict(self)
        for k, v in d.items():
            if isinstance(v, datetime):
                d[k] = v.isoformat()
            elif isinstance(v, tuple):
                d[k] = list(v)
        return json.dumps(d, sort_keys=True)

    def validate(self) -> None:
        if self.session_gap_seconds <= 0:
            raise ValueError("session_gap_seconds must be > 0")
        if self.sessionize_mode not in ("per_file", "per_run"):
            raise ValueError("sessionize_mode must be 'per_file' or 'per_run'")
        if self.naive_timestamp_policy not in ("assume_utc", "reject"):
            raise ValueError("naive_timestamp_policy must be 'assume_utc' or 'reject'")
        if not self.allowed_event_types:
            raise ValueError("allowed_event_types must not be empty")


def load_config(path: str | Path | None = None) -> PipelineConfig:
    """Load config from TOML. Missing keys fall back to dataclass defaults."""
    path = Path(path) if path else DEFAULT_CONFIG_PATH
    if not path.exists():
        cfg = PipelineConfig()
        cfg.validate()
        return cfg
    with path.open("rb") as fh:
        raw = tomllib.load(fh)
    s = raw.get("sessionization", {})
    v = raw.get("validation", {})
    i = raw.get("input", {})
    defaults = PipelineConfig()
    cfg = PipelineConfig(
        session_gap_seconds=int(s.get("session_gap_minutes", defaults.session_gap_seconds / 60) * 60),
        sessionize_mode=s.get("sessionize_mode", defaults.sessionize_mode),
        allowed_event_types=tuple(t.strip().lower() for t in v.get("allowed_event_types", defaults.allowed_event_types)),
        max_future_skew_seconds=int(v.get("max_future_skew_minutes", defaults.max_future_skew_seconds / 60) * 60),
        min_event_ts=parse_iso_utc(v["min_event_ts"]) if "min_event_ts" in v else defaults.min_event_ts,
        naive_timestamp_policy=v.get("naive_timestamp_policy", defaults.naive_timestamp_policy),
        event_id_pattern=v.get("event_id_pattern", defaults.event_id_pattern),
        input_extensions=tuple(e.lower() for e in i.get("extensions", defaults.input_extensions)),
    )
    cfg.validate()
    return cfg
