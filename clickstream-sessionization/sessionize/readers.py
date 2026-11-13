"""Source file readers. They never raise on bad records: they yield them with an error."""
from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator


@dataclass
class RawRecord:
    line_no: int                 # physical line number in the source file (CSV header = line 1)
    fields: dict | None          # column -> raw string value; None when the record is malformed
    raw: str                     # original content, kept for quarantine traceability
    error: str | None = None     # parse error description when malformed


@dataclass
class SourceFile:
    path: Path
    fmt: str                     # 'csv' | 'jsonl'
    columns: list[str]           # normalized header (CSV) or [] for JSONL / empty files
    records: Iterator[RawRecord]
    closer: Callable[[], None] | None = None

    def close(self) -> None:
        close = getattr(self.records, "close", None)  # generators: runs their finally/with
        if close:
            close()
        if self.closer:
            self.closer()


def _norm_col(name: str) -> str:
    return name.strip().lower()


def open_source(path: Path) -> SourceFile:
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return _open_csv(path)
    if suffix == ".jsonl":
        return SourceFile(path, "jsonl", [], _iter_jsonl(path))
    raise ValueError(f"unsupported file type: {path.name}")


def _open_csv(path: Path) -> SourceFile:
    # Read the header eagerly so the caller can run the file-level schema check
    # before any row is processed. utf-8-sig strips a BOM if present.
    fh = path.open("r", encoding="utf-8-sig", newline="")
    reader = csv.reader(fh)
    header: list[str] = []
    for row in reader:
        if any(cell.strip() for cell in row):
            header = [_norm_col(c) for c in row]
            break
    if not header:
        fh.close()
        return SourceFile(path, "csv", [], iter(()))

    def rows() -> Iterator[RawRecord]:
        try:
            for row in reader:
                if not row or not any(cell.strip() for cell in row):
                    continue  # blank line: not a record
                raw = json.dumps(row, ensure_ascii=False)  # exact cell values, unambiguous
                if len(row) != len(header):
                    yield RawRecord(reader.line_num, None, raw,
                                    f"expected {len(header)} fields, got {len(row)}")
                    continue
                yield RawRecord(reader.line_num, dict(zip(header, row)), raw)
        finally:
            fh.close()

    return SourceFile(path, "csv", header, rows(), closer=fh.close)


def _iter_jsonl(path: Path) -> Iterator[RawRecord]:
    with path.open("r", encoding="utf-8-sig") as fh:
        for line_no, line in enumerate(fh, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                obj = json.loads(text)
            except json.JSONDecodeError as exc:
                yield RawRecord(line_no, None, text, f"invalid JSON: {exc.msg}")
                continue
            if not isinstance(obj, dict):
                yield RawRecord(line_no, None, text, "JSON value is not an object")
                continue
            fields = {_norm_col(str(k)): (None if v is None else str(v)) for k, v in obj.items()}
            yield RawRecord(line_no, fields, text)


def read_users(path: Path) -> tuple[list[str], list[RawRecord]]:
    src = _open_csv(Path(path))
    return src.columns, list(src.records)
