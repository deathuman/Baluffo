from __future__ import annotations

import gzip
import json
from pathlib import Path

from src.shared.partial_json import read_json_rows_prefix


def _rows(count: int) -> list[dict[str, object]]:
    return [{"id": index, "title": f"Job {index}", "padding": "x" * 200} for index in range(count)]


def _write_gzip(path: Path, payload: str) -> Path:
    with gzip.open(path, mode="wt", encoding="utf-8") as handle:
        handle.write(payload)
    return path


def test_matches_full_parse_prefix_across_blocks(tmp_path: Path) -> None:
    rows = _rows(400)
    path = _write_gzip(tmp_path / "feed.json.gz", json.dumps(rows))

    assert read_json_rows_prefix(path, 10) == rows[:10]
    assert read_json_rows_prefix(path, 4) == rows[:4]


def test_plain_array_and_limit_zero(tmp_path: Path) -> None:
    rows = _rows(3)
    path = tmp_path / "feed.json"
    path.write_text(json.dumps(rows), encoding="utf-8")

    assert read_json_rows_prefix(path, 10) == rows
    assert read_json_rows_prefix(path, 0) == []


def test_element_spanning_a_block_boundary(tmp_path: Path) -> None:
    rows = [{"id": 1, "blob": "y" * (200 * 1024)}, {"id": 2}]
    path = _write_gzip(tmp_path / "feed.json.gz", json.dumps(rows))

    assert read_json_rows_prefix(path, 1) == [rows[0]]


def test_skips_non_object_entries_when_counting_rows(tmp_path: Path) -> None:
    path = tmp_path / "feed.json"
    path.write_text('["skip", {"id": 1}, {"id": 2}]', encoding="utf-8")

    assert read_json_rows_prefix(path, 1) == [{"id": 1}]


def test_stops_before_an_unreadable_tail(tmp_path: Path) -> None:
    path = tmp_path / "feed.json"
    path.write_text('[{"id": 1}, "not-a-row", {"id": 2}', encoding="utf-8")

    assert read_json_rows_prefix(path, 1) == [{"id": 1}]


def test_returns_none_when_the_prefix_cannot_settle(tmp_path: Path) -> None:
    envelope = tmp_path / "envelope.json"
    envelope.write_text(json.dumps({"jobs": [{"id": 1}]}), encoding="utf-8")
    truncated = tmp_path / "truncated.json"
    truncated.write_text('[{"id": 1}', encoding="utf-8")
    empty = tmp_path / "empty.json"
    empty.write_text("", encoding="utf-8")

    assert read_json_rows_prefix(envelope, 5) is None
    assert read_json_rows_prefix(truncated, 5) is None
    assert read_json_rows_prefix(empty, 5) is None
    assert read_json_rows_prefix(tmp_path / "missing.json", 5) is None
