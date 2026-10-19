"""Tests for the bounded feed-presence check behind desktop cold start."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from src.ship import jobs_first_run_state as feed_state


def _write_gz(path: Path, payload: bytes) -> Path:
    with gzip.open(path, "wb") as handle:
        handle.write(payload)
    return path


def _forbid_full_parse(monkeypatch: pytest.MonkeyPatch) -> None:
    def _unexpected(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("the bounded prefix should have settled row presence")

    monkeypatch.setattr(feed_state, "read_json", _unexpected)


def test_gz_row_list_is_detected_from_the_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _write_gz(
        tmp_path / "jobs-unified-light.json.gz",
        json.dumps([{"id": 1, "title": "Tools Programmer"}, {"id": 2}]).encode("utf-8"),
    )
    _forbid_full_parse(monkeypatch)

    assert feed_state.json_feed_artifact_has_rows(path) is True


def test_gz_empty_row_list_is_rejected_from_the_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _write_gz(tmp_path / "jobs-unified-light.json.gz", b"[]\n")
    _forbid_full_parse(monkeypatch)

    assert feed_state.json_feed_artifact_has_rows(path) is False


def test_gz_wrapped_rows_are_detected_from_the_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _write_gz(
        tmp_path / "jobs-unified.json.gz",
        b'{"jobs": [{"id": 1, "title": "Tools Programmer"}], "generatedAt": "2026-05-17"}',
    )
    _forbid_full_parse(monkeypatch)

    assert feed_state.json_feed_artifact_has_rows(path) is True


def test_gz_wrapped_empty_rows_are_rejected_from_the_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _write_gz(
        tmp_path / "jobs-unified.json.gz", b'{"jobs": [], "generatedAt": "2026-05-17"}'
    )
    _forbid_full_parse(monkeypatch)

    assert feed_state.json_feed_artifact_has_rows(path) is False


def test_gz_prefix_that_cannot_decide_falls_back_to_the_full_parse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    padding = "x" * (feed_state.FEED_PRESENCE_PREFIX_BYTES + 1024)
    payload = json.dumps({"generatedAt": padding, "jobs": [{"id": 1}]}).encode("utf-8")
    path = _write_gz(tmp_path / "jobs-unified.json.gz", payload)
    calls: list[Path] = []
    real_read_json = feed_state.read_json

    def _counting(path_arg: Path, default: object) -> object:
        calls.append(Path(path_arg))
        return real_read_json(path_arg, default)

    monkeypatch.setattr(feed_state, "read_json", _counting)

    assert feed_state.json_feed_artifact_has_rows(path) is True
    assert calls == [path], "an undecidable prefix must fall back to parsing the whole file"


def test_uncompressed_feed_is_parsed_as_before(tmp_path: Path) -> None:
    rows = tmp_path / "jobs-unified-light.json"
    rows.write_text(json.dumps([{"id": 1}]), encoding="utf-8")
    empty = tmp_path / "jobs-unified.json"
    empty.write_text("[]", encoding="utf-8")

    assert feed_state.json_feed_artifact_has_rows(rows) is True
    assert feed_state.json_feed_artifact_has_rows(empty) is False


def test_missing_gz_feed_reports_no_rows(tmp_path: Path) -> None:
    assert feed_state.json_feed_artifact_has_rows(tmp_path / "jobs-unified.json.gz") is False
