"""The conflicts refresh reads a bounded prefix of the jobs feed.

`load_registry_conflicts_payload` runs inside the background refresh worker. A
single whole-feed `json.loads` there holds the GIL for the artifact's full parse,
so the payload reads a bounded row prefix instead. A truncated prefix must be
discarded: partial job evidence can make boards look independent and suppress a
real conflict, so truncation keeps conflicts for review.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.bridge import registry_conflicts as conflicts_module
from src.bridge.registry_conflicts import load_registry_conflicts_payload
from tests.bridge.test_registry_conflicts_independent_providers import (
    _greenhouse_job_row,
    _sony_greenhouse_state,
)


def _write_feed(path: Path) -> None:
    path.write_text(
        json.dumps(
            [
                _greenhouse_job_row("siei", "100001"),
                _greenhouse_job_row("siei", "100002"),
                _greenhouse_job_row("pdi", "200001"),
                _greenhouse_job_row("naughtydog", "300001"),
                _greenhouse_job_row("haven", "400001"),
            ]
        ),
        encoding="utf-8",
    )


def _payload(source_state_path: Path) -> dict[str, Any]:
    return load_registry_conflicts_payload(
        load_state=_sony_greenhouse_state,
        load_json_object=lambda _path, default: default,
        source_state_path=source_state_path,
    )


def test_payload_reads_a_bounded_prefix_of_the_jobs_feed(tmp_path: Path, monkeypatch: Any) -> None:
    source_state_path = tmp_path / "jobs-source-state.json"
    source_state_path.write_text("{}", encoding="utf-8")
    feed_path = tmp_path / "jobs-unified.json"
    _write_feed(feed_path)
    read_calls: list[tuple[Path, int]] = []
    prefix_reader = conflicts_module.read_json_rows_prefix

    def record_read(path: Path, limit: int) -> list[dict[str, Any]] | None:
        read_calls.append((Path(path), limit))
        return prefix_reader(path, limit)

    monkeypatch.setattr(conflicts_module, "read_json_rows_prefix", record_read)

    payload = _payload(source_state_path)

    assert read_calls == [(feed_path, conflicts_module._CONFLICT_JOB_ROW_LIMIT + 1)]
    assert payload["conflicts"] == []
    assert payload["suppressedIndependentProviderBoards"]["summary"] == {
        "familyCount": 1,
        "rowCount": 4,
    }
    assert "jobs_feed_row_limit_exceeded" not in payload.get("warnings", [])


def test_truncated_prefix_keeps_conflicts_instead_of_suppressing(
    tmp_path: Path, monkeypatch: Any
) -> None:
    monkeypatch.setattr(conflicts_module, "_CONFLICT_JOB_ROW_LIMIT", 3)
    source_state_path = tmp_path / "jobs-source-state.json"
    source_state_path.write_text("{}", encoding="utf-8")
    _write_feed(tmp_path / "jobs-unified.json")

    payload = _payload(source_state_path)

    assert payload["summary"]["conflictCount"] == 1
    assert payload["suppressedIndependentProviderBoards"]["summary"] == {
        "familyCount": 0,
        "rowCount": 0,
    }
    assert "jobs_feed_row_limit_exceeded" in payload["warnings"]
