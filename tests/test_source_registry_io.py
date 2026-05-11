from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import src.source_registry_io as registry_io


def test_lean_registry_metadata_write_lock_does_not_fail_required_registry_write(
    tmp_path: Path,
    monkeypatch,
) -> None:
    active_path = tmp_path / "source-registry-active.json"
    metadata_name = "source-registry-metadata.json.gz"
    original_replace = registry_io._replace_path_with_retry

    def replace_with_locked_metadata(
        tmp: Path,
        target: Path,
        *,
        policy: str = registry_io._WRITE_POLICY_REQUIRED,
    ) -> bool:
        if target.name == metadata_name:
            return registry_io._finish_write_failure(PermissionError("locked metadata"), policy)
        return original_replace(tmp, target, policy=policy)

    monkeypatch.setattr(registry_io, "_replace_path_with_retry", replace_with_locked_metadata)
    monkeypatch.setattr(registry_io, "DATA_DIR", tmp_path)

    payload: list[dict[str, Any]] = [
        {
            "id": "static:example.com",
            "name": "Example",
            "adapter": "static",
            "url": "https://example.com/jobs",
        }
    ]

    registry_io.save_json_atomic(active_path, payload)

    # The registry write succeeds even when the metadata file cannot be saved.
    # Because the canonical file (lean rows) is newer than the journal, the mtime
    # guard returns lean core rows; non-core fields are missing since metadata
    # recovery failed.
    result = registry_io.load_json_array(active_path, [])
    assert len(result) == 1
    assert result[0]["id"] == payload[0]["id"]
    assert result[0]["name"] == payload[0]["name"]
    assert result[0]["adapter"] == payload[0]["adapter"]
    assert "url" not in result[0]


def test_load_json_object_uses_newer_json_when_stale_journal_exists(tmp_path: Path) -> None:
    path = tmp_path / "jobs-fetch-report.json"
    stale_journal_payload = {
        "runId": "fetch_1",
        "finishedAt": "",
        "summary": {"outputCount": 0},
    }
    current_payload = {
        "runId": "fetch_1",
        "finishedAt": "2026-05-08T09:56:02+02:00",
        "summary": {"outputCount": 34879},
    }

    registry_io._append_json_journal_record(path, stale_journal_payload)
    path.write_text(json.dumps(current_payload), encoding="utf-8")
    journal_path = path.with_name("jobs-fetch-report.jsonl")
    os.utime(journal_path, (1000, 1000))
    os.utime(path, (2000, 2000))

    assert registry_io.load_json_object(path, {}) == current_payload


def test_load_json_object_uses_newer_journal_when_base_json_is_stale(tmp_path: Path) -> None:
    path = tmp_path / "jobs-fetch-report.json"
    stale_base_payload = {
        "runId": "fetch_1",
        "finishedAt": "",
        "summary": {"outputCount": 0},
    }
    current_journal_payload = {
        "runId": "fetch_1",
        "finishedAt": "2026-05-08T09:56:02+02:00",
        "summary": {"outputCount": 34879},
    }

    path.write_text(json.dumps(stale_base_payload), encoding="utf-8")
    registry_io._append_json_journal_record(path, current_journal_payload)
    journal_path = path.with_name("jobs-fetch-report.jsonl")
    os.utime(path, (1000, 1000))
    os.utime(journal_path, (2000, 2000))

    assert registry_io.load_json_object(path, {}) == current_journal_payload


def test_load_json_array_uses_newer_canonical_when_stale_journal_exists(tmp_path: Path) -> None:
    path = tmp_path / "source-registry-active.json"
    stale_journal_payload = [{"id": "stale", "name": "Stale"}]
    current_payload = [{"id": "current", "name": "Current"}]

    registry_io._append_json_journal_record(path, stale_journal_payload)
    path.write_text(json.dumps(current_payload), encoding="utf-8")
    journal_path = path.with_name("source-registry-active.jsonl")
    os.utime(journal_path, (1000, 1000))
    os.utime(path, (2000, 2000))

    assert registry_io.load_json_array(path, []) == current_payload


def test_load_json_array_uses_newer_journal_when_canonical_is_stale(tmp_path: Path) -> None:
    path = tmp_path / "source-registry-active.json"
    stale_base_payload = [{"id": "stale", "name": "Stale"}]
    current_journal_payload = [{"id": "current", "name": "Current"}]

    path.write_text(json.dumps(stale_base_payload), encoding="utf-8")
    registry_io._append_json_journal_record(path, current_journal_payload)
    journal_path = path.with_name("source-registry-active.jsonl")
    os.utime(path, (1000, 1000))
    os.utime(journal_path, (2000, 2000))

    assert registry_io.load_json_array(path, []) == current_journal_payload


def test_load_runtime_evidence_reads_canonical_only_ignores_journal(tmp_path: Path) -> None:
    path = tmp_path / "jobs-fetch-report.json"
    journal_payload = {
        "runId": "fetch_1",
        "finishedAt": "",
    }
    canonical_payload = {
        "runId": "fetch_1",
        "finishedAt": "2026-05-08T09:56:02+02:00",
    }

    path.write_text(json.dumps(canonical_payload), encoding="utf-8")
    registry_io._append_json_journal_record(path, journal_payload)
    # Make journal newer than canonical
    journal_path = path.with_name("jobs-fetch-report.jsonl")
    os.utime(path, (1000, 1000))
    os.utime(journal_path, (2000, 2000))

    # load_runtime_evidence must return canonical content, ignoring the newer journal
    assert registry_io.load_runtime_evidence(path, {}) == canonical_payload
