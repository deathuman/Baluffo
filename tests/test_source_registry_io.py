from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

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


def test_load_json_object_ignores_stale_non_registry_journal(tmp_path: Path) -> None:
    path = tmp_path / "source-approval-state.json"
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

    path.with_name("source-approval-state.jsonl").write_text(
        registry_io._json_journal_record_text(stale_journal_payload),
        encoding="utf-8",
    )
    path.write_text(json.dumps(current_payload), encoding="utf-8")
    journal_path = path.with_name("source-approval-state.jsonl")
    os.utime(journal_path, (1000, 1000))
    os.utime(path, (2000, 2000))

    assert registry_io.load_json_object(path, {}) == current_payload


def test_load_json_object_ignores_newer_non_registry_journal(tmp_path: Path) -> None:
    path = tmp_path / "source-approval-state.json"
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
    path.with_name("source-approval-state.jsonl").write_text(
        registry_io._json_journal_record_text(current_journal_payload),
        encoding="utf-8",
    )
    journal_path = path.with_name("source-approval-state.jsonl")
    os.utime(path, (1000, 1000))
    os.utime(journal_path, (2000, 2000))

    assert registry_io.load_json_object(path, {}) == stale_base_payload


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
    path.with_name("jobs-fetch-report.jsonl").write_text(
        registry_io._json_journal_record_text(journal_payload),
        encoding="utf-8",
    )
    # Make journal newer than canonical
    journal_path = path.with_name("jobs-fetch-report.jsonl")
    os.utime(path, (1000, 1000))
    os.utime(journal_path, (2000, 2000))

    # load_runtime_evidence must return canonical content, ignoring the newer journal
    assert registry_io.load_runtime_evidence(path, {}) == canonical_payload


@pytest.mark.parametrize(
    "filename",
    [
        "jobs-fetch-report.json",
        "jobs-fetch-tasks.json",
        "sync-live-task.json",
        "source-discovery-report.json",
    ],
)
def test_save_json_atomic_does_not_journal_runtime_evidence(
    tmp_path: Path,
    filename: str,
) -> None:
    path = tmp_path / filename
    payload = {"runId": "runtime_1", "status": "running"}

    registry_io.save_json_atomic(path, payload)

    assert path.exists()
    assert not path.with_suffix(".jsonl").exists()
    assert registry_io.load_runtime_evidence(path, {}) == payload


def test_save_json_atomic_does_not_journal_runtime_evidence_array(
    tmp_path: Path,
) -> None:
    path = tmp_path / "source-discovery-candidates.json"
    payload = [{"id": "candidate-1", "name": "Candidate"}, "ignored"]

    registry_io.save_json_atomic(path, payload)

    assert path.exists()
    assert not path.with_suffix(".jsonl").exists()
    assert registry_io.load_runtime_evidence_array(path, []) == [
        {"id": "candidate-1", "name": "Candidate"}
    ]


def test_load_json_array_rejects_discovery_candidates_runtime_evidence(
    tmp_path: Path,
) -> None:
    path = tmp_path / "source-discovery-candidates.json"
    path.write_text(json.dumps([{"id": "candidate-1"}]), encoding="utf-8")

    with pytest.raises(RuntimeError, match="load_runtime_evidence_array"):
        registry_io.load_json_array(path, [])


def test_save_json_atomic_runtime_evidence_noop_ignores_stale_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "jobs-fetch-report.json"
    canonical_payload = {"runId": "fetch_1", "finishedAt": "done"}
    stale_journal_payload = {"runId": "fetch_1", "finishedAt": ""}
    path.write_text(json.dumps(canonical_payload), encoding="utf-8")
    path.with_name("jobs-fetch-report.jsonl").write_text(
        registry_io._json_journal_record_text(stale_journal_payload),
        encoding="utf-8",
    )
    os.utime(path, (1000, 1000))
    os.utime(path.with_name("jobs-fetch-report.jsonl"), (2000, 2000))
    writes: list[tuple[Path, object]] = []
    monkeypatch.setattr(
        registry_io,
        "_write_json_payload_atomic",
        lambda write_path, write_payload: writes.append((Path(write_path), write_payload)),
    )

    registry_io.save_json_atomic(path, canonical_payload)

    assert writes == []


def test_append_json_journal_record_rejects_runtime_evidence(tmp_path: Path) -> None:
    path = tmp_path / "jobs-fetch-report.json"

    with pytest.raises(ValueError, match="Runtime evidence files must not be journaled"):
        registry_io._append_json_journal_record(path, {"runId": "fetch_1"})


def test_append_json_journal_record_rejects_non_registry_artifact(tmp_path: Path) -> None:
    path = tmp_path / "source-approval-state.json"

    with pytest.raises(ValueError, match="JSON journaling is registry-only"):
        registry_io._append_json_journal_record(path, {"approvedSinceLastRun": 1})


def test_append_json_journal_record_rewrites_before_hard_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "source-registry-active.json"
    journal_path = tmp_path / "source-registry-active.jsonl"
    stale_payload = [{"id": "stale", "name": "Stale" * 20}]
    current_payload = [{"id": "current", "name": "Current"}]
    monkeypatch.setattr(registry_io, "_JSON_JOURNAL_HARD_MAX_BYTES", 128)
    journal_path.write_text(
        registry_io._json_journal_record_text(stale_payload) * 2,
        encoding="utf-8",
    )

    registry_io._append_json_journal_record(path, current_payload)

    assert journal_path.read_text(encoding="utf-8").count("\n") == 1
    assert registry_io._load_json_journal_latest_payload(path) == current_payload


def test_append_json_journal_record_hard_cap_failure_does_not_append(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "source-registry-active.json"
    journal_path = tmp_path / "source-registry-active.jsonl"
    original_journal = registry_io._json_journal_record_text(
        [{"id": "stale", "name": "Stale" * 20}]
    )
    journal_path.write_text(original_journal, encoding="utf-8")
    monkeypatch.setattr(registry_io, "_JSON_JOURNAL_HARD_MAX_BYTES", 128)
    monkeypatch.setattr(registry_io, "_WRITE_RETRY_ATTEMPTS", 1)
    monkeypatch.setattr(registry_io, "_WRITE_RETRY_BACKOFF_BASE_S", 0)
    monkeypatch.setattr(
        registry_io.os,
        "replace",
        lambda _src, _dst: (_ for _ in ()).throw(PermissionError("journal locked")),
    )

    with pytest.raises(PermissionError, match="journal locked"):
        registry_io._append_json_journal_record(
            path,
            [{"id": "current", "name": "Current"}],
        )

    assert journal_path.read_text(encoding="utf-8") == original_journal


def test_compact_json_journal_if_needed_uses_required_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "source-registry-tombstones.json"
    journal_path = tmp_path / "source-registry-tombstones.jsonl"
    journal_path.write_text(
        registry_io._json_journal_record_text({"source-1": {"deletedAt": "old"}}),
        encoding="utf-8",
    )
    calls: list[str] = []

    def write_text_atomic(
        _path: Path,
        _text: str,
        *,
        policy: str = registry_io._WRITE_POLICY_REQUIRED,
    ) -> bool:
        calls.append(policy)
        return True

    monkeypatch.setattr(registry_io, "_JSON_JOURNAL_COMPACT_MAX_BYTES", 1)
    monkeypatch.setattr(registry_io, "_write_text_atomic", write_text_atomic)

    registry_io._compact_json_journal_if_needed(
        path,
        {"source-1": {"deletedAt": "current"}},
    )

    assert calls == [registry_io._WRITE_POLICY_REQUIRED]


def test_compact_registry_journals_rewrites_oversized_registry_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "source-registry-tombstones.json"
    journal_path = tmp_path / "source-registry-tombstones.jsonl"
    payload_one = {"source-1": {"deletedAt": "2026-04-01T00:00:00+00:00"}}
    payload_two = {"source-1": {"deletedAt": "2026-04-01T00:01:00+00:00"}}
    journal_path.write_text(
        registry_io._json_journal_record_text(payload_one)
        + registry_io._json_journal_record_text(payload_two),
        encoding="utf-8",
    )
    monkeypatch.setattr(registry_io, "_JSON_JOURNAL_COMPACT_MAX_BYTES", 1)

    result = registry_io.compact_registry_journals(tmp_path)

    assert result["ok"] is True
    assert len(result["compacted"]) == 1
    assert journal_path.read_text(encoding="utf-8").count("\n") == 1
    assert registry_io._load_json_journal_latest_payload(path) == payload_two


def test_cleanup_runtime_evidence_journals_quarantines_known_stale_journals(
    tmp_path: Path,
) -> None:
    stale_report_journal = tmp_path / "jobs-fetch-report.jsonl"
    stale_candidates_journal = tmp_path / "source-discovery-candidates.jsonl"
    stale_sync_journal = tmp_path / "sync-live-task.jsonl"
    registry_journal = tmp_path / "source-approval-state.jsonl"
    stale_report_journal.write_text('{"payload":{"status":"stale"}}\n', encoding="utf-8")
    stale_candidates_journal.write_text('{"payload":[{"id":"stale"}]}\n', encoding="utf-8")
    stale_sync_journal.write_text('{"payload":{"status":"stale"}}\n', encoding="utf-8")
    registry_journal.write_text('{"payload":{"status":"keep"}}\n', encoding="utf-8")

    result = registry_io.cleanup_runtime_evidence_journals(tmp_path)

    assert result["ok"] is True
    assert result["checked"] == 5
    assert len(result["quarantined"]) == 3
    assert not stale_report_journal.exists()
    assert not stale_candidates_journal.exists()
    assert not stale_sync_journal.exists()
    assert registry_journal.exists()
    quarantined_names = {
        Path(row["quarantinePath"]).name.split(".", maxsplit=1)[0] for row in result["quarantined"]
    }
    assert quarantined_names == {
        "jobs-fetch-report",
        "source-discovery-candidates",
        "sync-live-task",
    }
    assert all(Path(row["quarantinePath"]).exists() for row in result["quarantined"])
