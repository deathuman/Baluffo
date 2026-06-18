from __future__ import annotations

from pathlib import Path

import pytest

import src.source_registry_io as registry_io


def _oversized_tombstone_journal(tmp_path: Path) -> Path:
    journal_path = tmp_path / "source-registry-tombstones.jsonl"
    journal_path.write_text(
        registry_io._json_journal_record_text({"source-1": {"deletedAt": "old"}}),
        encoding="utf-8",
    )
    return journal_path


def test_compact_registry_journals_reports_expected_write_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal_path = _oversized_tombstone_journal(tmp_path)
    monkeypatch.setattr(registry_io, "_JSON_JOURNAL_COMPACT_MAX_BYTES", 1)
    monkeypatch.setattr(
        registry_io,
        "_write_text_atomic",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(PermissionError("locked")),
    )

    result = registry_io.compact_registry_journals(tmp_path)

    assert result["ok"] is False
    assert result["errors"] == [
        {
            "path": str(journal_path),
            "errorType": "PermissionError",
            "error": "locked",
        }
    ]


def test_compact_registry_journals_propagates_unexpected_runtime_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _oversized_tombstone_journal(tmp_path)
    monkeypatch.setattr(registry_io, "_JSON_JOURNAL_COMPACT_MAX_BYTES", 1)
    monkeypatch.setattr(
        registry_io,
        "_registry_journal_repair_payload",
        lambda _path: (_ for _ in ()).throw(RuntimeError("unexpected compaction bug")),
    )

    with pytest.raises(RuntimeError, match="unexpected compaction bug"):
        registry_io.compact_registry_journals(tmp_path)
