from __future__ import annotations

import json
from pathlib import Path

import src.source_registry_io as registry_io


def test_delta_journal_records_reconstruct_latest_with_row_order(tmp_path: Path) -> None:
    path = tmp_path / "source-registry-active.json"
    payload_one = [
        {"id": "source-a", "name": "Source A"},
        {"id": "source-b", "name": "Source B"},
    ]
    payload_two = [
        {"id": "source-b", "name": "Source B Updated"},
        {"id": "source-c", "name": "Source C"},
    ]
    payload_three = [
        {"id": "source-c", "name": "Source C"},
        {"id": "source-b", "name": "Source B Updated"},
    ]

    registry_io.save_json_atomic(path, payload_one)
    registry_io.save_json_atomic(path, payload_two)
    registry_io.save_json_atomic(path, payload_three)

    journal_path = tmp_path / "source-registry-active.jsonl"
    records = [
        json.loads(line)
        for line in journal_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [record["schemaVersion"] for record in records] == [2, 2, 2]
    assert records[1]["changed"] == payload_two
    assert records[1]["removed"] == ["source-a"]
    assert records[1]["rowIds"] == ["source-b", "source-c"]
    assert records[2]["changed"] == []
    assert records[2]["removed"] == []
    assert records[2]["rowIds"] == ["source-c", "source-b"]
    assert registry_io._load_json_journal_latest_payload(path, base_payload=[]) == payload_three


def test_delta_journal_record_rejects_base_hash_mismatch(tmp_path: Path) -> None:
    path = tmp_path / "source-registry-active.json"
    payload = [{"id": "source-a", "name": "Source A"}]
    journal_path = tmp_path / "source-registry-active.jsonl"
    journal_path.write_text(
        json.dumps(
            {
                "schemaVersion": 2,
                "kind": "array_delta",
                "baseContentHash": "0" * 64,
                "contentHash": registry_io._json_journal_payload_hash(payload),
                "changed": payload,
                "removed": [],
                "rowIds": ["source-a"],
                "rowCount": 1,
                "timestamp": "2026-05-11T00:00:00+00:00",
            },
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )

    assert registry_io._load_json_journal_latest_payload(path, base_payload=[]) is None


def test_delta_journal_records_support_empty_registry_payload(tmp_path: Path) -> None:
    path = tmp_path / "source-registry-active.json"

    registry_io.save_json_atomic(path, [{"id": "source-a", "name": "Source A"}])
    registry_io.save_json_atomic(path, [])

    assert registry_io._load_json_journal_latest_payload(path, base_payload=[]) == []


def test_delta_journal_record_ignores_malformed_row_count(tmp_path: Path) -> None:
    path = tmp_path / "source-registry-active.json"
    payload = [{"id": "source-a", "name": "Source A"}]
    journal_path = tmp_path / "source-registry-active.jsonl"
    journal_path.write_text(
        json.dumps(
            {
                "schemaVersion": 2,
                "kind": "array_delta",
                "baseContentHash": registry_io._json_journal_payload_hash([]),
                "contentHash": registry_io._json_journal_payload_hash(payload),
                "changed": payload,
                "removed": [],
                "rowIds": ["source-a"],
                "rowCount": "not-an-int",
                "timestamp": "2026-05-11T00:00:00+00:00",
            },
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )

    assert registry_io._load_json_journal_latest_payload(path, base_payload=[]) is None


def test_legacy_full_payload_journal_record_remains_readable(tmp_path: Path) -> None:
    path = tmp_path / "source-registry-active.json"
    payload = [{"id": "source-a", "name": "Source A"}]
    journal_path = tmp_path / "source-registry-active.jsonl"
    journal_path.write_text(
        registry_io._json_journal_record_text(payload),
        encoding="utf-8",
    )

    assert registry_io._load_json_journal_latest_payload(path, base_payload=[]) == payload
