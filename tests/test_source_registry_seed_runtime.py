import gzip
import json
from pathlib import Path
from urllib.parse import urlparse

import pytest

from src import source_registry as sr
from src import source_registry_io as srio
from src.jobs.common.sources import load_registry_from_file
from tests.helpers.temp_paths import workspace_tmpdir


def _write_seed(root: Path, bucket: str, rows: list[dict]) -> Path:
    path = root / "defaults" / f"source-registry-{bucket}.seed.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows), encoding="utf-8")
    return path


def _normalized_host(url: object) -> str:
    return (urlparse(str(url or "")).hostname or "").lower().removeprefix("www.")


def test_registry_loads_seed_when_runtime_file_is_missing() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        root = Path(tmp)
        runtime_path = root / "source-registry-active.json"
        _write_seed(root, "active", [{"id": "seed-active", "adapter": "static"}])

        assert not runtime_path.exists()
        assert sr.load_json_array(runtime_path, [])[0]["id"] == "seed-active"


def test_runtime_registry_file_overrides_seed_file() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        root = Path(tmp)
        runtime_path = root / "source-registry-active.json"
        _write_seed(root, "active", [{"id": "seed-active", "adapter": "static"}])
        sr.save_json_atomic(runtime_path, [{"id": "runtime-active", "adapter": "greenhouse"}])

        assert sr.load_json_array(runtime_path, [])[0]["id"] == "runtime-active"


def test_registry_writes_target_runtime_file_without_mutating_seed() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        root = Path(tmp)
        runtime_path = root / "source-registry-pending.json"
        seed_payload = [{"id": "seed-pending", "adapter": "static"}]
        seed_path = _write_seed(root, "pending", seed_payload)
        runtime_row = {
            "id": "runtime-pending",
            "name": "Runtime Pending",
            "adapter": "lever",
            "studio": "Runtime Pending Studio",
            "registryState": "pending",
            "pendingReason": "manual",
            "stateChangedAt": "2026-04-01T00:00:00+00:00",
            "stateChangedBy": "tester",
            "lastPromotedAt": "",
            "lastDemotedAt": "2026-04-01T00:00:00+00:00",
            "candidateState": "validated",
            "enabledByDefault": False,
            "approvedAt": "",
            "approvedBy": "",
            "liveAt": "",
            "quarantinedAt": "",
            "quarantineReason": "",
            "company_id": "Runtime Pending Studio",
        }

        sr.save_json_atomic(runtime_path, [runtime_row])

        assert json.loads(seed_path.read_text(encoding="utf-8")) == seed_payload
        assert (root / "source-registry-pending.json.gz").exists()
        journal_path = root / "source-registry-pending.jsonl"
        assert journal_path.exists()
        journal_record = json.loads(journal_path.read_text(encoding="utf-8"))
        assert journal_record["schemaVersion"] == 1
        assert journal_record["payload"] == [runtime_row]
        assert len(journal_record["contentHash"]) == 64
        assert (root / "source-registry-metadata.json.gz").exists()
        assert sr.load_json_array(runtime_path, [])[0] == runtime_row


def test_save_json_atomic_splits_lean_registry_storage() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        path = Path(tmp) / "source-registry-active.json"
        payload = [
            {
                "id": "smartrecruiters:company_id:Gameloft",
                "name": "Gameloft",
                "adapter": "smartrecruiters",
                "studio": "Gameloft",
                "registryState": "active",
                "pendingReason": "",
                "stateChangedAt": "2026-04-01T00:00:00+00:00",
                "stateChangedBy": "registry_migration_v2",
                "lastPromotedAt": "2026-04-01T00:00:00+00:00",
                "lastDemotedAt": "",
                "candidateState": "live",
                "enabledByDefault": True,
                "approvedAt": "2026-04-01T00:00:00+00:00",
                "approvedBy": "registry_migration_v2",
                "liveAt": "2026-04-01T00:00:00+00:00",
                "promotionLane": "structured_batch",
                "promotionReason": "structured_batch_family",
                "rankScore": 61,
                "rankReasons": ["high_confidence"],
                "company_id": "Gameloft",
            }
        ]

        sr.save_json_atomic(path, payload)

        compressed_path = Path(tmp) / "source-registry-active.json.gz"
        metadata_path = Path(tmp) / "source-registry-metadata.json.gz"
        journal_path = Path(tmp) / "source-registry-active.jsonl"
        with gzip.open(compressed_path, mode="rt", encoding="utf-8") as handle:
            core_rows = json.load(handle)
        with gzip.open(metadata_path, mode="rt", encoding="utf-8") as handle:
            metadata_map = json.load(handle)
        journal_record = json.loads(journal_path.read_text(encoding="utf-8"))

        assert core_rows == [
            {
                "id": "smartrecruiters:company_id:Gameloft",
                "name": "Gameloft",
                "adapter": "smartrecruiters",
                "studio": "Gameloft",
                "registryState": "active",
                "pendingReason": "",
                "stateChangedAt": "2026-04-01T00:00:00+00:00",
                "stateChangedBy": "registry_migration_v2",
                "lastPromotedAt": "2026-04-01T00:00:00+00:00",
                "lastDemotedAt": "",
            }
        ]
        assert metadata_map == {
            "smartrecruiters:company_id:Gameloft": {
                "candidateState": "live",
                "enabledByDefault": True,
                "approvedAt": "2026-04-01T00:00:00+00:00",
                "approvedBy": "registry_migration_v2",
                "liveAt": "2026-04-01T00:00:00+00:00",
                "promotionLane": "structured_batch",
                "promotionReason": "structured_batch_family",
                "rankScore": 61,
                "rankReasons": ["high_confidence"],
                "company_id": "Gameloft",
            }
        }
        assert journal_record["schemaVersion"] == 1
        assert journal_record["payload"] == payload
        assert len(journal_record["contentHash"]) == 64
        assert sr.load_json_array(path, []) == payload


def test_save_json_atomic_skips_unchanged_lean_registry_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("source-registry") as tmp:
        path = Path(tmp) / "source-registry-active.json"
        payload = [
            {
                "id": "smartrecruiters:company_id:Gameloft",
                "name": "Gameloft",
                "adapter": "smartrecruiters",
                "studio": "Gameloft",
                "registryState": "active",
                "pendingReason": "",
                "stateChangedAt": "2026-04-01T00:00:00+00:00",
                "stateChangedBy": "registry_migration_v2",
                "lastPromotedAt": "2026-04-01T00:00:00+00:00",
                "lastDemotedAt": "",
                "candidateState": "live",
                "enabledByDefault": True,
                "approvedAt": "2026-04-01T00:00:00+00:00",
                "approvedBy": "registry_migration_v2",
                "liveAt": "2026-04-01T00:00:00+00:00",
                "promotionLane": "structured_batch",
                "promotionReason": "structured_batch_family",
                "rankScore": 61,
                "rankReasons": ["high_confidence"],
                "company_id": "Gameloft",
            }
        ]

        sr.save_json_atomic(path, payload)

        writes: list[tuple[str, str, object]] = []
        monkeypatch.setattr(
            srio,
            "_append_json_journal_record",
            lambda write_path, write_payload: writes.append(
                ("journal", Path(write_path).name, write_payload)
            ),
        )
        monkeypatch.setattr(
            srio,
            "_write_json_payload_atomic",
            lambda write_path, write_payload: writes.append(
                ("snapshot", Path(write_path).name, write_payload)
            ),
        )

        sr.save_json_atomic(path, payload)
        assert writes == []


def test_save_json_atomic_skips_unchanged_plain_object_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("source-registry") as tmp:
        path = Path(tmp) / "source-approval-state.json"
        payload = {
            "approvedSinceLastRun": 1,
            "updatedAt": "2026-04-01T00:00:00+00:00",
        }

        sr.save_json_atomic(path, payload)

        writes: list[tuple[str, str, object]] = []
        monkeypatch.setattr(
            srio,
            "_append_json_journal_record",
            lambda write_path, write_payload: writes.append(
                ("journal", Path(write_path).name, write_payload)
            ),
        )
        monkeypatch.setattr(
            srio,
            "_write_json_payload_atomic",
            lambda write_path, write_payload: writes.append(
                ("snapshot", Path(write_path).name, write_payload)
            ),
        )

        sr.save_json_atomic(path, payload)
        assert writes == []


def test_load_json_array_ignores_trailing_partial_journal_record() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        root = Path(tmp)
        path = root / "source-registry-active.json"
        payload = [{"id": "registry-journal", "name": "Registry Journal", "adapter": "static"}]

        sr.save_json_atomic(path, payload)

        snapshot_path = root / "source-registry-active.json.gz"
        snapshot_path.write_text("not-json", encoding="utf-8")
        journal_path = root / "source-registry-active.jsonl"
        with journal_path.open("a", encoding="utf-8") as handle:
            handle.write('{"schemaVersion":1,"contentHash":"broken"')

        # Ensure the journal mtime is newer than the corrupt snapshot so the
        # mtime guard in load_json_array allows the journal overlay to win.
        import os as _os

        _snap_stat = snapshot_path.stat()
        _os.utime(
            journal_path,
            (_snap_stat.st_atime + 1, _snap_stat.st_mtime + 1),
        )

        assert sr.load_json_array(path, []) == payload


def test_save_json_atomic_compacts_json_journal_for_object_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("source-registry") as tmp:
        root = Path(tmp)
        path = root / "source-registry-tombstones.json"
        monkeypatch.setattr(srio, "_JSON_JOURNAL_COMPACT_MAX_BYTES", 1)

        payload_one = {
            "source-1": {"deletedAt": "2026-04-01T00:00:00+00:00"},
        }
        payload_two = {
            "source-1": {"deletedAt": "2026-04-01T00:01:00+00:00"},
        }
        payload_three = {
            "source-1": {"deletedAt": "2026-04-01T00:02:00+00:00"},
        }

        sr.save_json_atomic(path, payload_one)
        sr.save_json_atomic(path, payload_two)
        sr.save_json_atomic(path, payload_three)

        journal_path = root / "source-registry-tombstones.jsonl"
        journal_record = json.loads(journal_path.read_text(encoding="utf-8"))
        assert journal_path.read_text(encoding="utf-8").count("\n") == 1
        assert journal_record["schemaVersion"] == 1
        assert journal_record["payload"] == payload_three
        assert len(journal_record["contentHash"]) == 64
        assert sr.load_json_object(path, {}) == payload_three


def test_required_json_snapshot_replace_retries_and_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("source-registry") as tmp:
        path = Path(tmp) / "source-approval-state.json"
        payload = {
            "approvedSinceLastRun": 1,
            "updatedAt": "2026-04-01T00:00:00+00:00",
        }
        real_replace = srio.os.replace
        calls = 0

        monkeypatch.setattr(srio, "_WRITE_RETRY_ATTEMPTS", 3)
        monkeypatch.setattr(srio, "_WRITE_RETRY_BACKOFF_BASE_S", 0)

        def flaky_replace(src: object, dst: object) -> None:
            nonlocal calls
            calls += 1
            if calls < 3:
                raise PermissionError("locked")
            real_replace(src, dst)

        monkeypatch.setattr(srio.os, "replace", flaky_replace)

        sr.save_json_atomic(path, payload)

        assert calls == 3
        assert path.exists()
        assert sr.load_json_object(path, {}) == payload


def test_required_json_snapshot_replace_persistent_failure_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("source-registry") as tmp:
        path = Path(tmp) / "source-approval-state.json"
        payload = {
            "approvedSinceLastRun": 1,
            "updatedAt": "2026-04-01T00:00:00+00:00",
        }

        monkeypatch.setattr(srio, "_WRITE_RETRY_ATTEMPTS", 1)
        monkeypatch.setattr(srio, "_WRITE_RETRY_BACKOFF_BASE_S", 0)
        monkeypatch.setattr(
            srio.os,
            "replace",
            lambda _src, _dst: (_ for _ in ()).throw(PermissionError("locked")),
        )

        with pytest.raises(PermissionError, match="locked"):
            sr.save_json_atomic(path, payload)


def test_best_effort_journal_compaction_failure_preserves_latest_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("source-registry") as tmp:
        root = Path(tmp)
        path = root / "source-registry-tombstones.json"
        payload_one = {
            "source-1": {"deletedAt": "2026-04-01T00:00:00+00:00"},
        }
        payload_two = {
            "source-1": {"deletedAt": "2026-04-01T00:01:00+00:00"},
        }
        real_replace = srio.os.replace

        monkeypatch.setattr(srio, "_JSON_JOURNAL_COMPACT_MAX_BYTES", 1)
        monkeypatch.setattr(srio, "_WRITE_RETRY_ATTEMPTS", 1)
        monkeypatch.setattr(srio, "_WRITE_RETRY_BACKOFF_BASE_S", 0)

        def fail_journal_compaction_replace(src: object, dst: object) -> None:
            if Path(dst).suffix == ".jsonl":
                raise PermissionError("journal locked")
            real_replace(src, dst)

        monkeypatch.setattr(srio.os, "replace", fail_journal_compaction_replace)

        sr.save_json_atomic(path, payload_one)
        sr.save_json_atomic(path, payload_two)

        journal_path = root / "source-registry-tombstones.jsonl"
        assert journal_path.read_text(encoding="utf-8").count("\n") == 2
        assert sr.load_json_object(path, {}) == payload_two


def test_required_journal_append_retries_and_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("source-registry") as tmp:
        path = Path(tmp) / "source-registry-tombstones.json"
        journal_path = Path(tmp) / "source-registry-tombstones.jsonl"
        payload = {
            "source-1": {"deletedAt": "2026-04-01T00:00:00+00:00"},
        }
        real_open = Path.open
        calls = 0

        monkeypatch.setattr(srio, "_WRITE_RETRY_ATTEMPTS", 2)
        monkeypatch.setattr(srio, "_WRITE_RETRY_BACKOFF_BASE_S", 0)

        def flaky_open(self: Path, *args: object, **kwargs: object):
            nonlocal calls
            if self == journal_path:
                calls += 1
                if calls == 1:
                    raise PermissionError("journal locked")
            return real_open(self, *args, **kwargs)

        monkeypatch.setattr(Path, "open", flaky_open)

        sr.save_json_atomic(path, payload)

        assert calls == 2
        assert sr.load_json_object(path, {}) == payload


def test_required_journal_append_persistent_failure_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("source-registry") as tmp:
        path = Path(tmp) / "source-registry-tombstones.json"
        journal_path = Path(tmp) / "source-registry-tombstones.jsonl"
        payload = {
            "source-1": {"deletedAt": "2026-04-01T00:00:00+00:00"},
        }
        real_open = Path.open

        monkeypatch.setattr(srio, "_WRITE_RETRY_ATTEMPTS", 1)
        monkeypatch.setattr(srio, "_WRITE_RETRY_BACKOFF_BASE_S", 0)

        def locked_journal_open(self: Path, *args: object, **kwargs: object):
            if self == journal_path:
                raise PermissionError("journal locked")
            return real_open(self, *args, **kwargs)

        monkeypatch.setattr(Path, "open", locked_journal_open)

        with pytest.raises(PermissionError, match="journal locked"):
            sr.save_json_atomic(path, payload)


def test_registry_loads_legacy_monolithic_rows_without_sidecar() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        root = Path(tmp)
        path = root / "source-registry-active.json"
        legacy_row = {
            "id": "legacy:source:1",
            "name": "Legacy Source",
            "adapter": "smartrecruiters",
            "studio": "Legacy Studio",
            "registryState": "active",
            "pendingReason": "",
            "stateChangedAt": "2026-04-01T00:00:00+00:00",
            "stateChangedBy": "registry_migration_v2",
            "lastPromotedAt": "2026-04-01T00:00:00+00:00",
            "lastDemotedAt": "",
            "candidateState": "live",
            "enabledByDefault": True,
            "approvedAt": "2026-04-01T00:00:00+00:00",
            "approvedBy": "registry_migration_v2",
            "liveAt": "2026-04-01T00:00:00+00:00",
            "promotionLane": "structured_batch",
            "company_id": "Legacy Studio",
        }
        path.write_text(json.dumps([legacy_row]), encoding="utf-8")

        assert sr.load_json_array(path, []) == [legacy_row]


def test_jobs_registry_loader_reads_gzip_runtime_rows_with_sparse_metadata() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        root = Path(tmp)
        runtime_path = root / "source-registry-active.json"
        runtime_row = {
            "id": "runtime:source:1",
            "name": "Runtime Source",
            "adapter": "smartrecruiters",
            "studio": "Runtime Studio",
            "registryState": "active",
            "pendingReason": "",
            "stateChangedAt": "2026-04-01T00:00:00+00:00",
            "stateChangedBy": "registry_migration_v2",
            "lastPromotedAt": "2026-04-01T00:00:00+00:00",
            "lastDemotedAt": "",
            "candidateState": "live",
            "enabledByDefault": True,
            "approvedAt": "2026-04-01T00:00:00+00:00",
            "approvedBy": "registry_migration_v2",
            "liveAt": "2026-04-01T00:00:00+00:00",
            "promotionLane": "structured_batch",
            "company_id": "Runtime Studio",
        }

        sr.save_json_atomic(runtime_path, [runtime_row])
        rows = load_registry_from_file(runtime_path, [{"id": "fallback", "adapter": "static"}])

        assert rows == [runtime_row]


def test_jobs_registry_loader_uses_seed_when_runtime_file_is_missing() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        root = Path(tmp)
        runtime_path = root / "source-registry-active.json"
        _write_seed(root, "active", [{"id": "jobs-seed", "adapter": "static"}])

        rows = load_registry_from_file(runtime_path, [{"id": "fallback", "adapter": "static"}])

        assert rows == [{"id": "jobs-seed", "adapter": "static"}]


def test_default_super_lucky_seed_stays_on_listing_host() -> None:
    seed_path = Path("data/defaults/source-registry-active.seed.json")
    rows = json.loads(seed_path.read_text(encoding="utf-8"))
    super_lucky = next(
        row
        for row in rows
        if row.get("id") == "static:listing_url:https://www.superluckycasino.com"
    )
    listing_host = _normalized_host(super_lucky.get("listing_url"))
    page_hosts = {_normalized_host(page) for page in super_lucky.get("pages", [])}
    detail_page_hosts = {
        _normalized_host(page) for page in super_lucky.get("detailPagesSample", [])
    }

    assert super_lucky["listing_url"] == "https://www.superluckycasino.com"
    assert super_lucky["careersUrl"] == "https://www.superluckycasino.com"
    assert super_lucky["pages"] == ["https://www.superluckycasino.com"]
    assert super_lucky["id"] == "static:listing_url:https://www.superluckycasino.com"
    assert page_hosts == {listing_host}
    assert detail_page_hosts <= {listing_host}
    assert super_lucky["detailPageCount"] == 0
    assert super_lucky["detailPagesSample"] == []
