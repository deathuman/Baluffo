import gzip
import json
from pathlib import Path

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
        with gzip.open(compressed_path, mode="rt", encoding="utf-8") as handle:
            core_rows = json.load(handle)
        with gzip.open(metadata_path, mode="rt", encoding="utf-8") as handle:
            metadata_map = json.load(handle)

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
        assert sr.load_json_array(path, [])[0] == payload[0]


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

        writes: list[tuple[str, object]] = []
        monkeypatch.setattr(
            srio,
            "_write_json_payload_atomic",
            lambda write_path, write_payload: writes.append((Path(write_path).name, write_payload)),
        )

        sr.save_json_atomic(path, payload)
        assert writes == []

        changed_payload = [
            {
                **payload[0],
                "promotionReason": "updated_reason",
            }
        ]
        sr.save_json_atomic(path, changed_payload)
        assert writes


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

        writes: list[tuple[str, object]] = []
        monkeypatch.setattr(
            srio,
            "_write_json_payload_atomic",
            lambda write_path, write_payload: writes.append((Path(write_path).name, write_payload)),
        )

        sr.save_json_atomic(path, payload)
        assert writes == []

        sr.save_json_atomic(
            path,
            {
                **payload,
                "approvedSinceLastRun": 2,
            },
        )
        assert writes


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
