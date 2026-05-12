import base64
from types import SimpleNamespace

import pytest

from src.source_sync_shard import (
    build_sharded_snapshot_bundle,
    push_manifest,
    push_sharded_snapshot,
)


class _FakeSyncModule:
    def __init__(self, responses: list[tuple[int, dict]]):
        self.responses = list(responses)
        self.calls: list[dict] = []

    def _github_api_base(self) -> str:
        return "https://api.github.test"

    def validate_sync_config(self, config) -> None:  # noqa: ANN001
        assert config.repo == "owner/repo"

    def _request_json(self, **kwargs) -> tuple[int, dict, dict]:
        self.calls.append(dict(kwargs))
        if not self.responses:
            raise AssertionError("No fake responses left")
        status, payload = self.responses.pop(0)
        return status, payload, {}


class _ConflictSyncModule(_FakeSyncModule):
    RUNTIME_STATE_REMOTE_CONFLICT = "remote_conflict"

    class SyncOperationError(RuntimeError):
        def __init__(self, code: str, message: str):
            super().__init__(message)
            self.code = code

    def _set_runtime_state(self, code: str, message: str) -> None:
        self.runtime_state = (code, message)


def _config():
    return SimpleNamespace(
        repo="owner/repo",
        branch="main",
        path="baluffo/source-sync.json",
        timeout_s=20,
    )


def _snapshot() -> dict:
    return {
        "schemaVersion": 2,
        "generatedAt": "2026-05-12T10:00:00+00:00",
        "source": {"name": "admin_bridge"},
        "active": [
            {
                "id": "static:listing_url:https://studio.example/jobs",
                "adapter": "static",
                "listing_url": "https://studio.example/jobs",
                "name": "Studio",
            }
        ],
        "pending": [],
    }


def _encoded_bytes(payload: bytes) -> str:
    return base64.b64encode(payload).decode("ascii")


def test_push_sharded_snapshot_pushes_shards_before_manifest() -> None:
    bundle = build_sharded_snapshot_bundle(_snapshot(), max_shard_size=10_000)
    shard = bundle["changedShards"][0]
    module = _FakeSyncModule(
        [
            (200, {"content": {"sha": "shard-sha"}}),
            (200, {"content": _encoded_bytes(shard.payload_bytes)}),
            (200, {"content": {"sha": "manifest-sha"}}),
        ]
    )

    result = push_sharded_snapshot(
        module,
        _config(),
        _snapshot(),
        max_shard_size=10_000,
        opener=object(),
    )

    assert result["pushed"] is True
    assert result["remoteSha"] == "manifest-sha"
    assert result["metrics"]["changedShardCount"] == 1
    assert len(module.calls) == 3
    assert "/shards/" in module.calls[0]["url"]
    assert module.calls[0]["method"] == "PUT"
    assert module.calls[1]["method"] == "GET"
    assert module.calls[2]["url"].endswith("baluffo/source-sync/manifest.json")


def test_push_sharded_snapshot_updates_manifest_with_committed_sha() -> None:
    snapshot = _snapshot()
    committed = build_sharded_snapshot_bundle(
        {**snapshot, "active": []},
        max_shard_size=10_000,
    )
    bundle = build_sharded_snapshot_bundle(
        snapshot,
        max_shard_size=10_000,
        committed_manifest=committed["manifest"],
    )
    shard = bundle["changedShards"][0]
    module = _FakeSyncModule(
        [
            (200, {"content": {"sha": "shard-sha"}}),
            (200, {"content": _encoded_bytes(shard.payload_bytes)}),
            (200, {"content": {"sha": "manifest-sha"}}),
        ]
    )

    result = push_sharded_snapshot(
        module,
        _config(),
        snapshot,
        max_shard_size=10_000,
        committed_manifest=committed["manifest"],
        committed_manifest_sha="old-manifest-sha",
        opener=object(),
    )

    assert result["remoteSha"] == "manifest-sha"
    manifest_call = module.calls[-1]
    assert manifest_call["url"].endswith("baluffo/source-sync/manifest.json")
    assert manifest_call["payload"]["sha"] == "old-manifest-sha"


def test_push_manifest_maps_conflict_to_sync_operation_error() -> None:
    module = _ConflictSyncModule([(409, {"message": "manifest moved"})])

    with pytest.raises(module.SyncOperationError) as ctx:
        push_manifest(
            module,
            _config(),
            build_sharded_snapshot_bundle(_snapshot(), max_shard_size=10_000)["manifest"],
            sha="oldsha",
            opener=object(),
        )

    assert ctx.value.code == "remote_conflict"
    assert module.runtime_state == ("remote_conflict", "manifest moved")


def test_push_sharded_snapshot_noops_when_committed_manifest_matches() -> None:
    snapshot = _snapshot()
    bundle = build_sharded_snapshot_bundle(snapshot, max_shard_size=10_000)
    module = _FakeSyncModule([])

    result = push_sharded_snapshot(
        module,
        _config(),
        snapshot,
        max_shard_size=10_000,
        committed_manifest=bundle["manifest"],
        opener=object(),
    )

    assert result["pushed"] is False
    assert result["skipped"] is True
    assert result["skipReason"] == "no_changed_shards"
    assert result["metrics"]["changedShardCount"] == 0
    assert module.calls == []
