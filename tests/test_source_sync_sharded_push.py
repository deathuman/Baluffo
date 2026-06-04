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
            (404, {"message": "Not Found"}),
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
    assert len(module.calls) == 4
    assert "/shards/" in module.calls[0]["url"]
    assert module.calls[0]["method"] == "PUT"
    assert module.calls[1]["method"] == "GET"
    assert module.calls[2]["url"].endswith("baluffo/source-sync/manifest.json")
    assert module.calls[3]["method"] == "GET"


def test_push_sharded_snapshot_emits_remote_write_progress() -> None:
    bundle = build_sharded_snapshot_bundle(_snapshot(), max_shard_size=10_000)
    shard = bundle["changedShards"][0]
    module = _FakeSyncModule(
        [
            (200, {"content": {"sha": "shard-sha"}}),
            (200, {"content": _encoded_bytes(shard.payload_bytes)}),
            (200, {"content": {"sha": "manifest-sha"}}),
            (404, {"message": "Not Found"}),
        ]
    )
    progress: list[dict] = []

    result = push_sharded_snapshot(
        module,
        _config(),
        _snapshot(),
        max_shard_size=10_000,
        progress_callback=lambda **kwargs: progress.append(kwargs),
        opener=object(),
    )

    assert result["pushed"] is True
    assert [row["phase_label"] for row in progress] == [
        "Uploading shard 1 of 1",
        "Verified shard 1 of 1",
        "Committing sync manifest",
        "Pruning old sync shards",
        "Pruned old sync shards",
    ]
    assert progress[0]["mode"] == "determinate"
    assert progress[0]["ratio"] == 0.0
    assert progress[0]["counts"]["changedShardCount"] == 1
    assert progress[0]["counts"]["completedShardCount"] == 0
    assert progress[1]["ratio"] == 1.0
    assert progress[1]["counts"]["verifiedShardCount"] == 1
    assert progress[-1]["counts"]["manifestCommitted"] is True
    assert progress[-1]["counts"]["gcDeletedCount"] == 0


def test_push_sharded_snapshot_ignores_progress_callback_failure() -> None:
    bundle = build_sharded_snapshot_bundle(_snapshot(), max_shard_size=10_000)
    shard = bundle["changedShards"][0]
    module = _FakeSyncModule(
        [
            (200, {"content": {"sha": "shard-sha"}}),
            (200, {"content": _encoded_bytes(shard.payload_bytes)}),
            (200, {"content": {"sha": "manifest-sha"}}),
            (404, {"message": "Not Found"}),
        ]
    )

    def fail_progress(**_kwargs) -> None:
        raise RuntimeError("progress sink unavailable")

    result = push_sharded_snapshot(
        module,
        _config(),
        _snapshot(),
        max_shard_size=10_000,
        progress_callback=fail_progress,
        opener=object(),
    )

    assert result["pushed"] is True
    assert result["remoteSha"] == "manifest-sha"


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
            (404, {"message": "Not Found"}),
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
    manifest_call = module.calls[-2]
    assert manifest_call["url"].endswith("baluffo/source-sync/manifest.json")
    assert manifest_call["payload"]["sha"] == "old-manifest-sha"


def test_push_sharded_snapshot_prunes_unreferenced_shards_after_manifest() -> None:
    bundle = build_sharded_snapshot_bundle(_snapshot(), max_shard_size=10_000)
    shard = bundle["changedShards"][0]
    old_path = "baluffo/source-sync/shards/active/old/oldsha.json.gz"
    module = _FakeSyncModule(
        [
            (200, {"content": {"sha": "shard-sha"}}),
            (200, {"content": _encoded_bytes(shard.payload_bytes)}),
            (200, {"content": {"sha": "manifest-sha"}}),
            (
                200,
                [
                    {"type": "file", "path": old_path, "sha": "old-remote-sha"},
                    {"type": "file", "path": shard.path, "sha": "current-remote-sha"},
                ],
            ),
            (200, {"content": None}),
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
    assert result["gc"]["deletedCount"] == 1
    assert result["gc"]["deletedPaths"] == [old_path]
    assert result["warnings"] == []
    assert [call["method"] for call in module.calls] == ["PUT", "GET", "PUT", "GET", "DELETE"]
    delete_call = module.calls[-1]
    assert delete_call["url"].endswith(old_path)
    assert delete_call["payload"]["sha"] == "old-remote-sha"
    assert delete_call["payload"]["branch"] == "main"


def test_push_sharded_snapshot_ignores_remote_gc_entries_without_paths() -> None:
    bundle = build_sharded_snapshot_bundle(_snapshot(), max_shard_size=10_000)
    shard = bundle["changedShards"][0]
    module = _FakeSyncModule(
        [
            (200, {"content": {"sha": "shard-sha"}}),
            (200, {"content": _encoded_bytes(shard.payload_bytes)}),
            (200, {"content": {"sha": "manifest-sha"}}),
            (
                200,
                [
                    {"type": "file", "path": "", "sha": "missing-path-sha"},
                    {"type": "file", "sha": "missing-path-key-sha"},
                    {"type": "file", "path": shard.path, "sha": "current-remote-sha"},
                ],
            ),
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
    assert result["gc"]["deletedCount"] == 0
    assert result["warnings"] == []
    assert [call["method"] for call in module.calls] == ["PUT", "GET", "PUT", "GET"]


def test_push_sharded_snapshot_reports_gc_failure_without_rollback() -> None:
    bundle = build_sharded_snapshot_bundle(_snapshot(), max_shard_size=10_000)
    shard = bundle["changedShards"][0]
    old_path = "baluffo/source-sync/shards/active/old/oldsha.json.gz"
    module = _FakeSyncModule(
        [
            (200, {"content": {"sha": "shard-sha"}}),
            (200, {"content": _encoded_bytes(shard.payload_bytes)}),
            (200, {"content": {"sha": "manifest-sha"}}),
            (200, [{"type": "file", "path": old_path, "sha": "old-remote-sha"}]),
            (500, {"message": "delete denied"}),
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
    assert result["gc"]["deletedCount"] == 0
    assert result["warnings"] == ["delete denied"]


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
    progress: list[dict] = []

    result = push_sharded_snapshot(
        module,
        _config(),
        snapshot,
        max_shard_size=10_000,
        committed_manifest=bundle["manifest"],
        progress_callback=lambda **kwargs: progress.append(kwargs),
        opener=object(),
    )

    assert result["pushed"] is False
    assert result["skipped"] is True
    assert result["skipReason"] == "no_changed_shards"
    assert result["metrics"]["changedShardCount"] == 0
    assert module.calls == []
    assert progress == []
