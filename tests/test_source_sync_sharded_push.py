import json
from typing import Any

import pytest

from src.source_sync_shard import (
    SourceSyncShardError,
    build_sharded_snapshot_bundle,
    git_blob_sha1,
    manifest_payload_bytes,
    push_manifest,
    push_sharded_snapshot,
)
from tests.helpers.git_data_sync_module import GitDataSyncModule
from tests.helpers.report_state import simple_config

_config = simple_config


class _FakeSyncModule:
    """FIFO response module for the legacy Contents `push_manifest` path."""

    def __init__(self, responses: list[tuple[int, Any]]):
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


def _snapshot_with_many_sources(count: int) -> dict:
    return {
        "schemaVersion": 2,
        "generatedAt": "2026-05-12T10:00:00+00:00",
        "source": {"name": "admin_bridge"},
        "active": [
            {
                "id": f"static:listing_url:https://studio-{idx}.example/jobs",
                "adapter": "static",
                "listing_url": f"https://studio-{idx}.example/jobs",
                "name": f"Studio {idx}",
            }
            for idx in range(count)
        ],
        "pending": [],
    }


def test_push_sharded_snapshot_commits_shards_and_manifest_in_one_commit() -> None:
    bundle = build_sharded_snapshot_bundle(_snapshot(), max_shard_size=10_000)
    shard = bundle["changedShards"][0]
    module = GitDataSyncModule()

    result = push_sharded_snapshot(
        module,
        _config(),
        _snapshot(),
        max_shard_size=10_000,
        opener=lambda *_a, **_kw: None,
    )

    assert result["pushed"] is True
    assert len(module.commits) == 1
    commit = module.commits[0]
    assert result["commitSha"] == commit["sha"]
    assert commit["parents"] == ["parent-commit"]
    assert module.head_commit == commit["sha"]
    tree = module.branch_tree()
    assert tree[shard.path] == git_blob_sha1(shard.payload_bytes)
    assert tree[module.manifest_path] == git_blob_sha1(manifest_payload_bytes(bundle["manifest"]))
    assert result["remoteSha"] == tree[module.manifest_path]
    assert result["metrics"]["changedShardCount"] == 1
    # One atomic commit: no manifest PUT, exactly one ref update.
    assert [call["method"] for call in module.calls].count("PATCH") == 1
    assert not [call for call in module.calls if call["method"] == "PUT"]
    assert result["shardResult"]["verifiedShards"] == [
        {"path": shard.path, "sha256": shard.sha256, "rowCount": shard.row_count}
    ]
    assert result["remoteTiming"]["methodCounts"] == {"GET": 3, "POST": 4, "PATCH": 1}


def test_push_sharded_snapshot_request_budget_is_one_commit_plus_blobs() -> None:
    snapshot = _snapshot_with_many_sources(32)
    bundle = build_sharded_snapshot_bundle(snapshot, max_shard_size=1_000)
    module = GitDataSyncModule()

    result = push_sharded_snapshot(
        module,
        _config(),
        snapshot,
        max_shard_size=1_000,
        bundle=bundle,
        opener=lambda *_a, **_kw: None,
    )

    changed_count = int(result["metrics"]["changedShardCount"])
    assert changed_count >= 16
    # The serial Contents protocol cost 2N + 2 requests. The atomic commit
    # costs one blob per changed file plus a fixed seven-call generation.
    # (GC's own listing and manifest-history reads are summarized by its single
    # pruneShards timing row, not counted individually here.)
    assert result["remoteTiming"]["requestCount"] == changed_count + 7
    assert "PUT" not in result["remoteTiming"]["methodCounts"]
    assert len(module.commits) == 1


def test_push_sharded_snapshot_emits_remote_write_progress() -> None:
    module = GitDataSyncModule()
    progress: list[dict] = []

    result = push_sharded_snapshot(
        module,
        _config(),
        _snapshot(),
        max_shard_size=10_000,
        progress_callback=lambda **kwargs: progress.append(kwargs),
        opener=lambda *_a, **_kw: None,
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
    module = GitDataSyncModule()

    def fail_progress(**_kwargs) -> None:
        raise RuntimeError("progress sink unavailable")

    result = push_sharded_snapshot(
        module,
        _config(),
        _snapshot(),
        max_shard_size=10_000,
        progress_callback=fail_progress,
        opener=lambda *_a, **_kw: None,
    )

    assert result["pushed"] is True
    assert len(module.commits) == 1


def test_push_sharded_snapshot_rejects_stale_committed_manifest_before_writing() -> None:
    module = GitDataSyncModule()

    with pytest.raises(module.SyncOperationError) as ctx:
        push_sharded_snapshot(
            module,
            _config(),
            _snapshot(),
            max_shard_size=10_000,
            committed_manifest_sha="manifest-sha-from-preflight-read",
            opener=lambda *_a, **_kw: None,
        )

    assert ctx.value.code == "remote_conflict"
    assert module.runtime_state is not None
    assert module.runtime_state[0] == "remote_conflict"
    assert module.commits == []
    assert module.head_commit == "parent-commit"
    assert not [call for call in module.calls if call["method"] in {"POST", "PATCH"}]


def test_push_sharded_snapshot_maps_ref_conflict_without_moving_branch() -> None:
    module = GitDataSyncModule()
    module.conflict_on_ref_update = True

    with pytest.raises(module.SyncOperationError) as ctx:
        push_sharded_snapshot(
            module,
            _config(),
            _snapshot(),
            max_shard_size=10_000,
            opener=lambda *_a, **_kw: None,
        )

    assert ctx.value.code == "remote_conflict"
    assert module.runtime_state is not None
    assert module.runtime_state[0] == "remote_conflict"
    assert module.head_commit == "parent-commit"
    assert module.manifest_path not in module.branch_tree()


def test_push_sharded_snapshot_rejects_remote_blob_sha_mismatch_without_commit() -> None:
    module = GitDataSyncModule()
    module.forced_blob_sha = "0" * 40

    with pytest.raises(RuntimeError, match="stored source-sync blob with sha"):
        push_sharded_snapshot(
            module,
            _config(),
            _snapshot(),
            max_shard_size=10_000,
            opener=lambda *_a, **_kw: None,
        )

    assert module.commits == []
    assert module.head_commit == "parent-commit"
    assert module.manifest_path not in module.branch_tree()


def test_push_sharded_snapshot_noops_when_committed_manifest_matches() -> None:
    snapshot = _snapshot()
    bundle = build_sharded_snapshot_bundle(snapshot, max_shard_size=10_000)
    module = GitDataSyncModule()
    progress: list[dict] = []

    result = push_sharded_snapshot(
        module,
        _config(),
        snapshot,
        max_shard_size=10_000,
        committed_manifest=bundle["manifest"],
        progress_callback=lambda **kwargs: progress.append(kwargs),
        opener=lambda *_a, **_kw: None,
    )

    assert result["pushed"] is False
    assert result["skipped"] is True
    assert result["skipReason"] == "no_changed_shards"
    assert result["metrics"]["changedShardCount"] == 0
    assert module.calls == []
    assert progress == []


def test_push_sharded_snapshot_prunes_unreferenced_shards_after_commit() -> None:
    bundle = build_sharded_snapshot_bundle(_snapshot(), max_shard_size=10_000)
    shard = bundle["changedShards"][0]
    old_path = "baluffo/source-sync/shards/active/old/oldsha.json.gz"
    module = GitDataSyncModule()
    module.contents_listing = [
        {"type": "file", "path": old_path, "sha": "old-remote-sha"},
        {"type": "file", "path": shard.path, "sha": "current-remote-sha"},
    ]

    result = push_sharded_snapshot(
        module,
        _config(),
        _snapshot(),
        max_shard_size=10_000,
        opener=lambda *_a, **_kw: None,
    )

    assert result["pushed"] is True
    assert result["gc"]["deletedCount"] == 1
    assert result["gc"]["deletedPaths"] == [old_path]
    assert result["warnings"] == []
    assert module.deleted_paths == [old_path]
    assert module.head_commit == module.commits[0]["sha"]


def test_push_sharded_snapshot_ignores_remote_gc_entries_without_paths() -> None:
    bundle = build_sharded_snapshot_bundle(_snapshot(), max_shard_size=10_000)
    shard = bundle["changedShards"][0]
    module = GitDataSyncModule()
    module.contents_listing = [
        {"type": "file", "path": "", "sha": "missing-path-sha"},
        {"type": "file", "sha": "missing-path-key-sha"},
        {"type": "file", "path": shard.path, "sha": "current-remote-sha"},
    ]

    result = push_sharded_snapshot(
        module,
        _config(),
        _snapshot(),
        max_shard_size=10_000,
        opener=lambda *_a, **_kw: None,
    )

    assert result["pushed"] is True
    assert result["gc"]["deletedCount"] == 0
    assert result["warnings"] == []
    assert module.deleted_paths == []


def test_push_sharded_snapshot_reports_gc_failure_without_rollback() -> None:
    old_path = "baluffo/source-sync/shards/active/old/oldsha.json.gz"
    module = GitDataSyncModule()
    module.contents_listing = [{"type": "file", "path": old_path, "sha": "old-remote-sha"}]
    module.delete_status = 500

    result = push_sharded_snapshot(
        module,
        _config(),
        _snapshot(),
        max_shard_size=10_000,
        opener=lambda *_a, **_kw: None,
    )

    assert result["pushed"] is True
    assert result["gc"]["deletedCount"] == 0
    assert result["warnings"] == ["delete denied"]
    assert module.head_commit == module.commits[0]["sha"]


def test_push_manifest_maps_conflict_to_sync_operation_error() -> None:
    module = _ConflictSyncModule([(409, {"message": "manifest moved"})])

    with pytest.raises(module.SyncOperationError) as ctx:
        push_manifest(
            module,
            _config(),
            build_sharded_snapshot_bundle(_snapshot(), max_shard_size=10_000)["manifest"],
            sha="oldsha",
            opener=lambda *_a, **_kw: None,
        )

    assert ctx.value.code == "remote_conflict"
    assert module.runtime_state == ("remote_conflict", "manifest moved")


def test_manifest_payload_bytes_matches_the_contents_wire_format() -> None:
    bundle = build_sharded_snapshot_bundle(_snapshot(), max_shard_size=10_000)

    encoded = manifest_payload_bytes(bundle["manifest"])

    assert json.loads(encoded.decode("utf-8")) == bundle["manifest"]
    assert encoded == manifest_payload_bytes(bundle["manifest"])
    with pytest.raises(SourceSyncShardError, match="uncommitted"):
        manifest_payload_bytes({**bundle["manifest"], "phase": "proposed"})
