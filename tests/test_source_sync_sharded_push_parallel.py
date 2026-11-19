from src.source_sync_shard import (
    build_sharded_snapshot_bundle,
    git_blob_sha1,
    push_sharded_snapshot,
)
from tests.helpers.git_data_sync_module import GitDataSyncModule
from tests.helpers.report_state import simple_config

_config = simple_config


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


def test_push_sharded_snapshot_creates_blobs_serially_and_commits_once() -> None:
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
    assert result["pushed"] is True
    assert changed_count > 4
    # Blob creation never advances the branch, but the push still writes one
    # blob at a time so the single-writer contract is unchanged.
    assert module.max_in_flight == 1
    assert result["shardResult"]["workerCount"] == 1
    assert result["shardResult"]["parallelWallMs"] >= 0
    assert len(module.commits) == 1
    assert result["remoteTiming"]["methodCounts"] == {
        "GET": 3,
        "POST": changed_count + 3,
        "PATCH": 1,
    }
    assert (
        result["remoteTiming"]["stageWallMs"]["pushChangedShards"]
        == result["shardResult"]["parallelWallMs"]
    )
    assert result["remoteTiming"]["stageWallMs"]["pushManifest"] >= 0
    assert result["remoteTiming"]["stageWallMs"]["pruneShards"] >= 0


def test_push_sharded_snapshot_exposes_shards_and_manifest_in_the_same_tree() -> None:
    snapshot = _snapshot_with_many_sources(16)
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

    assert result["pushed"] is True
    tree = module.branch_tree()
    assert module.manifest_path in tree
    for shard in bundle["changedShards"]:
        assert tree[shard.path] == git_blob_sha1(shard.payload_bytes)
    assert module.head_commit == module.commits[0]["sha"]
