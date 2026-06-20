import threading
from types import SimpleNamespace

from src.source_sync_shard import build_sharded_snapshot_bundle, push_sharded_snapshot


class _ConcurrentSyncModule:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self._lock = threading.Lock()
        self._content_by_url: dict[str, str] = {}
        self.max_in_flight = 0
        self._in_flight = 0

    def _github_api_base(self) -> str:
        return "https://api.github.test"

    def validate_sync_config(self, config) -> None:  # noqa: ANN001
        assert config.repo == "owner/repo"

    def _request_json(self, **kwargs) -> tuple[int, dict, dict]:
        with self._lock:
            self._in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self._in_flight)
            self._in_flight -= 1
        with self._lock:
            self.calls.append(dict(kwargs))
        method = str(kwargs.get("method") or "").upper()
        url = str(kwargs.get("url") or "")
        content_key = url.split("?", 1)[0]
        if method == "PUT" and "/manifest.json" not in url:
            payload = kwargs.get("payload") or {}
            self._content_by_url[content_key] = str(payload.get("content") or "")
            return 200, {"content": {"sha": f"sha-{len(self.calls)}"}}, {}
        if method == "GET" and "/shards/" in url:
            return 200, {"content": self._content_by_url.get(content_key, "")}, {}
        if method == "PUT" and url.endswith("baluffo/source-sync/manifest.json"):
            return 200, {"content": {"sha": "manifest-sha"}}, {}
        if method == "GET":
            return 404, {"message": "Not Found"}, {}
        raise AssertionError(f"unexpected request: {method} {url}")


def _config():
    return SimpleNamespace(
        repo="owner/repo",
        branch="main",
        path="baluffo/source-sync.json",
        timeout_s=20,
    )


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


def test_push_sharded_snapshot_serializes_branch_mutating_shard_writes() -> None:
    snapshot = _snapshot_with_many_sources(32)
    bundle = build_sharded_snapshot_bundle(snapshot, max_shard_size=1_000)
    module = _ConcurrentSyncModule()

    result = push_sharded_snapshot(
        module,
        _config(),
        snapshot,
        max_shard_size=1_000,
        bundle=bundle,
        opener=object(),
    )

    changed_count = int(result["metrics"]["changedShardCount"])
    assert result["pushed"] is True
    assert changed_count > 4
    assert module.max_in_flight == 1
    assert result["shardResult"]["workerCount"] == 1
    assert result["shardResult"]["parallelWallMs"] >= 0
    assert result["remoteTiming"]["methodCounts"] == {
        "PUT": changed_count + 1,
        "GET": changed_count + 1,
    }
    assert (
        result["remoteTiming"]["stageWallMs"]["pushChangedShards"]
        == result["shardResult"]["parallelWallMs"]
    )
    assert result["remoteTiming"]["stageWallMs"]["pushManifest"] >= 0
    assert result["remoteTiming"]["stageWallMs"]["pruneShards"] >= 0


def test_push_sharded_snapshot_commits_manifest_after_shard_verification() -> None:
    snapshot = _snapshot_with_many_sources(16)
    bundle = build_sharded_snapshot_bundle(snapshot, max_shard_size=1_000)
    module = _ConcurrentSyncModule()

    result = push_sharded_snapshot(
        module,
        _config(),
        snapshot,
        max_shard_size=1_000,
        bundle=bundle,
        opener=object(),
    )

    manifest_index = next(
        index
        for index, call in enumerate(module.calls)
        if call["method"] == "PUT" and call["url"].endswith("baluffo/source-sync/manifest.json")
    )
    shard_verify_indices = [
        index
        for index, call in enumerate(module.calls)
        if call["method"] == "GET" and "/shards/" in call["url"]
    ]
    assert result["pushed"] is True
    assert shard_verify_indices
    assert max(shard_verify_indices) < manifest_index
