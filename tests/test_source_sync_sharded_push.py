from types import SimpleNamespace

from src.source_sync_shard import build_sharded_snapshot_bundle, push_sharded_snapshot


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


def test_push_sharded_snapshot_pushes_shards_before_manifest() -> None:
    module = _FakeSyncModule(
        [
            (200, {"content": {"sha": "shard-sha"}}),
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
    assert len(module.calls) == 2
    assert "/shards/" in module.calls[0]["url"]
    assert module.calls[0]["method"] == "PUT"
    assert module.calls[1]["url"].endswith("baluffo/source-sync/manifest.json")


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
