import base64
import json

from src import source_sync as sync
from src.source_sync_shard import build_sharded_snapshot_bundle
from tests.source_sync_helpers import source_sync_test_root  # noqa: F401


class _FakeResponse:
    def __init__(self, status: int, payload: dict):
        self._status = int(status)
        self._payload = dict(payload)
        self.headers: dict[str, str] = {}

    def getcode(self):
        return self._status

    def read(self):
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _Recorder:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, req, timeout=20):  # noqa: ANN001
        self.calls.append({"url": req.full_url, "timeout": timeout})
        if not self.responses:
            raise AssertionError("No fake responses left")
        return self.responses.pop(0)


def test_pull_and_merge_sources_merges_distinct_sources_by_identity_after_v2_fallback(
    source_sync_test_root,
):
    source_sync_test_root.write_packaged_config()
    remote_snapshot = {
        "schemaVersion": 1,
        "generatedAt": "2026-03-09T11:00:00+00:00",
        "active": [{"adapter": "static", "listing_url": "https://b.com/jobs", "studio": "Remote"}],
        "pending": [{"adapter": "teamtailor", "listing_url": "https://c.com/jobs"}],
        "rejected": [],
    }
    opener = _Recorder(
        [
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            _FakeResponse(404, {"message": "Not Found"}),
            _FakeResponse(
                200,
                {
                    "sha": "s1",
                    "content": base64.b64encode(json.dumps(remote_snapshot).encode()).decode(
                        "ascii"
                    ),
                },
            ),
        ]
    )
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"
        result = sync.pull_and_merge_sources(
            cfg,
            {"active": [{"adapter": "static", "listing_url": "https://a.com/jobs"}]},
            opener=opener,
            known_remote_sha="old-manifest-sha",
            max_shard_read_workers=1,
        )
    finally:
        sync.build_app_jwt = original_build_jwt

    assert result["changed"]
    assert len(result["mergedState"]["active"]) == 2
    assert any(row.get("studio") == "Remote" for row in result["mergedState"]["active"])
    assert any(
        row.get("listing_url") == "https://a.com/jobs" for row in result["mergedState"]["active"]
    )
    assert len(result["mergedState"]["pending"]) == 1
    assert opener.calls[1]["url"].endswith("baluffo/source-sync/manifest.json?ref=main")
    assert opener.calls[2]["url"].endswith("baluffo/source-sync.json?ref=main")


def test_pull_and_merge_sources_prefers_committed_v3_shards(source_sync_test_root):
    source_sync_test_root.write_packaged_config()
    remote_snapshot = {
        "schemaVersion": 2,
        "generatedAt": "2026-05-12T10:00:00+00:00",
        "source": {"name": "admin_bridge"},
        "active": [{"adapter": "static", "listing_url": "https://b.com/jobs", "studio": "Remote"}],
        "pending": [{"adapter": "teamtailor", "listing_url": "https://c.com/jobs"}],
    }
    bundle = build_sharded_snapshot_bundle(
        remote_snapshot,
        max_shard_size=10_000,
        base_path="baluffo/source-sync/shards",
    )
    manifest = bundle["manifest"]
    shards_by_path = {shard.path: shard for shard in bundle["shards"]}
    opener = _Recorder(
        [
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            _FakeResponse(
                200,
                {
                    "sha": "manifest-sha",
                    "content": base64.b64encode(json.dumps(manifest).encode()).decode("ascii"),
                },
            ),
            *[
                _FakeResponse(
                    200,
                    {
                        "content": base64.b64encode(
                            shards_by_path[entry["path"]].payload_bytes
                        ).decode("ascii")
                    },
                )
                for entry in manifest["shards"]
            ],
        ]
    )
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"
        result = sync.pull_and_merge_sources(
            cfg,
            {"active": [{"adapter": "static", "listing_url": "https://a.com/jobs"}]},
            opener=opener,
            known_remote_sha="old-manifest-sha",
            max_shard_read_workers=1,
        )
    finally:
        sync.build_app_jwt = original_build_jwt

    assert result["remoteSha"] == "manifest-sha"
    assert result["remoteGeneratedAt"] == remote_snapshot["generatedAt"]
    assert len(result["mergedState"]["active"]) == 2
    assert len(result["mergedState"]["pending"]) == 1
    assert opener.calls[1]["url"].endswith("baluffo/source-sync/manifest.json?ref=main")
    assert all("source-sync.json" not in call["url"] for call in opener.calls[1:])


def test_pull_and_merge_sources_skips_shards_when_manifest_sha_is_unchanged(
    source_sync_test_root,
):
    source_sync_test_root.write_packaged_config()
    remote_snapshot = {
        "schemaVersion": 2,
        "generatedAt": "2026-05-12T10:00:00+00:00",
        "source": {"name": "admin_bridge"},
        "active": [{"adapter": "static", "listing_url": "https://b.com/jobs"}],
        "pending": [],
    }
    bundle = build_sharded_snapshot_bundle(
        remote_snapshot,
        max_shard_size=10_000,
        base_path="baluffo/source-sync/shards",
    )
    manifest = bundle["manifest"]
    opener = _Recorder(
        [
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            _FakeResponse(
                200,
                {
                    "sha": "manifest-sha",
                    "content": base64.b64encode(json.dumps(manifest).encode()).decode("ascii"),
                },
            ),
        ]
    )
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"
        result = sync.pull_and_merge_sources(
            cfg,
            {"active": [{"adapter": "static", "listing_url": "https://a.com/jobs"}]},
            opener=opener,
            known_remote_sha="manifest-sha",
        )
    finally:
        sync.build_app_jwt = original_build_jwt

    assert result["remoteSha"] == "manifest-sha"
    assert result["remoteGeneratedAt"] == remote_snapshot["generatedAt"]
    assert result["changed"] is False
    assert result["skipped"] is True
    assert result["skipReason"] == "remote_manifest_unchanged"
    assert result["shardCount"] == len(manifest["shards"])
    assert result["shardsReadBytes"] == 0
    assert len(opener.calls) == 2
    assert opener.calls[1]["url"].endswith("baluffo/source-sync/manifest.json?ref=main")
    assert all("source-sync.json" not in call["url"] for call in opener.calls[1:])
