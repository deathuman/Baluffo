import threading
from typing import Any

import pytest

from src.source_sync_shard import (
    SourceSyncShardError,
    build_manifest,
    build_shards,
    read_shard,
    read_sharded_snapshot,
)
from tests.test_source_sync_shard_io import (
    _config,
    _encoded_bytes,
    _encoded_json,
    _FakeSyncModule,
    _payload,
    _row,
)


class _MappedFakeSyncModule(_FakeSyncModule):
    def __init__(self, responses_by_suffix: dict[str, Any]):
        super().__init__([])
        self.responses_by_suffix = dict(responses_by_suffix)
        self._lock = threading.Lock()

    def _request_json(self, **kwargs) -> tuple[int, dict, dict]:
        with self._lock:
            self.calls.append(dict(kwargs))
        url = str(kwargs.get("url") or "")
        for suffix, response in self.responses_by_suffix.items():
            if url.endswith(suffix):
                if len(response) == 2:
                    status, payload = response
                    return status, payload, {}
                status, payload, headers = response
                return status, payload, headers
        raise AssertionError(f"No fake response for {url}")


def test_read_shard_validates_payload_and_returns_rows() -> None:
    shard = build_shards(
        [_row(1)],
        max_size=10_000,
        base_path="baluffo/source-sync/shards/stable",
    )[0]
    module = _FakeSyncModule([(200, {"content": _encoded_bytes(shard.payload_bytes)})])

    result = read_shard(module, _config(), shard.manifest_entry(), opener=lambda *_a, **_kw: None)

    assert result["entry"] == shard.manifest_entry()
    assert result["rows"] == _payload(shard)["rows"]
    assert module.calls[0]["method"] == "GET"
    assert module.calls[0]["url"] == (
        f"https://api.github.test/repos/owner/repo/contents/{shard.path}?ref=main"
    )


def test_read_shard_rejects_payload_hash_mismatch() -> None:
    shard = build_shards([_row(1)], max_size=10_000)[0]
    payload = bytearray(shard.payload_bytes)
    payload[-1] = (payload[-1] + 1) % 255
    module = _FakeSyncModule([(200, {"content": _encoded_bytes(bytes(payload))})])

    with pytest.raises(SourceSyncShardError, match="sha256 mismatch"):
        read_shard(module, _config(), shard.manifest_entry(), opener=lambda *_a, **_kw: None)


def test_read_sharded_snapshot_returns_none_when_manifest_absent_for_v2_fallback() -> None:
    module = _FakeSyncModule([(404, {"message": "Not Found"})])

    assert read_sharded_snapshot(module, _config(), opener=lambda *_a, **_kw: None) is None
    assert len(module.calls) == 1
    assert module.calls[0]["url"].endswith("baluffo/source-sync/manifest.json?ref=main")


def test_read_sharded_snapshot_skips_when_manifest_sha_is_unchanged() -> None:
    shards = build_shards(
        [_row(1), _row(2)],
        max_size=10_000,
        bucket="active",
        base_path="baluffo/source-sync/shards/stable",
    )
    manifest = build_manifest(
        shards,
        generated_at="2026-05-12T10:00:00+00:00",
        source_label="admin_bridge",
    )
    module = _FakeSyncModule([(200, {"sha": "manifestsha", "content": _encoded_json(manifest)})])
    progress: list[dict] = []

    snapshot = read_sharded_snapshot(
        module,
        _config(),
        opener=lambda *_a, **_kw: None,
        known_manifest_sha="manifestsha",
        progress_callback=lambda **payload: progress.append(payload),
    )

    assert snapshot is not None
    assert snapshot["skipped"] is True
    assert snapshot["skipReason"] == "remote_manifest_unchanged"
    assert snapshot["manifestSha"] == "manifestsha"
    assert len(module.calls) == 1
    assert progress[-1]["counts"]["skipped"] is True


def test_read_sharded_snapshot_reads_shards_in_parallel_and_reports_progress() -> None:
    active = build_shards(
        [_row(index) for index in range(12)],
        max_size=10_000,
        bucket="active",
        base_path="baluffo/source-sync/shards/stable",
    )
    manifest = build_manifest(
        active,
        generated_at="2026-05-12T10:00:00+00:00",
        source_label="admin_bridge",
    )
    shards_by_path = {shard.path: shard for shard in active}
    module = _MappedFakeSyncModule(
        {
            "baluffo/source-sync/manifest.json?ref=main": (
                200,
                {"sha": "manifestsha", "content": _encoded_json(manifest)},
            ),
            **{
                f"{entry['path']}?ref=main": (
                    200,
                    {"content": _encoded_bytes(shards_by_path[entry["path"]].payload_bytes)},
                )
                for entry in manifest["shards"]
            },
        }
    )
    progress: list[dict] = []

    snapshot = read_sharded_snapshot(
        module,
        _config(),
        opener=lambda *_a, **_kw: None,
        progress_callback=lambda **payload: progress.append(payload),
        max_workers=2,
    )

    assert snapshot is not None
    assert snapshot["active"] == [row for shard in active for row in _payload(shard)["rows"]]
    assert snapshot["shardCount"] == len(manifest["shards"])
    assert snapshot["shardsReadBytes"] == sum(shard.size_bytes for shard in active)
    assert progress[-1]["counts"]["completedShardCount"] == len(manifest["shards"])
    assert progress[-1]["counts"]["action"] == "pull"


def test_read_sharded_snapshot_fails_without_partial_snapshot_on_shard_error() -> None:
    active = build_shards(
        [_row(index) for index in range(6)],
        max_size=10_000,
        bucket="active",
        base_path="baluffo/source-sync/shards/stable",
    )
    manifest = build_manifest(
        active,
        generated_at="2026-05-12T10:00:00+00:00",
        source_label="admin_bridge",
    )
    responses = {
        "baluffo/source-sync/manifest.json?ref=main": (
            200,
            {"sha": "manifestsha", "content": _encoded_json(manifest)},
        )
    }
    shards_by_path = {shard.path: shard for shard in active}
    for index, entry in enumerate(manifest["shards"]):
        responses[f"{entry['path']}?ref=main"] = (
            (500, {"message": "boom"})
            if index == 0
            else (
                200,
                {"content": _encoded_bytes(shards_by_path[entry["path"]].payload_bytes)},
            )
        )
    module = _MappedFakeSyncModule(responses)

    with pytest.raises(RuntimeError, match="boom"):
        read_sharded_snapshot(module, _config(), opener=lambda *_a, **_kw: None, max_workers=2)
