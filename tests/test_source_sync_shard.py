import base64
import gzip
import hashlib
import json
from types import SimpleNamespace

import pytest

from scripts import build_ship_bundle
from src.source_sync_shard import (
    SourceSyncShardError,
    build_manifest,
    build_shards,
    changed_shards,
    manifest_path,
    push_changed_shards,
    push_manifest,
    push_shard,
    read_manifest,
    shard_key,
    trusted_committed_manifest,
)


class _FakeSyncModule:
    def __init__(self, responses: list[tuple[int, dict, dict] | tuple[int, dict]]):
        self.responses = list(responses)
        self.calls: list[dict] = []
        self.validate_count = 0

    def _github_api_base(self) -> str:
        return "https://api.github.test"

    def validate_sync_config(self, config) -> None:  # noqa: ANN001
        self.validate_count += 1
        assert config.repo == "owner/repo"

    def _request_json(self, **kwargs) -> tuple[int, dict, dict]:
        self.calls.append(dict(kwargs))
        if not self.responses:
            raise AssertionError("No fake responses left")
        response = self.responses.pop(0)
        if len(response) == 2:
            status, payload = response
            return status, payload, {}
        status, payload, headers = response
        return status, payload, headers


def _row(index: int, *, extra_chunks: int = 8) -> dict[str, str]:
    return {
        "id": f"static:listing_url:https://studio-{index:05d}.example/jobs",
        "adapter": "static",
        "name": f"Studio {index:05d}",
        "listing_url": f"https://studio-{index:05d}.example/jobs",
        "notes": "".join(
            hashlib.sha256(f"{index}:{chunk}".encode()).hexdigest() for chunk in range(extra_chunks)
        ),
    }


def _payload(shard) -> dict:
    return json.loads(gzip.decompress(shard.payload_bytes).decode("utf-8"))


def _config():
    return SimpleNamespace(
        repo="owner/repo",
        branch="main",
        path="baluffo/source-sync.json",
        timeout_s=20,
    )


def _encoded_json(payload: dict) -> str:
    return base64.b64encode(json.dumps(payload).encode()).decode("ascii")


def _rows_with_same_prefix(prefix: str, count: int) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    index = 0
    while len(rows) < count:
        row = _row(index, extra_chunks=10)
        if shard_key(row) == prefix:
            rows.append(row)
        index += 1
        assert index < 20000, f"could not find {count} rows for prefix {prefix}"
    return rows


def test_build_shards_is_deterministic_and_manifest_ready() -> None:
    rows = [_row(index) for index in range(20)]

    first = build_shards(rows, max_size=10_000, bucket="active", base_path="sync/shards/gen1")
    second = build_shards(
        list(reversed(rows)), max_size=10_000, bucket="active", base_path="sync/shards/gen1"
    )

    assert [shard.manifest_entry() for shard in first] == [
        shard.manifest_entry() for shard in second
    ]
    assert b"".join(shard.payload_bytes for shard in first) == b"".join(
        shard.payload_bytes for shard in second
    )
    assert sum(shard.row_count for shard in first) == len(rows)
    assert all(shard.path == f"sync/shards/gen1/active/{shard.key}.json.gz" for shard in first)
    assert all(len(shard.key) == 2 for shard in first)
    assert all(shard.sha256 == hashlib.sha256(shard.payload_bytes).hexdigest() for shard in first)


def test_shard_payload_contains_canonical_rows() -> None:
    rows = [_row(index) for index in range(3)]

    shards = build_shards(list(reversed(rows)), max_size=10_000, bucket="pending")

    assert len(shards) == 3
    payload = _payload(shards[0])
    assert payload["schemaVersion"] == 3
    assert payload["bucket"] == "pending"
    assert payload["key"] == shards[0].key
    assert payload["rows"] == sorted(payload["rows"], key=lambda row: row["id"])


def test_oversized_shard_splits_by_longer_hash_prefix() -> None:
    rows = _rows_with_same_prefix("00", 16)
    single_row_size = max(
        shard.size_bytes for row in rows for shard in build_shards([row], max_size=10_000)
    )
    max_size = single_row_size + 240

    shards = build_shards(rows, max_size=max_size, bucket="active")

    assert len(shards) > 1
    assert all(shard.key.startswith("00") for shard in shards)
    assert all(len(shard.key) >= 4 for shard in shards)
    assert all(shard.size_bytes <= max_size for shard in shards)
    assert sum(shard.row_count for shard in shards) == len(rows)


def test_oversized_single_row_raises_clear_error() -> None:
    row = _row(1, extra_chunks=80)

    with pytest.raises(SourceSyncShardError, match="cannot be split further"):
        build_shards([row], max_size=100, bucket="active")


def test_build_shards_rejects_unsafe_paths() -> None:
    with pytest.raises(ValueError, match="bucket"):
        build_shards([_row(1)], max_size=10_000, bucket="../active")

    with pytest.raises(ValueError, match="base_path"):
        build_shards([_row(1)], max_size=10_000, base_path="sync//")


def test_ship_bundle_includes_shard_module() -> None:
    assert "source_sync_shard.py" in build_ship_bundle.APP_RUNTIME_SCRIPTS


def test_manifest_path_derives_v3_manifest_from_v2_snapshot_path() -> None:
    assert manifest_path("baluffo/source-sync.json") == "baluffo/source-sync/manifest.json"

    with pytest.raises(ValueError, match="snapshot_path"):
        manifest_path("../source-sync.json")

    with pytest.raises(ValueError, match="snapshot path"):
        manifest_path("/baluffo/source-sync.json")


def test_build_manifest_summarizes_shards_without_uncommitted_phase() -> None:
    shards = build_shards(
        [_row(index) for index in range(5)],
        max_size=10_000,
        bucket="active",
        base_path="baluffo/source-sync/shards/gen1",
    )

    manifest = build_manifest(
        shards,
        generated_at="2026-05-12T10:00:00+00:00",
        shard_cap_bytes=10_000,
    )

    assert manifest["schemaVersion"] == 3
    assert "phase" not in manifest
    assert manifest["shardCount"] == len(shards)
    assert manifest["totalRowCount"] == 5
    assert manifest["totalSizeBytes"] == sum(shard.size_bytes for shard in shards)
    assert manifest["shardCapBytes"] == 10_000
    assert manifest["shards"] == sorted(
        [shard.manifest_entry() for shard in shards],
        key=lambda entry: (entry["bucket"], entry["key"], entry["path"]),
    )


def test_uncommitted_manifest_is_not_trusted_or_pushed() -> None:
    shards = build_shards([_row(1)], max_size=10_000)
    manifest = build_manifest(shards, generated_at="2026-05-12T10:00:00+00:00")
    proposed = {**manifest, "phase": "proposed"}

    assert trusted_committed_manifest(proposed) is None
    with pytest.raises(SourceSyncShardError, match="uncommitted"):
        push_manifest(_FakeSyncModule([]), _config(), proposed, opener=object())


def test_read_manifest_ignores_proposed_manifest_without_v2_side_effect() -> None:
    shards = build_shards([_row(1)], max_size=10_000)
    manifest = build_manifest(shards, generated_at="2026-05-12T10:00:00+00:00")
    module = _FakeSyncModule(
        [(200, {"sha": "oldsha", "content": _encoded_json({**manifest, "phase": "proposed"})})]
    )

    result = read_manifest(module, _config(), opener=object())

    assert result is None
    assert module.calls[0]["method"] == "GET"
    assert module.calls[0]["url"] == (
        "https://api.github.test/repos/owner/repo/contents/"
        "baluffo/source-sync/manifest.json?ref=main"
    )


def test_read_and_push_manifest_use_committed_manifest_path() -> None:
    shards = build_shards([_row(1)], max_size=10_000)
    manifest = build_manifest(shards, generated_at="2026-05-12T10:00:00+00:00")
    module = _FakeSyncModule(
        [
            (200, {"sha": "oldsha", "content": _encoded_json(manifest)}),
            (201, {"content": {"sha": "newsha"}}),
        ]
    )

    read_result = read_manifest(module, _config(), opener=object())
    push_result = push_manifest(
        module,
        _config(),
        read_result["manifest"],
        sha=read_result["sha"],
        opener=object(),
    )

    assert read_result == {"sha": "oldsha", "manifest": manifest}
    assert push_result == {"ok": True, "sha": "newsha"}
    put_call = module.calls[1]
    assert put_call["method"] == "PUT"
    assert put_call["url"] == (
        "https://api.github.test/repos/owner/repo/contents/baluffo/source-sync/manifest.json"
    )
    assert put_call["payload"]["branch"] == "main"
    assert put_call["payload"]["sha"] == "oldsha"
    decoded_manifest = json.loads(base64.b64decode(put_call["payload"]["content"]))
    assert decoded_manifest == manifest


def test_changed_shards_compare_committed_path_and_sha() -> None:
    shards = build_shards(
        [_row(index) for index in range(8)],
        max_size=10_000,
        bucket="active",
        base_path="baluffo/source-sync/shards/stable",
    )
    committed = build_manifest(shards, generated_at="2026-05-12T10:00:00+00:00")
    committed["shards"][1] = {**committed["shards"][1], "sha256": "0" * 64}
    committed["shards"] = committed["shards"][:-1]
    committed["shardCount"] = len(committed["shards"])
    committed["totalRowCount"] = sum(entry["rowCount"] for entry in committed["shards"])
    committed["totalSizeBytes"] = sum(entry["sizeBytes"] for entry in committed["shards"])

    changed = changed_shards(shards, committed)

    assert [shard.path for shard in changed] == [shards[1].path, shards[-1].path]


def test_changed_shards_treat_untrusted_manifest_as_all_changed() -> None:
    shards = build_shards([_row(1), _row(2)], max_size=10_000)
    proposed = {
        **build_manifest(shards, generated_at="2026-05-12T10:00:00+00:00"),
        "phase": "proposed",
    }

    assert changed_shards(shards, proposed) == shards


def test_push_shard_writes_payload_to_immutable_shard_path() -> None:
    shard = build_shards([_row(1)], max_size=10_000, base_path="baluffo/source-sync/shards/stable")[
        0
    ]
    module = _FakeSyncModule([(201, {"content": {"sha": "blobsha"}})])

    result = push_shard(module, _config(), shard, opener=object())

    assert result == {
        "ok": True,
        "path": shard.path,
        "sha256": shard.sha256,
        "remoteSha": "blobsha",
        "sizeBytes": shard.size_bytes,
        "rowCount": shard.row_count,
    }
    put_call = module.calls[0]
    assert put_call["method"] == "PUT"
    assert put_call["url"] == f"https://api.github.test/repos/owner/repo/contents/{shard.path}"
    assert put_call["payload"]["branch"] == "main"
    assert (
        hashlib.sha256(base64.b64decode(put_call["payload"]["content"])).hexdigest() == shard.sha256
    )
    assert "sha" not in put_call["payload"]


def test_push_shard_rejects_payload_sha_mismatch() -> None:
    shard = build_shards([_row(1)], max_size=10_000)[0]
    tampered = type(shard)(
        bucket=shard.bucket,
        key=shard.key,
        path=shard.path,
        row_count=shard.row_count,
        size_bytes=shard.size_bytes,
        sha256="0" * 64,
        payload_bytes=shard.payload_bytes,
    )

    with pytest.raises(SourceSyncShardError, match="sha256"):
        push_shard(_FakeSyncModule([]), _config(), tampered, opener=object())


def test_push_changed_shards_only_puts_missing_or_changed_shards() -> None:
    old_shards = build_shards(
        [_row(index) for index in range(5)],
        max_size=10_000,
        bucket="active",
        base_path="baluffo/source-sync/shards/stable",
    )
    new_shards = build_shards(
        [_row(index) for index in range(6)],
        max_size=10_000,
        bucket="active",
        base_path="baluffo/source-sync/shards/stable",
    )
    committed = build_manifest(old_shards, generated_at="2026-05-12T10:00:00+00:00")
    expected_changed = changed_shards(new_shards, committed)
    module = _FakeSyncModule(
        [(201, {"content": {"sha": f"blob{index}"}}) for index in range(len(expected_changed))]
    )

    result = push_changed_shards(module, _config(), new_shards, committed, opener=object())

    assert result["shardCount"] == len(new_shards)
    assert result["changedShardCount"] == len(expected_changed)
    assert result["shardsPushedBytes"] == sum(shard.size_bytes for shard in expected_changed)
    assert result["changedShards"] == [shard.manifest_entry() for shard in expected_changed]
    assert [call["url"].rsplit("/contents/", 1)[1] for call in module.calls] == [
        shard.path for shard in expected_changed
    ]
