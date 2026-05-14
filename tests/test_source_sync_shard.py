import gzip
import hashlib
import json

import pytest

from scripts import build_ship_bundle
from src.source_sync_shard import (
    SourceSyncShardError,
    build_sharded_snapshot_bundle,
    build_shards,
    content_addressed_shards,
    shard_key,
)


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


def _rows_with_same_prefix(prefix: str, count: int) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    index = 0
    while len(rows) < count:
        row = _row(index, extra_chunks=10)
        if shard_key(row, prefix_length=len(prefix)) == prefix:
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
    assert all(len(shard.key) == 1 for shard in first)
    assert all(shard.sha256 == hashlib.sha256(shard.payload_bytes).hexdigest() for shard in first)


def test_shard_payload_contains_canonical_rows() -> None:
    rows = [_row(index) for index in range(3)]

    shards = build_shards(list(reversed(rows)), max_size=10_000, bucket="pending")

    assert 1 <= len(shards) <= 3
    assert sum(shard.row_count for shard in shards) == len(rows)
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
    assert all(len(shard.key) > 2 for shard in shards)
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


def test_content_addressed_shards_use_payload_hash_paths() -> None:
    shards = build_shards([_row(1), _row(2)], max_size=10_000, base_path="sync/shards")

    addressed = content_addressed_shards(shards, base_path="sync/shards")

    assert [shard.manifest_entry() for shard in addressed] != [
        shard.manifest_entry() for shard in shards
    ]
    assert all(
        shard.path == f"sync/shards/{shard.bucket}/{shard.key}/{shard.sha256}.json.gz"
        for shard in addressed
    )
    assert all(
        shard.payload_bytes == original.payload_bytes
        for shard, original in zip(addressed, shards, strict=True)
    )


def test_build_sharded_snapshot_bundle_tracks_noop_and_changed_metrics() -> None:
    snapshot = {
        "schemaVersion": 2,
        "generatedAt": "2026-05-12T10:00:00+00:00",
        "source": {"name": "admin_bridge"},
        "active": [_row(1), _row(2)],
        "pending": [_row(3)],
    }

    first = build_sharded_snapshot_bundle(
        snapshot,
        max_shard_size=10_000,
        base_path="sync/shards",
    )
    second = build_sharded_snapshot_bundle(
        {**snapshot, "generatedAt": "2026-05-12T10:01:00+00:00"},
        max_shard_size=10_000,
        committed_manifest=first["manifest"],
        base_path="sync/shards",
    )
    changed = build_sharded_snapshot_bundle(
        {**snapshot, "active": [_row(1), _row(22)], "generatedAt": "2026-05-12T10:02:00+00:00"},
        max_shard_size=10_000,
        committed_manifest=first["manifest"],
        base_path="sync/shards",
    )

    assert first["metrics"]["changedShardCount"] == len(first["shards"])
    assert second["metrics"]["changedShardCount"] == 0
    assert [shard.path for shard in second["shards"]] == [shard.path for shard in first["shards"]]
    assert changed["metrics"]["changedShardCount"] == len(changed["changedShards"])
    assert changed["metrics"]["changedShardCount"] >= 1
    assert changed["metrics"]["shardsPushedBytes"] == sum(
        shard.size_bytes for shard in changed["changedShards"]
    )
    assert changed["metrics"]["manifestSizeBytes"] > 0
    assert changed["manifest"]["shardCapBytes"] == 10_000


def test_build_sharded_snapshot_bundle_rejects_invalid_snapshot_rows() -> None:
    with pytest.raises(SourceSyncShardError, match="active rows"):
        build_sharded_snapshot_bundle(
            {
                "generatedAt": "2026-05-12T10:00:00+00:00",
                "source": {"name": "admin_bridge"},
                "active": ["bad-row"],
                "pending": [],
            },
            max_shard_size=10_000,
        )


def test_ship_bundle_includes_shard_module() -> None:
    assert "source_sync_shard.py" in build_ship_bundle.APP_RUNTIME_SCRIPTS
