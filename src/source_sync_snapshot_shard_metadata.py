"""Source-sync snapshot shard metadata: fingerprints, sizing, and shard payload fields.

AI boundary owns: source-sync snapshot content fingerprinting, snapshot byte sizing, shard bundle construction, shard metric payloads, and snapshot identity assertions.
AI boundary implement in: this leaf for shard-facing snapshot metadata; push orchestration stays in source_sync_snapshot_push and merge semantics in the source_sync_snapshot coordinator.
AI boundary search before contracts: source sync facade, shard bundle builder, storage metrics, and shard metadata tests.
AI boundary verify: `python -m pytest tests/test_source_sync.py tests/test_source_sync_shard.py -q`.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from typing import Any, cast

from src.shared.json_shapes import as_json_object as _as_dict
from src.source_registry import source_identity
from src.source_sync_shard import (
    SHARD_SCHEMA_VERSION,
    SourceSyncShardError,
    build_sharded_snapshot_bundle,
    push_sharded_snapshot,
)
from src.source_sync_snapshot import normalize_snapshot


def _snapshot_content_view(module: Any, snapshot: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_snapshot(module, snapshot)
    schema_version = int(normalized.get("schemaVersion") or module.SYNC_SCHEMA_VERSION)
    if schema_version == SHARD_SCHEMA_VERSION:
        schema_version = int(module.SYNC_SCHEMA_VERSION)
    # Fingerprint only the semantic rows that should trigger a remote write.
    return {
        "schemaVersion": schema_version,
        "active": list(normalized.get("active") or []),
        "pending": list(normalized.get("pending") or []),
    }


def _snapshot_content_fingerprint(module: Any, snapshot: dict[str, Any]) -> str:
    view = _snapshot_content_view(module, snapshot)
    encoded = json.dumps(view, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _snapshot_size_bytes(snapshot: dict[str, Any]) -> int:
    encoded = json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return len(encoded.encode("utf-8"))


def _shard_size_bytes(module: Any) -> int:
    return int(getattr(module, "DEFAULT_SOURCE_SYNC_SHARD_SIZE_BYTES", 10 * 1024 * 1024) or 0)


def _remote_committed_manifest(remote: Mapping[str, Any]) -> dict[str, Any] | None:
    manifest = remote.get("committedManifest")
    return dict(manifest) if isinstance(manifest, dict) else None


def _sharded_snapshot_bundle(
    module: Any,
    snapshot: dict[str, Any],
    remote: Mapping[str, Any],
) -> dict[str, Any]:
    return build_sharded_snapshot_bundle(
        snapshot,
        max_shard_size=_shard_size_bytes(module),
        committed_manifest=_remote_committed_manifest(remote),
    )


def _shard_push_metadata(result: Mapping[str, Any]) -> dict[str, Any]:
    metrics = _as_dict(result.get("metrics"))
    payload = _shard_metrics_payload(metrics)
    remote_timing = result.get("remoteTiming")
    if isinstance(remote_timing, Mapping):
        payload["remoteTiming"] = dict(remote_timing)
    return payload


def _shard_bundle_metadata(bundle: Mapping[str, Any]) -> dict[str, Any]:
    return _shard_metrics_payload(_as_dict(bundle.get("metrics")))


def _manifest_noop_shard_metadata(module: Any, manifest: Mapping[str, Any]) -> dict[str, Any]:
    rows = list(manifest.get("shards") or [])
    shard_hashes: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        path = str(row.get("path") or "").strip()
        sha256 = str(row.get("sha256") or "").strip()
        if path and sha256:
            shard_hashes[path] = sha256
    manifest_size_bytes = len(
        json.dumps(dict(manifest), sort_keys=True, separators=(",", ":")).encode("utf-8")
    )
    return {
        "snapshotFormat": "sharded-v3",
        "shardCount": int(manifest.get("shardCount") or len(shard_hashes)),
        "changedShardCount": 0,
        "shardsPushedBytes": 0,
        "manifestSizeBytes": manifest_size_bytes,
        "shardCapBytes": int(manifest.get("shardCapBytes") or _shard_size_bytes(module)),
        "shardHashes": shard_hashes,
    }


def _shard_metrics_payload(metrics: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "snapshotFormat": "sharded-v3",
        "shardCount": int(metrics.get("shardCount") or 0),
        "changedShardCount": int(metrics.get("changedShardCount") or 0),
        "shardsPushedBytes": int(metrics.get("shardsPushedBytes") or 0),
        "manifestSizeBytes": int(metrics.get("manifestSizeBytes") or 0),
        "shardCapBytes": int(metrics.get("shardCapBytes") or 0),
        "shardHashes": dict(metrics.get("shardHashes") or {}),
    }


def _snapshot_too_large_error(
    module: Any,
    exc: SourceSyncShardError,
    *,
    snapshot_size_bytes: int,
    max_snapshot_size_bytes: int,
    size_warning: bool,
) -> Exception:
    return cast(
        Exception,
        module.SyncOperationError(
            "snapshot_too_large",
            str(exc),
            sizeBytes=snapshot_size_bytes,
            maxSnapshotSizeBytes=max_snapshot_size_bytes,
            sizeWarning=size_warning,
        ),
    )


def _push_sharded_snapshot_result(
    module: Any,
    config: Any,
    snapshot: dict[str, Any],
    remote: Mapping[str, Any],
    *,
    bundle: dict[str, Any] | None = None,
    progress_callback: Callable[..., None] | None = None,
    opener: Callable[..., Any],
) -> dict[str, Any]:
    return push_sharded_snapshot(
        module,
        config,
        snapshot,
        max_shard_size=_shard_size_bytes(module),
        committed_manifest=_remote_committed_manifest(remote),
        committed_manifest_sha=str(remote.get("sha") or "")
        if str(remote.get("snapshotFormat") or "") == "sharded-v3"
        else "",
        bundle=bundle,
        progress_callback=progress_callback,
        opener=opener,
    )


def _assert_unique_snapshot_identity(
    module: Any, rows: list[dict[str, Any]], *, scope: str
) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_id = source_identity(row)
        if not row_id:
            continue
        if row_id in seen:
            duplicates.add(row_id)
        else:
            seen.add(row_id)
    if duplicates:
        joined = ", ".join(sorted(duplicates))
        raise module.SyncOperationError(
            "duplicate_source_identity",
            f"Duplicate canonical source identity in {scope}: {joined}",
        )
