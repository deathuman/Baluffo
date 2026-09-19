"""Source-sync shard manifest semantics: build, validate, trust, and diff shard entries.

AI boundary owns: source-sync manifest path derivation, manifest construction, manifest validation, and committed-vs-current shard diffing.
AI boundary implement in: this leaf for manifest semantics; shard payload construction stays in source_sync_shard_model and transport in source_sync_shard_remote.
AI boundary search before contracts: source sync facade, snapshot normalization, shard tests, and remote manifest readers.
AI boundary verify: `python -m pytest tests/test_source_sync_shard_io.py tests/test_source_sync_shard.py -q`.
"""

from __future__ import annotations

import re
from typing import Any

from src.source_sync_shard_model import (
    DEFAULT_MANIFEST_FILE_NAME,
    SHARD_SCHEMA_VERSION,
    Shard,
    SourceSyncShardError,
    _safe_path_component,
)

_TRUSTED_MANIFEST_PHASES = {"", "committed"}


def manifest_path(snapshot_path: str) -> str:
    normalized = str(snapshot_path or "").replace("\\", "/").strip()
    if not normalized:
        raise ValueError("snapshot_path must not be empty")
    if normalized.endswith(".json"):
        normalized = normalized[: -len(".json")]
    if not normalized or normalized != normalized.strip("/") or "//" in normalized:
        raise ValueError(f"invalid source-sync snapshot path: {snapshot_path!r}")
    for part in normalized.split("/"):
        _safe_path_component(part, field_name="snapshot_path")
    return f"{normalized}/{DEFAULT_MANIFEST_FILE_NAME}"


def build_manifest(
    shards: list[Shard],
    *,
    generated_at: str,
    source_label: str = "admin_bridge",
    shard_cap_bytes: int | None = None,
    phase: str | None = None,
) -> dict[str, Any]:
    normalized_phase = str(phase or "").strip().lower()
    if normalized_phase and normalized_phase not in _TRUSTED_MANIFEST_PHASES:
        raise ValueError(f"unsupported committed manifest phase: {phase!r}")
    ordered_shards = sorted(shards, key=lambda shard: (shard.bucket, shard.key, shard.path))
    manifest: dict[str, Any] = {
        "schemaVersion": SHARD_SCHEMA_VERSION,
        "generatedAt": str(generated_at or "").strip(),
        "source": {"name": str(source_label or "admin_bridge")},
        "shardCount": len(ordered_shards),
        "totalRowCount": sum(shard.row_count for shard in ordered_shards),
        "totalSizeBytes": sum(shard.size_bytes for shard in ordered_shards),
        "shards": [shard.manifest_entry() for shard in ordered_shards],
    }
    if shard_cap_bytes is not None:
        manifest["shardCapBytes"] = int(shard_cap_bytes)
    if normalized_phase:
        manifest["phase"] = normalized_phase
    return validate_manifest(manifest)


def validate_manifest(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise SourceSyncShardError("source-sync manifest must be a JSON object")
    if int(payload.get("schemaVersion") or 0) != SHARD_SCHEMA_VERSION:
        raise SourceSyncShardError("source-sync manifest schemaVersion must be 3")
    generated_at = str(payload.get("generatedAt") or "").strip()
    if not generated_at:
        raise SourceSyncShardError("source-sync manifest generatedAt is required")
    phase = str(payload.get("phase") or "").strip().lower()
    if phase and phase not in _TRUSTED_MANIFEST_PHASES and phase != "proposed":
        raise SourceSyncShardError(f"unsupported source-sync manifest phase: {phase}")
    raw_shards = payload.get("shards")
    if not isinstance(raw_shards, list):
        raise SourceSyncShardError("source-sync manifest shards must be a list")
    shards = [_validate_manifest_shard_entry(entry) for entry in raw_shards]
    totals = _validate_manifest_totals(payload, shards)
    normalized: dict[str, Any] = {
        "schemaVersion": SHARD_SCHEMA_VERSION,
        "generatedAt": generated_at,
        "source": _manifest_source(payload.get("source")),
        "shardCount": totals["shardCount"],
        "totalRowCount": totals["totalRowCount"],
        "totalSizeBytes": totals["totalSizeBytes"],
        "shards": sorted(shards, key=lambda entry: (entry["bucket"], entry["key"], entry["path"])),
    }
    if "shardCapBytes" in payload:
        normalized["shardCapBytes"] = int(payload.get("shardCapBytes") or 0)
    if phase:
        normalized["phase"] = phase
    return normalized


def _validate_manifest_totals(
    payload: dict[str, Any], shards: list[dict[str, Any]]
) -> dict[str, int]:
    expected = {
        "shardCount": len(shards),
        "totalRowCount": sum(entry["rowCount"] for entry in shards),
        "totalSizeBytes": sum(entry["sizeBytes"] for entry in shards),
    }
    totals = {key: int(payload.get(key, value)) for key, value in expected.items()}
    for key, value in expected.items():
        if totals[key] != value:
            raise SourceSyncShardError(f"source-sync manifest {key} does not match shards")
    return totals


def trusted_committed_manifest(payload: dict[str, Any]) -> dict[str, Any] | None:
    manifest = validate_manifest(payload)
    phase = str(manifest.get("phase") or "").strip().lower()
    if phase not in _TRUSTED_MANIFEST_PHASES:
        return None
    return manifest


def changed_shards(shards: list[Shard], committed_manifest: dict[str, Any] | None) -> list[Shard]:
    if committed_manifest is None:
        return sorted(shards, key=lambda shard: (shard.bucket, shard.key, shard.path))
    trusted = trusted_committed_manifest(committed_manifest)
    if trusted is None:
        return sorted(shards, key=lambda shard: (shard.bucket, shard.key, shard.path))
    previous_by_path = {
        str(entry.get("path") or ""): str(entry.get("sha256") or "").lower()
        for entry in trusted.get("shards", [])
        if isinstance(entry, dict)
    }
    changed = [
        shard for shard in shards if previous_by_path.get(shard.path) != shard.sha256.lower()
    ]
    return sorted(changed, key=lambda shard: (shard.bucket, shard.key, shard.path))


def _validate_manifest_shard_entry(entry: Any) -> dict[str, Any]:
    if not isinstance(entry, dict):
        raise SourceSyncShardError("source-sync manifest shard entries must be objects")
    bucket = _safe_path_component(str(entry.get("bucket") or ""), field_name="bucket")
    key = _safe_path_component(str(entry.get("key") or ""), field_name="key")
    path = str(entry.get("path") or "").replace("\\", "/").strip()
    if not path or path != path.strip("/") or "//" in path:
        raise SourceSyncShardError(f"invalid source-sync manifest shard path: {path!r}")
    for part in path.split("/"):
        _safe_path_component(part, field_name="path")
    row_count = int(entry.get("rowCount") or 0)
    size_bytes = int(entry.get("sizeBytes") or 0)
    sha256 = str(entry.get("sha256") or "").strip().lower()
    if row_count < 0:
        raise SourceSyncShardError("source-sync manifest rowCount must be non-negative")
    if size_bytes <= 0:
        raise SourceSyncShardError("source-sync manifest sizeBytes must be positive")
    if not re.fullmatch(r"[0-9a-f]{64}", sha256):
        raise SourceSyncShardError("source-sync manifest sha256 must be a 64-char hex digest")
    return {
        "bucket": bucket,
        "key": key,
        "path": path,
        "rowCount": row_count,
        "sizeBytes": size_bytes,
        "sha256": sha256,
    }


def _manifest_source(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {"name": "admin_bridge"}
    name = str(value.get("name") or "admin_bridge").strip() or "admin_bridge"
    return {"name": name}
