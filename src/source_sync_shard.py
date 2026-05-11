"""Deterministic source-sync shard construction."""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

from src.source_registry_identity import source_identity

SHARD_SCHEMA_VERSION = 3
DEFAULT_PREFIX_LENGTH = 2
PREFIX_LENGTH_STEP = 2
MAX_PREFIX_LENGTH = 64
DEFAULT_BASE_PATH = "baluffo/source-sync/shards"
DEFAULT_MANIFEST_FILE_NAME = "manifest.json"
_SAFE_PATH_COMPONENT = re.compile(r"^[A-Za-z0-9._=-]+$")
_TRUSTED_MANIFEST_PHASES = {"", "committed"}


class SourceSyncShardError(ValueError):
    """Raised when rows cannot be represented as bounded source-sync shards."""


@dataclass(frozen=True)
class Shard:
    bucket: str
    key: str
    path: str
    row_count: int
    size_bytes: int
    sha256: str
    payload_bytes: bytes = field(repr=False, compare=False)

    def manifest_entry(self) -> dict[str, Any]:
        return {
            "bucket": self.bucket,
            "key": self.key,
            "path": self.path,
            "rowCount": self.row_count,
            "sizeBytes": self.size_bytes,
            "sha256": self.sha256,
        }


def shard_key(source: dict[str, Any], *, prefix_length: int = DEFAULT_PREFIX_LENGTH) -> str:
    if prefix_length < 1 or prefix_length > MAX_PREFIX_LENGTH:
        raise ValueError(
            f"prefix_length must be between 1 and {MAX_PREFIX_LENGTH}: {prefix_length}"
        )
    identity = source_identity(source)
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:prefix_length]


def build_shards(
    rows: list[dict[str, Any]],
    max_size: int,
    *,
    bucket: str = "active",
    base_path: str = DEFAULT_BASE_PATH,
) -> list[Shard]:
    if max_size <= 0:
        raise ValueError(f"max_size must be positive: {max_size}")
    normalized_bucket = _safe_path_component(bucket, field_name="bucket")
    normalized_base_path = _normalize_base_path(base_path)
    grouped = _group_by_prefix(rows, prefix_length=DEFAULT_PREFIX_LENGTH)
    shards: list[Shard] = []
    for key in sorted(grouped):
        shards.extend(
            _build_bounded_shards(
                grouped[key],
                max_size=max_size,
                bucket=normalized_bucket,
                base_path=normalized_base_path,
                prefix_length=DEFAULT_PREFIX_LENGTH,
            )
        )
    return sorted(shards, key=lambda shard: (shard.bucket, shard.key, shard.path))


def content_addressed_shards(
    shards: list[Shard],
    *,
    base_path: str = DEFAULT_BASE_PATH,
) -> list[Shard]:
    normalized_base_path = _normalize_base_path(base_path)
    addressed: list[Shard] = []
    for shard in sorted(shards, key=lambda item: (item.bucket, item.key, item.path)):
        expected_sha256 = hashlib.sha256(shard.payload_bytes).hexdigest()
        if shard.sha256.lower() != expected_sha256:
            raise SourceSyncShardError(
                f"source-sync shard {shard.path} sha256 does not match payload bytes"
            )
        sha256 = shard.sha256.lower()
        addressed.append(
            Shard(
                bucket=shard.bucket,
                key=shard.key,
                path=f"{normalized_base_path}/{shard.bucket}/{shard.key}/{sha256}.json.gz",
                row_count=shard.row_count,
                size_bytes=shard.size_bytes,
                sha256=sha256,
                payload_bytes=shard.payload_bytes,
            )
        )
    return addressed


def build_sharded_snapshot_bundle(
    snapshot: dict[str, Any],
    *,
    max_shard_size: int,
    committed_manifest: dict[str, Any] | None = None,
    base_path: str = DEFAULT_BASE_PATH,
) -> dict[str, Any]:
    if not isinstance(snapshot, dict):
        raise SourceSyncShardError("source-sync snapshot must be a JSON object")
    if max_shard_size <= 0:
        raise ValueError(f"max_shard_size must be positive: {max_shard_size}")

    generated_at = str(snapshot.get("generatedAt") or "").strip()
    if not generated_at:
        raise SourceSyncShardError("source-sync snapshot generatedAt is required")

    source_label = _manifest_source(snapshot.get("source"))["name"]
    shards: list[Shard] = []
    for bucket in ("active", "pending"):
        shards.extend(
            build_shards(
                _snapshot_bucket_rows(snapshot, bucket),
                max_size=max_shard_size,
                bucket=bucket,
                base_path=base_path,
            )
        )
    shards = content_addressed_shards(shards, base_path=base_path)
    manifest = build_manifest(
        shards,
        generated_at=generated_at,
        source_label=source_label,
        shard_cap_bytes=max_shard_size,
    )
    changed = changed_shards(shards, committed_manifest)
    manifest_size_bytes = len(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )
    return {
        "manifest": manifest,
        "shards": shards,
        "changedShards": changed,
        "metrics": {
            "snapshotSchemaVersion": SHARD_SCHEMA_VERSION,
            "shardCount": len(shards),
            "changedShardCount": len(changed),
            "shardsPushedBytes": sum(shard.size_bytes for shard in changed),
            "manifestSizeBytes": manifest_size_bytes,
            "shardCapBytes": int(max_shard_size),
            "totalSizeBytes": sum(shard.size_bytes for shard in shards),
            "shardHashes": {shard.path: shard.sha256 for shard in shards},
        },
    }


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
    totals = {
        key: int(payload.get(key) if key in payload else value) for key, value in expected.items()
    }
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


def read_manifest(
    module: Any,
    config: Any,
    *,
    opener: Callable[..., Any],
) -> dict[str, Any] | None:
    module.validate_sync_config(config)
    status, payload, _headers = module._request_json(
        method="GET",
        url=_content_api_url(module, config, manifest_path(config.path), with_ref=True),
        config=config,
        timeout_s=config.timeout_s,
        opener=opener,
    )
    if status == 404:
        return None
    if status >= 400:
        message = str(payload.get("message") or f"GitHub GET failed with HTTP {status}")
        raise RuntimeError(message)
    encoded_content = str(payload.get("content") or "").strip()
    if not encoded_content:
        return None
    try:
        raw_bytes = base64.b64decode(encoded_content.replace("\n", ""))
        parsed = json.loads(raw_bytes.decode("utf-8"))
    except (ValueError, json.JSONDecodeError) as exc:
        raise SourceSyncShardError(f"invalid source-sync manifest JSON: {exc}") from exc
    manifest = trusted_committed_manifest(parsed)
    if manifest is None:
        return None
    return {"sha": str(payload.get("sha") or ""), "manifest": manifest}


def push_manifest(
    module: Any,
    config: Any,
    manifest: dict[str, Any],
    *,
    sha: str = "",
    message: str = "Update Baluffo source sync manifest",
    opener: Callable[..., Any],
) -> dict[str, Any]:
    module.validate_sync_config(config)
    trusted = trusted_committed_manifest(manifest)
    if trusted is None:
        raise SourceSyncShardError("refusing to push an uncommitted source-sync manifest")
    encoded = base64.b64encode(
        json.dumps(trusted, ensure_ascii=False, indent=2).encode("utf-8")
    ).decode("ascii")
    payload: dict[str, Any] = {
        "message": str(message or "Update Baluffo source sync manifest"),
        "content": encoded,
        "branch": config.branch,
    }
    if sha:
        payload["sha"] = sha
    status, body, _headers = module._request_json(
        method="PUT",
        url=_content_api_url(module, config, manifest_path(config.path), with_ref=False),
        config=config,
        timeout_s=config.timeout_s,
        payload=payload,
        opener=opener,
    )
    if status >= 400:
        message_text = str(body.get("message") or f"GitHub PUT failed with HTTP {status}")
        raise RuntimeError(message_text)
    content = body.get("content") if isinstance(body.get("content"), dict) else {}
    return {"ok": True, "sha": str(content.get("sha") or "")}


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


def push_shard(
    module: Any,
    config: Any,
    shard: Shard,
    *,
    message: str = "Update Baluffo source sync shard",
    opener: Callable[..., Any],
) -> dict[str, Any]:
    module.validate_sync_config(config)
    expected_sha256 = hashlib.sha256(shard.payload_bytes).hexdigest()
    if shard.sha256.lower() != expected_sha256:
        raise SourceSyncShardError(
            f"source-sync shard {shard.path} sha256 does not match payload bytes"
        )
    payload: dict[str, Any] = {
        "message": str(message or "Update Baluffo source sync shard"),
        "content": base64.b64encode(shard.payload_bytes).decode("ascii"),
        "branch": config.branch,
    }
    status, body, _headers = module._request_json(
        method="PUT",
        url=_content_api_url(module, config, shard.path, with_ref=False),
        config=config,
        timeout_s=config.timeout_s,
        payload=payload,
        opener=opener,
    )
    if status >= 400:
        message_text = str(body.get("message") or f"GitHub PUT failed with HTTP {status}")
        raise RuntimeError(message_text)
    content = body.get("content") if isinstance(body.get("content"), dict) else {}
    return {
        "ok": True,
        "path": shard.path,
        "sha256": shard.sha256,
        "remoteSha": str(content.get("sha") or ""),
        "sizeBytes": shard.size_bytes,
        "rowCount": shard.row_count,
    }


def push_changed_shards(
    module: Any,
    config: Any,
    shards: list[Shard],
    committed_manifest: dict[str, Any] | None,
    *,
    opener: Callable[..., Any],
) -> dict[str, Any]:
    changed = changed_shards(shards, committed_manifest)
    results = [
        push_shard(
            module,
            config,
            shard,
            message=f"Update Baluffo source sync shard {shard.bucket}/{shard.key}",
            opener=opener,
        )
        for shard in changed
    ]
    return {
        "shardCount": len(shards),
        "changedShardCount": len(changed),
        "shardsPushedBytes": sum(shard.size_bytes for shard in changed),
        "changedShards": [shard.manifest_entry() for shard in changed],
        "pushResults": results,
    }


def read_shard(
    module: Any,
    config: Any,
    entry: dict[str, Any],
    *,
    opener: Callable[..., Any],
) -> dict[str, Any]:
    module.validate_sync_config(config)
    shard_entry = _validate_manifest_shard_entry(entry)
    status, payload, _headers = module._request_json(
        method="GET",
        url=_content_api_url(module, config, shard_entry["path"], with_ref=True),
        config=config,
        timeout_s=config.timeout_s,
        opener=opener,
    )
    if status == 404:
        raise SourceSyncShardError(f"source-sync shard missing: {shard_entry['path']}")
    if status >= 400:
        message = str(payload.get("message") or f"GitHub GET failed with HTTP {status}")
        raise RuntimeError(message)
    raw_bytes = _decode_content_bytes(payload, context=f"source-sync shard {shard_entry['path']}")
    if hashlib.sha256(raw_bytes).hexdigest() != shard_entry["sha256"]:
        raise SourceSyncShardError(f"source-sync shard sha256 mismatch: {shard_entry['path']}")
    if len(raw_bytes) != shard_entry["sizeBytes"]:
        raise SourceSyncShardError(f"source-sync shard size mismatch: {shard_entry['path']}")
    try:
        parsed = json.loads(gzip.decompress(raw_bytes).decode("utf-8"))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise SourceSyncShardError(
            f"invalid source-sync shard payload {shard_entry['path']}: {exc}"
        ) from exc
    shard_payload = _validate_shard_payload(parsed, shard_entry)
    return {"entry": shard_entry, "payload": shard_payload, "rows": shard_payload["rows"]}


def read_sharded_snapshot(
    module: Any,
    config: Any,
    *,
    opener: Callable[..., Any],
) -> dict[str, Any] | None:
    manifest_result = read_manifest(module, config, opener=opener)
    if manifest_result is None:
        return None
    manifest = manifest_result["manifest"]
    rows_by_bucket: dict[str, list[dict[str, Any]]] = {"active": [], "pending": []}
    for entry in manifest["shards"]:
        shard_result = read_shard(module, config, entry, opener=opener)
        bucket = str(shard_result["entry"]["bucket"])
        rows_by_bucket.setdefault(bucket, []).extend(shard_result["rows"])
    snapshot: dict[str, Any] = {
        "schemaVersion": SHARD_SCHEMA_VERSION,
        "generatedAt": manifest["generatedAt"],
        "source": dict(manifest.get("source") or {"name": "admin_bridge"}),
        "active": rows_by_bucket.pop("active", []),
        "pending": rows_by_bucket.pop("pending", []),
        "manifest": manifest,
        "manifestSha": str(manifest_result.get("sha") or ""),
    }
    for bucket in sorted(rows_by_bucket):
        snapshot[bucket] = rows_by_bucket[bucket]
    return snapshot


def _build_bounded_shards(
    rows: list[dict[str, Any]],
    *,
    max_size: int,
    bucket: str,
    base_path: str,
    prefix_length: int,
) -> list[Shard]:
    canonical_rows = _canonical_rows(rows)
    key = shard_key(canonical_rows[0], prefix_length=prefix_length) if canonical_rows else "empty"
    shard = _build_shard(canonical_rows, bucket=bucket, base_path=base_path, key=key)
    if shard.size_bytes <= max_size:
        return [shard]
    if len(canonical_rows) <= 1 or prefix_length >= MAX_PREFIX_LENGTH:
        raise SourceSyncShardError(
            f"source-sync shard {bucket}/{key} is {shard.size_bytes} bytes, "
            f"exceeding {max_size} bytes and cannot be split further"
        )
    next_prefix_length = min(prefix_length + PREFIX_LENGTH_STEP, MAX_PREFIX_LENGTH)
    grouped = _group_by_prefix(canonical_rows, prefix_length=next_prefix_length)
    if len(grouped) == 1 and next(iter(grouped)) == key:
        raise SourceSyncShardError(
            f"source-sync shard {bucket}/{key} did not split at prefix length {next_prefix_length}"
        )
    shards: list[Shard] = []
    for next_key in sorted(grouped):
        shards.extend(
            _build_bounded_shards(
                grouped[next_key],
                max_size=max_size,
                bucket=bucket,
                base_path=base_path,
                prefix_length=next_prefix_length,
            )
        )
    return shards


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


def _decode_content_bytes(payload: dict[str, Any], *, context: str) -> bytes:
    encoded_content = str(payload.get("content") or "").strip()
    if not encoded_content:
        raise SourceSyncShardError(f"{context} content is empty")
    try:
        return base64.b64decode(encoded_content.replace("\n", ""))
    except ValueError as exc:
        raise SourceSyncShardError(f"{context} content is not valid base64: {exc}") from exc


def _validate_shard_payload(payload: Any, entry: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise SourceSyncShardError("source-sync shard payload must be a JSON object")
    if int(payload.get("schemaVersion") or 0) != SHARD_SCHEMA_VERSION:
        raise SourceSyncShardError("source-sync shard schemaVersion must be 3")
    if str(payload.get("bucket") or "") != entry["bucket"]:
        raise SourceSyncShardError(f"source-sync shard bucket mismatch: {entry['path']}")
    if str(payload.get("key") or "") != entry["key"]:
        raise SourceSyncShardError(f"source-sync shard key mismatch: {entry['path']}")
    rows = payload.get("rows")
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise SourceSyncShardError(f"source-sync shard rows must be objects: {entry['path']}")
    if len(rows) != entry["rowCount"]:
        raise SourceSyncShardError(f"source-sync shard rowCount mismatch: {entry['path']}")
    return {
        "schemaVersion": SHARD_SCHEMA_VERSION,
        "bucket": entry["bucket"],
        "key": entry["key"],
        "rows": [dict(row) for row in rows],
    }


def _snapshot_bucket_rows(snapshot: dict[str, Any], bucket: str) -> list[dict[str, Any]]:
    rows = snapshot.get(bucket) or []
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise SourceSyncShardError(f"source-sync snapshot {bucket} rows must be objects")
    return [dict(row) for row in rows]


def _manifest_source(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {"name": "admin_bridge"}
    name = str(value.get("name") or "admin_bridge").strip() or "admin_bridge"
    return {"name": name}


def _content_api_url(module: Any, config: Any, path: str, *, with_ref: bool) -> str:
    repo_token = quote(config.repo, safe="/")
    path_token = quote(path, safe="/")
    base = f"{module._github_api_base()}/repos/{repo_token}/contents/{path_token}"
    if with_ref:
        ref_token = quote(config.branch, safe="")
        return f"{base}?ref={ref_token}"
    return base


def _build_shard(rows: list[dict[str, Any]], *, bucket: str, base_path: str, key: str) -> Shard:
    payload = _serialize_shard(bucket=bucket, key=key, rows=rows)
    return Shard(
        bucket=bucket,
        key=key,
        path=f"{base_path}/{bucket}/{key}.json.gz",
        row_count=len(rows),
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        payload_bytes=payload,
    )


def _serialize_shard(*, bucket: str, key: str, rows: list[dict[str, Any]]) -> bytes:
    payload = {
        "schemaVersion": SHARD_SCHEMA_VERSION,
        "bucket": bucket,
        "key": key,
        "rows": rows,
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return gzip.compress(raw, mtime=0)


def _group_by_prefix(
    rows: list[dict[str, Any]], *, prefix_length: int
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise TypeError(f"source-sync rows must be dictionaries: {type(row).__name__}")
        grouped.setdefault(shard_key(row, prefix_length=prefix_length), []).append(dict(row))
    return grouped


def _canonical_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted((dict(row) for row in rows), key=lambda row: source_identity(row))


def _safe_path_component(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    if normalized in {".", ".."} or not normalized or not _SAFE_PATH_COMPONENT.match(normalized):
        raise ValueError(f"invalid source-sync shard {field_name}: {value!r}")
    return normalized


def _normalize_base_path(value: str) -> str:
    normalized = str(value or "").replace("\\", "/").strip()
    if not normalized or normalized != normalized.strip("/") or "//" in normalized:
        raise ValueError("base_path must not be empty")
    parts = normalized.split("/")
    for part in parts:
        _safe_path_component(part, field_name="base_path")
    return "/".join(parts)
