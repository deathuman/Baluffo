"""Deterministic source-sync shard construction."""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
import re
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

from src import source_sync_config as _source_sync_config
from src.source_registry_identity import source_identity

SHARD_SCHEMA_VERSION = 3
DEFAULT_PREFIX_LENGTH = 1
PREFIX_LENGTH_STEP = 1
MAX_PREFIX_LENGTH = 64
DEFAULT_SHARD_READ_WORKERS = 8
# GitHub Contents writes advance the target branch; concurrent PUTs can race each other.
DEFAULT_SHARD_WRITE_WORKERS = 1
DEFAULT_BASE_PATH = "baluffo/source-sync/shards"
DEFAULT_MANIFEST_FILE_NAME = "manifest.json"
DEFAULT_GC_DELETE_LIMIT = 32
_SAFE_PATH_COMPONENT = re.compile(r"^[A-Za-z0-9._=-]+$")
_TRUSTED_MANIFEST_PHASES = {"", "committed"}
_EXPECTED_PROGRESS_CALLBACK_EXCEPTIONS = (OSError, RuntimeError, TypeError, ValueError)


class SourceSyncShardError(ValueError):
    """Raised when rows cannot be represented as bounded source-sync shards."""


_EXPECTED_REMOTE_SYNC_EXCEPTIONS = (OSError, RuntimeError, ValueError)


def _duration_ms(started_at: float, finished_at: float) -> int:
    return max(0, int(round((finished_at - started_at) * 1000)))


def _remote_timing_row(
    *,
    operation: str,
    method: str,
    path: str,
    started_at: float,
    ok: bool,
    status: int = 0,
    size_bytes: int = 0,
    row_count: int = 0,
    already_existed: bool = False,
    error: str = "",
) -> dict[str, Any]:
    return {
        "operation": str(operation or "unknown"),
        "method": str(method or "").upper(),
        "path": str(path or ""),
        "durationMs": _duration_ms(started_at, time.perf_counter()),
        "ok": bool(ok),
        "status": int(status or 0),
        "sizeBytes": max(0, int(size_bytes or 0)),
        "rowCount": max(0, int(row_count or 0)),
        "alreadyExisted": bool(already_existed),
        "error": str(error or "")[:240],
    }


def _remote_timing_summary(
    rows: list[dict[str, Any]],
    *,
    wall_duration_ms: int = 0,
    stage_wall_ms: dict[str, int] | None = None,
) -> dict[str, Any]:
    operation_totals: dict[str, int] = {}
    method_counts: dict[str, int] = {}
    total_duration_ms = 0
    for row in rows:
        duration_ms = int(row.get("durationMs") or 0)
        total_duration_ms += duration_ms
        operation = str(row.get("operation") or "unknown")
        operation_totals[operation] = operation_totals.get(operation, 0) + duration_ms
        method = str(row.get("method") or "").upper()
        if method:
            method_counts[method] = method_counts.get(method, 0) + 1
    operation_top = [
        {"operation": operation, "durationMs": duration_ms}
        for operation, duration_ms in sorted(
            operation_totals.items(),
            key=lambda item: item[1],
            reverse=True,
        )
    ]
    slowest = sorted(
        rows,
        key=lambda row: int(row.get("durationMs") or 0),
        reverse=True,
    )
    return {
        "requestCount": len(rows),
        "methodCounts": method_counts,
        "totalRequestDurationMs": total_duration_ms,
        "wallDurationMs": max(0, int(wall_duration_ms or 0)),
        "stageWallMs": {
            str(key): max(0, int(value or 0)) for key, value in dict(stage_wall_ms or {}).items()
        },
        "operationTotalsMs": operation_totals,
        "operationTop": operation_top[:12],
        "slowestRequests": slowest[:20],
        "errorRequests": [
            row for row in slowest if not bool(row.get("ok")) or int(row.get("status") or 0) >= 400
        ][:20],
    }


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
    return {
        "sha": str(payload.get("sha") or ""),
        "manifest": manifest,
        "manifestSizeBytes": len(raw_bytes),
    }


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
        if int(status or 0) == 409 and hasattr(module, "SyncOperationError"):
            conflict_code = getattr(module, "RUNTIME_STATE_REMOTE_CONFLICT", "remote_conflict")
            if hasattr(module, "_set_runtime_state"):
                module._set_runtime_state(conflict_code, message_text)
            raise module.SyncOperationError(conflict_code, message_text)
        raise RuntimeError(message_text)
    content = body.get("content") if isinstance(body.get("content"), dict) else {}
    if hasattr(module, "_clear_runtime_state") and hasattr(module, "RUNTIME_STATE_REMOTE_CONFLICT"):
        module._clear_runtime_state(module.RUNTIME_STATE_REMOTE_CONFLICT)
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
        if int(status or 0) in {409, 422}:
            try:
                read_shard(module, config, shard.manifest_entry(), opener=opener)
            except _EXPECTED_REMOTE_SYNC_EXCEPTIONS as exc:
                raise RuntimeError(message_text) from exc
            return {
                "ok": True,
                "path": shard.path,
                "sha256": shard.sha256,
                "remoteSha": "",
                "sizeBytes": shard.size_bytes,
                "rowCount": shard.row_count,
                "alreadyExisted": True,
            }
        raise RuntimeError(message_text)
    content = body.get("content") if isinstance(body.get("content"), dict) else {}
    return {
        "ok": True,
        "path": shard.path,
        "sha256": shard.sha256,
        "remoteSha": str(content.get("sha") or ""),
        "sizeBytes": shard.size_bytes,
        "rowCount": shard.row_count,
        "alreadyExisted": False,
    }


def _shard_progress_counts(
    *,
    shard_count: int,
    changed_shard_count: int,
    completed_shard_count: int = 0,
    verified_shard_count: int = 0,
    current_shard_index: int = 0,
    current_shard_label: str = "",
    shards_pushed_bytes: int = 0,
    total_shard_bytes: int = 0,
    manifest_committed: bool = False,
    gc_deleted_count: int = 0,
) -> dict[str, Any]:
    return {
        "action": "push",
        "shardCount": max(0, int(shard_count or 0)),
        "changedShardCount": max(0, int(changed_shard_count or 0)),
        "completedShardCount": max(0, int(completed_shard_count or 0)),
        "verifiedShardCount": max(0, int(verified_shard_count or 0)),
        "currentShardIndex": max(0, int(current_shard_index or 0)),
        "currentShardLabel": str(current_shard_label or ""),
        "shardsPushedBytes": max(0, int(shards_pushed_bytes or 0)),
        "totalShardBytes": max(0, int(total_shard_bytes or 0)),
        "manifestCommitted": bool(manifest_committed),
        "gcDeletedCount": max(0, int(gc_deleted_count or 0)),
    }


def _shard_pull_progress_counts(
    *,
    shard_count: int,
    completed_shard_count: int = 0,
    current_shard_index: int = 0,
    current_shard_label: str = "",
    shards_read_bytes: int = 0,
    total_shard_bytes: int = 0,
    manifest_size_bytes: int = 0,
    skipped: bool = False,
    skip_reason: str = "",
) -> dict[str, Any]:
    return {
        "action": "pull",
        "shardCount": max(0, int(shard_count or 0)),
        "completedShardCount": max(0, int(completed_shard_count or 0)),
        "currentShardIndex": max(0, int(current_shard_index or 0)),
        "currentShardLabel": str(current_shard_label or ""),
        "shardsReadBytes": max(0, int(shards_read_bytes or 0)),
        "totalShardBytes": max(0, int(total_shard_bytes or 0)),
        "manifestSizeBytes": max(0, int(manifest_size_bytes or 0)),
        "skipped": bool(skipped),
        "skipReason": str(skip_reason or ""),
    }


def _emit_pull_progress(
    progress_callback: Callable[..., None] | None,
    *,
    phase_label: str,
    counts: dict[str, Any],
    ratio: float,
    message: str = "",
    event_level: str = "muted",
) -> None:
    if not callable(progress_callback):
        return
    try:
        progress_callback(
            phase_key="remote_read",
            phase_label=phase_label,
            mode="determinate",
            ratio=max(0.0, min(1.0, float(ratio or 0.0))),
            counts=counts,
            target_url="",
            event_level=event_level,
            message=message,
        )
    except _EXPECTED_PROGRESS_CALLBACK_EXCEPTIONS:
        return


def _emit_push_progress(
    progress_callback: Callable[..., None] | None,
    *,
    phase_label: str,
    counts: dict[str, Any],
    ratio: float,
    message: str = "",
    event_level: str = "muted",
) -> None:
    if not callable(progress_callback):
        return
    try:
        progress_callback(
            phase_key="remote_write",
            phase_label=phase_label,
            mode="determinate",
            ratio=max(0.0, min(1.0, float(ratio or 0.0))),
            counts=counts,
            target_url="",
            event_level=event_level,
            message=message,
        )
    except _EXPECTED_PROGRESS_CALLBACK_EXCEPTIONS:
        return


def _push_and_verify_changed_shard(
    module: Any,
    config: Any,
    shard: Shard,
    *,
    opener: Callable[..., Any],
) -> dict[str, Any]:
    remote_requests: list[dict[str, Any]] = []
    put_started_at = time.perf_counter()
    try:
        result = push_shard(
            module,
            config,
            shard,
            message=f"Update Baluffo source sync shard {shard.bucket}/{shard.key}",
            opener=opener,
        )
    except _EXPECTED_REMOTE_SYNC_EXCEPTIONS as exc:
        remote_requests.append(
            _remote_timing_row(
                operation="pushShard",
                method="PUT",
                path=shard.path,
                started_at=put_started_at,
                ok=False,
                size_bytes=shard.size_bytes,
                row_count=shard.row_count,
                error=str(exc),
            )
        )
        return {"ok": False, "exception": exc, "remoteRequests": remote_requests}
    remote_requests.append(
        _remote_timing_row(
            operation="pushShard",
            method="PUT",
            path=shard.path,
            started_at=put_started_at,
            ok=bool(result.get("ok")),
            size_bytes=shard.size_bytes,
            row_count=shard.row_count,
            already_existed=bool(result.get("alreadyExisted")),
        )
    )
    verify_started_at = time.perf_counter()
    try:
        verified = read_shard(module, config, shard.manifest_entry(), opener=opener)
    except _EXPECTED_REMOTE_SYNC_EXCEPTIONS as exc:
        remote_requests.append(
            _remote_timing_row(
                operation="verifyShard",
                method="GET",
                path=shard.path,
                started_at=verify_started_at,
                ok=False,
                size_bytes=shard.size_bytes,
                row_count=shard.row_count,
                error=str(exc),
            )
        )
        return {"ok": False, "exception": exc, "remoteRequests": remote_requests}
    verified_row_count = len(verified.get("rows") or [])
    remote_requests.append(
        _remote_timing_row(
            operation="verifyShard",
            method="GET",
            path=shard.path,
            started_at=verify_started_at,
            ok=True,
            size_bytes=shard.size_bytes,
            row_count=verified_row_count,
        )
    )
    return {
        "ok": True,
        "pushResult": result,
        "verification": {
            "path": shard.path,
            "sha256": shard.sha256,
            "rowCount": verified_row_count,
        },
        "remoteRequests": remote_requests,
    }


def push_changed_shards(
    module: Any,
    config: Any,
    shards: list[Shard],
    committed_manifest: dict[str, Any] | None,
    *,
    progress_callback: Callable[..., None] | None = None,
    opener: Callable[..., Any],
) -> dict[str, Any]:
    changed = changed_shards(shards, committed_manifest)
    results: list[dict[str, Any]] = []
    verifications: list[dict[str, Any]] = []
    remote_requests: list[dict[str, Any]] = []
    changed_count = len(changed)
    total_bytes = sum(shard.size_bytes for shard in changed)
    completed_count = 0
    verified_count = 0
    pushed_bytes = 0
    parallel_started_at = time.perf_counter()
    parallel_finished_at = parallel_started_at
    if changed_count:
        first_shard = changed[0]
        _emit_push_progress(
            progress_callback,
            phase_label=f"Uploading shard 1 of {changed_count}",
            ratio=0.0,
            counts=_shard_progress_counts(
                shard_count=len(shards),
                changed_shard_count=changed_count,
                completed_shard_count=0,
                verified_shard_count=0,
                current_shard_index=1,
                current_shard_label=f"{first_shard.bucket}/{first_shard.key}",
                shards_pushed_bytes=0,
                total_shard_bytes=total_bytes,
            ),
            message=f"Uploading {changed_count} source-sync shard(s).",
        )
    worker_count = max(1, min(DEFAULT_SHARD_WRITE_WORKERS, changed_count or 1))
    shard_outputs: list[dict[str, Any] | None] = [None] * changed_count
    if changed:
        parallel_started_at = time.perf_counter()
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_to_index = {
                executor.submit(
                    _push_and_verify_changed_shard,
                    module,
                    config,
                    shard,
                    opener=opener,
                ): index
                for index, shard in enumerate(changed)
            }
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                shard = changed[index]
                output = future.result()
                shard_outputs[index] = output
                if not output.get("ok"):
                    for pending in future_to_index:
                        if pending is not future:
                            pending.cancel()
                    raise output["exception"]
                completed_count += 1
                verified_count += 1
                pushed_bytes += shard.size_bytes
                shard_label = f"{shard.bucket}/{shard.key}"
                _emit_push_progress(
                    progress_callback,
                    phase_label=f"Verified shard {completed_count} of {changed_count}",
                    ratio=(completed_count / changed_count) if changed_count else 1.0,
                    counts=_shard_progress_counts(
                        shard_count=len(shards),
                        changed_shard_count=changed_count,
                        completed_shard_count=completed_count,
                        verified_shard_count=verified_count,
                        current_shard_index=index + 1,
                        current_shard_label=shard_label,
                        shards_pushed_bytes=pushed_bytes,
                        total_shard_bytes=total_bytes,
                    ),
                    message=(
                        f"Verified {verified_count} of {changed_count} source-sync shards."
                        if verified_count % 25 == 0 or verified_count == changed_count
                        else ""
                    ),
                )
        parallel_finished_at = time.perf_counter()
    for output in shard_outputs:
        if output is None:
            continue
        results.append(dict(output.get("pushResult") or {}))
        verifications.append(dict(output.get("verification") or {}))
        remote_requests.extend(list(output.get("remoteRequests") or []))
    return {
        "shardCount": len(shards),
        "changedShardCount": len(changed),
        "shardsPushedBytes": sum(shard.size_bytes for shard in changed),
        "workerCount": worker_count if changed else 0,
        "parallelWallMs": _duration_ms(parallel_started_at, parallel_finished_at) if changed else 0,
        "changedShards": [shard.manifest_entry() for shard in changed],
        "pushResults": results,
        "verifiedShards": verifications,
        "remoteRequests": remote_requests,
    }


def prune_unreferenced_shards(
    module: Any,
    config: Any,
    manifest: dict[str, Any],
    *,
    base_path: str = DEFAULT_BASE_PATH,
    delete_limit: int = DEFAULT_GC_DELETE_LIMIT,
    opener: Callable[..., Any],
) -> dict[str, Any]:
    module.validate_sync_config(config)
    trusted = validate_manifest(manifest)
    normalized_base = _normalize_base_path(base_path)
    referenced = {
        entry["path"]
        for entry in trusted.get("shards", [])
        if _is_gc_candidate_path(str(entry.get("path") or ""), normalized_base)
    }
    warnings: list[str] = []
    deleted_paths: list[str] = []
    skipped_paths: list[str] = []
    delete_attempts = 0
    delete_cap = max(0, int(delete_limit or 0))
    if delete_cap <= 0:
        return {
            "ok": True,
            "deletedCount": 0,
            "deleteAttemptCount": 0,
            "skippedCount": 0,
            "deleteLimit": delete_cap,
            "deletedPaths": [],
            "warnings": [],
        }
    for item in _list_content_tree(module, config, normalized_base, opener=opener):
        path = str(item.get("path") or "").replace("\\", "/").strip()
        if not _is_gc_candidate_path(path, normalized_base):
            skipped_paths.append(path)
            warnings.append(f"skipped invalid source-sync shard GC path: {path}")
            continue
        if path in referenced:
            skipped_paths.append(path)
            continue
        sha = str(item.get("sha") or "").strip()
        if not sha:
            skipped_paths.append(path)
            warnings.append(f"skipped source-sync shard without remote sha: {path}")
            continue
        if delete_attempts >= delete_cap:
            skipped_paths.append(path)
            continue
        delete_attempts += 1
        warning = _delete_shard_object(module, config, path, sha, opener=opener)
        if warning:
            warnings.append(warning)
            skipped_paths.append(path)
            continue
        deleted_paths.append(path)
    return {
        "ok": not warnings,
        "deletedCount": len(deleted_paths),
        "deleteAttemptCount": delete_attempts,
        "skippedCount": len(skipped_paths),
        "deleteLimit": delete_cap,
        "deletedPaths": deleted_paths,
        "warnings": warnings,
    }


def push_sharded_snapshot(
    module: Any,
    config: Any,
    snapshot: dict[str, Any],
    *,
    max_shard_size: int,
    committed_manifest: dict[str, Any] | None = None,
    committed_manifest_sha: str = "",
    bundle: dict[str, Any] | None = None,
    gc_delete_limit: int = DEFAULT_GC_DELETE_LIMIT,
    progress_callback: Callable[..., None] | None = None,
    opener: Callable[..., Any],
) -> dict[str, Any]:
    if bundle is None:
        bundle = build_sharded_snapshot_bundle(
            snapshot,
            max_shard_size=max_shard_size,
            committed_manifest=committed_manifest,
        )
    metrics = dict(bundle["metrics"])
    if committed_manifest is not None and not bundle["changedShards"]:
        return {
            "ok": True,
            "pushed": False,
            "skipped": True,
            "skipReason": "no_changed_shards",
            "manifest": bundle["manifest"],
            "metrics": metrics,
        }
    remote_wall_started_at = time.perf_counter()
    shard_result = push_changed_shards(
        module,
        config,
        bundle["shards"],
        committed_manifest,
        progress_callback=progress_callback,
        opener=opener,
    )
    changed_count = int(shard_result.get("changedShardCount") or 0)
    total_bytes = int(shard_result.get("shardsPushedBytes") or 0)
    manifest_counts = _shard_progress_counts(
        shard_count=len(bundle["shards"]),
        changed_shard_count=changed_count,
        completed_shard_count=changed_count,
        verified_shard_count=changed_count,
        shards_pushed_bytes=total_bytes,
        total_shard_bytes=total_bytes,
    )
    _emit_push_progress(
        progress_callback,
        phase_label="Committing sync manifest",
        ratio=1.0,
        counts=manifest_counts,
        message="Committing sync manifest.",
    )
    remote_requests = list(shard_result.get("remoteRequests") or [])
    push_changed_wall_ms = int(shard_result.get("parallelWallMs") or 0)
    manifest_path_text = manifest_path(config.path)
    manifest_started_at = time.perf_counter()
    try:
        manifest_result = push_manifest(
            module,
            config,
            bundle["manifest"],
            sha=committed_manifest_sha,
            opener=opener,
        )
    except _EXPECTED_REMOTE_SYNC_EXCEPTIONS as exc:
        remote_requests.append(
            _remote_timing_row(
                operation="pushManifest",
                method="PUT",
                path=manifest_path_text,
                started_at=manifest_started_at,
                ok=False,
                size_bytes=int(metrics.get("manifestSizeBytes") or 0),
                row_count=int(bundle["manifest"].get("totalRowCount") or 0),
                error=str(exc),
            )
        )
        raise
    remote_requests.append(
        _remote_timing_row(
            operation="pushManifest",
            method="PUT",
            path=manifest_path_text,
            started_at=manifest_started_at,
            ok=bool(manifest_result.get("ok")),
            size_bytes=int(metrics.get("manifestSizeBytes") or 0),
            row_count=int(bundle["manifest"].get("totalRowCount") or 0),
        )
    )
    manifest_wall_ms = _duration_ms(manifest_started_at, time.perf_counter())
    gc_result: dict[str, Any] = {}
    gc_warnings: list[str] = []
    gc_counts = {
        **manifest_counts,
        "manifestCommitted": True,
    }
    _emit_push_progress(
        progress_callback,
        phase_label="Pruning old sync shards",
        ratio=1.0,
        counts=gc_counts,
        message="Pruning old sync shards.",
    )
    gc_started_at = time.perf_counter()
    try:
        gc_result = prune_unreferenced_shards(
            module,
            config,
            bundle["manifest"],
            delete_limit=gc_delete_limit,
            opener=opener,
        )
        gc_warnings = list(gc_result.get("warnings") or [])
    except (KeyError, RuntimeError, SourceSyncShardError, TypeError, ValueError) as exc:
        gc_warnings = [f"source-sync shard GC failed: {exc}"]
        gc_result = {
            "ok": False,
            "deletedCount": 0,
            "deleteAttemptCount": 0,
            "skippedCount": 0,
            "deleteLimit": max(0, int(gc_delete_limit or 0)),
            "deletedPaths": [],
            "warnings": gc_warnings,
        }
    remote_requests.append(
        _remote_timing_row(
            operation="pruneShards",
            method="GET",
            path=DEFAULT_BASE_PATH,
            started_at=gc_started_at,
            ok=not gc_warnings,
            size_bytes=0,
            row_count=int(gc_result.get("deleteAttemptCount") or 0),
            error="; ".join(gc_warnings),
        )
    )
    gc_wall_ms = _duration_ms(gc_started_at, time.perf_counter())
    _emit_push_progress(
        progress_callback,
        phase_label="Pruned old sync shards",
        ratio=1.0,
        counts={
            **gc_counts,
            "gcDeletedCount": int(gc_result.get("deletedCount") or 0),
        },
        event_level="warn" if gc_warnings else "muted",
        message=(
            f"Pruned {int(gc_result.get('deletedCount') or 0)} old sync shards."
            if not gc_warnings
            else "; ".join(gc_warnings)
        ),
    )
    metrics.update(
        {
            "changedShardCount": int(shard_result.get("changedShardCount") or 0),
            "shardsPushedBytes": int(shard_result.get("shardsPushedBytes") or 0),
        }
    )
    return {
        "ok": True,
        "pushed": True,
        "skipped": False,
        "remoteSha": str(manifest_result.get("sha") or ""),
        "manifest": bundle["manifest"],
        "metrics": metrics,
        "shardResult": shard_result,
        "gc": gc_result,
        "warnings": gc_warnings,
        "remoteTiming": _remote_timing_summary(
            remote_requests,
            wall_duration_ms=_duration_ms(remote_wall_started_at, time.perf_counter()),
            stage_wall_ms={
                "pushChangedShards": push_changed_wall_ms,
                "pushManifest": manifest_wall_ms,
                "pruneShards": gc_wall_ms,
            },
        ),
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
    raw_bytes = _decode_content_bytes(
        module,
        config,
        payload,
        context=f"source-sync shard {shard_entry['path']}",
        opener=opener,
    )
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
    progress_callback: Callable[..., None] | None = None,
    known_manifest_sha: str = "",
    max_workers: int | None = None,
    opener: Callable[..., Any],
) -> dict[str, Any] | None:
    manifest_result = read_manifest(module, config, opener=opener)
    if manifest_result is None:
        return None
    manifest = manifest_result["manifest"]
    manifest_sha = str(manifest_result.get("sha") or "")
    manifest_size_bytes = int(manifest_result.get("manifestSizeBytes") or 0)
    shard_entries = list(manifest["shards"])
    shard_count = len(shard_entries)
    total_shard_bytes = sum(int(entry.get("sizeBytes") or 0) for entry in shard_entries)
    if known_manifest_sha and manifest_sha and str(known_manifest_sha or "") == manifest_sha:
        _emit_pull_progress(
            progress_callback,
            phase_label="Remote manifest unchanged",
            ratio=1.0,
            counts=_shard_pull_progress_counts(
                shard_count=shard_count,
                completed_shard_count=0,
                shards_read_bytes=0,
                total_shard_bytes=total_shard_bytes,
                manifest_size_bytes=manifest_size_bytes,
                skipped=True,
                skip_reason="remote_manifest_unchanged",
            ),
            message="Source-sync remote manifest is unchanged; skipping shard download.",
            event_level="success",
        )
        return {
            "schemaVersion": SHARD_SCHEMA_VERSION,
            "generatedAt": manifest["generatedAt"],
            "source": dict(manifest.get("source") or {"name": "admin_bridge"}),
            "active": [],
            "pending": [],
            "manifest": manifest,
            "manifestSha": manifest_sha,
            "manifestSizeBytes": manifest_size_bytes,
            "shardCount": shard_count,
            "shardsReadBytes": 0,
            "totalShardBytes": total_shard_bytes,
            "skipped": True,
            "skipReason": "remote_manifest_unchanged",
        }
    _emit_pull_progress(
        progress_callback,
        phase_label=f"Reading shard 0 of {shard_count}",
        ratio=0.0,
        counts=_shard_pull_progress_counts(
            shard_count=shard_count,
            completed_shard_count=0,
            shards_read_bytes=0,
            total_shard_bytes=total_shard_bytes,
            manifest_size_bytes=manifest_size_bytes,
        ),
        message="Reading source-sync shards.",
    )
    rows_by_bucket: dict[str, list[dict[str, Any]]] = {"active": [], "pending": []}
    worker_count = max(1, min(int(max_workers or DEFAULT_SHARD_READ_WORKERS), shard_count or 1))
    shard_results: list[dict[str, Any] | None] = [None] * shard_count
    completed_count = 0
    read_bytes = 0
    if shard_entries:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_to_index = {
                executor.submit(read_shard, module, config, entry, opener=opener): index
                for index, entry in enumerate(shard_entries)
            }
            try:
                for future in as_completed(future_to_index):
                    index = future_to_index[future]
                    shard_result = future.result()
                    shard_results[index] = shard_result
                    completed_count += 1
                    entry = shard_result["entry"]
                    read_bytes += int(entry.get("sizeBytes") or 0)
                    shard_label = f"{entry['bucket']}/{entry['key']}"
                    _emit_pull_progress(
                        progress_callback,
                        phase_label=f"Read shard {completed_count} of {shard_count}",
                        ratio=(completed_count / shard_count) if shard_count else 1.0,
                        counts=_shard_pull_progress_counts(
                            shard_count=shard_count,
                            completed_shard_count=completed_count,
                            current_shard_index=index + 1,
                            current_shard_label=shard_label,
                            shards_read_bytes=read_bytes,
                            total_shard_bytes=total_shard_bytes,
                            manifest_size_bytes=manifest_size_bytes,
                        ),
                        message=(
                            f"Read {completed_count} of {shard_count} source-sync shards."
                            if completed_count % 25 == 0 or completed_count == shard_count
                            else ""
                        ),
                        event_level="success" if completed_count == shard_count else "muted",
                    )
            except BaseException:
                for future in future_to_index:
                    future.cancel()
                raise
    for shard_result in shard_results:
        if shard_result is None:
            continue
        bucket = str(shard_result["entry"]["bucket"])
        rows_by_bucket.setdefault(bucket, []).extend(shard_result["rows"])
    snapshot: dict[str, Any] = {
        "schemaVersion": SHARD_SCHEMA_VERSION,
        "generatedAt": manifest["generatedAt"],
        "source": dict(manifest.get("source") or {"name": "admin_bridge"}),
        "active": rows_by_bucket.pop("active", []),
        "pending": rows_by_bucket.pop("pending", []),
        "manifest": manifest,
        "manifestSha": manifest_sha,
        "manifestSizeBytes": manifest_size_bytes,
        "shardCount": shard_count,
        "shardsReadBytes": read_bytes,
        "totalShardBytes": total_shard_bytes,
    }
    for bucket in sorted(rows_by_bucket):
        snapshot[bucket] = rows_by_bucket[bucket]
    return snapshot


def _list_content_tree(
    module: Any,
    config: Any,
    path: str,
    *,
    opener: Callable[..., Any],
) -> list[dict[str, Any]]:
    status, payload, _headers = module._request_json(
        method="GET",
        url=_content_api_url(module, config, path, with_ref=True),
        config=config,
        timeout_s=config.timeout_s,
        opener=opener,
    )
    if status == 404:
        return []
    if status >= 400:
        message = str(payload.get("message") or f"GitHub GET failed with HTTP {status}")
        raise RuntimeError(message)
    items = payload if isinstance(payload, list) else [payload] if isinstance(payload, dict) else []
    files: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        item_type = str(item.get("type") or "").strip().lower()
        item_path = str(item.get("path") or "").replace("\\", "/").strip()
        if item_type == "dir" and item_path:
            files.extend(_list_content_tree(module, config, item_path, opener=opener))
        elif item_type in {"file", ""} and item_path:
            files.append(dict(item))
    return files


def _delete_shard_object(
    module: Any,
    config: Any,
    path: str,
    sha: str,
    *,
    opener: Callable[..., Any],
) -> str:
    status, payload, _headers = module._request_json(
        method="DELETE",
        url=_content_api_url(module, config, path, with_ref=False),
        config=config,
        timeout_s=config.timeout_s,
        payload={
            "message": f"Prune Baluffo source sync shard {path}",
            "sha": sha,
            "branch": config.branch,
        },
        opener=opener,
    )
    if status in {200, 202, 204, 404}:
        return ""
    return str(payload.get("message") or f"GitHub DELETE failed with HTTP {status}")


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


def _is_gc_candidate_path(path: str, base_path: str) -> bool:
    normalized = str(path or "").replace("\\", "/").strip()
    if (
        not normalized
        or normalized != normalized.strip("/")
        or "//" in normalized
        or not normalized.endswith(".json.gz")
    ):
        return False
    try:
        _normalize_base_path(normalized)
    except ValueError:
        return False
    return normalized.startswith(f"{base_path}/")


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


def _decode_content_bytes(
    module: Any,
    config: Any,
    payload: dict[str, Any],
    *,
    context: str,
    opener: Callable[..., Any],
) -> bytes:
    encoded_content = str(payload.get("content") or "").strip()
    if not encoded_content:
        download_url = str(payload.get("download_url") or "").strip()
        if not download_url:
            raise SourceSyncShardError(f"{context} content is empty")
        status, raw_bytes, _headers = _request_download_bytes(
            module, config, download_url=download_url, opener=opener
        )
        if status == 404:
            raise SourceSyncShardError(f"{context} download URL is missing")
        if status >= 400:
            raise RuntimeError(f"{context} download failed with HTTP {status}")
        if not raw_bytes:
            raise SourceSyncShardError(f"{context} download is empty")
        return raw_bytes
    try:
        return base64.b64decode(encoded_content.replace("\n", ""))
    except ValueError as exc:
        raise SourceSyncShardError(f"{context} content is not valid base64: {exc}") from exc


def _request_download_bytes(
    module: Any,
    config: Any,
    *,
    download_url: str,
    opener: Callable[..., Any],
) -> tuple[int, bytes, dict[str, str]]:
    kwargs = {
        "url": download_url,
        "headers": {"Accept": "application/octet-stream"},
        "timeout_s": config.timeout_s,
        "opener": opener,
    }
    request_raw_bytes = getattr(module, "_request_raw_bytes", None)
    if callable(request_raw_bytes):
        return request_raw_bytes(**kwargs)
    return _source_sync_config.request_raw_bytes(module, **kwargs)


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
