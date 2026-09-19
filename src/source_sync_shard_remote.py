"""Source-sync shard remote transport: GitHub Contents reads, writes, verification, and timing.

AI boundary owns: source-sync shard and manifest HTTP transport, payload decoding, sha256 verification, and remote request timing rows.
AI boundary implement in: this leaf for remote IO semantics; shard payload construction stays in source_sync_shard_model, manifest semantics in source_sync_shard_manifest, and pruning in source_sync_shard_gc.
AI boundary search before contracts: source sync facade, shard tests, and remote timing contracts.
AI boundary verify: `python -m pytest tests/test_source_sync_shard_io.py tests/test_source_sync_sharded_push.py -q`.
"""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
import time
from collections.abc import Callable
from typing import Any, cast
from urllib.parse import quote

from src import source_sync_config as _source_sync_config
from src.source_sync_shard_manifest import (
    _validate_manifest_shard_entry,
    manifest_path,
    trusted_committed_manifest,
)
from src.source_sync_shard_model import (
    _EXPECTED_REMOTE_SYNC_EXCEPTIONS,
    SHARD_SCHEMA_VERSION,
    Shard,
    SourceSyncShardError,
)


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
        return cast(tuple[int, bytes, dict[str, str]], request_raw_bytes(**kwargs))
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


def _content_api_url(module: Any, config: Any, path: str, *, with_ref: bool) -> str:
    repo_token = quote(config.repo, safe="/")
    path_token = quote(path, safe="/")
    base = f"{module._github_api_base()}/repos/{repo_token}/contents/{path_token}"
    if with_ref:
        ref_token = quote(config.branch, safe="")
        return f"{base}?ref={ref_token}"
    return base
