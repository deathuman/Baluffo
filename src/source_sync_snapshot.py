"""Source-sync snapshot normalization and GitHub transport helpers.

AI boundary owns: source-sync snapshot parsing, validation, remote metadata, and transport error mapping.
AI boundary implement in: this file for snapshot semantics; shard construction and runtime state stay in sibling source-sync modules.
AI boundary search before contracts: source sync facade, registry IO, shard helpers, and snapshot tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused source-sync snapshot tests.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import ssl
import time
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from typing import Any, cast
from urllib.error import URLError

from src.shared.json_shapes import as_json_object as _as_dict
from src.source_registry import (
    REGISTRY_MIGRATION_V2,
    REGISTRY_REASON_PENDING_DEFAULT,
    canonicalize_registry_row,
    ensure_source_id,
    sort_sources_by_identity,
    source_identity,
)
from src.source_sync_runtime import parse_iso
from src.source_sync_shard import (
    SHARD_SCHEMA_VERSION,
    SourceSyncShardError,
    build_sharded_snapshot_bundle,
    push_sharded_snapshot,
    read_sharded_snapshot,
)
from src.storage_metrics import record_source_sync_snapshot

logger = logging.getLogger(__name__)

_REMOTE_SNAPSHOT_TOP_LEVEL_KEYS = {
    "schemaVersion",
    "generatedAt",
    "source",
    "active",
    "pending",
    "rejected",
}


def _duration_ms(started_at: float, finished_at: float) -> int:
    return max(0, int(round((finished_at - started_at) * 1000)))


class _SyncDetailTiming:
    def __init__(self) -> None:
        self._stage_totals_ms: dict[str, int] = {}

    @contextmanager
    def record(self, stage: str):
        stage_key = str(stage or "").strip() or "unknown"
        started_at = time.perf_counter()
        try:
            yield
        finally:
            duration_ms = _duration_ms(started_at, time.perf_counter())
            self._stage_totals_ms[stage_key] = self._stage_totals_ms.get(stage_key, 0) + duration_ms

    def snapshot(self) -> dict[str, Any]:
        stage_totals_ms = dict(self._stage_totals_ms)
        return {
            "stageTotalsMs": stage_totals_ms,
            "stageTop": [
                {"stage": stage, "durationMs": duration_ms}
                for stage, duration_ms in sorted(
                    stage_totals_ms.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )
            ],
        }


@contextmanager
def _record_detail_stage(timing: _SyncDetailTiming | None, stage: str):
    if timing is None:
        yield
        return
    with timing.record(stage):
        yield


def _with_detail_timing(
    payload: dict[str, Any], timing: _SyncDetailTiming | None
) -> dict[str, Any]:
    if timing is None:
        return payload
    data = dict(payload)
    data["detailTiming"] = timing.snapshot()
    return data


def _snapshot_transition_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _backfill_snapshot_transition_metadata(
    row: dict[str, Any], *, bucket: str, generated_at: str
) -> dict[str, Any]:
    updated = dict(row)
    bucket_token = str(bucket or "").strip().lower()
    generated_at = str(generated_at or "").strip()
    if bucket_token == "active":
        state_changed_at = _snapshot_transition_text(
            updated.get("stateChangedAt"),
            updated.get("approvedAt"),
            updated.get("liveAt"),
            generated_at,
        )
        state_changed_by = _snapshot_transition_text(
            updated.get("stateChangedBy"),
            updated.get("approvedBy"),
        )
        if state_changed_at and not state_changed_by:
            state_changed_by = REGISTRY_MIGRATION_V2
        updated["stateChangedAt"] = state_changed_at
        updated["stateChangedBy"] = state_changed_by
        updated["pendingReason"] = ""
        updated["lastPromotedAt"] = _snapshot_transition_text(
            updated.get("lastPromotedAt"),
            state_changed_at,
        )
        updated["approvedAt"] = _snapshot_transition_text(
            updated.get("approvedAt"),
            state_changed_at,
        )
        updated["approvedBy"] = _snapshot_transition_text(
            updated.get("approvedBy"),
            state_changed_by,
        )
        updated["liveAt"] = _snapshot_transition_text(updated.get("liveAt"), state_changed_at)
    elif bucket_token == "pending":
        state_changed_at = _snapshot_transition_text(
            updated.get("stateChangedAt"),
            updated.get("lastDemotedAt"),
            updated.get("quarantinedAt"),
            generated_at,
        )
        state_changed_by = _snapshot_transition_text(
            updated.get("stateChangedBy"),
            updated.get("quarantinedBy"),
            updated.get("approvedBy"),
        )
        if state_changed_at and not state_changed_by:
            state_changed_by = REGISTRY_MIGRATION_V2
        updated["stateChangedAt"] = state_changed_at
        updated["stateChangedBy"] = state_changed_by
        updated["pendingReason"] = _snapshot_transition_text(
            updated.get("pendingReason"),
            updated.get("quarantineReason"),
            updated.get("reason"),
            REGISTRY_REASON_PENDING_DEFAULT,
        )
        updated["lastDemotedAt"] = _snapshot_transition_text(
            updated.get("lastDemotedAt"),
            state_changed_at,
        )
    return ensure_source_id(updated)


def _canonicalize_snapshot_rows(
    rows: list[dict[str, Any]], *, bucket: str, generated_at: str = ""
) -> list[dict[str, Any]]:
    canonical = [
        _backfill_snapshot_transition_metadata(
            canonicalize_registry_row(row, bucket=bucket),
            bucket=bucket,
            generated_at=generated_at,
        )
        for row in rows
        if isinstance(row, dict)
    ]
    return sort_sources_by_identity(canonical)


def _row_transition_score(row: dict[str, Any]) -> int:
    timestamps = []
    for key in (
        "stateChangedAt",
        "lastPromotedAt",
        "lastDemotedAt",
        "approvedAt",
        "quarantinedAt",
        "liveAt",
    ):
        dt = parse_iso(row.get(key))
        if dt is not None:
            timestamps.append(int(dt.timestamp()))
    return max(timestamps) if timestamps else 0


def _row_bucket_rank(row: dict[str, Any]) -> int:
    bucket = str(row.get("registryState") or "").strip().lower()
    return {"active": 3, "pending": 2, "rejected": 1}.get(bucket, 0)


def _row_merge_key(row: dict[str, Any]) -> tuple[int, int]:
    return _row_transition_score(row), _row_bucket_rank(row)


def _choose_more_recent_row(
    local_row: dict[str, Any] | None,
    remote_row: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if local_row is None:
        return remote_row
    if remote_row is None:
        return local_row
    local_key = _row_merge_key(local_row)
    remote_key = _row_merge_key(remote_row)
    if remote_key > local_key:
        return remote_row
    return local_row


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


def _push_sources_snapshot_after_conflict(
    module: Any,
    config: Any,
    local_state: Mapping[str, Any],
    snapshot: dict[str, Any],
    snapshot_fingerprint: str,
    snapshot_size_bytes: int,
    size_warning: bool,
    max_snapshot_size_bytes: int,
    shard_fields: Mapping[str, Any],
    remote: Mapping[str, Any],
    opener: Callable[..., Any],
    progress_callback: Callable[..., None] | None,
    exc: Exception,
    detail_timing: _SyncDetailTiming | None = None,
) -> dict[str, Any]:
    base_shard_fields = dict(shard_fields or {"snapshotFormat": "sharded-v3"})
    module.record_sync_counters(conflictsDetected=1)
    module._clear_runtime_state(module.RUNTIME_STATE_REMOTE_CONFLICT)
    with _record_detail_stage(detail_timing, "conflictRefreshRemote"):
        refreshed_remote = read_remote_snapshot(module, config, opener=opener, prefer_sharded=True)
    refreshed_snapshot = _as_dict(refreshed_remote.get("snapshot"))
    refreshed_sha = str(refreshed_remote.get("sha") or "")
    with _record_detail_stage(detail_timing, "conflictFingerprintRemote"):
        refreshed_fingerprint = _snapshot_content_fingerprint(module, refreshed_snapshot)
    if refreshed_fingerprint == snapshot_fingerprint:
        counters = module.record_sync_counters(conflictsResolved=1)
        return _with_detail_timing(
            {
                "pushed": True,
                "remotePreviouslyExisted": bool(remote.get("exists")),
                "remoteSha": refreshed_sha,
                "snapshot": snapshot,
                "skipped": False,
                "sizeBytes": snapshot_size_bytes,
                "sizeWarning": size_warning,
                "maxSnapshotSizeBytes": max_snapshot_size_bytes,
                **base_shard_fields,
                "counters": counters,
            },
            detail_timing,
        )
    with _record_detail_stage(detail_timing, "conflictMergeRegistryState"):
        retry_state = merge_registry_state(module, local_state, refreshed_snapshot)
    with _record_detail_stage(detail_timing, "conflictBuildSnapshot"):
        retry_snapshot = build_snapshot(module, retry_state)
    with _record_detail_stage(detail_timing, "conflictMeasureSnapshotSize"):
        retry_snapshot_size_bytes = _snapshot_size_bytes(retry_snapshot)
    retry_max_snapshot_size_bytes = int(
        getattr(config, "max_snapshot_size_bytes", module.DEFAULT_MAX_SNAPSHOT_SIZE_BYTES)
        or module.DEFAULT_MAX_SNAPSHOT_SIZE_BYTES
    )
    retry_size_warning = retry_snapshot_size_bytes > module.SNAPSHOT_SIZE_WARN_BYTES
    try:
        with _record_detail_stage(detail_timing, "conflictWriteShardedSnapshot"):
            write_result = _push_sharded_snapshot_result(
                module,
                config,
                retry_snapshot,
                refreshed_remote,
                progress_callback=progress_callback,
                opener=opener,
            )
    except SourceSyncShardError as shard_exc:
        raise _snapshot_too_large_error(
            module,
            shard_exc,
            snapshot_size_bytes=retry_snapshot_size_bytes,
            max_snapshot_size_bytes=retry_max_snapshot_size_bytes,
            size_warning=retry_size_warning,
        ) from exc
    counters = module.record_sync_counters(conflictsResolved=1)
    warnings = list(write_result.get("warnings") or [])
    return _with_detail_timing(
        {
            "pushed": True,
            "remotePreviouslyExisted": bool(remote.get("exists")),
            "remoteSha": str(write_result.get("remoteSha") or refreshed_sha),
            "snapshot": retry_snapshot,
            "skipped": False,
            "sizeBytes": retry_snapshot_size_bytes,
            "sizeWarning": retry_size_warning,
            "maxSnapshotSizeBytes": retry_max_snapshot_size_bytes,
            **_shard_push_metadata(write_result),
            "warnings": warnings,
            "counters": counters,
        },
        detail_timing,
    )


def _push_sources_snapshot_after_transient(
    module: Any,
    config: Any,
    local_state: Mapping[str, Any],
    snapshot: dict[str, Any],
    snapshot_fingerprint: str,
    snapshot_size_bytes: int,
    size_warning: bool,
    max_snapshot_size_bytes: int,
    shard_fields: Mapping[str, Any],
    remote: Mapping[str, Any],
    remote_sha: str,
    opener: Callable[..., Any],
    progress_callback: Callable[..., None] | None,
    exc: Exception,
    detail_timing: _SyncDetailTiming | None = None,
) -> dict[str, Any]:
    base_shard_fields = dict(shard_fields or {"snapshotFormat": "sharded-v3"})
    with _record_detail_stage(detail_timing, "transientRefreshRemote"):
        refreshed_remote = read_remote_snapshot(module, config, opener=opener, prefer_sharded=True)
    refreshed_snapshot = _as_dict(refreshed_remote.get("snapshot"))
    refreshed_sha = str(refreshed_remote.get("sha") or "")
    with _record_detail_stage(detail_timing, "transientFingerprintRemote"):
        refreshed_fingerprint = _snapshot_content_fingerprint(module, refreshed_snapshot)
    if (
        refreshed_sha == remote_sha
        and str(refreshed_remote.get("snapshotFormat") or "") == "sharded-v3"
    ):
        with _record_detail_stage(detail_timing, "transientWriteShardedSnapshot"):
            write_result = _push_sharded_snapshot_result(
                module,
                config,
                snapshot,
                refreshed_remote,
                progress_callback=progress_callback,
                opener=opener,
            )
        warnings = list(write_result.get("warnings") or [])
        return _with_detail_timing(
            {
                "pushed": True,
                "remotePreviouslyExisted": bool(remote.get("exists")),
                "remoteSha": str(write_result.get("remoteSha") or refreshed_sha),
                "snapshot": snapshot,
                "skipped": False,
                "sizeBytes": snapshot_size_bytes,
                "sizeWarning": size_warning,
                "maxSnapshotSizeBytes": max_snapshot_size_bytes,
                **_shard_push_metadata(write_result),
                "warnings": warnings,
                "counters": module.sync_counters_payload(),
            },
            detail_timing,
        )
    if refreshed_fingerprint == snapshot_fingerprint:
        return _with_detail_timing(
            {
                "pushed": True,
                "remotePreviouslyExisted": bool(remote.get("exists")),
                "remoteSha": refreshed_sha,
                "snapshot": snapshot,
                "skipped": False,
                "sizeBytes": snapshot_size_bytes,
                "sizeWarning": size_warning,
                "maxSnapshotSizeBytes": max_snapshot_size_bytes,
                **base_shard_fields,
                "counters": module.sync_counters_payload(),
            },
            detail_timing,
        )
    with _record_detail_stage(detail_timing, "transientMergeRegistryState"):
        retry_state = merge_registry_state(module, local_state, refreshed_snapshot)
    with _record_detail_stage(detail_timing, "transientBuildSnapshot"):
        retry_snapshot = build_snapshot(module, retry_state)
    with _record_detail_stage(detail_timing, "transientMeasureSnapshotSize"):
        retry_snapshot_size_bytes = _snapshot_size_bytes(retry_snapshot)
    retry_max_snapshot_size_bytes = int(
        getattr(config, "max_snapshot_size_bytes", module.DEFAULT_MAX_SNAPSHOT_SIZE_BYTES)
        or module.DEFAULT_MAX_SNAPSHOT_SIZE_BYTES
    )
    retry_size_warning = retry_snapshot_size_bytes > module.SNAPSHOT_SIZE_WARN_BYTES
    try:
        with _record_detail_stage(detail_timing, "transientWriteShardedSnapshot"):
            write_result = _push_sharded_snapshot_result(
                module,
                config,
                retry_snapshot,
                refreshed_remote,
                progress_callback=progress_callback,
                opener=opener,
            )
    except SourceSyncShardError as shard_exc:
        raise _snapshot_too_large_error(
            module,
            shard_exc,
            snapshot_size_bytes=retry_snapshot_size_bytes,
            max_snapshot_size_bytes=retry_max_snapshot_size_bytes,
            size_warning=retry_size_warning,
        ) from exc
    warnings = list(write_result.get("warnings") or [])
    return _with_detail_timing(
        {
            "pushed": True,
            "remotePreviouslyExisted": bool(remote.get("exists")),
            "remoteSha": str(write_result.get("remoteSha") or refreshed_sha),
            "snapshot": retry_snapshot,
            "skipped": False,
            "sizeBytes": retry_snapshot_size_bytes,
            "sizeWarning": retry_size_warning,
            "maxSnapshotSizeBytes": retry_max_snapshot_size_bytes,
            **_shard_push_metadata(write_result),
            "warnings": warnings,
            "counters": module.sync_counters_payload(),
        },
        detail_timing,
    )


def _is_transient_request_error(exc: BaseException) -> bool:
    return isinstance(getattr(exc, "__cause__", None), (URLError, ssl.SSLError))


def _retry_transient_get(
    request: Callable[[], dict[str, Any]], *, attempts: int = 3, base_backoff_s: float = 1.0
) -> dict[str, Any]:
    last_exc: RuntimeError | None = None
    for attempt in range(max(1, int(attempts))):
        try:
            return request()
        except RuntimeError as exc:
            if not _is_transient_request_error(exc):
                raise
            last_exc = exc
            if attempt >= max(0, int(attempts) - 1):
                raise
            delay_s = min(base_backoff_s * (2**attempt), 5.0)
            time.sleep(delay_s)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("GET retry failed unexpectedly")


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


def _remote_snapshot_error(detail: str) -> RuntimeError:
    message = f"Invalid remote sync snapshot payload: {detail}"
    logger.error(message)
    return RuntimeError(message)


def _warn_unexpected_remote_snapshot_keys(payload: dict[str, Any]) -> None:
    unexpected_keys = sorted(key for key in payload if key not in _REMOTE_SNAPSHOT_TOP_LEVEL_KEYS)
    if unexpected_keys:
        logger.warning(
            "Remote sync snapshot contains unexpected top-level keys: count=%d",
            len(unexpected_keys),
        )


def _require_remote_snapshot_schema_version(payload: dict[str, Any]) -> None:
    schema_version = payload.get("schemaVersion")
    if (
        not isinstance(schema_version, int)
        or isinstance(schema_version, bool)
        or schema_version < 1
    ):
        raise _remote_snapshot_error("schemaVersion must be an integer >= 1")


def _require_remote_snapshot_generated_at(payload: dict[str, Any]) -> None:
    generated_at = payload.get("generatedAt")
    if not isinstance(generated_at, str) or not generated_at.strip():
        raise _remote_snapshot_error("generatedAt must be a non-empty string")


def _require_remote_snapshot_row_list(payload: dict[str, Any], key: str) -> list[Any]:
    rows = payload.get(key)
    if not isinstance(rows, list):
        raise _remote_snapshot_error(f"{key} must be an array")
    return rows


def _validate_remote_snapshot_rows(bucket: str, rows: list[Any]) -> None:
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise _remote_snapshot_error(f"{bucket}[{index}] must be an object")


def _validate_remote_snapshot_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise _remote_snapshot_error("expected a JSON object")
    _warn_unexpected_remote_snapshot_keys(payload)
    _require_remote_snapshot_schema_version(payload)
    _require_remote_snapshot_generated_at(payload)
    _validate_remote_snapshot_rows("active", _require_remote_snapshot_row_list(payload, "active"))
    _validate_remote_snapshot_rows("pending", _require_remote_snapshot_row_list(payload, "pending"))
    rejected_rows = payload.get("rejected")
    if rejected_rows is not None:
        _validate_remote_snapshot_rows(
            "rejected", _require_remote_snapshot_row_list(payload, "rejected")
        )
    return payload


def _validate_normalized_remote_snapshot(snapshot: dict[str, Any]) -> None:
    for bucket in ("active", "pending"):
        rows = snapshot.get(bucket)
        if not isinstance(rows, list):
            raise _remote_snapshot_error(f"{bucket} must remain an array after normalization")
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                raise _remote_snapshot_error(
                    f"{bucket}[{index}] must remain an object after normalization"
                )
            if not str(row.get("id") or "").strip():
                raise _remote_snapshot_error(
                    f"{bucket}[{index}] missing source identity after normalization"
                )


def normalize_snapshot(module: Any, payload: Mapping[str, Any]) -> dict[str, Any]:
    data = payload if isinstance(payload, dict) else {}
    generated_at = str(data.get("generatedAt") or "")
    return {
        "schemaVersion": int(data.get("schemaVersion") or 1),
        "generatedAt": generated_at,
        "source": data.get("source") if isinstance(data.get("source"), dict) else {},
        "active": _canonicalize_snapshot_rows(
            list(data.get("active") or []), bucket="active", generated_at=generated_at
        ),
        "pending": _canonicalize_snapshot_rows(
            list(data.get("pending") or []), bucket="pending", generated_at=generated_at
        ),
        "rejected": _canonicalize_snapshot_rows(
            list(data.get("rejected") or []), bucket="rejected", generated_at=generated_at
        ),
    }


def _remote_snapshot_payload_view(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "schemaVersion": int(payload.get("schemaVersion") or 1),
        "generatedAt": str(payload.get("generatedAt") or ""),
        "source": payload.get("source") if isinstance(payload.get("source"), dict) else {},
        "active": list(payload.get("active") or []),
        "pending": list(payload.get("pending") or []),
        "rejected": list(payload.get("rejected") or []),
    }


def _normalized_remote_snapshot_result(
    module: Any,
    payload: dict[str, Any],
    *,
    sha: str,
    snapshot_format: str,
) -> dict[str, Any]:
    snapshot = normalize_snapshot(module, _validate_remote_snapshot_payload(payload))
    _validate_normalized_remote_snapshot(snapshot)
    module._clear_runtime_state(module.RUNTIME_STATE_REMOTE_CONFLICT)
    return {
        "exists": True,
        "sha": str(sha or ""),
        "snapshot": snapshot,
        "snapshotFormat": snapshot_format,
    }


def _read_sharded_remote_snapshot(
    module: Any,
    config: Any,
    *,
    progress_callback: Callable[..., None] | None = None,
    known_remote_sha: str = "",
    max_shard_read_workers: int | None = None,
    opener: Callable[..., Any],
) -> dict[str, Any] | None:
    sharded_snapshot = read_sharded_snapshot(
        module,
        config,
        progress_callback=progress_callback,
        known_manifest_sha=known_remote_sha,
        max_workers=max_shard_read_workers,
        opener=opener,
    )
    if sharded_snapshot is None:
        return None
    if bool(sharded_snapshot.get("skipped")):
        return {
            "exists": True,
            "sha": str(sharded_snapshot.get("manifestSha") or ""),
            "snapshot": {},
            "snapshotFormat": "sharded-v3",
            "committedManifest": dict(sharded_snapshot.get("manifest") or {}),
            "remoteGeneratedAt": str(sharded_snapshot.get("generatedAt") or ""),
            "skipped": True,
            "skipReason": str(sharded_snapshot.get("skipReason") or ""),
            "shardCount": int(sharded_snapshot.get("shardCount") or 0),
            "shardsReadBytes": int(sharded_snapshot.get("shardsReadBytes") or 0),
            "totalShardBytes": int(sharded_snapshot.get("totalShardBytes") or 0),
            "manifestSizeBytes": int(sharded_snapshot.get("manifestSizeBytes") or 0),
        }
    result = _normalized_remote_snapshot_result(
        module,
        _remote_snapshot_payload_view(sharded_snapshot),
        sha=str(sharded_snapshot.get("manifestSha") or ""),
        snapshot_format="sharded-v3",
    )
    result["committedManifest"] = dict(sharded_snapshot.get("manifest") or {})
    result["shardCount"] = int(sharded_snapshot.get("shardCount") or 0)
    result["shardsReadBytes"] = int(sharded_snapshot.get("shardsReadBytes") or 0)
    result["totalShardBytes"] = int(sharded_snapshot.get("totalShardBytes") or 0)
    result["manifestSizeBytes"] = int(sharded_snapshot.get("manifestSizeBytes") or 0)
    return result


def _read_remote_snapshot_download_url(
    module: Any,
    config: Any,
    payload: dict[str, Any],
    *,
    opener: Callable[..., Any],
) -> dict[str, Any]:
    download_url = str(payload.get("download_url") or "").strip()
    if not download_url:
        return {"exists": False, "sha": str(payload.get("sha") or ""), "snapshot": None}
    raw_status, raw_body, _raw_headers = module._request_raw_json(
        method="GET",
        url=download_url,
        headers=module._github_json_headers(
            f"Bearer {module._get_auth_manager(config).get_installation_token(opener=opener)}"
        ),
        timeout_s=config.timeout_s,
        opener=opener,
    )
    if raw_status == 200 and isinstance(raw_body, dict):
        return _normalized_remote_snapshot_result(
            module,
            raw_body,
            sha=str(payload.get("sha") or ""),
            snapshot_format="monolithic-v2",
        )
    return {"exists": False, "sha": str(payload.get("sha") or ""), "snapshot": None}


def _read_monolithic_remote_snapshot(
    module: Any,
    config: Any,
    *,
    opener: Callable[..., Any],
) -> dict[str, Any]:
    url = module._content_api_url(config, with_ref=True)
    status, payload, _headers = module._request_json(
        method="GET",
        url=url,
        config=config,
        timeout_s=config.timeout_s,
        opener=opener,
    )
    if status == 404:
        module._clear_runtime_state(module.RUNTIME_STATE_REMOTE_CONFLICT)
        return {"exists": False, "sha": "", "snapshot": None}
    if status >= 400:
        message = str(payload.get("message") or f"GitHub GET failed with HTTP {status}")
        raise RuntimeError(message)
    encoded_content = str(payload.get("content") or "").strip()
    if not encoded_content:
        return _read_remote_snapshot_download_url(module, config, payload, opener=opener)
    try:
        raw_bytes = base64.b64decode(encoded_content.replace("\n", ""))
        parsed = json.loads(raw_bytes.decode("utf-8"))
    except (ValueError, json.JSONDecodeError) as exc:
        raise _remote_snapshot_error(f"invalid JSON payload: {exc}") from exc
    return _normalized_remote_snapshot_result(
        module,
        parsed,
        sha=str(payload.get("sha") or ""),
        snapshot_format="monolithic-v2",
    )


def merge_registry_state(
    module: Any, local_state: Mapping[str, Any], remote_snapshot: Mapping[str, Any]
) -> dict[str, list[dict[str, Any]]]:
    remote = normalize_snapshot(module, remote_snapshot)
    tombstones = module.load_tombstones()
    generated_at = str(remote.get("generatedAt") or "")
    local = {
        "active": module.filter_tombstoned_rows(
            _canonicalize_snapshot_rows(
                list(local_state.get("active") or []), bucket="active", generated_at=generated_at
            ),
            tombstones,
        ),
        "pending": module.filter_tombstoned_rows(
            _canonicalize_snapshot_rows(
                list(local_state.get("pending") or []), bucket="pending", generated_at=generated_at
            ),
            tombstones,
        ),
        "rejected": module.filter_tombstoned_rows(
            _canonicalize_snapshot_rows(
                list(local_state.get("rejected") or []),
                bucket="rejected",
                generated_at=generated_at,
            ),
            tombstones,
        ),
    }
    local_rejected_ids = {
        source_identity(row) for row in local["rejected"] if isinstance(row, dict)
    }
    merged: dict[str, list[dict[str, Any]]] = {
        "active": [],
        "pending": [],
        "rejected": sort_sources_by_identity(local["rejected"]),
    }
    candidates: dict[str, dict[str, Any]] = {}
    for bucket in ("active", "pending"):
        for row in local[bucket]:
            candidates[source_identity(row)] = dict(row)
    for bucket in ("active", "pending"):
        for row in remote[bucket]:
            row_id = source_identity(row)
            if row_id in local_rejected_ids:
                continue
            candidates[row_id] = dict(_choose_more_recent_row(candidates.get(row_id), row) or row)
    for row in candidates.values():
        bucket = str(row.get("registryState") or "").strip().lower()
        if bucket == "active":
            merged["active"].append(ensure_source_id(row))
        elif bucket == "pending":
            merged["pending"].append(ensure_source_id(row))
    merged["active"] = sort_sources_by_identity(merged["active"])
    merged["pending"] = sort_sources_by_identity(merged["pending"])
    return merged


def read_remote_snapshot(
    module: Any,
    config: Any,
    *,
    opener: Callable[..., Any],
    prefer_sharded: bool = False,
    progress_callback: Callable[..., None] | None = None,
    known_remote_sha: str = "",
    max_shard_read_workers: int | None = None,
) -> dict[str, Any]:
    module.validate_sync_config(config)

    def _read_once() -> dict[str, Any]:
        if prefer_sharded:
            sharded_result = _read_sharded_remote_snapshot(
                module,
                config,
                progress_callback=progress_callback,
                known_remote_sha=known_remote_sha,
                max_shard_read_workers=max_shard_read_workers,
                opener=opener,
            )
            if sharded_result is not None:
                return sharded_result
        return _read_monolithic_remote_snapshot(module, config, opener=opener)

    return _retry_transient_get(_read_once)


def build_snapshot(
    module: Any, local_state: dict[str, Any], *, source_label: str = "admin_bridge"
) -> dict[str, Any]:
    generated_at = module.now_iso()
    canonical_state = merge_registry_state(
        module,
        local_state,
        {
            "schemaVersion": module.SYNC_SCHEMA_VERSION,
            "generatedAt": generated_at,
            "source": {"name": source_label},
            "active": [],
            "pending": [],
            "rejected": [],
        },
    )
    canonical_state = {
        "active": _canonicalize_snapshot_rows(
            list(canonical_state.get("active") or []), bucket="active", generated_at=generated_at
        ),
        "pending": _canonicalize_snapshot_rows(
            list(canonical_state.get("pending") or []), bucket="pending", generated_at=generated_at
        ),
    }
    return {
        "schemaVersion": module.SYNC_SCHEMA_VERSION,
        "generatedAt": generated_at,
        "source": {"name": source_label},
        "active": canonical_state["active"],
        "pending": canonical_state["pending"],
    }


def write_remote_snapshot(
    module: Any,
    config: Any,
    snapshot: dict[str, Any],
    *,
    sha: str = "",
    message: str = "Update Baluffo source sync snapshot",
    opener: Callable[..., Any],
) -> dict[str, Any]:
    module.validate_sync_config(config)
    encoded = base64.b64encode(
        json.dumps(snapshot, ensure_ascii=False, indent=2).encode("utf-8")
    ).decode("ascii")
    payload: dict[str, Any] = {
        "message": str(message or "Update Baluffo source sync snapshot"),
        "content": encoded,
        "branch": config.branch,
    }
    if sha:
        payload["sha"] = sha
    status, body, _headers = module._request_json(
        method="PUT",
        url=module._content_api_url(config, with_ref=False),
        config=config,
        timeout_s=config.timeout_s,
        payload=payload,
        opener=opener,
    )
    if status >= 400:
        msg = str(body.get("message") or f"GitHub PUT failed with HTTP {status}")
        if int(status or 0) == 409:
            module._set_runtime_state(module.RUNTIME_STATE_REMOTE_CONFLICT, msg)
            raise module.SyncOperationError(module.RUNTIME_STATE_REMOTE_CONFLICT, msg)
        raise RuntimeError(msg)
    content = body.get("content") if isinstance(body.get("content"), dict) else {}
    module._clear_runtime_state(module.RUNTIME_STATE_REMOTE_CONFLICT)
    return {"ok": True, "sha": str(content.get("sha") or "")}


def pull_and_merge_sources(
    module: Any,
    config: Any,
    local_state: dict[str, Any],
    *,
    progress_callback: Callable[..., None] | None = None,
    known_remote_sha: str = "",
    max_shard_read_workers: int | None = None,
    opener: Callable[..., Any],
) -> dict[str, Any]:
    module.record_sync_counters(totalPulls=1)
    remote = read_remote_snapshot(
        module,
        config,
        opener=opener,
        prefer_sharded=True,
        progress_callback=progress_callback,
        known_remote_sha=known_remote_sha,
        max_shard_read_workers=max_shard_read_workers,
    )
    empty_remote = {
        "schemaVersion": module.SYNC_SCHEMA_VERSION,
        "generatedAt": "",
        "source": {},
        "active": [],
        "pending": [],
        "rejected": [],
    }
    if not remote.get("exists"):
        canonical_local = merge_registry_state(module, local_state, empty_remote)
        return {
            "changed": False,
            "remoteFound": False,
            "mergedState": canonical_local,
            "remoteSha": "",
            "counters": module.sync_counters_payload(),
        }
    if bool(remote.get("skipped")):
        counters = module.record_sync_counters(noOpSkips=1)
        canonical_local = merge_registry_state(module, local_state, empty_remote)
        return {
            "changed": False,
            "remoteFound": True,
            "remoteSha": str(remote.get("sha") or ""),
            "mergedState": canonical_local,
            "remoteGeneratedAt": str(remote.get("remoteGeneratedAt") or ""),
            "snapshotFormat": str(remote.get("snapshotFormat") or ""),
            "shardCount": int(remote.get("shardCount") or 0),
            "shardsReadBytes": int(remote.get("shardsReadBytes") or 0),
            "totalShardBytes": int(remote.get("totalShardBytes") or 0),
            "manifestSizeBytes": int(remote.get("manifestSizeBytes") or 0),
            "skipped": True,
            "skipReason": str(remote.get("skipReason") or ""),
            "counters": counters,
        }
    snapshot = _as_dict(remote.get("snapshot"))
    merged_state = merge_registry_state(module, local_state, snapshot)
    changed = json.dumps(merged_state, sort_keys=True, ensure_ascii=False) != json.dumps(
        merge_registry_state(module, local_state, empty_remote),
        sort_keys=True,
        ensure_ascii=False,
    )
    local_count = len(list(_as_dict(local_state).get("active") or [])) + len(
        list(_as_dict(local_state).get("pending") or [])
    )
    merged_count = len(list(merged_state.get("active") or [])) + len(
        list(merged_state.get("pending") or [])
    )
    if merged_count > local_count:
        module.record_sync_counters(sourcesAdded=merged_count - local_count)
    elif merged_count < local_count:
        module.record_sync_counters(sourcesRemoved=local_count - merged_count)
    return {
        "changed": changed,
        "remoteFound": True,
        "remoteSha": str(remote.get("sha") or ""),
        "mergedState": merged_state,
        "remoteGeneratedAt": str(snapshot.get("generatedAt") or ""),
        "snapshotFormat": str(remote.get("snapshotFormat") or ""),
        "shardCount": int(remote.get("shardCount") or 0),
        "shardsReadBytes": int(remote.get("shardsReadBytes") or 0),
        "totalShardBytes": int(remote.get("totalShardBytes") or 0),
        "manifestSizeBytes": int(remote.get("manifestSizeBytes") or 0),
        "skipped": False,
        "skipReason": "",
        "counters": module.sync_counters_payload(),
    }


def push_sources_snapshot(
    module: Any,
    config: Any,
    local_state: dict[str, Any],
    *,
    dry_run: bool = False,
    progress_callback: Callable[..., None] | None = None,
    opener: Callable[..., Any],
) -> dict[str, Any]:
    detail_timing = _SyncDetailTiming()
    with detail_timing.record("readRemoteSnapshot"):
        remote = read_remote_snapshot(module, config, opener=opener, prefer_sharded=True)
    remote_snapshot = _as_dict(remote.get("snapshot"))
    remote_sha = str(remote.get("sha") or "")
    remote_format = str(remote.get("snapshotFormat") or "")
    with detail_timing.record("validateLocalIdentity"):
        _assert_unique_snapshot_identity(
            module,
            list(_as_dict(local_state).get("active") or [])
            + list(_as_dict(local_state).get("pending") or []),
            scope="local active/pending snapshot",
        )
    with detail_timing.record("validateRemoteIdentity"):
        _assert_unique_snapshot_identity(
            module,
            list(remote_snapshot.get("active") or []) + list(remote_snapshot.get("pending") or []),
            scope="remote active/pending snapshot",
        )
    with detail_timing.record("mergeRegistryState"):
        merged_state = merge_registry_state(module, local_state, remote_snapshot)
    with detail_timing.record("buildSnapshot"):
        snapshot = build_snapshot(module, merged_state)
    with detail_timing.record("fingerprintLocal"):
        snapshot_fingerprint = _snapshot_content_fingerprint(module, snapshot)
    with detail_timing.record("fingerprintRemote"):
        remote_fingerprint = _snapshot_content_fingerprint(module, remote_snapshot)
    remote_exists = bool(remote.get("exists"))
    with detail_timing.record("measureSnapshotSize"):
        snapshot_size_bytes = _snapshot_size_bytes(snapshot)
    max_snapshot_size_bytes = int(
        getattr(config, "max_snapshot_size_bytes", module.DEFAULT_MAX_SNAPSHOT_SIZE_BYTES)
        or module.DEFAULT_MAX_SNAPSHOT_SIZE_BYTES
    )
    size_warning = snapshot_size_bytes > module.SNAPSHOT_SIZE_WARN_BYTES
    would_change = (
        not remote_exists
        or snapshot_fingerprint != remote_fingerprint
        or remote_format != "sharded-v3"
    )
    committed_manifest = _remote_committed_manifest(remote)
    if (
        not dry_run
        and remote_exists
        and remote_format == "sharded-v3"
        and snapshot_fingerprint == remote_fingerprint
        and committed_manifest is not None
    ):
        with detail_timing.record("deriveNoopShardMetadata"):
            shard_fields = _manifest_noop_shard_metadata(module, committed_manifest)
        with detail_timing.record("recordStorageMetrics"):
            record_source_sync_snapshot(
                size_bytes=snapshot_size_bytes,
                max_snapshot_size_bytes=max_snapshot_size_bytes,
                size_warning=size_warning,
                would_change=False,
                snapshot_format=str(shard_fields.get("snapshotFormat") or ""),
                shard_count=int(shard_fields.get("shardCount") or 0),
                changed_shard_count=0,
                shards_pushed_bytes=0,
                manifest_size_bytes=int(shard_fields.get("manifestSizeBytes") or 0),
                shard_cap_bytes=int(shard_fields.get("shardCapBytes") or 0),
                shard_hashes=dict(shard_fields.get("shardHashes") or {}),
            )
        module.record_sync_counters(totalPushes=1)
        counters = module.record_sync_counters(noOpSkips=1)
        return _with_detail_timing(
            {
                "pushed": False,
                "remotePreviouslyExisted": True,
                "remoteSha": remote_sha,
                "snapshot": snapshot,
                "skipped": True,
                "skipReason": "no_meaningful_change",
                "sizeBytes": snapshot_size_bytes,
                "sizeWarning": size_warning,
                "maxSnapshotSizeBytes": max_snapshot_size_bytes,
                **shard_fields,
                "counters": counters,
            },
            detail_timing,
        )
    try:
        with detail_timing.record("buildShardBundle"):
            shard_bundle = _sharded_snapshot_bundle(module, snapshot, remote)
    except SourceSyncShardError as exc:
        raise _snapshot_too_large_error(
            module,
            exc,
            snapshot_size_bytes=snapshot_size_bytes,
            max_snapshot_size_bytes=max_snapshot_size_bytes,
            size_warning=size_warning,
        ) from exc
    shard_fields = _shard_bundle_metadata(shard_bundle)
    with detail_timing.record("recordStorageMetrics"):
        record_source_sync_snapshot(
            size_bytes=snapshot_size_bytes,
            max_snapshot_size_bytes=max_snapshot_size_bytes,
            size_warning=size_warning,
            would_change=would_change,
            snapshot_format=str(shard_fields.get("snapshotFormat") or ""),
            shard_count=int(shard_fields.get("shardCount") or 0),
            changed_shard_count=int(shard_fields.get("changedShardCount") or 0),
            shards_pushed_bytes=int(shard_fields.get("shardsPushedBytes") or 0),
            manifest_size_bytes=int(shard_fields.get("manifestSizeBytes") or 0),
            shard_cap_bytes=int(shard_fields.get("shardCapBytes") or 0),
            shard_hashes=dict(shard_fields.get("shardHashes") or {}),
        )
    if dry_run:
        return _with_detail_timing(
            {
                "pushed": False,
                "remotePreviouslyExisted": remote_exists,
                "remoteSha": remote_sha,
                "snapshot": snapshot,
                "skipped": True,
                "skipReason": "dryRun",
                "dryRun": True,
                "wouldChange": would_change,
                "sizeBytes": snapshot_size_bytes,
                "sizeWarning": size_warning,
                "maxSnapshotSizeBytes": max_snapshot_size_bytes,
                **shard_fields,
                "counters": module.sync_counters_payload(),
            },
            detail_timing,
        )
    module.record_sync_counters(totalPushes=1)
    if (
        remote_exists
        and remote_format == "sharded-v3"
        and snapshot_fingerprint == remote_fingerprint
    ):
        counters = module.record_sync_counters(noOpSkips=1)
        return _with_detail_timing(
            {
                "pushed": False,
                "remotePreviouslyExisted": True,
                "remoteSha": remote_sha,
                "snapshot": snapshot,
                "skipped": True,
                "skipReason": "no_meaningful_change",
                "sizeBytes": snapshot_size_bytes,
                "sizeWarning": size_warning,
                "maxSnapshotSizeBytes": max_snapshot_size_bytes,
                **shard_fields,
                "counters": counters,
            },
            detail_timing,
        )
    try:
        with detail_timing.record("writeShardedSnapshot"):
            write_result = _push_sharded_snapshot_result(
                module,
                config,
                snapshot,
                remote,
                bundle=shard_bundle,
                progress_callback=progress_callback,
                opener=opener,
            )
    except SourceSyncShardError as exc:
        raise _snapshot_too_large_error(
            module,
            exc,
            snapshot_size_bytes=snapshot_size_bytes,
            max_snapshot_size_bytes=max_snapshot_size_bytes,
            size_warning=size_warning,
        ) from exc
    except module.SyncOperationError as exc:
        if exc.code != module.RUNTIME_STATE_REMOTE_CONFLICT:
            raise
        return _push_sources_snapshot_after_conflict(
            module,
            config,
            local_state,
            snapshot,
            snapshot_fingerprint,
            snapshot_size_bytes,
            size_warning,
            max_snapshot_size_bytes,
            shard_fields,
            remote,
            opener,
            progress_callback,
            exc,
            detail_timing,
        )
    except RuntimeError as exc:
        if not _is_transient_request_error(exc):
            raise
        return _push_sources_snapshot_after_transient(
            module,
            config,
            local_state,
            snapshot,
            snapshot_fingerprint,
            snapshot_size_bytes,
            size_warning,
            max_snapshot_size_bytes,
            shard_fields,
            remote,
            remote_sha,
            opener,
            progress_callback,
            exc,
            detail_timing,
        )
    warnings = list(write_result.get("warnings") or [])
    return _with_detail_timing(
        {
            "pushed": bool(write_result.get("pushed", True)),
            "remotePreviouslyExisted": bool(remote.get("exists")),
            "remoteSha": str(write_result.get("remoteSha") or remote_sha),
            "snapshot": snapshot,
            "skipped": bool(write_result.get("skipped", False)),
            "skipReason": str(write_result.get("skipReason") or ""),
            "sizeBytes": snapshot_size_bytes,
            "sizeWarning": size_warning,
            "maxSnapshotSizeBytes": max_snapshot_size_bytes,
            **_shard_push_metadata(write_result),
            "warnings": warnings,
            "counters": module.sync_counters_payload(),
        },
        detail_timing,
    )
