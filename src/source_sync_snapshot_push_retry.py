"""Source-sync snapshot push recovery: remote-conflict and transient-error paths.

AI boundary owns: source-sync push recovery after a remote-conflict rejection or a transient GET failure, including re-merge and re-write.
AI boundary implement in: this leaf for push recovery semantics; the main push decision flow stays in source_sync_snapshot_push.
AI boundary search before contracts: bridge sync conflict handling, push churn tests, and transient retry tests.
AI boundary verify: `python -m pytest tests/test_source_sync.py tests/test_source_sync_push_churn.py -q`.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from src.shared.json_shapes import as_json_object as _as_dict
from src.source_sync_shard import SourceSyncShardError
from src.source_sync_snapshot import (
    build_snapshot,
    merge_registry_state,
)
from src.source_sync_snapshot_remote import read_remote_snapshot
from src.source_sync_snapshot_shard_metadata import (
    _push_sharded_snapshot_result,
    _shard_push_metadata,
    _snapshot_content_fingerprint,
    _snapshot_size_bytes,
    _snapshot_too_large_error,
)
from src.source_sync_snapshot_timing import (
    _record_detail_stage,
    _SyncDetailTiming,
    _with_detail_timing,
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
