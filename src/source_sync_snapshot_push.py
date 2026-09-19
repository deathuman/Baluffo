"""Source-sync snapshot push and pull orchestration.

AI boundary owns: source-sync push decision flow (dry-run, no-op, write), pull-and-merge flow, and storage metric recording.
AI boundary implement in: this leaf for push/pull orchestration; merge semantics stay in the source_sync_snapshot coordinator, remote IO in source_sync_snapshot_remote, recovery in source_sync_snapshot_push_retry, and shard metadata in source_sync_snapshot_shard_metadata.
AI boundary search before contracts: source sync facade, bridge sync task flow, snapshot size limits, and push churn tests.
AI boundary verify: `python -m pytest tests/test_source_sync.py tests/test_source_sync_push_churn.py tests/test_source_sync_push_churn_limits.py -q`.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from src.shared.json_shapes import as_json_object as _as_dict
from src.source_sync_shard import SourceSyncShardError
from src.source_sync_snapshot import (
    build_snapshot,
    merge_registry_state,
)
from src.source_sync_snapshot_push_retry import (
    _push_sources_snapshot_after_conflict,
    _push_sources_snapshot_after_transient,
)
from src.source_sync_snapshot_remote import (
    _is_transient_request_error,
    read_remote_snapshot,
)
from src.source_sync_snapshot_shard_metadata import (
    _assert_unique_snapshot_identity,
    _manifest_noop_shard_metadata,
    _push_sharded_snapshot_result,
    _remote_committed_manifest,
    _shard_bundle_metadata,
    _shard_push_metadata,
    _sharded_snapshot_bundle,
    _snapshot_content_fingerprint,
    _snapshot_size_bytes,
    _snapshot_too_large_error,
)
from src.source_sync_snapshot_timing import (
    _SyncDetailTiming,
    _with_detail_timing,
)
from src.storage_metrics import record_source_sync_snapshot


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
