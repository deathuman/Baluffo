"""Source-sync snapshot push and pull orchestration.

AI boundary owns: source-sync push decision flow (dry-run, no-op, write), pull-and-merge flow, and storage metric recording.
AI boundary implement in: this leaf for push/pull orchestration; merge semantics stay in the source_sync_snapshot coordinator, remote IO in source_sync_snapshot_remote, recovery in source_sync_snapshot_push_retry, and shard metadata in source_sync_snapshot_shard_metadata.
AI boundary search before contracts: source sync facade, bridge sync task flow, snapshot size limits, and push churn tests.
AI boundary verify: `python -m pytest tests/test_source_sync.py tests/test_source_sync_push_churn.py tests/test_source_sync_push_churn_limits.py -q`.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from typing import Any

from src.shared.json_shapes import as_json_object as _as_dict
from src.source_sync_shard import SourceSyncShardError, read_manifest
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


def _sorted_row_payloads(rows: Any) -> list[str]:
    return sorted(
        json.dumps(dict(row), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        for row in list(rows or [])
        if isinstance(row, dict)
    )


def _local_registry_revision(module: Any, local_state: dict[str, Any]) -> str:
    """Fingerprint every local input that can change a pushed snapshot.

    Rejected rows and tombstones are merge inputs: rejected rows keep a source
    from returning through the remote side, and tombstones filter both local and
    remote rows. Row order is normalized, so a rewrite that only reorders rows
    keeps the same revision.
    """
    payload = {
        "active": _sorted_row_payloads(local_state.get("active")),
        "pending": _sorted_row_payloads(local_state.get("pending")),
        "rejected": _sorted_row_payloads(local_state.get("rejected")),
        "tombstones": _as_dict(module.load_tombstones()),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _read_known_manifest(
    module: Any, config: Any, *, opener: Callable[..., Any]
) -> dict[str, Any] | None:
    """Read only the committed manifest so an unchanged remote costs one GET.

    A transport or format error falls through to the full read, which applies the
    normal retry and error mapping instead of failing the cheap check.
    """
    try:
        return read_manifest(module, config, opener=opener)
    except (SourceSyncShardError, RuntimeError):
        return None


def _manifest_bucket_counts(manifest: Mapping[str, Any], *, rejected_count: int) -> dict[str, int]:
    """Count remote rows from the committed manifest.

    The unchanged fast path never downloads shards, so the local snapshot it
    builds can be missing rows another client merged into the remote. The
    manifest's per-shard bucket and rowCount are the authoritative counts.
    """
    counts = {"active": 0, "pending": 0}
    for entry in list(manifest.get("shards") or []):
        if not isinstance(entry, Mapping):
            continue
        bucket = str(entry.get("bucket") or "")
        if bucket in counts:
            counts[bucket] += int(entry.get("rowCount") or 0)
    counts["rejected"] = max(0, int(rejected_count))
    return counts


def _unchanged_fast_path_result(
    module: Any,
    config: Any,
    local_state: dict[str, Any],
    *,
    manifest_read: dict[str, Any],
    dry_run: bool,
    detail_timing: _SyncDetailTiming,
) -> dict[str, Any]:
    with detail_timing.record("buildSnapshot"):
        snapshot = build_snapshot(module, local_state)
    with detail_timing.record("measureSnapshotSize"):
        snapshot_size_bytes = _snapshot_size_bytes(snapshot)
    max_snapshot_size_bytes = int(
        getattr(config, "max_snapshot_size_bytes", module.DEFAULT_MAX_SNAPSHOT_SIZE_BYTES)
        or module.DEFAULT_MAX_SNAPSHOT_SIZE_BYTES
    )
    size_warning = snapshot_size_bytes > module.SNAPSHOT_SIZE_WARN_BYTES
    manifest = _as_dict(manifest_read.get("manifest"))
    shard_fields = _manifest_noop_shard_metadata(module, manifest)
    counts = _manifest_bucket_counts(
        manifest, rejected_count=len(list(local_state.get("rejected") or []))
    )
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
    if dry_run:
        counters = module.sync_counters_payload()
    else:
        module.record_sync_counters(totalPushes=1)
        counters = module.record_sync_counters(noOpSkips=1)
    payload: dict[str, Any] = {
        "pushed": False,
        "remotePreviouslyExisted": True,
        "remoteSha": str(manifest_read.get("sha") or ""),
        "snapshot": snapshot,
        "counts": counts,
        "skipped": True,
        "skipReason": "dryRun" if dry_run else "remote_and_local_unchanged",
        "remoteReadSkipped": True,
        "sizeBytes": snapshot_size_bytes,
        "sizeWarning": size_warning,
        "maxSnapshotSizeBytes": max_snapshot_size_bytes,
        **shard_fields,
        "counters": counters,
    }
    if dry_run:
        payload["dryRun"] = True
        payload["wouldChange"] = False
    return _with_detail_timing(payload, detail_timing)


def push_sources_snapshot(
    module: Any,
    config: Any,
    local_state: dict[str, Any],
    *,
    dry_run: bool = False,
    progress_callback: Callable[..., None] | None = None,
    known_remote_sha: str = "",
    known_local_revision: str = "",
    opener: Callable[..., Any],
) -> dict[str, Any]:
    """Push the local registry snapshot to the committed remote generation.

    When the caller passes the manifest SHA and local registry revision persisted
    by the last successful push and neither has changed, the merge inputs are
    unchanged: only the manifest is read (one GET) and the push reports
    `skipReason: "remote_and_local_unchanged"` without downloading any shards.

    Returns:
        Push result with shard metrics, `counters`, and the computed
        `localRevision` for the caller to persist.
    """
    local_revision = _local_registry_revision(module, local_state)
    result = _push_sources_snapshot_impl(
        module,
        config,
        local_state,
        dry_run=dry_run,
        progress_callback=progress_callback,
        known_remote_sha=known_remote_sha,
        known_local_revision=known_local_revision,
        local_revision=local_revision,
        opener=opener,
    )
    result["localRevision"] = local_revision
    return result


def _try_unchanged_fast_path(
    module: Any,
    config: Any,
    local_state: dict[str, Any],
    *,
    known_remote_sha: str,
    known_local_revision: str,
    local_revision: str,
    dry_run: bool,
    detail_timing: _SyncDetailTiming,
    opener: Callable[..., Any],
) -> dict[str, Any] | None:
    if not known_remote_sha or not known_local_revision or known_local_revision != local_revision:
        return None
    with detail_timing.record("readRemoteManifest"):
        manifest_read = _read_known_manifest(module, config, opener=opener)
    if manifest_read is None or str(manifest_read.get("sha") or "") != known_remote_sha:
        return None
    return _unchanged_fast_path_result(
        module,
        config,
        local_state,
        manifest_read=manifest_read,
        dry_run=dry_run,
        detail_timing=detail_timing,
    )


def _try_noop_sharded_push(
    module: Any,
    *,
    snapshot: dict[str, Any],
    snapshot_size_bytes: int,
    max_snapshot_size_bytes: int,
    size_warning: bool,
    remote: dict[str, Any],
    snapshot_fingerprint: str,
    remote_fingerprint: str,
    dry_run: bool,
    detail_timing: _SyncDetailTiming,
) -> dict[str, Any] | None:
    if dry_run or not bool(remote.get("exists")):
        return None
    if str(remote.get("snapshotFormat") or "") != "sharded-v3":
        return None
    if snapshot_fingerprint != remote_fingerprint:
        return None
    committed_manifest = _remote_committed_manifest(remote)
    if committed_manifest is None:
        return None
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
            "remoteSha": str(remote.get("sha") or ""),
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


def _push_sharded_snapshot_with_recovery(
    module: Any,
    config: Any,
    local_state: dict[str, Any],
    snapshot: dict[str, Any],
    remote: dict[str, Any],
    snapshot_fingerprint: str,
    snapshot_size_bytes: int,
    size_warning: bool,
    max_snapshot_size_bytes: int,
    shard_fields: dict[str, Any],
    shard_bundle: Any,
    remote_sha: str,
    opener: Callable[..., Any],
    progress_callback: Callable[..., None] | None,
    detail_timing: _SyncDetailTiming,
) -> dict[str, Any]:
    try:
        with detail_timing.record("writeShardedSnapshot"):
            return _push_sharded_snapshot_result(
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


def _push_sources_snapshot_impl(
    module: Any,
    config: Any,
    local_state: dict[str, Any],
    *,
    dry_run: bool = False,
    progress_callback: Callable[..., None] | None = None,
    known_remote_sha: str = "",
    known_local_revision: str = "",
    local_revision: str = "",
    opener: Callable[..., Any],
) -> dict[str, Any]:
    detail_timing = _SyncDetailTiming()
    unchanged = _try_unchanged_fast_path(
        module,
        config,
        local_state,
        known_remote_sha=known_remote_sha,
        known_local_revision=known_local_revision,
        local_revision=local_revision,
        dry_run=dry_run,
        detail_timing=detail_timing,
        opener=opener,
    )
    if unchanged is not None:
        return unchanged
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
    no_op = _try_noop_sharded_push(
        module,
        snapshot=snapshot,
        snapshot_size_bytes=snapshot_size_bytes,
        max_snapshot_size_bytes=max_snapshot_size_bytes,
        size_warning=size_warning,
        remote=remote,
        snapshot_fingerprint=snapshot_fingerprint,
        remote_fingerprint=remote_fingerprint,
        dry_run=dry_run,
        detail_timing=detail_timing,
    )
    if no_op is not None:
        return no_op
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
    write_result = _push_sharded_snapshot_with_recovery(
        module,
        config,
        local_state,
        snapshot,
        remote,
        snapshot_fingerprint,
        snapshot_size_bytes,
        size_warning,
        max_snapshot_size_bytes,
        shard_fields,
        shard_bundle,
        remote_sha,
        opener,
        progress_callback,
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
        "shardReadDurationMs": int(remote.get("shardReadDurationMs") or 0),
        "totalShardBytes": int(remote.get("totalShardBytes") or 0),
        "manifestSizeBytes": int(remote.get("manifestSizeBytes") or 0),
        "skipped": False,
        "skipReason": "",
        "counters": module.sync_counters_payload(),
    }
