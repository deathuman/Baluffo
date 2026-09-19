"""Deterministic source-sync shard construction and remote transfer coordinator.

AI boundary owns: source-sync shard bundle assembly, changed-shard push orchestration, and sharded snapshot read/write sequencing.
AI boundary implement in: this coordinator for orchestration seams; shard payloads live in src.source_sync_shard_model, manifest rules in src.source_sync_shard_manifest, transport in src.source_sync_shard_remote, pruning in src.source_sync_shard_gc, and progress payloads in src.source_sync_shard_progress.
AI boundary search before contracts: source sync facade, snapshot normalization, registry persistence, and shard tests.
AI boundary verify: `python -m pytest tests/test_source_sync_shard.py tests/test_source_sync_shard_io.py tests/test_source_sync_sharded_push.py tests/test_source_sync_shard_pull_performance.py -q`.
"""

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
from typing import Any, cast
from urllib.parse import quote

from src.source_registry_identity import source_identity
from src.source_sync_shard_gc import prune_unreferenced_shards
from src.source_sync_shard_manifest import (
    _manifest_source,
    build_manifest,
    changed_shards,
    manifest_path,
    trusted_committed_manifest,
    validate_manifest,
)
from src.source_sync_shard_model import (
    _EXPECTED_REMOTE_SYNC_EXCEPTIONS,
    DEFAULT_BASE_PATH,
    DEFAULT_GC_DELETE_LIMIT,
    DEFAULT_MANIFEST_FILE_NAME,
    DEFAULT_PREFIX_LENGTH,
    DEFAULT_SHARD_READ_WORKERS,
    DEFAULT_SHARD_WRITE_WORKERS,
    MAX_PREFIX_LENGTH,
    PREFIX_LENGTH_STEP,
    SHARD_SCHEMA_VERSION,
    Shard,
    SourceSyncShardError,
    build_shards,
    content_addressed_shards,
    shard_key,
)
from src.source_sync_shard_progress import (
    _emit_pull_progress,
    _emit_push_progress,
    _shard_progress_counts,
    _shard_pull_progress_counts,
)
from src.source_sync_shard_remote import (
    _duration_ms,
    _remote_timing_row,
    _remote_timing_summary,
    push_manifest,
    push_shard,
    read_manifest,
    read_shard,
)

# Compatibility surface. The pre-split monolith also leaked its own implementation
# imports as module attributes (`base64`, `gzip`, `hashlib`, `re`, `quote`, `cast`,
# `dataclass`, `field`, `source_identity`, ...). Listing them in ``__all__`` keeps that
# exact non-underscore surface importable from this path and declares the re-export
# explicitly for ruff F401. The seams the exception-ratchet tests patch by module
# attribute (`read_manifest`, `_remote_timing_row`, `ThreadPoolExecutor`,
# `as_completed`) stay bound here because this coordinator still calls them.
__all__ = [
    "Any",
    "Callable",
    "DEFAULT_BASE_PATH",
    "DEFAULT_GC_DELETE_LIMIT",
    "DEFAULT_MANIFEST_FILE_NAME",
    "DEFAULT_PREFIX_LENGTH",
    "DEFAULT_SHARD_READ_WORKERS",
    "DEFAULT_SHARD_WRITE_WORKERS",
    "MAX_PREFIX_LENGTH",
    "PREFIX_LENGTH_STEP",
    "SHARD_SCHEMA_VERSION",
    "Shard",
    "SourceSyncShardError",
    "ThreadPoolExecutor",
    "annotations",
    "as_completed",
    "base64",
    "build_manifest",
    "build_sharded_snapshot_bundle",
    "build_shards",
    "cast",
    "changed_shards",
    "content_addressed_shards",
    "dataclass",
    "field",
    "gzip",
    "hashlib",
    "json",
    "manifest_path",
    "prune_unreferenced_shards",
    "push_changed_shards",
    "push_manifest",
    "push_shard",
    "push_sharded_snapshot",
    "quote",
    "re",
    "read_manifest",
    "read_shard",
    "read_sharded_snapshot",
    "shard_key",
    "source_identity",
    "time",
    "trusted_committed_manifest",
    "validate_manifest",
]


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


def _snapshot_bucket_rows(snapshot: dict[str, Any], bucket: str) -> list[dict[str, Any]]:
    rows = snapshot.get(bucket) or []
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise SourceSyncShardError(f"source-sync snapshot {bucket} rows must be objects")
    return [dict(row) for row in rows]


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
    for shard_output in shard_outputs:
        if shard_output is None:
            continue
        results.append(dict(shard_output.get("pushResult") or {}))
        verifications.append(dict(shard_output.get("verification") or {}))
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
    for completed_shard in shard_results:
        if completed_shard is None:
            continue
        bucket = str(completed_shard["entry"]["bucket"])
        rows_by_bucket.setdefault(bucket, []).extend(completed_shard["rows"])
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
