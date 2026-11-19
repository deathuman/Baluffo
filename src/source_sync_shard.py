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
    DEFAULT_GC_RETAIN_GENERATIONS,
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
    create_git_blob,
    create_git_commit,
    create_git_tree,
    git_blob_sha1,
    manifest_payload_bytes,
    push_manifest,
    push_shard,
    read_branch_commit,
    read_git_tree,
    read_manifest,
    read_shard,
    update_branch_ref,
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
    "DEFAULT_GC_RETAIN_GENERATIONS",
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
    "create_git_blob",
    "create_git_commit",
    "create_git_tree",
    "dataclass",
    "field",
    "git_blob_sha1",
    "gzip",
    "hashlib",
    "json",
    "manifest_path",
    "manifest_payload_bytes",
    "prune_unreferenced_shards",
    "push_changed_shards",
    "push_generation_atomic",
    "push_manifest",
    "push_shard",
    "push_sharded_snapshot",
    "quote",
    "re",
    "read_branch_commit",
    "read_git_tree",
    "read_manifest",
    "read_shard",
    "read_sharded_snapshot",
    "shard_key",
    "source_identity",
    "time",
    "trusted_committed_manifest",
    "update_branch_ref",
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
        remote_requests.extend(list(shard_output.get("remoteRequests") or []))
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


def _validate_shard_payloads(shards: list[Shard]) -> None:
    for shard in shards:
        if hashlib.sha256(shard.payload_bytes).hexdigest() != shard.sha256.lower():
            raise SourceSyncShardError(
                f"source-sync shard {shard.path} sha256 does not match payload bytes"
            )


def _upload_generation_shards(
    module: Any,
    config: Any,
    ordered: list[Shard],
    *,
    shard_count: int,
    changed_count: int,
    total_bytes: int,
    remote_requests: list[dict[str, Any]],
    push_results: list[dict[str, Any]],
    verified_shards: list[dict[str, Any]],
    progress_callback: Callable[..., None] | None,
    opener: Callable[..., Any],
) -> int:
    upload_started_at = time.perf_counter()
    completed_count = 0
    pushed_bytes = 0
    for index, shard in enumerate(ordered):
        blob_started_at = time.perf_counter()
        try:
            blob = create_git_blob(module, config, shard.payload_bytes, opener=opener)
        except _EXPECTED_REMOTE_SYNC_EXCEPTIONS as exc:
            remote_requests.append(
                _remote_timing_row(
                    operation="pushShard",
                    method="POST",
                    path=shard.path,
                    started_at=blob_started_at,
                    ok=False,
                    size_bytes=shard.size_bytes,
                    row_count=shard.row_count,
                    error=str(exc),
                )
            )
            raise
        remote_requests.append(
            _remote_timing_row(
                operation="pushShard",
                method="POST",
                path=shard.path,
                started_at=blob_started_at,
                ok=True,
                size_bytes=shard.size_bytes,
                row_count=shard.row_count,
            )
        )
        push_results.append(
            {
                "ok": True,
                "path": shard.path,
                "sha256": shard.sha256,
                "remoteSha": str(blob.get("sha") or ""),
                "sizeBytes": shard.size_bytes,
                "rowCount": shard.row_count,
                "alreadyExisted": False,
            }
        )
        verified_shards.append(
            {"path": shard.path, "sha256": shard.sha256, "rowCount": shard.row_count}
        )
        completed_count += 1
        pushed_bytes += shard.size_bytes
        _emit_push_progress(
            progress_callback,
            phase_label=f"Verified shard {completed_count} of {changed_count}",
            ratio=(completed_count / changed_count) if changed_count else 1.0,
            counts=_shard_progress_counts(
                shard_count=shard_count,
                changed_shard_count=changed_count,
                completed_shard_count=completed_count,
                verified_shard_count=completed_count,
                current_shard_index=index + 1,
                current_shard_label=f"{shard.bucket}/{shard.key}",
                shards_pushed_bytes=pushed_bytes,
                total_shard_bytes=total_bytes,
            ),
            message=(
                f"Verified {completed_count} of {changed_count} source-sync shards."
                if completed_count % 25 == 0 or completed_count == changed_count
                else ""
            ),
        )
    return _duration_ms(upload_started_at, time.perf_counter())


def _push_generation_manifest(
    module: Any,
    config: Any,
    manifest_bytes: bytes,
    *,
    manifest_path_text: str,
    total_row_count: int,
    remote_requests: list[dict[str, Any]],
    opener: Callable[..., Any],
) -> dict[str, Any]:
    started_at = time.perf_counter()
    try:
        manifest_blob = create_git_blob(module, config, manifest_bytes, opener=opener)
    except _EXPECTED_REMOTE_SYNC_EXCEPTIONS as exc:
        remote_requests.append(
            _remote_timing_row(
                operation="pushManifest",
                method="POST",
                path=manifest_path_text,
                started_at=started_at,
                ok=False,
                size_bytes=len(manifest_bytes),
                row_count=total_row_count,
                error=str(exc),
            )
        )
        raise
    remote_requests.append(
        _remote_timing_row(
            operation="pushManifest",
            method="POST",
            path=manifest_path_text,
            started_at=started_at,
            ok=True,
            size_bytes=len(manifest_bytes),
            row_count=total_row_count,
        )
    )
    return manifest_blob


def _create_verified_generation_tree(
    module: Any,
    config: Any,
    tree_entries: list[dict[str, str]],
    head: dict[str, Any],
    *,
    manifest_path_text: str,
    commit_started_at: float,
    remote_requests: list[dict[str, Any]],
    opener: Callable[..., Any],
) -> dict[str, Any]:
    try:
        tree = create_git_tree(
            module,
            config,
            base_tree=str(head.get("treeSha") or ""),
            entries=tree_entries,
            opener=opener,
        )
    except _EXPECTED_REMOTE_SYNC_EXCEPTIONS as exc:
        remote_requests.append(
            _remote_timing_row(
                operation="createTree",
                method="POST",
                path=manifest_path_text,
                started_at=commit_started_at,
                ok=False,
                error=str(exc),
            )
        )
        raise
    remote_requests.append(
        _remote_timing_row(
            operation="createTree",
            method="POST",
            path=manifest_path_text,
            started_at=commit_started_at,
            ok=True,
        )
    )

    verify_started_at = time.perf_counter()
    expected_blobs = {str(entry["path"]): str(entry["sha"]).lower() for entry in tree_entries}
    try:
        verified_tree = read_git_tree(
            module, config, str(tree.get("treeSha") or ""), recursive=True, opener=opener
        )
    except _EXPECTED_REMOTE_SYNC_EXCEPTIONS as exc:
        remote_requests.append(
            _remote_timing_row(
                operation="verifyTree",
                method="GET",
                path=manifest_path_text,
                started_at=verify_started_at,
                ok=False,
                error=str(exc),
            )
        )
        raise
    observed_blobs = {
        entry["path"]: entry["sha"] for entry in verified_tree["entries"] if entry["type"] == "blob"
    }
    mismatched_paths = [
        path for path, sha in expected_blobs.items() if observed_blobs.get(path) != sha
    ]
    truncated = bool(verified_tree["truncated"])
    mismatch_text = ", ".join(sorted(mismatched_paths)[:5])
    remote_requests.append(
        _remote_timing_row(
            operation="verifyTree",
            method="GET",
            path=manifest_path_text,
            started_at=verify_started_at,
            ok=not truncated and not mismatched_paths,
            size_bytes=sum(int(entry["sizeBytes"] or 0) for entry in verified_tree["entries"]),
            row_count=len(expected_blobs),
            error="tree listing truncated" if truncated else mismatch_text,
        )
    )
    # A truncated listing cannot prove the expected blobs are in the tree, so it
    # is a failed verification, not a pass with less detail.
    if truncated:
        raise RuntimeError(
            "source-sync tree verification was truncated; refusing to commit an "
            "unverified generation"
        )
    if mismatched_paths:
        raise RuntimeError(
            f"source-sync committed tree does not match the pushed generation: {mismatch_text}"
        )
    return tree


def _commit_generation_tree(
    module: Any,
    config: Any,
    tree: dict[str, Any],
    head: dict[str, Any],
    *,
    changed_count: int,
    manifest_path_text: str,
    remote_requests: list[dict[str, Any]],
    opener: Callable[..., Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    commit_request_started_at = time.perf_counter()
    try:
        commit = create_git_commit(
            module,
            config,
            tree_sha=str(tree.get("treeSha") or ""),
            parent_sha=str(head.get("commitSha") or ""),
            message=(
                "Update Baluffo source sync generation "
                f"({changed_count} shard{'s' if changed_count != 1 else ''})"
            ),
            opener=opener,
        )
    except _EXPECTED_REMOTE_SYNC_EXCEPTIONS as exc:
        remote_requests.append(
            _remote_timing_row(
                operation="createCommit",
                method="POST",
                path=manifest_path_text,
                started_at=commit_request_started_at,
                ok=False,
                error=str(exc),
            )
        )
        raise
    remote_requests.append(
        _remote_timing_row(
            operation="createCommit",
            method="POST",
            path=manifest_path_text,
            started_at=commit_request_started_at,
            ok=True,
        )
    )

    ref_started_at = time.perf_counter()
    try:
        ref = update_branch_ref(
            module, config, commit_sha=str(commit.get("commitSha") or ""), opener=opener
        )
    except _EXPECTED_REMOTE_SYNC_EXCEPTIONS as exc:
        remote_requests.append(
            _remote_timing_row(
                operation="updateRef",
                method="PATCH",
                path=config.branch,
                started_at=ref_started_at,
                ok=False,
                error=str(exc),
            )
        )
        raise
    remote_requests.append(
        _remote_timing_row(
            operation="updateRef",
            method="PATCH",
            path=config.branch,
            started_at=ref_started_at,
            ok=True,
        )
    )
    return commit, ref


def push_generation_atomic(
    module: Any,
    config: Any,
    changed: list[Shard],
    manifest: dict[str, Any],
    *,
    committed_manifest_sha: str = "",
    progress_callback: Callable[..., None] | None = None,
    opener: Callable[..., Any],
) -> dict[str, Any]:
    """Commit the changed shards and the manifest in one atomic git commit.

    Readers only reach a generation through the branch ref, so the new shards
    and the manifest that references them become visible in the same instant.
    Every uploaded blob is checked against the git object id GitHub reports for
    the stored bytes, the resulting tree is verified before it is committed,
    and the ref update is a fast-forward so a concurrent writer is rejected
    instead of overwritten.
    """
    ordered = sorted(changed, key=lambda shard: (shard.bucket, shard.key, shard.path))
    _validate_shard_payloads(ordered)
    trusted = trusted_committed_manifest(manifest)
    if trusted is None:
        raise SourceSyncShardError("refusing to push an uncommitted source-sync manifest")
    manifest_bytes = manifest_payload_bytes(trusted)
    manifest_path_text = manifest_path(config.path)
    shard_count = int(trusted.get("shardCount") or len(ordered))
    changed_count = len(ordered)
    total_bytes = sum(shard.size_bytes for shard in ordered)
    total_row_count = int(trusted.get("totalRowCount") or 0)
    remote_requests: list[dict[str, Any]] = []
    push_results: list[dict[str, Any]] = []
    verified_shards: list[dict[str, Any]] = []

    if ordered:
        _emit_push_progress(
            progress_callback,
            phase_label=f"Uploading shard 1 of {changed_count}",
            ratio=0.0,
            counts=_shard_progress_counts(
                shard_count=shard_count,
                changed_shard_count=changed_count,
                completed_shard_count=0,
                verified_shard_count=0,
                current_shard_index=1,
                current_shard_label=f"{ordered[0].bucket}/{ordered[0].key}",
                shards_pushed_bytes=0,
                total_shard_bytes=total_bytes,
            ),
            message=f"Uploading {changed_count} source-sync shard(s).",
        )

    head_started_at = time.perf_counter()
    try:
        head = read_branch_commit(module, config, opener=opener)
    except _EXPECTED_REMOTE_SYNC_EXCEPTIONS as exc:
        remote_requests.append(
            _remote_timing_row(
                operation="readBranchCommit",
                method="GET",
                path=config.branch,
                started_at=head_started_at,
                ok=False,
                error=str(exc),
            )
        )
        raise
    remote_requests.append(
        _remote_timing_row(
            operation="readBranchCommit",
            method="GET",
            path=config.branch,
            started_at=head_started_at,
            ok=True,
        )
    )

    if committed_manifest_sha:
        guard_started_at = time.perf_counter()
        try:
            current_manifest = read_manifest(module, config, opener=opener)
        except _EXPECTED_REMOTE_SYNC_EXCEPTIONS as exc:
            remote_requests.append(
                _remote_timing_row(
                    operation="verifyManifestSha",
                    method="GET",
                    path=manifest_path_text,
                    started_at=guard_started_at,
                    ok=False,
                    error=str(exc),
                )
            )
            raise
        current_sha = str((current_manifest or {}).get("sha") or "")
        remote_requests.append(
            _remote_timing_row(
                operation="verifyManifestSha",
                method="GET",
                path=manifest_path_text,
                started_at=guard_started_at,
                ok=current_sha == committed_manifest_sha,
                size_bytes=int((current_manifest or {}).get("manifestSizeBytes") or 0),
            )
        )
        if current_sha != committed_manifest_sha:
            conflict_message = (
                "source-sync manifest moved since it was read: "
                f"expected {committed_manifest_sha}, found {current_sha or '<missing>'}"
            )
            conflict_code = getattr(module, "RUNTIME_STATE_REMOTE_CONFLICT", "remote_conflict")
            if hasattr(module, "_set_runtime_state"):
                module._set_runtime_state(conflict_code, conflict_message)
            if hasattr(module, "SyncOperationError"):
                raise module.SyncOperationError(conflict_code, conflict_message)
            raise RuntimeError(conflict_message)

    upload_wall_ms = _upload_generation_shards(
        module,
        config,
        ordered,
        shard_count=shard_count,
        changed_count=changed_count,
        total_bytes=total_bytes,
        remote_requests=remote_requests,
        push_results=push_results,
        verified_shards=verified_shards,
        progress_callback=progress_callback,
        opener=opener,
    )
    manifest_blob = _push_generation_manifest(
        module,
        config,
        manifest_bytes,
        manifest_path_text=manifest_path_text,
        total_row_count=total_row_count,
        remote_requests=remote_requests,
        opener=opener,
    )

    tree_entries = [
        {
            "path": str(result["path"]),
            "mode": "100644",
            "type": "blob",
            "sha": str(result["remoteSha"]),
        }
        for result in push_results
    ]
    tree_entries.append(
        {
            "path": manifest_path_text,
            "mode": "100644",
            "type": "blob",
            "sha": str(manifest_blob.get("sha") or ""),
        }
    )
    commit_started_at = time.perf_counter()
    tree = _create_verified_generation_tree(
        module,
        config,
        tree_entries,
        head,
        manifest_path_text=manifest_path_text,
        commit_started_at=commit_started_at,
        remote_requests=remote_requests,
        opener=opener,
    )

    _emit_push_progress(
        progress_callback,
        phase_label="Committing sync manifest",
        ratio=1.0,
        counts=_shard_progress_counts(
            shard_count=shard_count,
            changed_shard_count=changed_count,
            completed_shard_count=changed_count,
            verified_shard_count=changed_count,
            shards_pushed_bytes=total_bytes,
            total_shard_bytes=total_bytes,
        ),
        message="Committing sync manifest.",
    )
    commit, ref = _commit_generation_tree(
        module,
        config,
        tree,
        head,
        changed_count=changed_count,
        manifest_path_text=manifest_path_text,
        remote_requests=remote_requests,
        opener=opener,
    )
    commit_wall_ms = _duration_ms(commit_started_at, time.perf_counter())
    return {
        "commitSha": str(ref.get("commitSha") or ""),
        "parentCommitSha": str(head.get("commitSha") or ""),
        "treeSha": str(tree.get("treeSha") or ""),
        "manifestSha": str(manifest_blob.get("sha") or ""),
        "shardCount": shard_count,
        "changedShardCount": changed_count,
        "shardsPushedBytes": total_bytes,
        "workerCount": 1,
        "parallelWallMs": upload_wall_ms,
        "uploadWallMs": upload_wall_ms,
        "commitWallMs": commit_wall_ms,
        "changedShards": [shard.manifest_entry() for shard in ordered],
        "pushResults": push_results,
        "verifiedShards": verified_shards,
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
    gc_retain_generations: int = DEFAULT_GC_RETAIN_GENERATIONS,
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
    generation = push_generation_atomic(
        module,
        config,
        bundle["changedShards"],
        bundle["manifest"],
        committed_manifest_sha=committed_manifest_sha,
        progress_callback=progress_callback,
        opener=opener,
    )
    shard_result: dict[str, Any] = {
        "shardCount": int(generation.get("shardCount") or 0),
        "changedShardCount": int(generation.get("changedShardCount") or 0),
        "shardsPushedBytes": int(generation.get("shardsPushedBytes") or 0),
        "workerCount": int(generation.get("workerCount") or 0),
        "parallelWallMs": int(generation.get("parallelWallMs") or 0),
        "changedShards": list(generation.get("changedShards") or []),
        "pushResults": list(generation.get("pushResults") or []),
        "verifiedShards": list(generation.get("verifiedShards") or []),
        "remoteRequests": list(generation.get("remoteRequests") or []),
    }
    changed_count = int(shard_result.get("changedShardCount") or 0)
    total_bytes = int(shard_result.get("shardsPushedBytes") or 0)
    manifest_counts = _shard_progress_counts(
        shard_count=len(bundle["shards"]),
        changed_shard_count=changed_count,
        completed_shard_count=changed_count,
        verified_shard_count=changed_count,
        shards_pushed_bytes=total_bytes,
        total_shard_bytes=total_bytes,
        manifest_committed=True,
    )
    remote_requests = list(shard_result.get("remoteRequests") or [])
    push_changed_wall_ms = int(generation.get("uploadWallMs") or 0)
    manifest_wall_ms = int(generation.get("commitWallMs") or 0)
    gc_result: dict[str, Any] = {}
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
            retain_generations=gc_retain_generations,
            current_commit_sha=str(generation.get("commitSha") or ""),
            opener=opener,
        )
    except (KeyError, RuntimeError, TypeError, ValueError) as exc:
        gc_result = {
            "ok": False,
            "deletedCount": 0,
            "deleteAttemptCount": 0,
            "skippedCount": 0,
            "deleteLimit": max(0, int(gc_delete_limit or 0)),
            "retainGenerations": max(0, int(gc_retain_generations or 0)),
            "retainedGenerationCount": 0,
            "retainedShardCount": 0,
            "deletedPaths": [],
            "warnings": [f"source-sync shard GC failed: {exc}"],
        }
    gc_warnings = list(gc_result.get("warnings") or [])
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
        "remoteSha": str(generation.get("manifestSha") or ""),
        "commitSha": str(generation.get("commitSha") or ""),
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
    shard_read_duration_ms = 0
    if shard_entries:
        shard_read_started_at = time.perf_counter()
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
        shard_read_duration_ms = _duration_ms(shard_read_started_at, time.perf_counter())
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
        "shardReadDurationMs": shard_read_duration_ms,
        "totalShardBytes": total_shard_bytes,
    }
    for bucket in sorted(rows_by_bucket):
        snapshot[bucket] = rows_by_bucket[bucket]
    return snapshot
