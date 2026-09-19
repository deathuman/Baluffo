"""Source-sync shard push/pull progress counters and progress-callback emission.

AI boundary owns: source-sync shard progress count payloads and the guarded progress callback emission seam.
AI boundary implement in: this leaf for progress payload semantics; remote transport stays in source_sync_shard_remote and orchestration in the shard coordinator.
AI boundary search before contracts: shard push/pull callers, bridge progress sinks, and shard progress tests.
AI boundary verify: `python -m pytest tests/test_source_sync_shard_progress_exception_ratchet.py tests/test_source_sync_sharded_push.py -q`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

_EXPECTED_PROGRESS_CALLBACK_EXCEPTIONS = (OSError, RuntimeError, TypeError, ValueError)


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


def _emit_remote_progress(
    progress_callback: Callable[..., None] | None,
    *,
    phase_key: str,
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
            phase_key=phase_key,
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


def _emit_pull_progress(
    progress_callback: Callable[..., None] | None,
    *,
    phase_label: str,
    counts: dict[str, Any],
    ratio: float,
    message: str = "",
    event_level: str = "muted",
) -> None:
    _emit_remote_progress(
        progress_callback,
        phase_key="remote_read",
        phase_label=phase_label,
        counts=counts,
        ratio=ratio,
        message=message,
        event_level=event_level,
    )


def _emit_push_progress(
    progress_callback: Callable[..., None] | None,
    *,
    phase_label: str,
    counts: dict[str, Any],
    ratio: float,
    message: str = "",
    event_level: str = "muted",
) -> None:
    _emit_remote_progress(
        progress_callback,
        phase_key="remote_write",
        phase_label=phase_label,
        counts=counts,
        ratio=ratio,
        message=message,
        event_level=event_level,
    )
