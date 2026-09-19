"""Source-sync snapshot coordinator: merge core, logging surface, and leaf composition.

AI boundary owns: the source-sync snapshot merge core (row canonicalization, transition metadata, registry merge, snapshot building), the module logging surface, and the compatibility re-export surface of this module.
AI boundary implement in: this coordinator only for merge semantics and logging; transport and payload validation live in source_sync_snapshot_remote, shard fingerprints and sizing in source_sync_snapshot_shard_metadata, push/pull flow in source_sync_snapshot_push, recovery in source_sync_snapshot_push_retry, and detail timing in source_sync_snapshot_timing.
AI boundary search before contracts: source sync facade, registry IO and canonicalization, shard helpers, and snapshot tests.
AI boundary verify: `python -m pytest tests/test_source_sync.py tests/test_source_sync_push_churn.py -q`.
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

# Compatibility surface: every name below was reachable from this module before
# the split. The preamble above is kept whole (not trimmed to what this
# coordinator calls) so incidental names such as base64, ssl and cast stay
# reachable, and `__all__` marks them as intentional re-exports for ruff.
__all__ = [
    "Any",
    "Callable",
    "Mapping",
    "REGISTRY_MIGRATION_V2",
    "REGISTRY_REASON_PENDING_DEFAULT",
    "SHARD_SCHEMA_VERSION",
    "SourceSyncShardError",
    "URLError",
    "annotations",
    "base64",
    "build_sharded_snapshot_bundle",
    "build_snapshot",
    "canonicalize_registry_row",
    "cast",
    "contextmanager",
    "ensure_source_id",
    "hashlib",
    "json",
    "logger",
    "logging",
    "merge_registry_state",
    "normalize_snapshot",
    "parse_iso",
    "pull_and_merge_sources",
    "push_sharded_snapshot",
    "push_sources_snapshot",
    "read_remote_snapshot",
    "read_sharded_snapshot",
    "record_source_sync_snapshot",
    "sort_sources_by_identity",
    "source_identity",
    "ssl",
    "time",
    "write_remote_snapshot",
]


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
    # Lazy import: the coordinator holds no module-level leaf import, so it is
    # always fully initialized before a leaf body runs, in either import order.
    from src.source_sync_snapshot_remote import read_remote_snapshot as _remote_mod

    return _remote_mod(
        module,
        config,
        opener=opener,
        prefer_sharded=prefer_sharded,
        progress_callback=progress_callback,
        known_remote_sha=known_remote_sha,
        max_shard_read_workers=max_shard_read_workers,
    )


def write_remote_snapshot(
    module: Any,
    config: Any,
    snapshot: dict[str, Any],
    *,
    sha: str = "",
    message: str = "Update Baluffo source sync snapshot",
    opener: Callable[..., Any],
) -> dict[str, Any]:
    # Lazy import: the coordinator holds no module-level leaf import, so it is
    # always fully initialized before a leaf body runs, in either import order.
    from src.source_sync_snapshot_remote import write_remote_snapshot as _remote_mod

    return _remote_mod(
        module,
        config,
        snapshot,
        sha=sha,
        message=message,
        opener=opener,
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
    # Lazy import: the coordinator holds no module-level leaf import, so it is
    # always fully initialized before a leaf body runs, in either import order.
    from src.source_sync_snapshot_push import pull_and_merge_sources as _push_mod

    return _push_mod(
        module,
        config,
        local_state,
        progress_callback=progress_callback,
        known_remote_sha=known_remote_sha,
        max_shard_read_workers=max_shard_read_workers,
        opener=opener,
    )


def push_sources_snapshot(
    module: Any,
    config: Any,
    local_state: dict[str, Any],
    *,
    dry_run: bool = False,
    progress_callback: Callable[..., None] | None = None,
    opener: Callable[..., Any],
) -> dict[str, Any]:
    # Lazy import: the coordinator holds no module-level leaf import, so it is
    # always fully initialized before a leaf body runs, in either import order.
    from src.source_sync_snapshot_push import push_sources_snapshot as _push_mod

    return _push_mod(
        module,
        config,
        local_state,
        dry_run=dry_run,
        progress_callback=progress_callback,
        opener=opener,
    )
