"""Publish a discovery auto-approval through the registry's storage authority.

The discovery worker used to call ``save_registry_state_atomic`` directly. With
``sourceRegistry=sqlite`` that JSON write is not the authority, and because it
writes a whole state built from the run's start-of-run snapshot, it can erase a
registry change the admin made while discovery ran. This leaf merges the run's
pending rows into the *current* authority state and applies the approval delta
under the registry write lock, so concurrent mutations compose; then it publishes
through SQLite first (when SQLite is the authority) and regenerates the JSON
exports exactly like ``RegistryService._save_state`` does.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from src.source_registry_auto_approval import apply_discovery_auto_approval
from src.source_registry_identity import source_identity
from src.source_registry_pending_merge import merge_run_pending_rows
from src.source_registry_write_lock import source_registry_write_lock
from src.storage.baluffo_store import DEFAULT_DB_NAME, BaluffoStore, BaluffoStoreError
from src.storage.source_registry_runtime import (
    SourceRegistryRuntimeStore,
    source_registry_state_hash,
    source_registry_tombstone_hash,
)

_AUTHORITY_ERRORS = (BaluffoStoreError, OSError, RuntimeError, TypeError, ValueError)

JsonState = dict[str, list[dict[str, Any]]]


def _json_bucket_state(
    *,
    active_path: Path,
    pending_path: Path,
    rejected_path: Path,
    load_json_array: Callable[..., Any],
) -> JsonState:
    return {
        "active": [
            dict(row) for row in load_json_array(active_path, []) if isinstance(row, Mapping)
        ],
        "pending": [
            dict(row) for row in load_json_array(pending_path, []) if isinstance(row, Mapping)
        ],
        "rejected": [
            dict(row) for row in load_json_array(rejected_path, []) if isinstance(row, Mapping)
        ],
    }


def _open_store(data_dir: Path) -> BaluffoStore | None:
    if not (data_dir / DEFAULT_DB_NAME).exists():
        return None
    # A transient read must not schedule maintenance: its write path starts a
    # compaction, and close() would join it while this lock is held.
    return BaluffoStore(data_dir, defer_maintenance=True)


def _publish_authority_state(
    *,
    runtime: SourceRegistryRuntimeStore,
    state: JsonState,
    tombstones: dict[str, dict[str, Any]],
) -> None:
    runtime.replace_state(
        state=state,
        tombstones=tombstones,
        reason="discovery_auto_approval",
    )
    hashes = runtime.parity_hash()
    if hashes["stateHash"] != source_registry_state_hash(state) or hashes[
        "tombstoneHash"
    ] != source_registry_tombstone_hash(tombstones):
        raise ValueError("source registry projection mismatch")


def _merge_pending_rows(
    *,
    current: JsonState,
    run_pending: Sequence[Mapping[str, Any]],
    tombstones: Mapping[str, Any],
    merge_audit: dict[str, int] | None = None,
    merge_conflicts: list[dict[str, Any]] | None = None,
    active_merge_conflicts: list[dict[str, Any]] | None = None,
    routing_conflict_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Merge the run's pending rows through the shared pending merge.

    Current pending rows win on identity, routing, display and lifecycle because
    they may carry admin or sync edits. A candidate the admin approved, rejected,
    or deleted while the run was going must not come back through the run's stale
    pending snapshot. The run's newer observations (jobsFound, lastProbedAt, probe
    evidence) are carried forward, blank display/routing fields may be filled, and
    the run's repeated-zero-job hide remains a run-owned lifecycle exception.
    ``merge_audit``, when passed, receives the merge's drop and skip counters;
    ``merge_conflicts`` receives the bounded sample of kept-authority conflicts.
    ``routing_conflict_ids`` receives every routing-field conflict identity, not
    just the sampled ones, so auto-approval cannot promote a conflicted route.
    ``active_merge_conflicts`` receives the bounded sample of fill-only conflicts
    found on run rows already placed in the active or rejected bucket; those rows
    are compared read-only and never changed.
    """
    placed_rows = {
        source_identity(row): row
        for bucket in ("active", "rejected")
        for row in current[bucket]
        if source_identity(row)
    }
    merged, _, _ = merge_run_pending_rows(
        current["pending"],
        run_pending,
        placed_ids=placed_rows,
        placed_rows=placed_rows,
        tombstones=tombstones,
        audit=merge_audit,
        conflicts=merge_conflicts,
        active_conflicts=active_merge_conflicts,
        routing_conflict_ids=routing_conflict_ids,
    )
    return merged


def _write_json_exports(
    *,
    active_path: Path,
    pending_path: Path,
    rejected_path: Path,
    state: JsonState,
    tombstones: dict[str, dict[str, Any]],
    save_registry_state_atomic: Callable[..., Any],
    save_json_atomic: Callable[..., Any],
) -> None:
    save_registry_state_atomic(active_path, pending_path, rejected_path, state)
    save_json_atomic(Path(active_path).with_name("source-registry-tombstones.json"), tombstones)


def apply_discovery_approval_to_authority(
    *,
    active_path: Path,
    pending_path: Path,
    rejected_path: Path,
    pending_rows: Sequence[Mapping[str, Any]],
    report: dict[str, Any],
    auto_approve_enabled: bool,
    tombstones: Mapping[str, Any],
    approval_state_path: Path,
    now_iso_fn: Callable[[], str],
    save_registry_state_atomic: Callable[..., Any],
    save_json_atomic: Callable[..., Any],
    load_json_array: Callable[..., Any],
    merge_audit: dict[str, int] | None = None,
    merge_conflicts: list[dict[str, Any]] | None = None,
    active_merge_conflicts: list[dict[str, Any]] | None = None,
    lock_timeout_s: float | None = None,
) -> tuple[JsonState, int, str]:
    """Merge and publish a discovery run; return (state, approved, authority).

    The run contributes pending candidates only: they are unioned into the
    current pending bucket (current rows win on identity, because they may carry
    admin edits) and the approval policy then runs against that merged state.
    The authority keeps its own active/rejected buckets, so a stale run snapshot
    cannot overwrite a concurrent admin decision. Publishing through the current
    authority under the write lock keeps that read-merge-write atomic.
    """
    data_dir = Path(active_path).resolve().parent
    with source_registry_write_lock(data_dir, timeout_s=lock_timeout_s):
        store = _open_store(data_dir)
        try:
            runtime: SourceRegistryRuntimeStore | None = None
            current: JsonState
            current_tombstones: dict[str, dict[str, Any]] = {}
            authority = "json"
            if store is not None and store.get_authority_modes().get("sourceRegistry") == "sqlite":
                runtime = SourceRegistryRuntimeStore(store)
                if runtime.current_generation():
                    current = runtime.current_state()
                    current_tombstones = runtime.current_tombstones()
                    authority = "sqlite"
                else:
                    current = _json_bucket_state(
                        active_path=active_path,
                        pending_path=pending_path,
                        rejected_path=rejected_path,
                        load_json_array=load_json_array,
                    )
            else:
                current = _json_bucket_state(
                    active_path=active_path,
                    pending_path=pending_path,
                    rejected_path=rejected_path,
                    load_json_array=load_json_array,
                )

            merged_tombstones = {
                **dict(tombstones or {}),
                **current_tombstones,
            }
            routing_conflict_ids: set[str] = set()
            merged_pending = _merge_pending_rows(
                current=current,
                run_pending=[dict(row) for row in pending_rows if isinstance(row, Mapping)],
                tombstones=merged_tombstones,
                merge_audit=merge_audit,
                merge_conflicts=merge_conflicts,
                active_merge_conflicts=active_merge_conflicts,
                routing_conflict_ids=routing_conflict_ids,
            )
            merged: JsonState = {
                "active": current["active"],
                "pending": merged_pending,
                "rejected": current["rejected"],
            }
            next_state, approved = apply_discovery_auto_approval(
                merged,
                report,
                auto_approve_enabled=auto_approve_enabled,
                approval_state_path=approval_state_path,
                blocked_ids=routing_conflict_ids,
                now_iso_fn=now_iso_fn,
            )
            publish_error: Exception | None = None
            if runtime is not None:
                try:
                    _publish_authority_state(
                        runtime=runtime,
                        state=next_state,
                        tombstones=merged_tombstones,
                    )
                    # Also covers the first publish from JSON when no generation existed.
                    authority = "sqlite"
                except _AUTHORITY_ERRORS as exc:
                    publish_error = exc
            if publish_error is not None and store is not None:
                # Same rollback contract as RegistryService._save_state: a failed
                # authoritative publish returns the surface to JSON instead of
                # leaving the exports disagreeing with it. The store check only
                # narrows the type: a publish error implies the store exists.
                store.set_authority_mode(
                    "sourceRegistry",
                    "json",
                    reason="discovery_auto_approval_write_failed",
                )
                authority = "json"
            _write_json_exports(
                active_path=active_path,
                pending_path=pending_path,
                rejected_path=rejected_path,
                state=next_state,
                tombstones=merged_tombstones,
                save_registry_state_atomic=save_registry_state_atomic,
                save_json_atomic=save_json_atomic,
            )
            return next_state, approved, authority
        finally:
            if store is not None:
                store.close()


__all__ = ["apply_discovery_approval_to_authority"]
