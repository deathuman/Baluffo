"""Registry sync flow for auto-sync workflows.

This module provides registry auto-sync persistence and start logic.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

RegistryState = dict[str, list[dict[str, Any]]]
PersistStateFunc = Callable[[RegistryState], RegistryState]
SyncGuardFunc = Callable[[], dict[str, Any] | None]
SyncTaskRunningFunc = Callable[[], bool]
StartSyncTaskFunc = Callable[..., dict[str, Any]]

AUTO_SYNC_PUSH_REASONS = frozenset(
    {
        "manual_source",
        "manual_source_variant_added",
        "discovery_auto_approve",
        "discovery_queue",
        "registry_approve",
        "registry_reject",
        "registry_rollback",
        "registry_restore_rejected",
        "registry_restore_deleted",
        "fetch_empty_demote",
        "fetch_failure_demote",
    }
)


def should_trigger_auto_sync_push(reason: str) -> bool:
    return str(reason or "").strip().lower() in AUTO_SYNC_PUSH_REASONS


def maybe_trigger_auto_sync_push(
    *,
    reason: str,
    sync_guard: SyncGuardFunc,
    sync_task_running: SyncTaskRunningFunc,
    start_sync_task: StartSyncTaskFunc,
) -> bool:
    if not should_trigger_auto_sync_push(reason):
        return False
    guard = sync_guard()
    if guard:
        return False
    if sync_task_running():
        return False
    result = start_sync_task("push", reason=reason, automatic=True)
    return bool(result.get("started"))


def persist_state_and_auto_sync(
    state: RegistryState,
    *,
    reason: str,
    persist_state: PersistStateFunc,
    maybe_trigger_auto_sync_push: Callable[[str], bool],
) -> RegistryState:
    normalized = persist_state(state)
    maybe_trigger_auto_sync_push(reason)
    return normalized
