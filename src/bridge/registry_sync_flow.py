"""Registry sync flow for auto-sync workflows.

This module provides registry auto-sync persistence and start logic.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List


RegistryState = Dict[str, List[Dict[str, Any]]]
PersistStateFunc = Callable[[RegistryState], RegistryState]
SyncGuardFunc = Callable[[], Dict[str, Any] | None]
SyncTaskRunningFunc = Callable[[], bool]
StartSyncTaskFunc = Callable[..., Dict[str, Any]]


def maybe_trigger_auto_sync_push(
    *,
    reason: str,
    sync_guard: SyncGuardFunc,
    sync_task_running: SyncTaskRunningFunc,
    start_sync_task: StartSyncTaskFunc,
) -> bool:
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
