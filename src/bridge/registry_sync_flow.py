"""Registry sync flow for auto-sync workflows.

This module provides registry auto-sync persistence and start logic.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

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
        "fetch_empty_demote",
        "fetch_failure_demote",
    }
)


def should_trigger_auto_sync_push(reason: str) -> bool:
    return str(reason or "").strip().lower() in AUTO_SYNC_PUSH_REASONS


def maybe_trigger_auto_sync_push(
    reason: str,
    sync_guard: Callable[[], dict[str, Any] | None],
    sync_task_running: Callable[[], bool],
    start_sync_task: Callable[..., dict[str, Any]],
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
    state: dict[str, list[dict[str, Any]]],
    *,
    reason: str,
    persist_state: Callable[[dict[str, list[dict[str, Any]]]], dict[str, list[dict[str, Any]]]],
    maybe_trigger_auto_sync_push: Callable[[str], bool],
) -> dict[str, list[dict[str, Any]]]:
    normalized = persist_state(state)
    maybe_trigger_auto_sync_push(reason)
    return normalized
