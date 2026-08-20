"""Discovery service construction types and shared mixin state.

AI boundary owns: bridge-owned discovery service construction types (paths/deps injection) and the shared mixin state base.
AI boundary implement in: this leaf for construction types + state base; launch, watch, lifecycle, registry reconciliation, and config behavior stay in sibling discovery_service_* leaves.
AI boundary search before contracts: discovery routes, task launch API, source discovery config, and admin discovery frontend callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused discovery service tests.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

BridgeLogFunc = Callable[..., None]


@dataclass(frozen=True)
class DiscoveryPaths:
    report: Any
    candidates: Any
    pending: Any
    log: Any
    settings: Any
    approval_state: Any
    task_state: Any | None = None
    active_task_snapshot: Any | None = None


@dataclass(frozen=True)
class DiscoveryDeps:
    schema_version: int
    now_iso: Callable[[], str]
    now_utc: Callable[[], Any]
    parse_iso: Callable[[Any], Any]
    pid_is_running: Callable[[int], bool]
    bridge_log: BridgeLogFunc
    load_json_object: Callable[[Any, Any], Any]
    save_json_atomic: Callable[[Any, Any], Any]
    run_background_script: Callable[..., int]
    append_run_history: Callable[[dict[str, Any]], dict[str, Any]]
    upsert_run_history: Callable[..., dict[str, Any]]
    prune_started_rows_for_type: Callable[..., None]
    clear_task_state: Callable[[str], None]
    normalize_discovery_report_contract: Callable[[dict[str, Any]], dict[str, Any]]
    load_state: Callable[[], dict[str, list[dict[str, Any]]]]
    persist_state_and_auto_sync: Callable[..., dict[str, list[dict[str, Any]]]]
    load_sync_runtime_state: Callable[[], dict[str, Any]]
    maybe_trigger_auto_sync_push: Callable[[str], bool]
    mark_discovery_sync_finished: Callable[[str], None]
    task_state_lock: Any | None = None
    start_lifecycle_run: Callable[..., dict[str, Any]] = lambda **_kwargs: {}
    heartbeat_lifecycle_run: Callable[..., dict[str, Any] | None] = lambda *_args, **_kwargs: None
    finish_lifecycle_run: Callable[..., dict[str, Any]] = lambda *_args, **_kwargs: {}
    fail_lifecycle_run: Callable[..., dict[str, Any]] = lambda *_args, **_kwargs: {}
    cancel_lifecycle_run: Callable[..., dict[str, Any]] = lambda *_args, **_kwargs: {}
    get_lifecycle_current_runs: Callable[[], list[dict[str, Any]]] = lambda: []
    get_lifecycle_row: Callable[[str, str], dict[str, Any] | None] = lambda _run_id, _task_type: (
        None
    )
    load_runtime_evidence: Callable[[Any, Any], Any] | None = None


class DiscoveryServiceState:
    """Shared instance state + cross-mixin surface for the DiscoveryService mixins."""

    _paths: DiscoveryPaths
    _deps: DiscoveryDeps

    def get_saved_discovery_config_payload(self) -> dict[str, Any]:
        raise NotImplementedError

    def _reconcile_terminal_discovery_report_from_state(self) -> dict[str, Any] | None:
        raise NotImplementedError

    def _read_discovery_report(self) -> dict[str, Any]:
        raise NotImplementedError

    def _repair_terminal_discovery_report_from_row(
        self,
        row: dict[str, Any],
        report: dict[str, Any],
        *,
        finished_at: str = "",
    ) -> dict[str, Any] | None:
        raise NotImplementedError

    def _finalize_discovery_run(
        self,
        *,
        run_id: str,
        started_at: str,
        finished_at: str,
        summary: dict[str, Any],
    ) -> None:
        raise NotImplementedError

    def _cancel_discovery_run(
        self,
        *,
        run_id: str,
        finished_at: str = "",
        reason: str = "",
    ) -> dict[str, Any]:
        raise NotImplementedError

    def _reconcile_terminal_discovery_registry_state(
        self,
        *,
        run_id: str,
        finished_at: str,
        report: dict[str, Any],
        saved_config_enabled: bool | None = None,
    ) -> tuple[dict[str, Any], int, bool]:
        raise NotImplementedError

    def _terminal_report_auto_approval_enabled(
        self,
        report: dict[str, Any],
        *,
        saved_config_enabled: bool,
    ) -> bool:
        raise NotImplementedError

    @staticmethod
    def _discovery_report_finalization_settled(report: dict[str, Any]) -> bool:
        raise NotImplementedError

    def watch_discovery_run_for_auto_sync(self, run_id: str, pid: int, started_at: str) -> None:
        raise NotImplementedError
