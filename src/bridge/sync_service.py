"""Sync service for Baluffo admin bridge.

This module extracts the sync business logic from admin_bridge.py
to improve modularity and reduce the "God Object" complexity.

The SyncService class provides:
- Configuration management (load, update, test sync config)
- Status tracking (get sync status payload)
- Sync operations (pull, push sources)
- Task management (start sync tasks, wait for completion)
"""

from __future__ import annotations

import os
import threading
import uuid
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, cast

from src import source_sync_runtime
from src.bridge import sync_task_flow as _sync_task_flow
from src.bridge.sync_state import (
    SYNC_CONFIG_LOCK,
    SYNC_STATE_LOCK,
    SyncState,
    now_iso,
    now_utc,
)
from src.bridge.sync_timing import (
    SyncTimingRecorder,
    append_sync_timing_record,
    load_sync_timing_history,
)
from src.shared.json_shapes import as_json_object
from src.shared.profile_utils import run_profiled
from src.source_registry import load_json_object, save_json_atomic


class SourceSyncModule(Protocol):
    """Protocol for the source_sync module interface."""

    def config_status(self, config: Any) -> dict[str, Any]: ...
    def resolve_sync_config(
        self, settings: dict[str, Any] | None = None, env: Mapping[str, str] | None = None
    ) -> Any: ...
    def read_remote_snapshot(self, config: Any) -> dict[str, Any]: ...
    def pull_and_merge_sources(
        self, config: Any, local_state: dict[str, Any]
    ) -> dict[str, Any]: ...
    def push_sources_snapshot(self, config: Any, state: dict[str, Any]) -> dict[str, Any]: ...
    def rate_limit_payload(self) -> dict[str, Any]: ...


class BridgeLogFunc(Protocol):
    """Protocol for the bridge_log function."""

    def __call__(self, level: str, message: str, **fields: Any) -> None: ...


class LoadStateFunc(Protocol):
    """Protocol for load_state function."""

    def __call__(self) -> dict[str, list[dict[str, Any]]]: ...


class PersistStateFunc(Protocol):
    """Protocol for persist_state function."""

    def __call__(
        self, state: dict[str, list[dict[str, Any]]]
    ) -> dict[str, list[dict[str, Any]]]: ...


class SummarizeStateFunc(Protocol):
    """Protocol for summarize_state function."""

    def __call__(self, state: dict[str, list[dict[str, Any]]]) -> dict[str, int]: ...


class RunHistoryFuncs(Protocol):
    """Protocol for run history functions."""

    def append(self, row: dict[str, Any]) -> dict[str, Any]: ...
    def upsert(
        self, entry: dict[str, Any], *, dedupe_fields: tuple[str, ...]
    ) -> dict[str, Any]: ...
    def load(self) -> list[dict[str, Any]]: ...
    def prune_started_rows_for_type(self, entry_type: str, *, finished_at: str) -> None: ...


class SyncService:
    """Service for managing source synchronization with GitHub.

    This class encapsulates all sync-related business logic, providing
    a clean interface for configuration, operations, and task management.

    Dependencies are injected via constructor for testability and modularity.
    """

    def __init__(
        self,
        data_dir: Path,
        source_sync: SourceSyncModule,
        bridge_log: BridgeLogFunc,
        load_state: LoadStateFunc,
        persist_state: PersistStateFunc,
        summarize_state: SummarizeStateFunc,
        run_history: RunHistoryFuncs,
        ops_state_lock: threading.RLock,
        get_security_defaults: Callable[[], dict[str, Any]],
        sync_state: SyncState | None = None,
        get_registry_auto_heal_report: Callable[[], dict[str, Any]] | None = None,
        task_lifecycle: Any | None = None,
    ):
        """Initialize SyncService with dependencies.

        Args:
            data_dir: Base data directory for sync files
            source_sync: The source_sync module (or mock for testing)
            bridge_log: Logging function
            load_state: Function to load registry state
            persist_state: Function to persist registry state
            summarize_state: Function to summarize registry state
            run_history: Run history management functions
            ops_state_lock: Lock for operations state
            get_security_defaults: Function to get security defaults
            sync_state: Optional SyncState instance (created if not provided)
        """
        self._data_dir = data_dir
        self._source_sync = source_sync
        self._bridge_log = bridge_log
        self._load_state = load_state
        self._persist_state = persist_state
        self._summarize_state = summarize_state
        self._run_history = run_history
        self._ops_state_lock = ops_state_lock
        self._get_security_defaults = get_security_defaults
        self._get_registry_auto_heal_report = get_registry_auto_heal_report or (
            lambda: {
                "autoHealed": False,
                "duplicateSourceIdCount": 0,
                "duplicates": [],
                "safeAutomation": {
                    "autoDemoted": False,
                    "demoted": 0,
                    "skipped": 0,
                    "applied": [],
                    "skippedRows": [],
                },
            }
        )
        self._task_lifecycle = task_lifecycle

        # Initialize sync state
        self._sync_state = sync_state or SyncState(data_dir=data_dir)

        # Path for sync config
        self._sync_config_path = data_dir / "source-sync-config.json"
        self._sync_live_task_path = data_dir / "sync-live-task.json"
        self._sync_timing_history_path = data_dir / "sync-timing-history.json"

        # Initialize sync config
        self._sync_config = self._resolve_effective_sync_config()

    # === Configuration Methods ===

    def _normalize_sync_settings(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """Normalize sync settings payload.

        Args:
            payload: Raw settings payload

        Returns:
            Normalized settings dict with 'enabled' key
        """
        security_defaults = self._get_security_defaults()
        data = payload if isinstance(payload, dict) else {}
        enabled_raw = data.get("enabled", bool(security_defaults["github_app_enabled_default"]))
        if isinstance(enabled_raw, bool):
            enabled = enabled_raw
        else:
            enabled = str(enabled_raw or "").strip().lower() not in {"", "0", "false", "no", "off"}
        return {"enabled": bool(enabled)}

    def load_saved_sync_settings(self) -> dict[str, Any]:
        """Load saved sync settings from file.

        Returns:
            Settings dict or empty dict if not configured
        """
        raw = load_json_object(self._sync_config_path, {})
        if isinstance(raw, dict) and "enabled" in raw:
            return self._normalize_sync_settings(raw)
        return {}

    def _resolve_effective_sync_config(self) -> Any:
        """Resolve the effective sync configuration.

        Returns:
            SyncConfig from source_sync module
        """
        return self._source_sync.resolve_sync_config(
            settings=self.load_saved_sync_settings(), env=os.environ
        )

    def refresh_sync_config(self) -> Any:
        """Refresh and return the current sync configuration.

        Updates the internal _sync_config and returns it.

        Returns:
            The current SyncConfig
        """
        global SYNC_CONFIG
        with SYNC_CONFIG_LOCK:
            self._sync_config = self._resolve_effective_sync_config()
            import src.bridge.sync_state as state_module

            state_module.SYNC_CONFIG = self._sync_config
            return self._sync_config

    def get_saved_sync_config_payload(self) -> dict[str, Any]:
        """Get the saved sync config as a payload dict.

        Returns:
            Dict with 'enabled' key
        """
        settings = self.load_saved_sync_settings()
        if "enabled" in settings:
            return {"enabled": bool(settings.get("enabled"))}
        return {
            "enabled": bool(
                self._source_sync.config_status(self.refresh_sync_config()).get("enabled")
            )
        }

    def update_saved_sync_settings(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Update saved sync settings.

        Args:
            payload: New settings to save

        Returns:
            The normalized settings that were saved
        """
        normalized = self._normalize_sync_settings(payload)
        save_json_atomic(self._sync_config_path, normalized)
        self.refresh_sync_config()
        return normalized

    def test_sync_config(self) -> dict[str, Any]:
        """Test the sync configuration.

        Returns:
            Dict with test results
        """
        config = self.refresh_sync_config()
        guard = self._sync_guard()
        if guard:
            return guard
        remote = self._source_sync.read_remote_snapshot(config)
        return {
            "ok": True,
            "remoteFound": bool(remote.get("exists")),
            "remoteSha": str(remote.get("sha") or ""),
            "message": "GitHub sync connection verified.",
        }

    # === Status Methods ===

    def _sync_guard(self) -> dict[str, Any] | None:
        """Check if sync is enabled and configured.

        Returns:
            None if sync is ready, or error dict if not
        """
        config_status = self._source_sync.config_status(self.refresh_sync_config())
        if not config_status.get("enabled"):
            return {"ok": False, "error": "Sync is disabled", "config": config_status}
        if not config_status.get("ready"):
            return {"ok": False, "error": "Sync is not configured", "config": config_status}
        return None

    def get_sync_status_payload(self) -> dict[str, Any]:
        """Get comprehensive sync status payload.

        Returns:
            Dict with config, savedConfig, and runtime state
        """
        from src.app_version import get_app_version

        config_status = self._source_sync.config_status(self.refresh_sync_config())
        with SYNC_STATE_LOCK:
            runtime_state = {**self._sync_state.get_sync_status()}
        rate_limit_payload = getattr(self._source_sync, "rate_limit_payload", None)
        runtime_state["rateLimit"] = as_json_object(
            rate_limit_payload()
            if callable(rate_limit_payload)
            else source_sync_runtime.rate_limit_payload(self._source_sync)
        )
        timing_history = load_sync_timing_history(self._sync_timing_history_path)
        return {
            "ok": True,
            "appVersion": get_app_version(),
            "config": config_status,
            "savedConfig": self.get_saved_sync_config_payload(),
            "runtime": runtime_state,
            "registryAutoHeal": self._get_registry_auto_heal_report(),
            "timing": timing_history[-1] if timing_history else {},
            "timingHistory": timing_history,
        }

    def sync_config_status(self) -> dict[str, Any]:
        return self._source_sync.config_status(self.refresh_sync_config())

    def set_sync_status(
        self,
        *,
        action: str = "",
        result: str = "",
        error: str = "",
        pulled: bool = False,
        pushed: bool = False,
    ) -> None:
        self._sync_state.set_sync_status(
            action=action,
            result=result,
            error=error,
            pulled=bool(pulled),
            pushed=bool(pushed),
        )

    # === Sync Operations ===

    def sync_pull_sources(
        self, *, progress_callback: Callable[..., None] | None = None
    ) -> dict[str, Any]:
        """Pull sources from remote and merge with local.

        Returns:
            Dict with pull results
        """
        guard = self._sync_guard()
        if guard:
            return guard

        timing = SyncTimingRecorder()
        emit_progress = progress_callback or (lambda **_kwargs: None)
        emit_progress(
            phase_key="prepare",
            phase_label="Preparing sync pull",
            counts={"action": "pull"},
            event_level="info",
            message="Preparing sync pull.",
        )
        with timing.record_stage("loadLocalRegistry"):
            local_state = self._load_state()
        emit_progress(
            phase_key="remote_read",
            phase_label="Reading remote snapshot",
            counts={"action": "pull"},
            event_level="muted",
            message="Reading remote snapshot.",
        )
        with timing.record_stage("pullMergeRemote"):
            result = run_profiled(
                self._source_sync.pull_and_merge_sources,
                self._sync_config,
                local_state,
                profile_name="sync_pull_merge",
            )
        merged_state = local_state
        if isinstance(result.get("mergedState"), dict):
            merged_state = cast(dict[str, list[dict[str, Any]]], result.get("mergedState"))
        emit_progress(
            phase_key="merge_apply",
            phase_label="Applying remote changes",
            counts={
                "action": "pull",
                "changed": bool(result.get("changed")),
                "remoteFound": bool(result.get("remoteFound")),
            },
            event_level="muted",
            message="Applying remote sync results.",
        )

        if bool(result.get("changed")):
            emit_progress(
                phase_key="persist_state",
                phase_label="Persisting merged state",
                counts={"action": "pull"},
                event_level="muted",
                message="Persisting merged registry state.",
            )
            with timing.record_stage("applyLocal"):
                self._persist_state(merged_state)

            self.set_sync_status(
                action="pull",
                result="ok",
                pulled=True,
                error="",
            )

        counters = as_json_object(result.get("counters"))
        if counters:
            self._sync_state.save_sync_runtime_state({"counters": counters})

        with timing.record_stage("summarizeLocal"):
            summary = self._summarize_state(self._load_state())
        timing_record = timing.finish(
            {
                "action": "pull",
                "ok": True,
                "changed": bool(result.get("changed")),
                "remoteFound": bool(result.get("remoteFound")),
                "pushed": False,
                "pulled": True,
            }
        )
        append_sync_timing_record(self._sync_timing_history_path, timing_record)
        emit_progress(
            phase_key="finalize",
            phase_label="Finalizing pull",
            counts={
                "action": "pull",
                "activeCount": int(summary.get("activeCount") or 0),
                "pendingCount": int(summary.get("pendingCount") or 0),
                "rejectedCount": int(summary.get("rejectedCount") or 0),
                "changed": bool(result.get("changed")),
            },
            event_level="success",
            message="Sync pull summary updated.",
        )
        return {
            "ok": True,
            "changed": bool(result.get("changed")),
            "remoteFound": bool(result.get("remoteFound")),
            "remoteSha": str(result.get("remoteSha") or ""),
            "remoteGeneratedAt": str(result.get("remoteGeneratedAt") or ""),
            "counters": counters,
            "summary": summary,
            "timing": timing_record,
        }

    def sync_push_sources(
        self, *, progress_callback: Callable[..., None] | None = None
    ) -> dict[str, Any]:
        """Push local sources snapshot to remote.

        Returns:
            Dict with push results
        """
        guard = self._sync_guard()
        if guard:
            return guard

        timing = SyncTimingRecorder()
        emit_progress = progress_callback or (lambda **_kwargs: None)
        emit_progress(
            phase_key="prepare",
            phase_label="Preparing sync push",
            counts={"action": "push"},
            event_level="info",
            message="Preparing sync push.",
        )
        with timing.record_stage("loadLocalRegistry"):
            state = self._load_state()
        emit_progress(
            phase_key="snapshot_build",
            phase_label="Building local snapshot",
            counts={
                "action": "push",
                "activeCount": len(state.get("active") or []),
                "pendingCount": len(state.get("pending") or []),
                "rejectedCount": len(state.get("rejected") or []),
            },
            event_level="muted",
            message="Building local source snapshot.",
        )
        emit_progress(
            phase_key="remote_write",
            phase_label="Writing remote snapshot",
            counts={"action": "push"},
            event_level="muted",
            message="Writing remote snapshot.",
        )
        with timing.record_stage("pushRemote"):
            result = run_profiled(
                self._source_sync.push_sources_snapshot,
                self._sync_config,
                state,
                profile_name="sync_push_remote",
            )
        snapshot = as_json_object(result.get("snapshot"))
        pushed = bool(result.get("pushed", True))
        size_warning = bool(result.get("sizeWarning"))
        if size_warning:
            self._bridge_log(
                "warn",
                "sync_push_snapshot_size_warning",
                sizeBytes=int(result.get("sizeBytes") or 0),
                maxSnapshotSizeBytes=int(result.get("maxSnapshotSizeBytes") or 0),
            )

        self.set_sync_status(
            action="push",
            result="ok",
            pushed=pushed,
            error="",
        )
        counters = as_json_object(result.get("counters"))
        if counters:
            self._sync_state.save_sync_runtime_state({"counters": counters})
        with timing.record_stage("summarizeSnapshot"):
            counts = {
                "active": len(snapshot.get("active") or []),
                "pending": len(snapshot.get("pending") or []),
                "rejected": len(state.get("rejected") or []),
            }
        timing_record = timing.finish(
            {
                "action": "push",
                "ok": True,
                "changed": pushed,
                "remoteFound": bool(result.get("remotePreviouslyExisted")),
                "pushed": pushed,
                "pulled": False,
                "noOp": not pushed,
                "sizeWarning": size_warning,
            }
        )
        append_sync_timing_record(self._sync_timing_history_path, timing_record)
        emit_progress(
            phase_key="finalize",
            phase_label="Finalizing push",
            counts={
                "action": "push",
                "activeCount": counts["active"],
                "pendingCount": counts["pending"],
                "rejectedCount": counts["rejected"],
            },
            event_level="success",
            message="Sync push summary updated."
            if pushed
            else "Sync push skipped; snapshot unchanged.",
        )

        return {
            "ok": True,
            "remoteSha": str(result.get("remoteSha") or ""),
            "remotePreviouslyExisted": bool(result.get("remotePreviouslyExisted")),
            "pushed": pushed,
            "sizeWarning": size_warning,
            "counters": counters,
            "counts": counts,
            "timing": timing_record,
        }

    def startup_sync_pull(self) -> None:
        """Perform startup sync pull if configured.

        This is called at bridge startup to sync from remote.
        """
        config_status = self._source_sync.config_status(self.refresh_sync_config())
        if not config_status.get("enabled"):
            return
        if not config_status.get("ready"):
            missing = ",".join(config_status.get("missing") or [])
            self._bridge_log(
                "warn", "sync_startup_skipped", reason="misconfigured", missing=missing
            )
            return
        try:
            result = self.sync_pull_sources()
            summary = as_json_object(result.get("summary"))
            self._bridge_log(
                "info",
                "sync_startup_pull_done",
                changed=bool(result.get("changed")),
                remoteFound=bool(result.get("remoteFound")),
                active=int(summary.get("activeCount") or 0),
                pending=int(summary.get("pendingCount") or 0),
                rejected=int(summary.get("rejectedCount") or 0),
            )
        except Exception as exc:  # noqa: BLE001
            self.set_sync_status(action="pull", result="error", error=str(exc), pulled=False)
            self._bridge_log("warn", "sync_startup_pull_failed", error=str(exc))

    def schedule_startup_sync_pull(self) -> dict[str, Any]:
        """Schedule startup sync pull without blocking bridge startup."""
        config_status = self._source_sync.config_status(self.refresh_sync_config())
        if not config_status.get("enabled"):
            return {
                "started": False,
                "task": "source_sync",
                "action": "pull",
                "reason": "disabled",
            }
        if not config_status.get("ready"):
            missing = ",".join(config_status.get("missing") or [])
            self._bridge_log(
                "warn", "sync_startup_skipped", reason="misconfigured", missing=missing
            )
            return {
                "started": False,
                "task": "source_sync",
                "action": "pull",
                "reason": "misconfigured",
                "missing": config_status.get("missing") or [],
            }
        try:
            result = self.start_sync_task("pull", reason="startup", automatic=True)
        except Exception as exc:  # noqa: BLE001
            self.set_sync_status(action="pull", result="error", error=str(exc), pulled=False)
            self._bridge_log("warn", "sync_startup_schedule_failed", error=str(exc))
            return {
                "started": False,
                "task": "source_sync",
                "action": "pull",
                "reason": "startup",
                "error": str(exc),
            }
        if bool(result.get("started")):
            self._bridge_log(
                "info",
                "sync_startup_pull_scheduled",
                runId=str(result.get("runId") or ""),
            )
        else:
            self._bridge_log(
                "info",
                "sync_startup_pull_not_started",
                reason=str(result.get("error") or result.get("reason") or ""),
            )
        return result

    # === Task Management ===

    def _reconcile_sync_history(self) -> None:
        """Reconcile sync history.

        Note: Run history persistence is owned by the bridge runtime. This service
        treats in-memory active run/thread tracking as canonical for whether a
        sync task is actually running, even if stale 'started' rows exist.
        """
        return

    def sync_task_running(self) -> bool:
        """Check if a sync task is currently running.

        Returns:
            True if a sync task is running
        """
        with self._ops_state_lock:
            self._reconcile_sync_history()
            threads = self._sync_state.get_active_sync_threads()
            if any(getattr(worker, "is_alive", lambda: False)() for worker in threads.values()):
                return True
            if self._sync_state.get_active_sync_runs():
                return True
            return False

    def wait_for_sync_tasks(self, timeout_s: float = 5.0) -> None:
        """Wait for all active sync tasks to complete.

        Args:
            timeout_s: Maximum time to wait in seconds
        """
        deadline = datetime.now(UTC).timestamp() + max(0.0, float(timeout_s))

        while True:
            with self._ops_state_lock:
                items = list(self._sync_state.get_active_sync_threads().items())

            pending = False
            for run_id, worker in items:
                remaining = max(0.0, deadline - datetime.now(UTC).timestamp())
                is_alive = getattr(worker, "is_alive", None)
                join = getattr(worker, "join", None)
                alive = bool(is_alive()) if callable(is_alive) else False
                if alive and callable(join) and remaining > 0.0:
                    worker.join(timeout=min(0.2, remaining))
                    alive = bool(is_alive()) if callable(is_alive) else False
                if alive:
                    pending = True
                    continue
                with self._ops_state_lock:
                    self._sync_state.remove_active_sync_thread(run_id)

            if not pending or datetime.now(UTC).timestamp() >= deadline:
                return

    def _run_sync_task_worker(
        self,
        run_id: str,
        action: str,
        started_at: str,
        *,
        reason: str = "",
        automatic: bool = False,
    ) -> None:
        def prune_started_rows_for_type(entry_type: str, *, finished_at: str) -> None:
            self._run_history.prune_started_rows_for_type(entry_type, finished_at=finished_at)

        def upsert_run_history(entry: dict[str, Any]) -> None:
            self._run_history.upsert(entry, dedupe_fields=("type", "finishedAt"))
            if self._task_lifecycle is None:
                return
            run_id_text = str(entry.get("runId") or run_id or "").strip()
            finished_at = str(entry.get("finishedAt") or "").strip()
            summary = as_json_object(entry.get("summary"))
            if str(entry.get("status") or "").strip().lower() == "error":
                self._task_lifecycle.fail_run(
                    run_id_text,
                    "sync",
                    finished_at=finished_at,
                    terminal_reason="failed",
                    summary=summary,
                )
            else:
                self._task_lifecycle.finish_run(
                    run_id_text,
                    "sync",
                    finished_at=finished_at,
                    terminal_reason="completed",
                    summary=summary,
                )

        _sync_task_flow.run_sync_task_worker(
            run_id=run_id,
            action=action,
            started_at=started_at,
            reason=reason,
            automatic=automatic,
            parse_iso=self._parse_iso,
            now_utc=now_utc,
            run_sync_pull=self.sync_pull_sources,
            run_sync_push=self.sync_push_sources,
            set_sync_status=self._sync_state.set_sync_status,
            remove_active_sync_run=self._sync_state.remove_active_sync_run,
            remove_active_sync_thread=self._sync_state.remove_active_sync_thread,
            prune_started_rows_for_type=prune_started_rows_for_type,
            upsert_run_history=upsert_run_history,
            bridge_log=self._bridge_log,
            save_json_atomic=save_json_atomic,
            live_task_path=self._sync_live_task_path,
        )

    def start_sync_task(
        self, action: str, *, reason: str = "", automatic: bool = False
    ) -> dict[str, Any]:
        """Start an asynchronous sync task.

        Args:
            action: Action to perform (pull/push)
            reason: Optional reason for the sync
            automatic: Whether this was automatically triggered

        Returns:
            Dict with task start status
        """
        normalized_action = str(action or "").strip().lower()
        if normalized_action not in {"pull", "push"}:
            raise ValueError("Invalid sync action")

        if self.sync_task_running():
            return {
                "started": False,
                "task": "source_sync",
                "action": normalized_action,
                "error": "Sync task already running",
            }

        run_id = f"sync_{uuid.uuid4().hex[:10]}"
        started_at = now_iso()
        if self._task_lifecycle is not None:
            self._task_lifecycle.start_run(
                run_id=run_id,
                task_type="sync",
                started_at=started_at,
                stage=normalized_action,
                owner_kind="bridge_thread",
                summary={
                    "action": normalized_action,
                    "reason": str(reason or ""),
                    "automatic": bool(automatic),
                },
            )

        self._run_history.append(
            {
                "id": run_id,
                "runId": run_id,
                "type": "sync",
                "status": "started",
                "startedAt": started_at,
                "finishedAt": "",
                "durationMs": 0,
                "summary": {
                    "action": normalized_action,
                    "reason": str(reason or ""),
                    "automatic": bool(automatic),
                },
            }
        )

        with self._ops_state_lock:
            self._sync_state.add_active_sync_run(run_id)

        worker = threading.Thread(
            target=self._run_sync_task_worker,
            args=(run_id, normalized_action, started_at),
            kwargs={"reason": str(reason or ""), "automatic": bool(automatic)},
            name=f"sync-task-{normalized_action}-{run_id}",
            daemon=True,
        )

        with self._ops_state_lock:
            self._sync_state.set_active_sync_thread(run_id, worker)

        worker.start()
        self._bridge_log(
            "info",
            "sync_task_started",
            runId=run_id,
            action=normalized_action,
            reason=reason,
            automatic=automatic,
        )

        return {
            "started": True,
            "runId": run_id,
            "task": "source_sync",
            "action": normalized_action,
            "automatic": bool(automatic),
            "reason": str(reason or ""),
        }

    @staticmethod
    def _parse_iso(value: Any) -> datetime | None:
        """Parse ISO timestamp to datetime.

        Args:
            value: ISO timestamp string

        Returns:
            datetime or None if parsing fails
        """
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None


__all__ = [
    "SyncService",
    "SourceSyncModule",
    "BridgeLogFunc",
    "LoadStateFunc",
    "PersistStateFunc",
    "SummarizeStateFunc",
    "RunHistoryFuncs",
]
