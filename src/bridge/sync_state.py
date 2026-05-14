"""Sync state management for Baluffo admin bridge.

This module extracts the sync-related state management from admin_bridge.py
to improve modularity and reduce the "God Object" complexity.

State Variables:
    SYNC_STATE_LOCK: Threading lock for sync state
    ACTIVE_SYNC_RUNS: Set of active sync run IDs
    ACTIVE_SYNC_THREADS: Dict of active sync threads
    SYNC_STATUS: Dict with sync status info
    SYNC_CONFIG: Current sync configuration
    SYNC_CONFIG_LOCK: Lock for sync config
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from src.shared.utils import now_iso, now_utc
from src.source_registry import load_json_object, save_json_atomic

# Default paths (can be overridden via SyncState initialization)
DEFAULT_DATA_DIR = Path(__file__).resolve().parents[2] / "data"
SYNC_CONFIG_PATH_DEFAULT = DEFAULT_DATA_DIR / "source-sync-config.json"
SYNC_RUNTIME_PATH_DEFAULT = DEFAULT_DATA_DIR / "source-sync-runtime.json"

# Global state locks and containers
SYNC_STATE_LOCK = threading.RLock()
SYNC_CONFIG_LOCK = threading.RLock()

# Active sync task tracking
ACTIVE_SYNC_RUNS: set[str] = set()
ACTIVE_SYNC_THREADS: dict[str, threading.Thread] = {}

# Sync status dictionary
SYNC_STATUS: dict[str, Any] = {
    "lastPullAt": "",
    "lastPushAt": "",
    "lastError": "",
    "lastAction": "",
    "lastResult": "",
    "lastPullRemoteSha": "",
    "lastPullRemoteGeneratedAt": "",
    "lastPullSnapshotFormat": "",
}

SYNC_COUNTER_KEYS = (
    "date",
    "totalPushes",
    "totalPulls",
    "noOpSkips",
    "conflictsDetected",
    "conflictsResolved",
    "tombstonesSuppressed",
    "sourcesAdded",
    "sourcesRemoved",
)

# Current sync configuration (will be set by SyncService)
SYNC_CONFIG: Any = None


def _default_sync_counters() -> dict[str, Any]:
    return {
        "date": now_utc().date().isoformat(),
        "totalPushes": 0,
        "totalPulls": 0,
        "noOpSkips": 0,
        "conflictsDetected": 0,
        "conflictsResolved": 0,
        "tombstonesSuppressed": 0,
        "sourcesAdded": 0,
        "sourcesRemoved": 0,
    }


def _normalize_sync_counters(raw: Any) -> dict[str, Any]:
    data = raw if isinstance(raw, dict) else {}
    today = now_utc().date().isoformat()
    date = str(data.get("date") or today).strip() or today
    if date < today:
        return _default_sync_counters()
    normalized = _default_sync_counters()
    normalized["date"] = date
    for key in SYNC_COUNTER_KEYS[1:]:
        try:
            normalized[key] = int(data.get(key) or 0)
        except (TypeError, ValueError):
            normalized[key] = 0
    return normalized


class SyncState:
    """Encapsulates sync state management with configurable paths.

    This class provides a clean interface for managing sync state,
    including runtime state persistence and status tracking.
    """

    def __init__(
        self,
        data_dir: Path | None = None,
        sync_config_path: Path | None = None,
        sync_runtime_path: Path | None = None,
    ):
        """Initialize SyncState with optional custom paths.

        Args:
            data_dir: Base data directory (used if specific paths not provided)
            sync_config_path: Path to sync config file
            sync_runtime_path: Path to sync runtime state file
        """
        if data_dir is None:
            data_dir = DEFAULT_DATA_DIR

        self.sync_config_path = sync_config_path or data_dir / "source-sync-config.json"
        self.sync_runtime_path = sync_runtime_path or data_dir / "source-sync-runtime.json"

        # Ensure data directory exists
        self.sync_config_path.parent.mkdir(parents=True, exist_ok=True)

    def load_sync_runtime_state(self) -> dict[str, Any]:
        """Load sync runtime state from file.

        Returns:
            Dict with runtime state fields (lastPullAt, lastPushAt, etc.)
        """
        payload = load_json_object(self.sync_runtime_path, {})
        raw = payload if isinstance(payload, dict) else {}
        return {
            "lastPullAt": str(raw.get("lastPullAt") or ""),
            "lastPushAt": str(raw.get("lastPushAt") or ""),
            "lastError": str(raw.get("lastError") or ""),
            "lastAction": str(raw.get("lastAction") or ""),
            "lastResult": str(raw.get("lastResult") or ""),
            "lastPullRemoteSha": str(raw.get("lastPullRemoteSha") or ""),
            "lastPullRemoteGeneratedAt": str(raw.get("lastPullRemoteGeneratedAt") or ""),
            "lastPullSnapshotFormat": str(raw.get("lastPullSnapshotFormat") or ""),
            "lastDiscoverySyncFinishedAt": str(raw.get("lastDiscoverySyncFinishedAt") or ""),
            "counters": _normalize_sync_counters(raw.get("counters")),
        }

    def save_sync_runtime_state(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Save sync runtime state to file.

        Args:
            payload: Dict with state fields to persist

        Returns:
            The normalized state that was saved
        """
        normalized = self.load_sync_runtime_state()
        # Only update fields that are valid for runtime state
        valid_keys = {
            "lastPullAt",
            "lastPushAt",
            "lastError",
            "lastAction",
            "lastResult",
            "lastPullRemoteSha",
            "lastPullRemoteGeneratedAt",
            "lastPullSnapshotFormat",
            "lastDiscoverySyncFinishedAt",
            "counters",
        }
        normalized.update({key: value for key, value in payload.items() if key in valid_keys})
        normalized["counters"] = _normalize_sync_counters(normalized.get("counters"))
        save_json_atomic(self.sync_runtime_path, normalized)
        return normalized

    def update_sync_counters(self, **deltas: Any) -> dict[str, Any]:
        """Increment sync counters and persist them."""
        runtime_state = self.load_sync_runtime_state()
        counters = dict(runtime_state.get("counters") or _default_sync_counters())
        for key, value in deltas.items():
            if key not in counters:
                continue
            try:
                counters[key] = int(counters.get(key) or 0) + int(value or 0)
            except (TypeError, ValueError):
                continue
        runtime_state["counters"] = _normalize_sync_counters(counters)
        self.save_sync_runtime_state(runtime_state)
        return dict(runtime_state["counters"])

    def set_sync_status(
        self,
        *,
        action: str = "",
        result: str = "",
        error: str = "",
        pulled: bool = False,
        pushed: bool = False,
        last_pull_remote_sha: str | None = None,
        last_pull_remote_generated_at: str | None = None,
        last_pull_snapshot_format: str | None = None,
    ) -> None:
        """Update sync status with new values.

        Updates both the global SYNC_STATUS and persists to runtime state file.

        Args:
            action: The action being performed (pull/push)
            result: Result of the action (ok/error)
            error: Error message if any
            pulled: Whether a pull was performed
            pushed: Whether a push was performed
        """
        global SYNC_STATUS

        with SYNC_STATE_LOCK:
            runtime_state = self.load_sync_runtime_state()

            if action:
                SYNC_STATUS["lastAction"] = str(action)
                runtime_state["lastAction"] = str(action)

            if result:
                SYNC_STATUS["lastResult"] = str(result)
                runtime_state["lastResult"] = str(result)

            if error:
                SYNC_STATUS["lastError"] = str(error)
                runtime_state["lastError"] = str(error)
            elif action:
                SYNC_STATUS["lastError"] = ""
                runtime_state["lastError"] = ""

            stamp = now_iso()

            if pulled:
                SYNC_STATUS["lastPullAt"] = stamp
                runtime_state["lastPullAt"] = stamp
                if last_pull_remote_sha is not None:
                    SYNC_STATUS["lastPullRemoteSha"] = str(last_pull_remote_sha or "")
                    runtime_state["lastPullRemoteSha"] = str(last_pull_remote_sha or "")
                if last_pull_remote_generated_at is not None:
                    generated_at = str(last_pull_remote_generated_at or "")
                    SYNC_STATUS["lastPullRemoteGeneratedAt"] = generated_at
                    runtime_state["lastPullRemoteGeneratedAt"] = generated_at
                if last_pull_snapshot_format is not None:
                    snapshot_format = str(last_pull_snapshot_format or "")
                    SYNC_STATUS["lastPullSnapshotFormat"] = snapshot_format
                    runtime_state["lastPullSnapshotFormat"] = snapshot_format

            if pushed:
                SYNC_STATUS["lastPushAt"] = stamp
                runtime_state["lastPushAt"] = stamp

            self.save_sync_runtime_state(runtime_state)

    def get_sync_status(self) -> dict[str, Any]:
        """Get current sync status combined with runtime state.

        Returns:
            Dict with sync status fields
        """
        with SYNC_STATE_LOCK:
            runtime_state = {**dict(SYNC_STATUS), **self.load_sync_runtime_state()}
        return runtime_state

    @staticmethod
    def get_active_sync_runs() -> set[str]:
        """Get copy of active sync runs set."""
        with SYNC_STATE_LOCK:
            return set(ACTIVE_SYNC_RUNS)

    @staticmethod
    def add_active_sync_run(run_id: str) -> None:
        """Add a run ID to active sync runs."""
        with SYNC_STATE_LOCK:
            ACTIVE_SYNC_RUNS.add(str(run_id))

    @staticmethod
    def remove_active_sync_run(run_id: str) -> None:
        """Remove a run ID from active sync runs."""
        with SYNC_STATE_LOCK:
            ACTIVE_SYNC_RUNS.discard(str(run_id))

    @staticmethod
    def get_active_sync_threads() -> dict[str, threading.Thread]:
        """Get copy of active sync threads dict."""
        with SYNC_STATE_LOCK:
            return dict(ACTIVE_SYNC_THREADS)

    @staticmethod
    def set_active_sync_thread(run_id: str, thread: threading.Thread) -> None:
        """Add a thread to active sync threads."""
        with SYNC_STATE_LOCK:
            ACTIVE_SYNC_THREADS[str(run_id)] = thread

    @staticmethod
    def remove_active_sync_thread(run_id: str) -> None:
        """Remove a thread from active sync threads."""
        with SYNC_STATE_LOCK:
            ACTIVE_SYNC_THREADS.pop(str(run_id), None)


__all__ = [
    # State variables
    "SYNC_STATE_LOCK",
    "SYNC_CONFIG_LOCK",
    "ACTIVE_SYNC_RUNS",
    "ACTIVE_SYNC_THREADS",
    "SYNC_STATUS",
    "SYNC_CONFIG",
    # Path constants
    "DEFAULT_DATA_DIR",
    "SYNC_CONFIG_PATH_DEFAULT",
    "SYNC_RUNTIME_PATH_DEFAULT",
    # Datetime utilities
    "now_utc",
    "now_iso",
    # Class
    "SyncState",
]
