"""Bridge modules for Baluffo admin operations.

This package contains modular components extracted from admin_bridge.py
to improve maintainability and reduce the "God Object" complexity.

Modules:
    sync_state: State management for sync operations
    sync_service: Core sync business logic
"""

from src.bridge.sync_service import (
    BridgeLogFunc,
    LoadStateFunc,
    PersistStateFunc,
    RunHistoryFuncs,
    SourceSyncModule,
    SummarizeStateFunc,
    SyncService,
)
from src.bridge.sync_state import (
    ACTIVE_SYNC_RUNS,
    ACTIVE_SYNC_THREADS,
    SYNC_CONFIG,
    SYNC_CONFIG_LOCK,
    SYNC_STATE_LOCK,
    SYNC_STATUS,
    SyncState,
    get_default_sync_state,
    load_sync_runtime_state,
    now_iso,
    now_utc,
    save_sync_runtime_state,
    set_sync_status,
)

__all__ = [
    # State variables
    "SYNC_STATE_LOCK",
    "SYNC_CONFIG_LOCK",
    "ACTIVE_SYNC_RUNS",
    "ACTIVE_SYNC_THREADS",
    "SYNC_STATUS",
    "SYNC_CONFIG",
    # State class and functions
    "SyncState",
    "get_default_sync_state",
    "load_sync_runtime_state",
    "save_sync_runtime_state",
    "set_sync_status",
    # Datetime utilities
    "now_iso",
    "now_utc",
    # Service class
    "SyncService",
    # Protocol types
    "SourceSyncModule",
    "BridgeLogFunc",
    "LoadStateFunc",
    "PersistStateFunc",
    "SummarizeStateFunc",
    "RunHistoryFuncs",
]
