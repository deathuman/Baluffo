"""Bridge modules for Baluffo admin operations.

This package contains modular components extracted from admin_bridge.py to improve maintainability and reduce the compatibility root surface.

AI boundary owns: bridge package re-exports for sync service and state compatibility.
AI boundary implement in: this file for package surface changes; bridge behavior stays in bridge service leaves.
AI boundary search before contracts: admin_bridge compatibility root, sync service/state modules, and bridge package tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused bridge package tests.
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
    now_iso,
    now_utc,
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
