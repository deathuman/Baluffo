"""Bridge API composition object for admin bridge.

This module provides the BridgeApi composition object that wires
all bridge services together for use by HTTP routes.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from src.source_registry import normalize_source_url as normalize_source_url_impl
from src.source_registry import source_identity as source_identity_impl
from src.source_registry import source_url_fingerprint as source_url_fingerprint_impl
from src.source_registry import unique_sources as unique_sources_impl

if TYPE_CHECKING:
    from src.bridge.discovery_service import DiscoveryService
    from src.bridge.pipeline_service import PipelineService
    from src.bridge.registry_service import RegistryService
    from src.bridge.sync_service import SyncService


class RuntimeConfigLike(Protocol):
    host: str
    port: int
    quiet_requests: bool
    desktop_mode: bool
    owner_mode: str
    owner_token: str
    started_by: str
    owner_idle_timeout_s: float
    root: Any
    data_dir: Any


DesktopLocalDataStoreFactory = Callable[[], Any]
LoadJsonObjectFunc = Callable[[Path, Any], Any]
SaveJsonAtomicFunc = Callable[[Path, Any], None]

LoadStateFunc = Callable[[], dict[str, list[dict[str, Any]]]]
SummarizeStateFunc = Callable[[dict[str, list[dict[str, Any]]]], dict[str, int]]
PersistStateFunc = Callable[[dict[str, list[dict[str, Any]]]], dict[str, list[dict[str, Any]]]]

BridgeLogFunc = Callable[[str, str], None]

TriggerDiscoveryTaskFunc = Callable[..., tuple[int, dict[str, Any]]]
StartTaskFunc = Callable[[dict[str, Any] | None], dict[str, Any]]
StartSyncTaskFunc = Callable[..., dict[str, Any]]
NormalizeReportContractFunc = Callable[[dict[str, Any]], dict[str, Any]]


@dataclass
class BridgeApi:
    """
    Explicit API object for the local admin bridge HTTP routes.

    This is intentionally a thin composition layer: concrete behavior lives in
    services/modules under `src.bridge.*` and/or is injected from the entrypoint
    during migration away from `src.admin_bridge` globals.
    """

    # Runtime/config
    runtime_config: RuntimeConfigLike

    # Frequently used paths (routes access these directly today).
    DISCOVERY_REPORT_PATH: Path
    JOBS_FETCH_REPORT_PATH: Path
    APPROVAL_STATE_PATH: Path
    DISCOVERY_LOG_PATH: Path
    FETCHER_LOG_PATH: Path
    STARTUP_METRICS_PATH: Path
    DESKTOP_UPDATE_STATE_PATH: Path | None = None

    # Grouped services (optional during migration).
    registry: RegistryService | None = None
    sync: SyncService | None = None
    pipeline: PipelineService | None = None
    discovery: DiscoveryService | None = None

    # Report contract normalizers used by GET routes.
    normalize_discovery_report_contract: NormalizeReportContractFunc = lambda payload: payload  # type: ignore[assignment]
    normalize_fetch_report_contract: NormalizeReportContractFunc = lambda payload: payload  # type: ignore[assignment]

    # Minimal state shared with routes/handler.
    DESKTOP_SESSION_ACTIVITY_AT: str = ""

    # Core capabilities used by routes.
    bridge_log: Callable[..., None] = lambda *args, **kwargs: None  # type: ignore[assignment]
    now_iso: Callable[[], str] = lambda: ""  # type: ignore[assignment]
    _mark_desktop_session_activity: Callable[[str], None] = lambda _path: None  # type: ignore[assignment]

    desktop_local_data_store: DesktopLocalDataStoreFactory = lambda: None  # type: ignore[assignment]
    append_startup_metric: Callable[[str, dict[str, Any] | None], None] = (
        lambda _event, _payload=None: None
    )  # type: ignore[assignment]
    read_startup_metrics: Callable[[int], list[dict[str, Any]]] = lambda _limit=200: []  # type: ignore[assignment]
    get_update_status_payload: Callable[[], dict[str, Any]] = lambda: {  # type: ignore[assignment]
        "schemaVersion": 1,
        "currentVersion": "",
        "latestVersion": "",
        "updateAvailable": False,
        "availability": "unknown",
        "downloadState": "idle",
        "installState": "idle",
        "releaseNotesUrl": "",
        "lastCheckedAt": "",
        "lastError": "",
    }
    check_for_update: Callable[..., dict[str, Any]] = lambda **_kw: {  # type: ignore[assignment]
        "started": False,
        "error": "not_implemented",
    }
    download_update: Callable[[], dict[str, Any]] = lambda: {  # type: ignore[assignment]
        "started": False,
        "error": "not_implemented",
    }
    install_update: Callable[[], dict[str, Any]] = lambda: {  # type: ignore[assignment]
        "started": False,
        "error": "not_implemented",
    }

    load_state: LoadStateFunc = lambda: {"active": [], "pending": [], "rejected": []}  # type: ignore[assignment]
    summarize_state: SummarizeStateFunc = lambda _state: {
        "activeCount": 0,
        "pendingCount": 0,
        "rejectedCount": 0,
    }  # type: ignore[assignment]
    persist_state_and_auto_sync: Callable[..., dict[str, list[dict[str, Any]]]] = (  # type: ignore[assignment]
        lambda state, **_kw: state
    )
    add_manual_source: Callable[[str], dict[str, Any]] = lambda _url: {
        "status": "invalid",
        "error": "not_implemented",
    }  # type: ignore[assignment]
    trigger_source_check: Callable[..., dict[str, Any]] = lambda _source_id, **_kw: {
        "started": False,
        "error": "not_implemented",
    }  # type: ignore[assignment]

    # Registry helpers used by POST routes.
    move_entries: Callable[
        [list[dict[str, Any]], list[str]], tuple[list[dict[str, Any]], list[dict[str, Any]]]
    ] = (  # type: ignore[assignment]
        lambda pending, _ids: ([], list(pending))
    )
    unique_sources: Callable[[list[dict[str, Any]]], list[dict[str, Any]]] = unique_sources_impl  # type: ignore[assignment]
    source_identity: Callable[[dict[str, Any]], str] = source_identity_impl  # type: ignore[assignment]
    source_url_fingerprint: Callable[[dict[str, Any]], str] = source_url_fingerprint_impl  # type: ignore[assignment]
    normalize_source_url: Callable[[str], str] = normalize_source_url_impl  # type: ignore[assignment]

    load_json_object: LoadJsonObjectFunc = lambda _path, default: default  # type: ignore[assignment]
    save_json_atomic: SaveJsonAtomicFunc = lambda _path, _payload: None  # type: ignore[assignment]

    # Task / ops helpers used by routes.
    trigger_discovery_task: TriggerDiscoveryTaskFunc = lambda **_kw: (
        500,
        {"started": False, "error": "not_implemented"},
    )  # type: ignore[assignment]
    start_fetcher_task: StartTaskFunc = lambda _payload=None: {
        "started": False,
        "error": "not_implemented",
    }  # type: ignore[assignment]
    start_jobs_pipeline_task: StartTaskFunc = lambda _payload=None: {
        "started": False,
        "error": "not_implemented",
    }  # type: ignore[assignment]
    start_sync_task: StartSyncTaskFunc = lambda *_a, **_kw: {
        "started": False,
        "error": "not_implemented",
    }  # type: ignore[assignment]

    compute_ops_health: Callable[[], dict[str, Any]] = lambda: {"ok": True}  # type: ignore[assignment]
    compute_fetcher_metrics: Callable[..., dict[str, Any]] = lambda **_kw: {"ok": True}  # type: ignore[assignment]
    sync_history_from_reports: Callable[[], list[dict[str, Any]]] = lambda: []  # type: ignore[assignment]
    get_projected_run_history: Callable[[], Any] = lambda: {"rows": []}  # type: ignore[assignment]
    get_current_task_state_payload: Callable[[], dict[str, Any]] = lambda: {"tasks": [], "count": 0}  # type: ignore[assignment]
    should_exit_for_owner_timeout: Callable[[], bool] = lambda: False  # type: ignore[assignment]

    # Sync-specific helpers used by routes.
    get_sync_status_payload: Callable[[], dict[str, Any]] = lambda: {"ok": True}  # type: ignore[assignment]
    refresh_sync_config: Callable[[], Any] = lambda: None  # type: ignore[assignment]
    test_sync_config: Callable[[], dict[str, Any]] = lambda: {
        "ok": False,
        "error": "not_implemented",
    }  # type: ignore[assignment]
    sync_pull_sources: Callable[[], dict[str, Any]] = lambda: {
        "ok": False,
        "error": "not_implemented",
    }  # type: ignore[assignment]
    sync_push_sources: Callable[[], dict[str, Any]] = lambda: {
        "ok": False,
        "error": "not_implemented",
    }  # type: ignore[assignment]
    update_saved_sync_settings: Callable[[dict[str, Any]], dict[str, Any]] = lambda _payload: {}  # type: ignore[assignment]
    sync_config_status: Callable[[], dict[str, Any]] = lambda: {"enabled": False, "ready": False}  # type: ignore[assignment]
    set_sync_status: Callable[..., None] = lambda **_kw: None  # type: ignore[assignment]
    get_discovery_config_payload: Callable[[], dict[str, Any]] = lambda: {
        "ok": True,
        "savedConfig": {},
    }  # type: ignore[assignment]
    update_saved_discovery_settings: Callable[[dict[str, Any]], dict[str, Any]] = lambda _payload: {}  # type: ignore[assignment]
    load_alert_state: Callable[[], dict[str, Any]] = lambda: {"acked": {}}  # type: ignore[assignment]
    save_alert_state: Callable[[dict[str, Any]], None] = lambda _payload: None  # type: ignore[assignment]

    # Jobs pipeline status (GET route).
    get_jobs_pipeline_status_payload: Callable[[], dict[str, Any]] = lambda: {"active": False}  # type: ignore[assignment]

    def _field_is_default(self, field_name: str) -> bool:
        try:
            default = type(self).__dataclass_fields__[field_name].default  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            return False
        try:
            current = getattr(self, field_name)
        except Exception:  # noqa: BLE001
            return False
        return current is default

    def __post_init__(self) -> None:
        # Prefer typed services when provided, but only override behaviors that
        # were left at the default stubs.
        if self.registry is not None:
            if self._field_is_default("load_state"):
                self.load_state = self.registry.load_state  # type: ignore[assignment]
            if self._field_is_default("summarize_state"):
                self.summarize_state = self.registry.summarize_state  # type: ignore[assignment]
            if self._field_is_default("move_entries"):
                self.move_entries = self.registry.move_entries  # type: ignore[assignment]
            if self._field_is_default("unique_sources"):
                self.unique_sources = self.registry.unique_sources  # type: ignore[assignment]
            if self._field_is_default("source_identity"):
                self.source_identity = self.registry.source_identity  # type: ignore[assignment]
            if self._field_is_default("source_url_fingerprint"):
                self.source_url_fingerprint = self.registry.source_url_fingerprint  # type: ignore[assignment]
            if self._field_is_default("normalize_source_url"):
                self.normalize_source_url = self.registry.normalize_source_url  # type: ignore[assignment]
        if self.sync is not None:
            if self._field_is_default("get_sync_status_payload"):
                self.get_sync_status_payload = self.sync.get_sync_status_payload  # type: ignore[assignment]
            if self._field_is_default("refresh_sync_config"):
                self.refresh_sync_config = self.sync.refresh_sync_config  # type: ignore[assignment]
            if self._field_is_default("test_sync_config"):
                self.test_sync_config = self.sync.test_sync_config  # type: ignore[assignment]
            if self._field_is_default("sync_pull_sources"):
                self.sync_pull_sources = self.sync.sync_pull_sources  # type: ignore[assignment]
            if self._field_is_default("sync_push_sources"):
                self.sync_push_sources = self.sync.sync_push_sources  # type: ignore[assignment]
            if self._field_is_default("update_saved_sync_settings"):
                self.update_saved_sync_settings = self.sync.update_saved_sync_settings  # type: ignore[assignment]
            if self._field_is_default("sync_config_status"):
                self.sync_config_status = self.sync.sync_config_status  # type: ignore[assignment]
            if self._field_is_default("set_sync_status"):
                self.set_sync_status = self.sync.set_sync_status  # type: ignore[assignment]
        if self.pipeline is not None:
            if self._field_is_default("get_jobs_pipeline_status_payload"):
                self.get_jobs_pipeline_status_payload = self.pipeline.get_status_payload  # type: ignore[assignment]
            if self._field_is_default("start_jobs_pipeline_task"):
                self.start_jobs_pipeline_task = self.pipeline.start_task  # type: ignore[assignment]
        if self.discovery is not None:
            if self._field_is_default("trigger_discovery_task"):
                self.trigger_discovery_task = self.discovery.trigger_discovery_task  # type: ignore[assignment]
            if self._field_is_default("get_discovery_config_payload"):
                self.get_discovery_config_payload = self.discovery.get_discovery_config_payload  # type: ignore[assignment]
            if self._field_is_default("update_saved_discovery_settings"):
                self.update_saved_discovery_settings = (
                    self.discovery.update_saved_discovery_settings
                )  # type: ignore[assignment]

    def mark_desktop_session_activity(self, path: str) -> None:
        # Keep routes compatible with the legacy module-global `DESKTOP_SESSION_ACTIVITY_AT`.
        self._mark_desktop_session_activity(path)
        try:
            self.DESKTOP_SESSION_ACTIVITY_AT = str(self.now_iso() or "")
        except Exception:  # noqa: BLE001
            pass
