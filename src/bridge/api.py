from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Protocol, Tuple, TYPE_CHECKING

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
    root: Any
    data_dir: Any


DesktopLocalDataStoreFactory = Callable[[], Any]
LoadJsonObjectFunc = Callable[[Path, Any], Any]
SaveJsonAtomicFunc = Callable[[Path, Any], None]

LoadStateFunc = Callable[[], Dict[str, List[Dict[str, Any]]]]
SummarizeStateFunc = Callable[[Dict[str, List[Dict[str, Any]]]], Dict[str, int]]
PersistStateFunc = Callable[[Dict[str, List[Dict[str, Any]]]], Dict[str, List[Dict[str, Any]]]]

BridgeLogFunc = Callable[[str, str], None]
BridgeLogWithFieldsFunc = Callable[[str, str], None]

TriggerDiscoveryTaskFunc = Callable[..., Tuple[int, Dict[str, Any]]]
StartTaskFunc = Callable[[Optional[Dict[str, Any]]], Dict[str, Any]]
StartSyncTaskFunc = Callable[..., Dict[str, Any]]
NormalizeReportContractFunc = Callable[[Dict[str, Any]], Dict[str, Any]]


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

    # Grouped services (optional during migration).
    registry: "RegistryService | None" = None
    sync: "SyncService | None" = None
    pipeline: "PipelineService | None" = None
    discovery: "DiscoveryService | None" = None

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
    read_startup_metrics: Callable[[int], List[Dict[str, Any]]] = lambda _limit=200: []  # type: ignore[assignment]

    load_state: LoadStateFunc = lambda: {"active": [], "pending": [], "rejected": []}  # type: ignore[assignment]
    summarize_state: SummarizeStateFunc = lambda _state: {"activeCount": 0, "pendingCount": 0, "rejectedCount": 0}  # type: ignore[assignment]
    persist_state_and_auto_sync: Callable[..., Dict[str, List[Dict[str, Any]]]] = (  # type: ignore[assignment]
        lambda state, **_kw: state
    )

    # Registry helpers used by POST routes.
    move_entries: Callable[[List[Dict[str, Any]], List[str]], Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]] = (  # type: ignore[assignment]
        lambda pending, _ids: ([], list(pending))
    )
    unique_sources: Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]] = lambda rows: list(rows)  # type: ignore[assignment]
    source_identity: Callable[[Dict[str, Any]], str] = lambda _row: ""  # type: ignore[assignment]
    source_url_fingerprint: Callable[[Dict[str, Any]], str] = lambda _row: ""  # type: ignore[assignment]
    normalize_source_url: Callable[[str], str] = lambda url: str(url or "")  # type: ignore[assignment]

    load_json_object: LoadJsonObjectFunc = lambda _path, default: default  # type: ignore[assignment]
    save_json_atomic: SaveJsonAtomicFunc = lambda _path, _payload: None  # type: ignore[assignment]

    # Task / ops helpers used by routes.
    trigger_discovery_task: TriggerDiscoveryTaskFunc = lambda **_kw: (500, {"started": False, "error": "not_implemented"})  # type: ignore[assignment]
    start_fetcher_task: StartTaskFunc = lambda _payload=None: {"started": False, "error": "not_implemented"}  # type: ignore[assignment]
    start_jobs_pipeline_task: StartTaskFunc = lambda _payload=None: {"started": False, "error": "not_implemented"}  # type: ignore[assignment]
    start_sync_task: StartSyncTaskFunc = lambda *_a, **_kw: {"started": False, "error": "not_implemented"}  # type: ignore[assignment]

    compute_ops_health: Callable[[], Dict[str, Any]] = lambda: {"ok": True}  # type: ignore[assignment]
    compute_fetcher_metrics: Callable[..., Dict[str, Any]] = lambda **_kw: {"ok": True}  # type: ignore[assignment]
    sync_history_from_reports: Callable[[], List[Dict[str, Any]]] = lambda: []  # type: ignore[assignment]

    # Sync-specific helpers used by routes.
    get_sync_status_payload: Callable[[], Dict[str, Any]] = lambda: {"ok": True}  # type: ignore[assignment]
    refresh_sync_config: Callable[[], Any] = lambda: None  # type: ignore[assignment]
    test_sync_config: Callable[[], Dict[str, Any]] = lambda: {"ok": False, "error": "not_implemented"}  # type: ignore[assignment]
    sync_pull_sources: Callable[[], Dict[str, Any]] = lambda: {"ok": False, "error": "not_implemented"}  # type: ignore[assignment]
    sync_push_sources: Callable[[], Dict[str, Any]] = lambda: {"ok": False, "error": "not_implemented"}  # type: ignore[assignment]
    update_saved_sync_settings: Callable[[Dict[str, Any]], Dict[str, Any]] = lambda _payload: {}  # type: ignore[assignment]
    sync_config_status: Callable[[], Dict[str, Any]] = lambda: {"enabled": False, "ready": False}  # type: ignore[assignment]
    set_sync_status: Callable[..., None] = lambda **_kw: None  # type: ignore[assignment]
    load_alert_state: Callable[[], Dict[str, Any]] = lambda: {"acked": {}}  # type: ignore[assignment]
    save_alert_state: Callable[[Dict[str, Any]], None] = lambda _payload: None  # type: ignore[assignment]

    # Jobs pipeline status (GET route).
    get_jobs_pipeline_status_payload: Callable[[], Dict[str, Any]] = lambda: {"active": False}  # type: ignore[assignment]

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
        if self.pipeline is not None:
            if self._field_is_default("get_jobs_pipeline_status_payload"):
                self.get_jobs_pipeline_status_payload = self.pipeline.get_status_payload  # type: ignore[assignment]
            if self._field_is_default("start_jobs_pipeline_task"):
                self.start_jobs_pipeline_task = self.pipeline.start_task  # type: ignore[assignment]
        if self.discovery is not None:
            if self._field_is_default("trigger_discovery_task"):
                self.trigger_discovery_task = self.discovery.trigger_discovery_task  # type: ignore[assignment]

    def mark_desktop_session_activity(self, path: str) -> None:
        # Keep routes compatible with the legacy module-global `DESKTOP_SESSION_ACTIVITY_AT`.
        self._mark_desktop_session_activity(path)
        try:
            self.DESKTOP_SESSION_ACTIVITY_AT = str(self.now_iso() or "")
        except Exception:  # noqa: BLE001
            pass

