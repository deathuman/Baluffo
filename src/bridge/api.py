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
    desktop_session_id: str
    started_by: str
    owner_idle_timeout_s: float
    root: Any
    data_dir: Any


JsonObject = dict[str, Any]
RegistryRow = dict[str, Any]
RegistryState = dict[str, list[RegistryRow]]
StateSummary = dict[str, int]

DesktopLocalDataStoreFactory = Callable[[], Any]
LoadJsonObjectFunc = Callable[[Path, JsonObject | None], JsonObject]
SaveJsonAtomicFunc = Callable[[Path, Any], None]

LoadStateFunc = Callable[[], RegistryState]
SummarizeStateFunc = Callable[[RegistryState], StateSummary]
PersistStateFunc = Callable[[RegistryState], RegistryState]
LoadTombstonesFunc = Callable[[], JsonObject]
SaveTombstonesFunc = Callable[[JsonObject], JsonObject]

BridgeLogFunc = Callable[..., None]

TriggerDiscoveryTaskFunc = Callable[..., tuple[int, JsonObject]]
StartTaskFunc = Callable[[JsonObject | None], JsonObject]
StartSyncTaskFunc = Callable[..., JsonObject]
NormalizeReportContractFunc = Callable[[JsonObject], JsonObject]


def _identity_report_contract(payload: JsonObject) -> JsonObject:
    return payload


def _noop(*_args: Any, **_kwargs: Any) -> None:
    return None


def _empty_string() -> str:
    return ""


def _noop_mark_desktop_session_activity(_path: str) -> None:
    return None


def _empty_desktop_session_payload() -> JsonObject:
    return {
        "sessionId": "",
        "ownerToken": "",
        "lastActivityAt": "",
    }


def _not_implemented_lifecycle(**_kw: Any) -> tuple[int, JsonObject]:
    return 409, {"ok": False, "error": "not_implemented"}


def _noop_desktop_local_data_store() -> None:
    return None


def _empty_startup_metrics(_limit: int = 200) -> list[JsonObject]:
    return []


def _default_update_status_payload() -> JsonObject:
    return {
        "schemaVersion": 1,
        "currentVersion": "",
        "latestVersion": "",
        "updateAvailable": False,
        "availability": "unknown",
        "downloadState": "idle",
        "installState": "idle",
        "releaseNotesUrl": "",
        "releaseNotesTitle": "",
        "releaseNotesBody": "",
        "releaseNotesPublishedAt": "",
        "releaseNotesHistory": [],
        "lastCheckedAt": "",
        "lastError": "",
    }


def _not_started_noarg() -> JsonObject:
    return _not_started_result()


def _not_started_result(_payload: JsonObject | None = None) -> JsonObject:
    return {
        "started": False,
        "error": "not_implemented",
    }


def _empty_registry_state() -> RegistryState:
    return {"active": [], "pending": [], "rejected": []}


def _empty_state_summary(_state: RegistryState) -> StateSummary:
    return {
        "activeCount": 0,
        "pendingCount": 0,
        "rejectedCount": 0,
    }


def _empty_registry_summary_payload() -> JsonObject:
    return {
        "activeCount": 0,
        "pendingCount": 0,
        "rejectedCount": 0,
        "tombstoneCount": 0,
        "stateHash": "",
        "tombstoneHash": "",
    }


def _empty_registry_auto_heal_report() -> JsonObject:
    return {
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


def _identity_registry_state(state: RegistryState, **_kw: Any) -> RegistryState:
    return state


def _empty_tombstones() -> JsonObject:
    return {}


def _identity_tombstones(tombstones: JsonObject) -> JsonObject:
    return dict(tombstones or {})


def _invalid_manual_source(_url: str) -> JsonObject:
    return {
        "status": "invalid",
        "error": "not_implemented",
    }


def _default_move_entries(
    pending: list[RegistryRow], _selected_ids: list[str]
) -> tuple[list[RegistryRow], list[RegistryRow]]:
    return [], list(pending)


def _default_load_json_object(_path: Path, default: JsonObject | None) -> JsonObject:
    return dict(default or {})


def _noop_save_json_atomic(_path: Path, _payload: Any) -> None:
    return None


def _not_started_discovery_task(**_kw: Any) -> tuple[int, JsonObject]:
    return 500, {"started": False, "error": "not_implemented"}


def _ok_payload() -> JsonObject:
    return {"ok": True}


def _empty_rows_payload() -> JsonObject:
    return {"rows": []}


def _identity_payload(payload: JsonObject) -> JsonObject:
    return dict(payload)


def _default_task_live_payload(_task_type: str = "") -> JsonObject:
    return {
        "taskType": "",
        "active": False,
    }


def _default_current_task_state_payload() -> JsonObject:
    return {"tasks": [], "count": 0}


def _default_current_task_state_summary_payload() -> JsonObject:
    return {"tasks": [], "count": 0, "summary": True}


def _disabled_sync_config_status() -> JsonObject:
    return {"enabled": False, "ready": False}


def _default_discovery_config_payload() -> JsonObject:
    return {
        "ok": True,
        "savedConfig": {},
    }


def _default_alert_state() -> JsonObject:
    return {"acked": {}}


def _save_alert_state(_payload: JsonObject) -> None:
    return None


def _inactive_jobs_pipeline_payload() -> JsonObject:
    return {"active": False}


def _always_false() -> bool:
    return False


def _ok_payload_with_kwargs(**_kw: Any) -> JsonObject:
    return _ok_payload()


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
    SOURCE_POLICY_RECOMMENDATIONS_PATH: Path = Path("source-policy-recommendations.json")
    SOURCE_POLICY_REVIEW_STATE_PATH: Path = Path("source-policy-review-state.json")
    DEDUP_REVIEW_STATE_PATH: Path = Path("data") / "dedup-review-state.json"
    DISCOVERY_CANDIDATES_PATH: Path | None = None
    DESKTOP_UPDATE_STATE_PATH: Path | None = None

    # Grouped services (optional during migration).
    registry: RegistryService | None = None
    sync: SyncService | None = None
    pipeline: PipelineService | None = None
    discovery: DiscoveryService | None = None

    # Report contract normalizers used by GET routes.
    normalize_discovery_report_contract: NormalizeReportContractFunc = _identity_report_contract
    normalize_fetch_report_contract: NormalizeReportContractFunc = _identity_report_contract

    # Minimal state shared with routes/handler.
    DESKTOP_SESSION_ACTIVITY_AT: str = ""

    # Core capabilities used by routes.
    bridge_log: BridgeLogFunc = _noop
    now_iso: Callable[[], str] = _empty_string
    _mark_desktop_session_activity: Callable[[str], None] = _noop_mark_desktop_session_activity
    get_desktop_session_payload: Callable[[], JsonObject] = _empty_desktop_session_payload
    update_desktop_session_lifecycle: Callable[..., tuple[int, JsonObject]] = (
        _not_implemented_lifecycle
    )

    desktop_local_data_store: DesktopLocalDataStoreFactory = _noop_desktop_local_data_store
    append_startup_metric: Callable[[str, dict[str, Any] | None], None] = _noop
    read_startup_metrics: Callable[[int], list[JsonObject]] = _empty_startup_metrics
    get_update_status_payload: Callable[[], JsonObject] = _default_update_status_payload
    check_for_update: Callable[..., JsonObject] = _not_started_result
    download_update: Callable[[], JsonObject] = _not_started_noarg
    install_update: Callable[[], JsonObject] = _not_started_noarg

    load_state: LoadStateFunc = _empty_registry_state
    summarize_state: SummarizeStateFunc = _empty_state_summary
    get_registry_summary_payload: Callable[[], JsonObject] = _empty_registry_summary_payload
    get_registry_auto_heal_report: Callable[[], JsonObject] = _empty_registry_auto_heal_report
    persist_state_and_auto_sync: Callable[..., RegistryState] = _identity_registry_state
    load_tombstones: LoadTombstonesFunc = _empty_tombstones
    save_tombstones: SaveTombstonesFunc = _identity_tombstones
    add_manual_source: Callable[[str], dict[str, Any]] = _invalid_manual_source
    trigger_source_check: Callable[..., dict[str, Any]] = _not_started_result
    check_registry_conflicts: Callable[[JsonObject | None], JsonObject] = _not_started_result
    load_registry_conflict_adjudication: Callable[[], JsonObject] = _ok_payload

    # Registry helpers used by POST routes.
    move_entries: Callable[
        [list[RegistryRow], list[str]], tuple[list[RegistryRow], list[RegistryRow]]
    ] = _default_move_entries
    unique_sources: Callable[[list[dict[str, Any]]], list[dict[str, Any]]] = unique_sources_impl
    source_identity: Callable[[dict[str, Any]], str] = source_identity_impl
    source_url_fingerprint: Callable[[dict[str, Any]], str] = source_url_fingerprint_impl
    normalize_source_url: Callable[[str], str] = normalize_source_url_impl

    load_json_object: LoadJsonObjectFunc = _default_load_json_object
    save_json_atomic: SaveJsonAtomicFunc = _noop_save_json_atomic

    # Task / ops helpers used by routes.
    trigger_discovery_task: TriggerDiscoveryTaskFunc = _not_started_discovery_task
    start_jobs_bootstrap_task: StartTaskFunc = _not_started_result
    start_fetcher_task: StartTaskFunc = _not_started_result
    start_jobs_pipeline_task: StartTaskFunc = _not_started_result
    start_sync_task: StartSyncTaskFunc = _not_started_result

    compute_ops_health: Callable[[], JsonObject] = _ok_payload
    compute_ops_dashboard_health: Callable[[], JsonObject] = _ok_payload
    get_storage_health_payload: Callable[[], JsonObject] = _ok_payload
    compute_fetcher_metrics: Callable[..., JsonObject] = _ok_payload_with_kwargs
    sync_history_from_reports: Callable[[], list[JsonObject]] = _empty_startup_metrics
    get_projected_run_history: Callable[[], JsonObject] = _empty_rows_payload
    get_lifecycle_run_history_rows: Callable[[], list[JsonObject]] = _empty_startup_metrics
    get_task_live_payload: Callable[[str], JsonObject] = _default_task_live_payload
    get_current_task_state_payload: Callable[[], JsonObject] = _default_current_task_state_payload
    get_current_task_state_summary_payload: Callable[
        [],
        JsonObject,
    ] = _default_current_task_state_summary_payload
    should_exit_for_owner_timeout: Callable[[], bool] = _always_false

    # Sync-specific helpers used by routes.
    get_sync_status_payload: Callable[[], JsonObject] = _ok_payload
    refresh_sync_config: Callable[[], Any] = _noop_desktop_local_data_store
    test_sync_config: Callable[[], JsonObject] = _not_started_noarg
    sync_pull_sources: Callable[[], JsonObject] = _not_started_noarg
    sync_push_sources: Callable[[], JsonObject] = _not_started_noarg
    update_saved_sync_settings: Callable[[JsonObject], JsonObject] = _identity_payload
    sync_config_status: Callable[[], JsonObject] = _disabled_sync_config_status
    set_sync_status: Callable[..., None] = _noop
    get_discovery_config_payload: Callable[[], JsonObject] = _default_discovery_config_payload
    update_saved_discovery_settings: Callable[[JsonObject], JsonObject] = _identity_payload
    load_alert_state: Callable[[], JsonObject] = _default_alert_state
    save_alert_state: Callable[[JsonObject], None] = _save_alert_state

    # Jobs pipeline status (GET route).
    get_jobs_pipeline_status_payload: Callable[[], JsonObject] = _inactive_jobs_pipeline_payload

    def _field_is_default(self, field_name: str) -> bool:
        try:
            default = type(self).__dataclass_fields__[field_name].default
        except Exception:  # noqa: BLE001
            return False
        try:
            current = getattr(self, field_name)
        except Exception:  # noqa: BLE001
            return False
        return current is default

    def _wire_registry_defaults(self) -> None:
        if self.registry is None:
            return
        registry_bindings = (
            ("load_state", self.registry.load_state),
            ("summarize_state", self.registry.summarize_state),
            (
                "get_registry_summary_payload",
                getattr(self.registry, "get_summary_payload", _empty_registry_summary_payload),
            ),
            ("get_registry_auto_heal_report", self.registry.get_auto_heal_report),
            ("load_tombstones", self.registry.load_tombstones),
            ("save_tombstones", self.registry.save_tombstones),
            ("move_entries", self.registry.move_entries),
            ("unique_sources", self.registry.unique_sources),
            ("source_identity", self.registry.source_identity),
            ("source_url_fingerprint", self.registry.source_url_fingerprint),
            ("normalize_source_url", self.registry.normalize_source_url),
        )
        for field_name, value in registry_bindings:
            if self._field_is_default(field_name):
                setattr(self, field_name, value)

    def __post_init__(self) -> None:
        # Prefer typed services when provided, but only override behaviors that
        # were left at the default stubs.
        self._wire_registry_defaults()
        if self.sync is not None:
            if self._field_is_default("get_sync_status_payload"):
                self.get_sync_status_payload = self.sync.get_sync_status_payload
            if self._field_is_default("refresh_sync_config"):
                self.refresh_sync_config = self.sync.refresh_sync_config
            if self._field_is_default("test_sync_config"):
                self.test_sync_config = self.sync.test_sync_config
            if self._field_is_default("sync_pull_sources"):
                self.sync_pull_sources = self.sync.sync_pull_sources
            if self._field_is_default("sync_push_sources"):
                self.sync_push_sources = self.sync.sync_push_sources
            if self._field_is_default("update_saved_sync_settings"):
                self.update_saved_sync_settings = self.sync.update_saved_sync_settings
            if self._field_is_default("sync_config_status"):
                self.sync_config_status = self.sync.sync_config_status
            if self._field_is_default("set_sync_status"):
                self.set_sync_status = self.sync.set_sync_status
        if self.pipeline is not None:
            if self._field_is_default("get_jobs_pipeline_status_payload"):
                self.get_jobs_pipeline_status_payload = self.pipeline.get_status_payload
            if self._field_is_default("start_jobs_pipeline_task"):
                self.start_jobs_pipeline_task = self.pipeline.start_task
        if self.discovery is not None:
            if self._field_is_default("trigger_discovery_task"):
                self.trigger_discovery_task = self.discovery.trigger_discovery_task
            if self._field_is_default("get_discovery_config_payload"):
                self.get_discovery_config_payload = self.discovery.get_discovery_config_payload
            if self._field_is_default("update_saved_discovery_settings"):
                self.update_saved_discovery_settings = (
                    self.discovery.update_saved_discovery_settings
                )

    def mark_desktop_session_activity(self, path: str) -> None:
        # Keep routes compatible with the legacy module-global `DESKTOP_SESSION_ACTIVITY_AT`.
        self._mark_desktop_session_activity(path)
        try:
            self.DESKTOP_SESSION_ACTIVITY_AT = str(self.now_iso() or "")
        except Exception:  # noqa: BLE001
            pass
