#!/usr/bin/env python3
"""Stable thin bridge entrypoint and compatibility wrapper surface."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

# Allow running via `py src/admin_bridge.py` from repo root (or elsewhere).
# When executed as a script, Python puts `.../Baluffo/src` on sys.path, not the repo root,
# so absolute imports like `import src.jobs...` would fail without this.
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import source_discovery as discovery
from src import source_registry as source_registry_module
from src import source_sync as source_sync_module
from src.app_version import get_app_version
from src.baluffo_config import get_bridge_defaults, get_security_defaults, get_storage_defaults

# Bridge service/runtime imports. Keep new bridge logic under `src.bridge.*`.
from src.bridge import SYNC_STATE_LOCK, SyncService, SyncState, report_normalizer
from src.bridge import bootstrap as bridge_bootstrap
from src.bridge import config as bridge_config
from src.bridge import html_extractor as _html_extractor
from src.bridge import admin_entrypoint_runtime as admin_entrypoint_runtime_mod
from src.bridge import admin_entrypoint_services as admin_entrypoint_services_mod
from src.bridge import admin_registry_api as admin_registry_api_mod
from src.bridge import admin_task_runtime as admin_task_runtime_mod
from src.bridge import ops_api as _ops_api
from src.bridge import registry_sync_flow as _registry_sync_flow
from src.bridge import run_history_api as _run_history_api
from src.bridge import source_check_api as _source_check_api
from src.bridge import source_check_fetch as _source_check_fetch
from src.bridge import source_check_http as _source_check_http
from src.bridge import source_checker as _source_checker
from src.bridge import sync_task_flow as _sync_task_flow
from src.bridge import task_launch_api as _task_launch_api
from src.bridge.admin_task_history import AdminTaskHistory
from src.bridge.api import BridgeApi
from src.bridge.discovery_service import DiscoveryDeps, DiscoveryPaths, DiscoveryService
from src.bridge.pipeline_service import PipelineService
from src.bridge.registry_service import RegistryPaths, RegistryService
from src.bridge.registry_tombstones import (
    is_tombstoned,
    load_tombstones,
)
from src.bridge.server import runtime_state as bridge_runtime_state
from src.bridge.source_helpers import (
    find_existing_source_by_url,
    find_existing_static_source_by_studio_domain,
    infer_studio_name_from_host,
)
from src.bridge.sync_state import SYNC_CONFIG_PATH_DEFAULT, SYNC_RUNTIME_PATH_DEFAULT
from src.contracts import SCHEMA_VERSION
from src.jobs.parsers import parse_jobpostings_from_html
from src.jobs.pipeline import default_source_loaders
from src.jobs.registry import DEFAULT_STUDIO_SOURCE_REGISTRY
from src.jobs.transport import normalize_url as normalize_job_url
from src.local_data_store import LocalDataPaths, LocalDataStore
from src.ship.desktop_update import DesktopUpdateService
from src.source_registry import (
    ACTIVE_PATH,
    APPROVAL_STATE_PATH,
    DISCOVERY_CANDIDATES_PATH,
    DISCOVERY_REPORT_PATH,
    PENDING_PATH,
    REGISTRY_REASON_MANUAL_SOURCE,
    REGISTRY_REASON_MANUAL_SOURCE_VARIANT,
    REJECTED_PATH,
    TOMBSTONES_PATH,
    ensure_source_id,
    load_json_array,
    load_json_object,
    normalize_source_url,
    save_json_atomic,
    source_identity,
    unique_sources,
)

normalize_fetch_report_contract = report_normalizer.normalize_fetch_report_contract
normalize_discovery_report_contract = report_normalizer.normalize_discovery_report_contract
_safe_int = report_normalizer.safe_int
source_url_fingerprint = source_registry_module.source_url_fingerprint
from src.shared.utils import now_iso, now_utc

OPS_HISTORY_PATH = ROOT / "data" / "admin-run-history.json"
OPS_ALERT_STATE_PATH = ROOT / "data" / "admin-alert-state.json"
JOBS_FETCH_REPORT_PATH = ROOT / "data" / "jobs-fetch-report.json"
JOBS_FETCH_TASKS_PATH = ROOT / "data" / "jobs-fetch-tasks.json"
TASKS_CONFIG_PATH = ROOT / ".vscode" / "tasks.json"
TASK_STATE_PATH = ROOT / "data" / "admin-task-state.json"
SYNC_LIVE_TASK_PATH = ROOT / "data" / "sync-live-task.json"
DISCOVERY_LOG_PATH = ROOT / "data" / "source-discovery.log"
FETCHER_LOG_PATH = ROOT / "data" / "jobs-fetcher.log"
SYNC_CONFIG_PATH = SYNC_CONFIG_PATH_DEFAULT
DISCOVERY_CONFIG_PATH = ROOT / "data" / "source-discovery-config.json"
SYNC_RUNTIME_PATH = SYNC_RUNTIME_PATH_DEFAULT
STARTUP_METRICS_PATH = ROOT / "data" / "desktop-startup-metrics.jsonl"
TOMBSTONES_PATH = ROOT / "data" / "source-registry-tombstones.json"
DESKTOP_UPDATE_STATE_PATH = ROOT / "data" / "updater" / "install-state.json"

MAX_HISTORY_ROWS = 240
OPS_SCHEMA_VERSION = 1
OPS_STATE_LOCK = threading.RLock()
_TASK_HISTORY = AdminTaskHistory(
    history_path=lambda: OPS_HISTORY_PATH,
    task_state_path=lambda: TASK_STATE_PATH,
    max_rows=lambda: MAX_HISTORY_ROWS,
    lock=OPS_STATE_LOCK,
    load_json_array=load_json_array,
    save_json_atomic=save_json_atomic,
    load_json_object=load_json_object,
    parse_iso=lambda value: parse_iso(value),
    now_utc=lambda: now_utc(),
    pid_is_running=lambda pid: pid_is_running(pid),
)


LOG_LEVEL_ORDER = bridge_config.LOG_LEVEL_ORDER
SYNC_CONFIG: Any = None
_SYNC_SERVICE: SyncService | None = None
_SYNC_SERVICE_DATA_DIR: Path | None = None
_SYNC_SERVICE_LOCK = threading.RLock()
_REGISTRY_SERVICE: RegistryService | None = None
_REGISTRY_SERVICE_PATHS: tuple[Path, Path, Path] | None = None
_REGISTRY_SERVICE_LOCK = threading.RLock()
_DISCOVERY_SERVICE: DiscoveryService | None = None
_DISCOVERY_SERVICE_PATHS: tuple[Path, Path, Path, Path] | None = None
_DISCOVERY_SERVICE_LOCK = threading.RLock()
_PIPELINE_SERVICE: PipelineService | None = None
_PIPELINE_SERVICE_LOCK = threading.RLock()
_DESKTOP_UPDATE_SERVICE: DesktopUpdateService | None = None
_DESKTOP_UPDATE_SERVICE_DATA_DIR: Path | None = None
_DESKTOP_UPDATE_SERVICE_LOCK = threading.RLock()


def _get_sync_service() -> SyncService:
    return admin_entrypoint_services_mod.get_sync_service()


def _get_sync_state() -> SyncState:
    return admin_entrypoint_services_mod.get_sync_state()


def _get_registry_service() -> RegistryService:
    return admin_entrypoint_services_mod.get_registry_service()


def _get_discovery_service() -> DiscoveryService:
    return admin_entrypoint_services_mod.get_discovery_service()


def _get_task_launch_api() -> _task_launch_api.TaskLaunchApi:
    return admin_entrypoint_services_mod.get_task_launch_api()


def _get_ops_api() -> _ops_api.OpsApi:
    return admin_entrypoint_services_mod.get_ops_api()


def _get_pipeline_service() -> PipelineService:
    return admin_entrypoint_services_mod.get_pipeline_service()


def _get_desktop_update_service() -> DesktopUpdateService:
    return admin_entrypoint_services_mod.get_desktop_update_service()


RuntimeConfig = bridge_config.RuntimeConfig


RUNTIME_CONFIG = RuntimeConfig(
    root=ROOT,
    data_dir=ROOT / "data",
    host="127.0.0.1",
    port=8877,
    log_format="human",
    log_level="info",
    quiet_requests=False,
    desktop_mode=False,
    owner_mode="",
    owner_token="",
    started_by="",
    owner_idle_timeout_s=0.0,
)

admin_entrypoint_runtime_mod.root = sys.modules[__name__]
admin_entrypoint_services_mod.root = sys.modules[__name__]
admin_registry_api_mod.root = sys.modules[__name__]
admin_task_runtime_mod.root = sys.modules[__name__]


def _normalize_log_level(value: Any, default: str = "info") -> str:
    return bridge_config._normalize_log_level(value, default)


def _normalize_log_format(value: Any, default: str = "human") -> str:
    return bridge_config._normalize_log_format(value, default)


def resolve_runtime_config(
    argv: list[str] | None = None,
    *,
    env: dict[str, str] | None = None,
) -> RuntimeConfig:
    return bridge_config.resolve_runtime_config(
        root=ROOT,
        get_bridge_defaults=get_bridge_defaults,
        get_storage_defaults=get_storage_defaults,
        argv=argv,
        env=env,
    )


def _log_enabled(level: str) -> bool:
    return admin_entrypoint_runtime_mod.log_enabled(level)


def bridge_log(level: str, message: str, **fields: Any) -> None:
    admin_entrypoint_runtime_mod.bridge_log(level, message, **fields)


def configure_runtime_paths(config: RuntimeConfig) -> None:
    admin_entrypoint_runtime_mod.configure_runtime_paths(config)


def startup_banner(config: RuntimeConfig) -> None:
    admin_entrypoint_runtime_mod.startup_banner(config)


def build_bridge_api(config: RuntimeConfig) -> BridgeApi:
    # Keep the admin_bridge surface stable while bridge bootstrap ownership lives under `src.bridge.*`.
    return bridge_bootstrap.build_bridge_api(
        config=config,
        registry=_get_registry_service(),
        sync=_get_sync_service(),
        pipeline=_get_pipeline_service(),
        discovery=_get_discovery_service(),
        normalize_fetch_report_contract=normalize_fetch_report_contract,
        normalize_discovery_report_contract=normalize_discovery_report_contract,
        discovery_report_path=DISCOVERY_REPORT_PATH,
        jobs_fetch_report_path=JOBS_FETCH_REPORT_PATH,
        approval_state_path=APPROVAL_STATE_PATH,
        discovery_log_path=DISCOVERY_LOG_PATH,
        fetcher_log_path=FETCHER_LOG_PATH,
        startup_metrics_path=STARTUP_METRICS_PATH,
        desktop_update_state_path=DESKTOP_UPDATE_STATE_PATH,
        desktop_session_activity_at=bridge_runtime_state.DESKTOP_SESSION_ACTIVITY_AT,
        bridge_log=bridge_log,
        now_iso=now_iso,
        mark_desktop_session_activity=mark_desktop_session_activity,
        get_desktop_session_payload=get_desktop_session_payload,
        update_desktop_session_lifecycle=update_desktop_session_lifecycle,
        desktop_local_data_store=desktop_local_data_store,
        append_startup_metric=append_startup_metric,
        read_startup_metrics=read_startup_metrics,
        get_update_status_payload=lambda: _get_desktop_update_service().get_status_payload(),
        check_for_update=lambda **kw: _get_desktop_update_service().check_for_update(**kw),
        download_update=lambda: _get_desktop_update_service().download_update(),
        install_update=lambda: _get_desktop_update_service().request_install(),
        persist_state_and_auto_sync=persist_state_and_auto_sync,
        add_manual_source=add_manual_source,
        trigger_source_check=trigger_source_check,
        load_json_object=load_json_object,
        save_json_atomic=save_json_atomic,
        start_fetcher_task=start_fetcher_task,
        start_sync_task=start_sync_task,
        get_discovery_config_payload=get_discovery_config_payload,
        update_saved_discovery_settings=update_saved_discovery_settings,
        compute_ops_health=compute_ops_health,
        compute_fetcher_metrics=compute_fetcher_metrics,
        sync_history_from_reports=sync_history_from_reports,
        get_projected_run_history=_get_ops_api().get_projected_run_history,
        get_task_live_payload=_get_ops_api().get_task_live_payload,
        get_current_task_state_payload=_get_ops_api().get_current_task_state_payload,
        should_exit_for_owner_timeout=owner_session_should_exit,
        load_alert_state=load_alert_state,
        save_alert_state=save_alert_state,
    )


def load_saved_sync_settings() -> dict[str, Any]:
    return _get_sync_service().load_saved_sync_settings()


def append_startup_metric(event: str, payload: dict[str, Any] | None = None) -> None:
    admin_entrypoint_runtime_mod.append_startup_metric(event, payload)


def read_startup_metrics(limit: int = 200) -> list[dict[str, Any]]:
    return admin_entrypoint_runtime_mod.read_startup_metrics(limit)


# noqa: SLF001


def refresh_sync_config() -> source_sync_module.SyncConfig:
    global SYNC_CONFIG
    SYNC_CONFIG = _get_sync_service().refresh_sync_config()
    return SYNC_CONFIG


def get_saved_sync_config_payload() -> dict[str, Any]:
    return _get_sync_service().get_saved_sync_config_payload()


def update_saved_sync_settings(payload: dict[str, Any]) -> dict[str, Any]:
    return _get_sync_service().update_saved_sync_settings(payload)


def load_saved_discovery_settings() -> dict[str, Any]:
    return _get_discovery_service().load_saved_discovery_settings()


def get_discovery_config_payload() -> dict[str, Any]:
    return _get_discovery_service().get_discovery_config_payload()


def update_saved_discovery_settings(payload: dict[str, Any]) -> dict[str, Any]:
    return _get_discovery_service().update_saved_discovery_settings(payload)


def load_sync_runtime_state() -> dict[str, Any]:
    return _get_sync_state().load_sync_runtime_state()


def save_sync_runtime_state(payload: dict[str, Any]) -> dict[str, Any]:
    return _get_sync_state().save_sync_runtime_state(payload)


def test_sync_config() -> dict[str, Any]:
    return _get_sync_service().test_sync_config()


def ensure_active_registry() -> list[dict[str, Any]]:
    return _get_registry_service().ensure_active_registry()


def normalize_state(state: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    return admin_registry_api_mod.normalize_state(state)


def load_state() -> dict[str, list[dict[str, Any]]]:
    return admin_registry_api_mod.load_state()


def summarize_state(state: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
    return admin_registry_api_mod.summarize_state(state)


def persist_state(state: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    return admin_registry_api_mod.persist_state(state)


def persist_state_and_auto_sync(
    state: dict[str, list[dict[str, Any]]], *, reason: str
) -> dict[str, list[dict[str, Any]]]:
    return admin_registry_api_mod.persist_state_and_auto_sync(state, reason=reason)


def move_entries(
    pending: list[dict[str, Any]], selected_ids: list[str]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return admin_registry_api_mod.move_entries(pending, selected_ids)


def build_manual_candidate(normalized_url: str) -> dict[str, Any] | None:
    return admin_registry_api_mod.build_manual_candidate(normalized_url)


def add_manual_source(raw_url: str) -> dict[str, Any]:
    return admin_registry_api_mod.add_manual_source(raw_url)


def _fetch_html_with_fallback_bound(url: str, timeout_s: int) -> tuple[str, str, bool, bool]:
    return admin_registry_api_mod.fetch_html_with_fallback_bound(url, timeout_s)


def _fetch_static_page_with_alternates_bound(
    page_url: str, timeout_s: int
) -> tuple[str, str, bool, bool, str]:
    return admin_registry_api_mod.fetch_static_page_with_alternates_bound(page_url, timeout_s)


def check_static_source(
    row: dict[str, Any], timeout_s: int = 12
) -> tuple[bool, int, str, bool, dict[str, Any]]:
    return admin_registry_api_mod.check_static_source(row, timeout_s)


def normalize_manual_static_studio_fields(row: dict[str, Any]) -> dict[str, Any]:
    return admin_registry_api_mod.normalize_manual_static_studio_fields(row)


def trigger_source_check(source_id: str, timeout_s: int = 12) -> dict[str, Any]:
    return admin_registry_api_mod.trigger_source_check(source_id, timeout_s=timeout_s)


def run_background_script(
    script_name: str,
    args: list[str] | None = None,
    *,
    extra_env: dict[str, str] | None = None,
) -> int:
    return _get_task_launch_api().run_background_script(
        script_name,
        args,
        extra_env,
        is_frozen=bool(getattr(sys, "frozen", False)),
        executable=str(sys.executable),
        spawn_process=subprocess.Popen,
        devnull=subprocess.DEVNULL,
        stdout_target=subprocess.STDOUT,
        create_no_window=int(getattr(subprocess, "CREATE_NO_WINDOW", 0)),
    )


def _failed_source_names_from_latest_report(*, allowed_names: set[str] | None = None) -> list[str]:
    return _get_ops_api().failed_source_names_from_latest_report(allowed_names=allowed_names)


def build_fetcher_args_from_payload(payload: dict[str, Any]) -> tuple[list[str], str]:
    return _get_task_launch_api().build_fetcher_args_from_payload(payload)


def mark_desktop_session_activity(path: str) -> None:
    return None


def get_desktop_session_payload() -> dict[str, Any]:
    return admin_entrypoint_runtime_mod.get_desktop_session_payload()


def update_desktop_session_lifecycle(
    *, owner_token: str, session_id: str, page_id: str, state: str
) -> tuple[int, dict[str, Any]]:
    return admin_entrypoint_runtime_mod.update_desktop_session_lifecycle(
        owner_token=owner_token,
        session_id=session_id,
        page_id=page_id,
        state=state,
    )


def owner_session_should_exit() -> bool:
    return admin_entrypoint_runtime_mod.owner_session_should_exit()


def parse_iso(value: Any) -> datetime | None:
    return admin_entrypoint_runtime_mod.parse_iso(value)


def pid_is_running(pid: int) -> bool:
    return admin_entrypoint_runtime_mod.pid_is_running(pid)


load_run_history = _TASK_HISTORY.load
save_run_history = _TASK_HISTORY.save_run_history
append_run_history = _TASK_HISTORY.append
upsert_run_history = _TASK_HISTORY.upsert
prune_started_rows_for_type = _TASK_HISTORY.prune_started_rows_for_type
_clear_task_state_locked = _TASK_HISTORY.clear_task_state_locked
clear_task_state = _TASK_HISTORY.clear_task_state
task_running_from_state = _TASK_HISTORY.task_running_from_state
report_is_stale_in_progress = _TASK_HISTORY.report_is_stale_in_progress


def _read_tasks_config() -> dict[str, Any]:
    return admin_task_runtime_mod.read_tasks_config()


def load_alert_state() -> dict[str, Any]:
    return _get_ops_api().load_alert_state()


def save_alert_state(state: dict[str, Any]) -> None:
    _get_ops_api().save_alert_state(state)


def detect_task_interval_hours(task: dict[str, Any]) -> float | None:
    return _get_ops_api().detect_task_interval_hours(task)


def parse_schedule_metadata() -> dict[str, Any]:
    return _get_ops_api().parse_schedule_metadata()


def summarize_fetch_report(report: dict[str, Any]) -> dict[str, Any]:
    return _get_ops_api().summarize_fetch_report(report)


def summarize_discovery_report(report: dict[str, Any]) -> tuple[dict[str, Any], str]:
    return _get_ops_api().summarize_discovery_report(report)


def sync_history_from_reports() -> list[dict[str, Any]]:
    return _get_ops_api().sync_history_from_reports()


def get_projected_run_history() -> _run_history_api.LifecycleProjection:
    return _get_ops_api().get_projected_run_history()


def compute_ops_health() -> dict[str, Any]:
    return _get_ops_api().compute_ops_health()


def compute_fetcher_metrics(window_runs: int = 20) -> dict[str, Any]:
    return _get_ops_api().compute_fetcher_metrics(window_runs=window_runs)


def _set_sync_status(
    *,
    action: str = "",
    result: str = "",
    error: str = "",
    pulled: bool = False,
    pushed: bool = False,
) -> None:
    admin_task_runtime_mod.set_sync_status(
        action=action,
        result=result,
        error=error,
        pulled=pulled,
        pushed=pushed,
    )


def get_sync_status_payload() -> dict[str, Any]:
    return admin_task_runtime_mod.get_sync_status_payload()


def _sync_guard() -> dict[str, Any] | None:
    return admin_task_runtime_mod.sync_guard()


def sync_pull_sources() -> dict[str, Any]:
    return admin_task_runtime_mod.sync_pull_sources()


def sync_push_sources() -> dict[str, Any]:
    return admin_task_runtime_mod.sync_push_sources()


def startup_sync_pull() -> None:
    admin_task_runtime_mod.startup_sync_pull()


def sync_task_running() -> bool:
    return admin_task_runtime_mod.sync_task_running()


def wait_for_sync_tasks(timeout_s: float = 5.0) -> None:
    admin_task_runtime_mod.wait_for_sync_tasks(timeout_s=float(timeout_s))


def _mark_discovery_sync_finished(finished_at: str) -> None:
    admin_task_runtime_mod.mark_discovery_sync_finished(finished_at)


def _maybe_trigger_auto_sync_push(reason: str) -> bool:
    return admin_task_runtime_mod.maybe_trigger_auto_sync_push(reason)


def _run_sync_task_worker(
    run_id: str, action: str, started_at: str, *, reason: str = "", automatic: bool = False
) -> None:
    admin_task_runtime_mod.run_sync_task_worker(
        run_id,
        action,
        started_at,
        reason=reason,
        automatic=automatic,
    )


def start_sync_task(action: str, *, reason: str = "", automatic: bool = False) -> dict[str, Any]:
    return _get_sync_service().start_sync_task(action, reason=reason, automatic=bool(automatic))


def trigger_discovery_task(
    payload: dict[str, Any] | None = None,
    *,
    route_name: str,
    enable_auto_sync_watch: bool = True,
) -> tuple[int, dict[str, Any]]:
    return _get_discovery_service().trigger_discovery_task(
        route_name=route_name,
        payload=payload if isinstance(payload, dict) else {},
        enable_auto_sync_watch=enable_auto_sync_watch,
    )


def _current_fetch_output_count() -> int:
    return admin_task_runtime_mod.current_fetch_output_count()


def get_jobs_pipeline_status_payload() -> dict[str, Any]:
    return admin_task_runtime_mod.get_jobs_pipeline_status_payload()


def _wait_for_report_completion(
    *,
    report_path: Path,
    started_at: str,
    timeout_s: float,
    report_name: str,
    fail_on_stale: bool = False,
) -> dict[str, Any]:
    return admin_task_runtime_mod.wait_for_report_completion(
        report_path=report_path,
        started_at=started_at,
        timeout_s=timeout_s,
        report_name=report_name,
        fail_on_stale=fail_on_stale,
    )


def _wait_for_sync_completion(run_id: str, timeout_s: float = 900.0) -> dict[str, Any]:
    return admin_task_runtime_mod.wait_for_sync_completion(run_id, timeout_s=timeout_s)


def start_fetcher_task(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return admin_task_runtime_mod.start_fetcher_task(payload)


def start_jobs_pipeline_task(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return admin_task_runtime_mod.start_jobs_pipeline_task(payload)


def desktop_local_data_store() -> LocalDataStore:
    return admin_entrypoint_runtime_mod.desktop_local_data_store()


def parse_args(argv: list[str] | None = None) -> RuntimeConfig:
    return resolve_runtime_config(argv)


def main() -> int:
    config = parse_args()
    configure_runtime_paths(config)
    refresh_sync_config()
    ensure_active_registry()
    startup_sync_pull()
    return bridge_bootstrap.run_bridge_server(
        api=build_bridge_api(config),
        host=config.host,
        port=config.port,
    )


if __name__ == "__main__":
    raise SystemExit(main())
