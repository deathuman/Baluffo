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
    global _SYNC_SERVICE, _SYNC_SERVICE_DATA_DIR
    data_dir = Path(RUNTIME_CONFIG.data_dir).resolve()
    with _SYNC_SERVICE_LOCK:
        if _SYNC_SERVICE is not None and _SYNC_SERVICE_DATA_DIR == data_dir:
            return _SYNC_SERVICE
        _SYNC_SERVICE_DATA_DIR = data_dir
        _SYNC_SERVICE = SyncService(
            data_dir=data_dir,
            source_sync=source_sync_module,
            bridge_log=bridge_log,
            load_state=load_state,
            persist_state=persist_state,
            summarize_state=summarize_state,
            run_history=_TASK_HISTORY,
            ops_state_lock=OPS_STATE_LOCK,
            get_security_defaults=get_security_defaults,
            sync_state=SyncState(data_dir=data_dir),
        )
        return _SYNC_SERVICE


def _get_sync_state() -> SyncState:
    return _get_sync_service()._sync_state  # noqa: SLF001


def _get_registry_service() -> RegistryService:
    global _REGISTRY_SERVICE, _REGISTRY_SERVICE_PATHS
    current_paths = (Path(ACTIVE_PATH), Path(PENDING_PATH), Path(REJECTED_PATH))
    with _REGISTRY_SERVICE_LOCK:
        if _REGISTRY_SERVICE is None or _REGISTRY_SERVICE_PATHS != current_paths:
            _REGISTRY_SERVICE_PATHS = current_paths
            _REGISTRY_SERVICE = RegistryService(
                paths=RegistryPaths(
                    active=ACTIVE_PATH, pending=PENDING_PATH, rejected=REJECTED_PATH
                ),
                default_active=[dict(row) for row in DEFAULT_STUDIO_SOURCE_REGISTRY],
                normalize_manual_static=normalize_manual_static_studio_fields,
            )
        return _REGISTRY_SERVICE


def _get_discovery_service() -> DiscoveryService:
    global _DISCOVERY_SERVICE, _DISCOVERY_SERVICE_PATHS
    current_paths = (
        Path(DISCOVERY_REPORT_PATH),
        Path(DISCOVERY_CANDIDATES_PATH),
        Path(PENDING_PATH),
        Path(DISCOVERY_LOG_PATH),
    )
    with _DISCOVERY_SERVICE_LOCK:
        if _DISCOVERY_SERVICE is None or _DISCOVERY_SERVICE_PATHS != current_paths:
            _DISCOVERY_SERVICE_PATHS = current_paths
            _DISCOVERY_SERVICE = DiscoveryService(
                paths=DiscoveryPaths(
                    report=DISCOVERY_REPORT_PATH,
                    candidates=DISCOVERY_CANDIDATES_PATH,
                    pending=PENDING_PATH,
                    log=DISCOVERY_LOG_PATH,
                    settings=DISCOVERY_CONFIG_PATH,
                    approval_state=APPROVAL_STATE_PATH,
                ),
                deps=DiscoveryDeps(
                    schema_version=SCHEMA_VERSION,
                    now_iso=now_iso,
                    now_utc=now_utc,
                    parse_iso=parse_iso,
                    pid_is_running=pid_is_running,
                    bridge_log=bridge_log,
                    load_json_object=load_json_object,
                    save_json_atomic=save_json_atomic,
                    run_background_script=run_background_script,
                    append_run_history=append_run_history,
                    upsert_run_history=upsert_run_history,
                    prune_started_rows_for_type=prune_started_rows_for_type,
                    clear_task_state=clear_task_state,
                    normalize_discovery_report_contract=normalize_discovery_report_contract,
                    load_state=load_state,
                    persist_state_and_auto_sync=persist_state_and_auto_sync,
                    load_sync_runtime_state=load_sync_runtime_state,
                    maybe_trigger_auto_sync_push=_maybe_trigger_auto_sync_push,
                    mark_discovery_sync_finished=_mark_discovery_sync_finished,
                ),
            )
        return _DISCOVERY_SERVICE


def _get_task_launch_api() -> _task_launch_api.TaskLaunchApi:
    return _task_launch_api.TaskLaunchApi(
        runtime=_task_launch_api.TaskLaunchRuntime(
            root=Path(RUNTIME_CONFIG.root),
            data_dir=Path(RUNTIME_CONFIG.data_dir),
        ),
        paths=_task_launch_api.TaskLaunchPaths(
            discovery_log=DISCOVERY_LOG_PATH,
            discovery_report=DISCOVERY_REPORT_PATH,
            fetcher_log=FETCHER_LOG_PATH,
            task_state=TASK_STATE_PATH,
            jobs_fetch_report=JOBS_FETCH_REPORT_PATH,
            approval_state=APPROVAL_STATE_PATH,
        ),
        deps=_task_launch_api.TaskLaunchDeps(
            now_iso=now_iso,
            bridge_log=bridge_log,
            load_json_object=load_json_object,
            save_json_atomic=save_json_atomic,
            task_state_lock=OPS_STATE_LOCK,
            default_source_loaders=default_source_loaders,
            failed_source_names_from_latest_report=lambda allowed: (
                _failed_source_names_from_latest_report(allowed_names=allowed)
            ),
            safe_int=_safe_int,
        ),
    )


def _get_ops_api() -> _ops_api.OpsApi:
    return _ops_api.OpsApi(
        paths=_ops_api.OpsPaths(
            ops_alert_state=OPS_ALERT_STATE_PATH,
            jobs_fetch_report=JOBS_FETCH_REPORT_PATH,
            jobs_fetch_tasks=JOBS_FETCH_TASKS_PATH,
            discovery_report=DISCOVERY_REPORT_PATH,
            sync_live_task=SYNC_LIVE_TASK_PATH,
            task_state=TASK_STATE_PATH,
        ),
        deps=_ops_api.OpsDeps(
            load_json_object=load_json_object,
            save_json_atomic=save_json_atomic,
            load_state=load_state,
            now_iso=now_iso,
            now_utc=now_utc,
            parse_iso=parse_iso,
            read_tasks_config=_read_tasks_config,
            ops_state_lock=OPS_STATE_LOCK,
            load_run_history=load_run_history,
            save_run_history=save_run_history,
            prune_started_rows_for_type=prune_started_rows_for_type,
            clear_task_state=clear_task_state,
            clear_task_state_locked=_clear_task_state_locked,
            upsert_run_history=upsert_run_history,
            task_running_from_state=task_running_from_state,
            report_is_stale_in_progress=report_is_stale_in_progress,
            get_active_sync_runs=SyncState.get_active_sync_runs,
            get_sync_status_payload=get_sync_status_payload,
            get_jobs_pipeline_status_payload=get_jobs_pipeline_status_payload,
            normalize_fetch_report_contract=normalize_fetch_report_contract,
            normalize_discovery_report_contract=normalize_discovery_report_contract,
            desktop_mode=RUNTIME_CONFIG.desktop_mode,
            get_desktop_last_activity_at=lambda: bridge_runtime_state.DESKTOP_SESSION_ACTIVITY_AT,
            get_owner_state=bridge_runtime_state.get_owner_state,
            ops_schema_version=OPS_SCHEMA_VERSION,
            get_updater_status_payload=lambda: _get_desktop_update_service().get_status_payload(),
            app_version=get_app_version(),
        ),
    )


def _get_pipeline_service() -> PipelineService:
    global _PIPELINE_SERVICE
    with _PIPELINE_SERVICE_LOCK:
        if _PIPELINE_SERVICE is None:
            smoke_mode = (
                str(os.getenv("BALUFFO_PACKAGED_SMOKE_PIPELINE_MODE") or "").strip().lower()
            )
            stub_success_mode = smoke_mode == "stub-success"
            pipeline_load_json_object = load_json_object
            pipeline_trigger_discovery_task = trigger_discovery_task
            pipeline_start_fetcher_task = start_fetcher_task
            pipeline_start_sync_task = start_sync_task
            pipeline_wait_for_sync_completion = _wait_for_sync_completion
            pipeline_current_fetch_output_count = _current_fetch_output_count

            if stub_success_mode:
                smoke_runtime: dict[str, Any] = {
                    "discoveryStartedAt": "",
                    "discoveryReadyAt": 0.0,
                    "fetchStartedAt": "",
                    "fetchReadyAt": 0.0,
                }

                def pipeline_load_json_object(path: Any, default: Any) -> Any:
                    resolved = Path(path).resolve()
                    if resolved == Path(DISCOVERY_REPORT_PATH).resolve():
                        started_at = str(smoke_runtime.get("discoveryStartedAt") or "")
                        if started_at:
                            finished_at = (
                                started_at
                                if time.monotonic()
                                >= float(smoke_runtime.get("discoveryReadyAt") or 0.0)
                                else ""
                            )
                            return {
                                "runId": "discovery_smoke",
                                "startedAt": started_at,
                                "finishedAt": finished_at,
                                "status": "ok" if finished_at else "running",
                                "summary": {},
                            }
                    if resolved == Path(JOBS_FETCH_REPORT_PATH).resolve():
                        started_at = str(smoke_runtime.get("fetchStartedAt") or "")
                        if started_at:
                            finished_at = (
                                started_at
                                if time.monotonic()
                                >= float(smoke_runtime.get("fetchReadyAt") or 0.0)
                                else ""
                            )
                            return {
                                "runId": "fetch_smoke",
                                "startedAt": started_at,
                                "finishedAt": finished_at,
                                "status": "ok" if finished_at else "running",
                                "summary": {"outputCount": 0},
                            }
                    return load_json_object(path, default)

                def pipeline_trigger_discovery_task(**kwargs):
                    started_at = now_iso()
                    smoke_runtime["discoveryStartedAt"] = started_at
                    smoke_runtime["discoveryReadyAt"] = time.monotonic() + 1.2
                    return 200, {
                        "started": True,
                        "startedAt": started_at,
                        "runId": "discovery_smoke",
                    }

                def pipeline_start_fetcher_task(
                    payload: dict[str, Any] | None = None,
                ) -> dict[str, Any]:
                    started_at = now_iso()
                    smoke_runtime["fetchStartedAt"] = started_at
                    smoke_runtime["fetchReadyAt"] = time.monotonic() + 1.2
                    return {"started": True, "startedAt": started_at, "runId": "fetch_smoke"}

                def pipeline_start_sync_task(
                    action: str, *, reason: str, automatic: bool
                ) -> dict[str, Any]:
                    return {"started": True, "runId": "sync_smoke"}

                def pipeline_wait_for_sync_completion(
                    run_id: str, timeout_s: float = 900.0
                ) -> dict[str, Any]:
                    finished_at = now_iso()
                    return {
                        "id": str(run_id or "sync_smoke"),
                        "type": "sync",
                        "status": "ok",
                        "finishedAt": finished_at,
                        "summary": {},
                    }

                def pipeline_current_fetch_output_count() -> int:
                    report = pipeline_load_json_object(JOBS_FETCH_REPORT_PATH, {})
                    summary = summarize_fetch_report(normalize_fetch_report_contract(report))
                    return int(summary.get("outputCount") or 0)

            _PIPELINE_SERVICE = PipelineService(
                pipeline_state_lock=bridge_runtime_state.PIPELINE_STATE_LOCK,
                pipeline_status=bridge_runtime_state.PIPELINE_STATUS,
                runtime=bridge_runtime_state.PIPELINE_RUNTIME,
                bridge_log=bridge_log,
                now_iso=now_iso,
                parse_iso=parse_iso,
                append_run_history=append_run_history,
                upsert_run_history=upsert_run_history,
                task_running_from_state=task_running_from_state,
                sync_task_running=sync_task_running,
                current_fetch_output_count=pipeline_current_fetch_output_count,
                load_json_object=pipeline_load_json_object,
                wait_for_sync_completion=pipeline_wait_for_sync_completion,
                discovery_report_path=DISCOVERY_REPORT_PATH,
                fetch_report_path=JOBS_FETCH_REPORT_PATH,
                trigger_discovery_task=pipeline_trigger_discovery_task,
                start_fetcher_task=pipeline_start_fetcher_task,
                start_sync_task=pipeline_start_sync_task,
                get_app_version=get_app_version,
                get_projected_run_history=_get_ops_api().get_projected_run_history,
            )
        return _PIPELINE_SERVICE


def _get_desktop_update_service() -> DesktopUpdateService:
    global _DESKTOP_UPDATE_SERVICE, _DESKTOP_UPDATE_SERVICE_DATA_DIR
    data_dir = Path(RUNTIME_CONFIG.data_dir).resolve()
    with _DESKTOP_UPDATE_SERVICE_LOCK:
        if _DESKTOP_UPDATE_SERVICE is not None and _DESKTOP_UPDATE_SERVICE_DATA_DIR == data_dir:
            return _DESKTOP_UPDATE_SERVICE
        _DESKTOP_UPDATE_SERVICE_DATA_DIR = data_dir
        _DESKTOP_UPDATE_SERVICE = DesktopUpdateService(
            data_dir=data_dir,
            current_version_getter=get_app_version,
        )
        return _DESKTOP_UPDATE_SERVICE


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
    current = LOG_LEVEL_ORDER.get(_normalize_log_level(RUNTIME_CONFIG.log_level), 20)
    target = LOG_LEVEL_ORDER.get(_normalize_log_level(level), 20)
    return target >= current


def bridge_log(level: str, message: str, **fields: Any) -> None:
    normalized_level = _normalize_log_level(level, "info")
    if not _log_enabled(normalized_level):
        return
    payload = {
        "ts": datetime.now(UTC).isoformat(),
        "level": normalized_level,
        "message": str(message or ""),
        **{key: value for key, value in fields.items() if value is not None and value != ""},
    }
    if _normalize_log_format(RUNTIME_CONFIG.log_format) == "jsonl":
        try:
            print(json.dumps(payload, ensure_ascii=False), flush=True)
        except OSError:
            pass
        return
    field_text = " ".join(
        f"{key}={value}" for key, value in payload.items() if key not in {"ts", "level", "message"}
    )
    line = f"[admin_bridge][{normalized_level.upper()}] {payload['message']}"
    if field_text:
        line = f"{line} {field_text}"
    try:
        print(line, flush=True)
    except OSError:
        pass


def configure_runtime_paths(config: RuntimeConfig) -> None:
    global RUNTIME_CONFIG
    global \
        OPS_HISTORY_PATH, \
        OPS_ALERT_STATE_PATH, \
        JOBS_FETCH_REPORT_PATH, \
        JOBS_FETCH_TASKS_PATH, \
        TASK_STATE_PATH, \
        SYNC_LIVE_TASK_PATH, \
        DISCOVERY_LOG_PATH, \
        FETCHER_LOG_PATH
    global \
        ACTIVE_PATH, \
        PENDING_PATH, \
        REJECTED_PATH, \
        TOMBSTONES_PATH, \
        DISCOVERY_REPORT_PATH, \
        APPROVAL_STATE_PATH
    global \
        TASKS_CONFIG_PATH, \
        SYNC_CONFIG_PATH, \
        DISCOVERY_CONFIG_PATH, \
        SYNC_RUNTIME_PATH, \
        STARTUP_METRICS_PATH, \
        DESKTOP_UPDATE_STATE_PATH
    global _REGISTRY_SERVICE, _REGISTRY_SERVICE_PATHS
    global _DISCOVERY_SERVICE, _DISCOVERY_SERVICE_PATHS
    global _PIPELINE_SERVICE
    global _DESKTOP_UPDATE_SERVICE, _DESKTOP_UPDATE_SERVICE_DATA_DIR

    RUNTIME_CONFIG = config
    data_dir = Path(config.data_dir).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)

    OPS_HISTORY_PATH = data_dir / "admin-run-history.json"
    OPS_ALERT_STATE_PATH = data_dir / "admin-alert-state.json"
    JOBS_FETCH_REPORT_PATH = data_dir / "jobs-fetch-report.json"
    JOBS_FETCH_TASKS_PATH = data_dir / "jobs-fetch-tasks.json"
    TASK_STATE_PATH = data_dir / "admin-task-state.json"
    SYNC_LIVE_TASK_PATH = data_dir / "sync-live-task.json"
    DISCOVERY_LOG_PATH = data_dir / "source-discovery.log"
    FETCHER_LOG_PATH = data_dir / "jobs-fetcher.log"
    SYNC_CONFIG_PATH = data_dir / SYNC_CONFIG_PATH_DEFAULT.name
    DISCOVERY_CONFIG_PATH = data_dir / "source-discovery-config.json"
    SYNC_RUNTIME_PATH = data_dir / SYNC_RUNTIME_PATH_DEFAULT.name
    STARTUP_METRICS_PATH = data_dir / "desktop-startup-metrics.jsonl"
    DESKTOP_UPDATE_STATE_PATH = data_dir / "updater" / "install-state.json"
    ACTIVE_PATH = data_dir / "source-registry-active.json"
    PENDING_PATH = data_dir / "source-registry-pending.json"
    REJECTED_PATH = data_dir / "source-registry-rejected.json"
    TOMBSTONES_PATH = data_dir / "source-registry-tombstones.json"
    DISCOVERY_REPORT_PATH = data_dir / "source-discovery-report.json"
    APPROVAL_STATE_PATH = data_dir / "source-approval-state.json"
    TASKS_CONFIG_PATH = Path(config.root) / ".vscode" / "tasks.json"

    source_registry_module.DATA_DIR = data_dir
    source_registry_module.ACTIVE_PATH = ACTIVE_PATH
    source_registry_module.PENDING_PATH = PENDING_PATH
    source_registry_module.REJECTED_PATH = REJECTED_PATH
    source_registry_module.TOMBSTONES_PATH = TOMBSTONES_PATH
    source_registry_module.DISCOVERY_REPORT_PATH = DISCOVERY_REPORT_PATH
    source_registry_module.APPROVAL_STATE_PATH = APPROVAL_STATE_PATH
    # Desktop local-data APIs are safe to keep available on localhost and are required
    # for the packaged desktop experience. Treat the LocalDataStore as the feature flag,
    # rather than relying exclusively on environment toggles.
    bridge_runtime_state.configure_runtime_paths(
        startup_metrics_path=STARTUP_METRICS_PATH,
        desktop_local_data_store=LocalDataStore(LocalDataPaths.from_data_dir(data_dir)),
        now_iso=now_iso,
        owner_mode=config.owner_mode,
        owner_token=config.owner_token,
        desktop_session_id=config.desktop_session_id,
        started_by=config.started_by,
        owner_idle_timeout_s=config.owner_idle_timeout_s,
    )
    with _REGISTRY_SERVICE_LOCK:
        _REGISTRY_SERVICE = None
        _REGISTRY_SERVICE_PATHS = None
    with _DISCOVERY_SERVICE_LOCK:
        _DISCOVERY_SERVICE = None
        _DISCOVERY_SERVICE_PATHS = None
    with _PIPELINE_SERVICE_LOCK:
        _PIPELINE_SERVICE = None
    with _DESKTOP_UPDATE_SERVICE_LOCK:
        _DESKTOP_UPDATE_SERVICE = None
        _DESKTOP_UPDATE_SERVICE_DATA_DIR = None


def startup_banner(config: RuntimeConfig) -> None:
    bridge_config.startup_banner(config=config, bridge_log=bridge_log)


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
    bridge_runtime_state.append_startup_metric(event, payload, now_iso=now_iso)


def read_startup_metrics(limit: int = 200) -> list[dict[str, Any]]:
    return bridge_runtime_state.read_startup_metrics(limit)


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
    return _get_registry_service().normalize_state(state)


def load_state() -> dict[str, list[dict[str, Any]]]:
    return _get_registry_service().load_state()


def summarize_state(state: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
    return RegistryService.summarize_state(state)


def persist_state(state: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    return _get_registry_service().persist_state(state)


def persist_state_and_auto_sync(
    state: dict[str, list[dict[str, Any]]], *, reason: str
) -> dict[str, list[dict[str, Any]]]:
    return _registry_sync_flow.persist_state_and_auto_sync(
        state,
        reason=reason,
        persist_state=persist_state,
        maybe_trigger_auto_sync_push=_maybe_trigger_auto_sync_push,
    )


def move_entries(
    pending: list[dict[str, Any]], selected_ids: list[str]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return RegistryService.move_entries(pending, selected_ids)


def build_manual_candidate(normalized_url: str) -> dict[str, Any] | None:
    if not normalized_url:
        return None
    studio = infer_studio_name_from_host(normalized_url)
    inferred = discovery.infer_web_candidate(
        normalized_url,
        studio,
        nl_priority=False,
    )
    if not isinstance(inferred, dict):
        fallback = {
            "name": f"{studio} (Manual Website)",
            "studio": studio,
            "company": studio,
            "adapter": "static",
            "pages": [normalized_url],
            "listing_url": normalized_url,
            "nlPriority": False,
            "enabledByDefault": False,
            "discoveryMethod": "manual",
            "discoveredAt": now_iso(),
            "manualAddedAt": now_iso(),
            "manualFallback": "generic_website",
        }
        return ensure_source_id(fallback)
    row = ensure_source_id(inferred)
    row["enabledByDefault"] = False
    row["discoveryMethod"] = "manual"
    row["discoveredAt"] = now_iso()
    row["manualAddedAt"] = now_iso()
    return row


def add_manual_source(raw_url: str) -> dict[str, Any]:
    normalized_url = normalize_source_url(raw_url)
    if not normalized_url:
        return {"status": "invalid", "message": "Invalid URL. Use a full http(s) URL."}

    candidate = build_manual_candidate(normalized_url)
    if not candidate:
        return {
            "status": "invalid",
            "message": "URL is valid but provider is not supported for discovery checks.",
        }

    tombstones = load_tombstones()
    if is_tombstoned(candidate, tombstones):
        return {
            "status": "tombstoned",
            "sourceId": source_identity(candidate),
            "source": ensure_source_id(candidate),
            "message": "Source was deleted locally. Restore it before adding it again.",
        }

    state = load_state()
    duplicate = find_existing_source_by_url(state, normalized_url)
    if duplicate:
        return {
            "status": "duplicate",
            "sourceId": source_identity(duplicate),
            "source": ensure_source_id(duplicate),
            "message": "Source already exists.",
        }

    # Collapse manual static variants by studio+domain (e.g. /careers, /career, /de/karriere).
    if str(candidate.get("adapter") or "").strip().lower() == "static":
        studio = str(candidate.get("studio") or "").strip()
        existing_match = find_existing_static_source_by_studio_domain(
            state, studio=studio, normalized_url=normalized_url
        )
        if existing_match is not None:
            bucket, idx, existing = existing_match
            updated = dict(existing)
            pages = (
                list(updated.get("pages") or []) if isinstance(updated.get("pages"), list) else []
            )
            normalized_pages = [normalize_source_url(str(page or "")) for page in pages]
            normalized_pages = [page for page in normalized_pages if page]
            if normalized_url not in normalized_pages:
                normalized_pages.append(normalized_url)
            updated["pages"] = normalized_pages
            if not str(updated.get("listing_url") or "").strip():
                updated["listing_url"] = normalized_pages[0] if normalized_pages else normalized_url
            updated = ensure_source_id(updated)
            state[bucket][idx] = updated
            state = persist_state_and_auto_sync(state, reason=REGISTRY_REASON_MANUAL_SOURCE_VARIANT)
            return {
                "status": "duplicate",
                "sourceId": source_identity(updated),
                "source": ensure_source_id(updated),
                "summary": summarize_state(state),
                "message": "Source already exists for this studio/domain. Added URL as page variant.",
            }

    state["pending"] = unique_sources([candidate, *state["pending"]])
    state = persist_state_and_auto_sync(state, reason=REGISTRY_REASON_MANUAL_SOURCE)
    added = next(
        (row for row in state["pending"] if source_identity(row) == source_identity(candidate)),
        candidate,
    )
    return {
        "status": "added",
        "sourceId": source_identity(added),
        "source": ensure_source_id(added),
        "summary": summarize_state(state),
        "message": "Manual source added with generic website scraping fallback."
        if str(added.get("adapter") or "").lower() == "static"
        else "Manual source added.",
    }


def _fetch_html_with_fallback_bound(url: str, timeout_s: int) -> tuple[str, str, bool, bool]:
    return _source_check_fetch.fetch_html_with_fallback(
        url,
        timeout_s,
        fetch_text=lambda u, t: discovery.fetch_text_with_retry(u, t, adapter="static"),
        looks_like_challenge=_source_check_http.looks_like_browser_challenge_page,
        has_extractable_job_data=lambda html, page_url: (
            _source_check_fetch.html_has_extractable_job_data(
                html, page_url, html_extractor=_html_extractor
            )
        ),
        try_playwright=_source_check_http.try_fetch_with_playwright,
        is_http_forbidden=_source_check_http.is_http_forbidden_error,
    )


def _fetch_static_page_with_alternates_bound(
    page_url: str, timeout_s: int
) -> tuple[str, str, bool, bool, str]:
    return _source_check_fetch.fetch_static_page_with_alternates(
        page_url,
        timeout_s,
        fetch_html_with_fallback_fn=_fetch_html_with_fallback_bound,
        suggest_alternate_urls=_source_check_http.suggest_alternate_career_urls,
        discover_redirect_career_candidates=_source_check_http.discover_redirect_career_candidates,
        is_not_found_error_text=_source_check_http.is_not_found_error_text,
    )


def check_static_source(
    row: dict[str, Any], timeout_s: int = 12
) -> tuple[bool, int, str, bool, dict[str, Any]]:
    return _source_checker.check_static_source(
        row,
        timeout_s,
        fetch_page_with_alternates=_fetch_static_page_with_alternates_bound,
        fetch_page=_fetch_html_with_fallback_bound,
        fetch_text=lambda url, timeout: discovery.fetch_text_with_retry(
            url, timeout, adapter="static"
        ),
        html_extractor=_html_extractor,
        parse_jobpostings_from_html=parse_jobpostings_from_html,
        normalize_job_url=normalize_job_url,
        source_identity=source_identity,
        suggest_alternate_career_urls=_source_check_http.suggest_alternate_career_urls,
    )


def normalize_manual_static_studio_fields(row: dict[str, Any]) -> dict[str, Any]:
    return _source_check_api.normalize_manual_static_studio_fields(
        row,
        normalize_source_url=normalize_source_url,
        infer_studio_name_from_host=infer_studio_name_from_host,
    )


def trigger_source_check(source_id: str, timeout_s: int = 12) -> dict[str, Any]:
    return _source_check_api.trigger_source_check(
        source_id,
        timeout_s=timeout_s,
        load_state=load_state,
        source_identity=source_identity,
        normalize_manual_static_studio_fields_fn=normalize_manual_static_studio_fields,
        check_static_source_fn=check_static_source,
        now_iso=now_iso,
        compute_candidate_score=discovery.compute_candidate_score,
        normalize_candidate=discovery.normalize_candidate,
        probe_candidate=discovery.probe_candidate,
        persist_state_and_auto_sync=persist_state_and_auto_sync,
        normalize_source_url=normalize_source_url,
        build_check_failure_details=_source_check_http.build_check_failure_details,
    )


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
    return bridge_runtime_state.get_desktop_session_payload()


def update_desktop_session_lifecycle(
    *, owner_token: str, session_id: str, page_id: str, state: str
) -> tuple[int, dict[str, Any]]:
    return bridge_runtime_state.update_desktop_session_lifecycle(
        owner_token=owner_token,
        session_id=session_id,
        page_id=page_id,
        state=state,
        now_iso=now_iso,
    )


def owner_session_should_exit() -> bool:
    expired = bridge_runtime_state.owner_session_should_exit(parse_iso=parse_iso, now_utc=now_utc)
    if expired:
        try:
            active_tasks_payload = _get_ops_api().get_current_task_state_payload()
            active_tasks = [
                {
                    "taskType": str(task.get("taskType") or task.get("type") or "").strip().lower(),
                    "runId": str(task.get("runId") or "").strip(),
                }
                for task in (
                    active_tasks_payload.get("tasks")
                    if isinstance(active_tasks_payload.get("tasks"), list)
                    else []
                )
                if isinstance(task, dict)
                and bool(task.get("active"))
                and str(task.get("taskType") or task.get("type") or "").strip().lower()
                in {"fetch", "discovery", "pipeline", "sync"}
            ]
        except Exception:
            active_tasks = []
        if active_tasks:
            owner_state = bridge_runtime_state.get_owner_state()
            bridge_log(
                "info",
                "admin_bridge_owner_session_exit_suppressed_for_active_tasks",
                owner_mode=str(owner_state.get("ownerMode") or ""),
                owner_token=str(owner_state.get("ownerToken") or ""),
                session_id=str(owner_state.get("sessionId") or ""),
                active_tasks=active_tasks,
            )
            return False
        owner_state = bridge_runtime_state.get_owner_state()
        bridge_log(
            "info",
            "admin_bridge_owner_session_exit_requested",
            owner_mode=str(owner_state.get("ownerMode") or ""),
            owner_token=str(owner_state.get("ownerToken") or ""),
            session_id=str(owner_state.get("sessionId") or ""),
            started_by=str(owner_state.get("startedBy") or ""),
            last_activity_at=str(owner_state.get("lastActivityAt") or ""),
            idle_timeout_seconds=float(owner_state.get("idleTimeoutSeconds") or 0.0),
        )
    return expired


def parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def pid_is_running(pid: int) -> bool:
    if int(pid or 0) <= 0:
        return False
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False
    return True


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
    try:
        return json.loads(TASKS_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


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
    _get_sync_state().set_sync_status(
        action=action,
        result=result,
        error=error,
        pulled=bool(pulled),
        pushed=bool(pushed),
    )


def get_sync_status_payload() -> dict[str, Any]:
    return _get_sync_service().get_sync_status_payload()


def _sync_guard() -> dict[str, Any] | None:
    return _get_sync_service()._sync_guard()  # noqa: SLF001


def sync_pull_sources() -> dict[str, Any]:
    return _get_sync_service().sync_pull_sources()


def sync_push_sources() -> dict[str, Any]:
    return _get_sync_service().sync_push_sources()


def startup_sync_pull() -> None:
    _get_sync_service().startup_sync_pull()


def sync_task_running() -> bool:
    with OPS_STATE_LOCK:
        _run_history_api.reconcile_sync_history_locked(
            _run_history_api.SyncHistoryDeps(
                ops_state_lock=OPS_STATE_LOCK,
                load_run_history=load_run_history,
                save_run_history=save_run_history,
                save_json_atomic=save_json_atomic,
                prune_started_rows_for_type=prune_started_rows_for_type,
                clear_task_state=clear_task_state,
                clear_task_state_locked=_clear_task_state_locked,
                upsert_run_history=upsert_run_history,
                task_running_from_state=task_running_from_state,
                report_is_stale_in_progress=report_is_stale_in_progress,
                load_json_object=load_json_object,
                normalize_fetch_report_contract=normalize_fetch_report_contract,
                normalize_discovery_report_contract=normalize_discovery_report_contract,
                summarize_fetch_report=summarize_fetch_report,
                summarize_discovery_report=summarize_discovery_report,
                jobs_fetch_report_path=JOBS_FETCH_REPORT_PATH,
                jobs_fetch_tasks_path=JOBS_FETCH_TASKS_PATH,
                discovery_report_path=DISCOVERY_REPORT_PATH,
                task_state_path=TASK_STATE_PATH,
                get_active_sync_runs=SyncState.get_active_sync_runs,
                parse_iso=parse_iso,
                now_iso=now_iso,
                now_utc=now_utc,
            )
        )
    return _get_sync_service().sync_task_running()


def wait_for_sync_tasks(timeout_s: float = 5.0) -> None:
    _get_sync_service().wait_for_sync_tasks(timeout_s=float(timeout_s))


def _mark_discovery_sync_finished(finished_at: str) -> None:
    with SYNC_STATE_LOCK:
        _get_sync_state().save_sync_runtime_state(
            {"lastDiscoverySyncFinishedAt": str(finished_at or "")}
        )


def _maybe_trigger_auto_sync_push(reason: str) -> bool:
    return _registry_sync_flow.maybe_trigger_auto_sync_push(
        reason=reason,
        sync_guard=_sync_guard,
        sync_task_running=sync_task_running,
        start_sync_task=start_sync_task,
    )


def _run_sync_task_worker(
    run_id: str, action: str, started_at: str, *, reason: str = "", automatic: bool = False
) -> None:
    _sync_task_flow.run_sync_task_worker(
        run_id=run_id,
        action=action,
        started_at=started_at,
        reason=reason,
        automatic=automatic,
        parse_iso=parse_iso,
        now_utc=now_utc,
        run_sync_pull=sync_pull_sources,
        run_sync_push=sync_push_sources,
        set_sync_status=_set_sync_status,
        remove_active_sync_run=SyncState.remove_active_sync_run,
        remove_active_sync_thread=SyncState.remove_active_sync_thread,
        prune_started_rows_for_type=lambda entry_type, *, finished_at: prune_started_rows_for_type(
            entry_type, finished_at=finished_at
        ),
        upsert_run_history=lambda entry: upsert_run_history(entry, dedupe_fields=("type", "runId")),
        bridge_log=bridge_log,
        save_json_atomic=save_json_atomic,
        live_task_path=SYNC_LIVE_TASK_PATH,
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
    report = normalize_fetch_report_contract(load_json_object(JOBS_FETCH_REPORT_PATH, {}))
    summary = summarize_fetch_report(report)
    return int(summary.get("outputCount") or 0)


def get_jobs_pipeline_status_payload() -> dict[str, Any]:
    return _get_pipeline_service().get_status_payload()


def _wait_for_report_completion(
    *,
    report_path: Path,
    started_at: str,
    timeout_s: float,
    report_name: str,
    fail_on_stale: bool = False,
) -> dict[str, Any]:
    return _get_pipeline_service().wait_for_report_completion(
        report_path=report_path,
        started_at=started_at,
        timeout_s=timeout_s,
        report_name=report_name,
        load_json_object=load_json_object,
        report_is_stale_in_progress=report_is_stale_in_progress,
        fail_on_stale=fail_on_stale,
    )


def _wait_for_sync_completion(run_id: str, timeout_s: float = 900.0) -> dict[str, Any]:
    deadline = datetime.now(UTC) + timedelta(seconds=max(10.0, float(timeout_s)))
    while datetime.now(UTC) < deadline:
        history = sync_history_from_reports()
        for row in reversed(history):
            if str(row.get("id") or "") != str(run_id or ""):
                continue
            if str(row.get("type") or "").strip().lower() != "sync":
                continue
            status = str(row.get("status") or "").strip().lower()
            if status in {"ok", "warning", "error"} and str(row.get("finishedAt") or "").strip():
                return row
        threading.Event().wait(1.0)
    raise TimeoutError("sync task did not finish within timeout")


def start_fetcher_task(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return _get_task_launch_api().start_fetcher_task(
        payload,
        append_run_history=append_run_history,
        normalize_fetch_report_contract=normalize_fetch_report_contract,
        prune_started_rows_for_type=prune_started_rows_for_type,
        run_background_script=run_background_script,
        save_json_atomic=save_json_atomic,
        schema_version=SCHEMA_VERSION,
        load_json_object=load_json_object,
    )


def start_jobs_pipeline_task(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return _get_pipeline_service().start_task(payload)


def desktop_local_data_store() -> LocalDataStore:
    return bridge_runtime_state.get_desktop_local_data_store()


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
