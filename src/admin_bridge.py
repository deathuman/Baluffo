#!/usr/bin/env python3
"""AI boundary owns: bridge startup, root exports, compatibility patch seams.
AI boundary implement in: `src.bridge.*` leaves and admin entrypoint helpers.
AI boundary search before contracts: route handlers, frontend callers, API docs.
AI boundary verify: `npm run test:refactor:changed` plus focused bridge tests."""

from __future__ import annotations

import os as _os
import subprocess
import sys
import threading
import time as _time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[1]

# Allow script execution from any cwd by adding the repo root to sys.path.
# Without this, `py src/admin_bridge.py` exposes only `.../Baluffo/src`.
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import source_discovery as _discovery_mod
from src import source_registry as source_registry_module
from src import source_sync as source_sync_module
from src.app_version import get_app_version as _get_app_version
from src.baluffo_config import get_bridge_defaults, get_storage_defaults
from src.baluffo_config import get_security_defaults as _get_security_defaults
from src.bridge import SYNC_STATE_LOCK as _SYNC_STATE_LOCK
from src.bridge import SyncService, report_normalizer
from src.bridge import SyncState as _SyncState
from src.bridge import admin_entrypoint_api as admin_entrypoint_api_mod
from src.bridge import admin_entrypoint_runtime as admin_entrypoint_runtime_mod
from src.bridge import admin_entrypoint_services as admin_entrypoint_services_mod
from src.bridge import admin_registry_api as admin_registry_api_mod
from src.bridge import admin_task_runtime as admin_task_runtime_mod
from src.bridge import bootstrap as bridge_bootstrap
from src.bridge import config as bridge_config
from src.bridge import diagnostic_events as _diagnostic_events
from src.bridge import html_extractor as _html_extractor_mod
from src.bridge import ops_api as _ops_api_mod
from src.bridge import registry_conflict_adjudication as _registry_conflict_adjudication_mod
from src.bridge import registry_sync_flow as _registry_sync_flow_mod
from src.bridge import run_history_api as _run_history_api
from src.bridge import source_check_api as _source_check_api_mod
from src.bridge import source_check_fetch as _source_check_fetch_mod
from src.bridge import source_check_http as _source_check_http_mod
from src.bridge import source_checker as _source_checker_mod
from src.bridge import sync_task_flow as _sync_task_flow_mod
from src.bridge import task_launch_api as _task_launch_api_mod
from src.bridge.admin_task_history import AdminTaskHistory
from src.bridge.admin_task_lifecycle import AdminTaskLifecycle
from src.bridge.discovery_service import (
    DiscoveryDeps as _DiscoveryDeps,
)
from src.bridge.discovery_service import (
    DiscoveryPaths as _DiscoveryPaths,
)
from src.bridge.discovery_service import (
    DiscoveryService,
)
from src.bridge.lifecycle_cleanup import cleanup_orphaned_startup_tasks
from src.bridge.pipeline_service import PipelineService
from src.bridge.registry_service import RegistryPaths as _RegistryPaths
from src.bridge.registry_service import RegistryService
from src.bridge.registry_tombstones import is_tombstoned as _is_tombstoned
from src.bridge.server import runtime_state as _bridge_runtime_state
from src.bridge.source_helpers import (
    find_existing_source_by_url as _find_existing_source_by_url,
)
from src.bridge.source_helpers import (
    find_existing_static_source_by_studio_domain as _find_existing_static_source_by_studio_domain,
)
from src.bridge.source_helpers import (
    infer_studio_name_from_host as _infer_studio_name_from_host,
)
from src.bridge.sync_state import SYNC_CONFIG_PATH_DEFAULT, SYNC_RUNTIME_PATH_DEFAULT
from src.contracts import SCHEMA_VERSION as _SCHEMA_VERSION
from src.jobs.adapters import default_source_loaders as _default_source_loaders
from src.jobs.adapters.html_parsers import parse_jobpostings_from_html as _parse_html
from src.jobs.common.registry_defaults import (
    DEFAULT_STUDIO_SOURCE_REGISTRY as _DEFAULT_STUDIO_SOURCE_REGISTRY,
)
from src.jobs.transport import normalize_url as _normalize_job_url
from src.shared.utils import now_iso as _now_iso
from src.shared.utils import now_utc
from src.ship.desktop_update_service import DesktopUpdateService
from src.source_registry import (
    ACTIVE_PATH,
    DISCOVERY_CANDIDATES_PATH,
    PENDING_PATH,
    REJECTED_PATH,
    TOMBSTONES_PATH,
    load_json_array,
    load_json_object,
    load_runtime_evidence,
    save_json_atomic,
)
from src.source_registry import (
    APPROVAL_STATE_PATH as _APPROVAL_STATE_PATH,
)
from src.source_registry import (
    REGISTRY_REASON_MANUAL_SOURCE as _REGISTRY_REASON_MANUAL_SOURCE,
)
from src.source_registry import (
    REGISTRY_REASON_MANUAL_SOURCE_VARIANT as _REGISTRY_REASON_MANUAL_SOURCE_VARIANT,
)
from src.source_registry import (
    ensure_source_id as _ensure_source_id,
)
from src.source_registry import (
    normalize_source_url as _normalize_source_url,
)
from src.source_registry import (
    source_identity as _source_identity,
)
from src.source_registry import (
    unique_sources as _unique_sources,
)

normalize_fetch_report_contract = report_normalizer.normalize_fetch_report_contract
normalize_discovery_report_contract = report_normalizer.normalize_discovery_report_contract
_safe_int = report_normalizer.safe_int
source_url_fingerprint = source_registry_module.source_url_fingerprint

os = _os
time = _time
discovery = _discovery_mod
bridge_runtime_state = _bridge_runtime_state
diagnostic_events = _diagnostic_events
get_app_version = _get_app_version
get_security_defaults = _get_security_defaults
SCHEMA_VERSION = _SCHEMA_VERSION
SYNC_STATE_LOCK = _SYNC_STATE_LOCK
SyncState = _SyncState
APPROVAL_STATE_PATH = _APPROVAL_STATE_PATH
_html_extractor = _html_extractor_mod
_ops_api = _ops_api_mod
_registry_sync_flow = _registry_sync_flow_mod
_registry_conflict_adjudication = _registry_conflict_adjudication_mod
_source_check_api = _source_check_api_mod
_source_check_fetch = _source_check_fetch_mod
_source_check_http = _source_check_http_mod
_source_checker = _source_checker_mod
_sync_task_flow = _sync_task_flow_mod
_task_launch_api = _task_launch_api_mod
RegistryPaths = _RegistryPaths
DiscoveryDeps = _DiscoveryDeps
DiscoveryPaths = _DiscoveryPaths
DEFAULT_STUDIO_SOURCE_REGISTRY = _DEFAULT_STUDIO_SOURCE_REGISTRY
default_source_loaders = _default_source_loaders
find_existing_source_by_url = _find_existing_source_by_url
find_existing_static_source_by_studio_domain = _find_existing_static_source_by_studio_domain
infer_studio_name_from_host = _infer_studio_name_from_host
is_tombstoned = _is_tombstoned
parse_jobpostings_from_html = _parse_html
normalize_job_url = _normalize_job_url
ensure_source_id = _ensure_source_id
normalize_source_url = _normalize_source_url
source_identity = _source_identity
unique_sources = _unique_sources
REGISTRY_REASON_MANUAL_SOURCE = _REGISTRY_REASON_MANUAL_SOURCE
REGISTRY_REASON_MANUAL_SOURCE_VARIANT = _REGISTRY_REASON_MANUAL_SOURCE_VARIANT
now_iso = _now_iso
load_tombstones = admin_registry_api_mod.load_tombstones
save_tombstones = admin_registry_api_mod.save_tombstones

OPS_HISTORY_PATH = ROOT / "data" / "admin-run-history.json"
TASK_LIFECYCLE_PATH = ROOT / "data" / "admin-task-lifecycle.json"
OPS_ALERT_STATE_PATH = ROOT / "data" / "admin-alert-state.json"
JOBS_FETCH_REPORT_PATH = ROOT / "data" / "jobs-fetch-report.json"
SOURCE_POLICY_RECOMMENDATIONS_PATH = ROOT / "data" / "source-policy-recommendations.json"
SOURCE_POLICY_REVIEW_STATE_PATH = ROOT / "data" / "source-policy-review-state.json"
DEDUP_REVIEW_STATE_PATH = ROOT / "data" / "dedup-review-state.json"
DISCOVERY_REPORT_PATH = ROOT / "data" / "source-discovery-report.json"
JOBS_FETCH_TASKS_PATH = ROOT / "data" / "jobs-fetch-tasks.json"
TASKS_CONFIG_PATH = ROOT / ".vscode" / "tasks.json"
TASK_STATE_PATH = ROOT / "data" / "admin-task-state.json"
SYNC_LIVE_TASK_PATH = ROOT / "data" / "sync-live-task.json"
DISCOVERY_LOG_PATH = ROOT / "data" / "source-discovery.log"
FETCHER_LOG_PATH = ROOT / "data" / "jobs-fetcher.log"
ADMIN_BRIDGE_EVENTS_PATH = ROOT / "data" / "admin-bridge-events.jsonl"
SYNC_CONFIG_PATH = SYNC_CONFIG_PATH_DEFAULT
DISCOVERY_CONFIG_PATH = ROOT / "data" / "source-discovery-config.json"
SYNC_RUNTIME_PATH = SYNC_RUNTIME_PATH_DEFAULT
STARTUP_METRICS_PATH = ROOT / "data" / "desktop-startup-metrics.jsonl"
ACTIVE_PATH = ROOT / "data" / "source-registry-active.json"
PENDING_PATH = ROOT / "data" / "source-registry-pending.json"
DEFAULTS_DIR = ROOT / "data" / "defaults"
ACTIVE_SEED_PATH = DEFAULTS_DIR / "source-registry-active.seed.json"
PENDING_SEED_PATH = DEFAULTS_DIR / "source-registry-pending.seed.json"
REJECTED_PATH = ROOT / "data" / "source-registry-rejected.json"
DISCOVERY_CANDIDATES_PATH = ROOT / "data" / "source-discovery-candidates.json"
TOMBSTONES_PATH = ROOT / "data" / "source-registry-tombstones.json"
DESKTOP_UPDATE_STATE_PATH = ROOT / "data" / "updater" / "install-state.json"

MAX_HISTORY_ROWS = 240
OPS_SCHEMA_VERSION = 1
OPS_STATE_LOCK = threading.RLock()
parse_iso: Callable[[Any], datetime | None]
pid_is_running: Callable[[int], bool]
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
_TASK_LIFECYCLE = AdminTaskLifecycle(
    lifecycle_path=lambda: TASK_LIFECYCLE_PATH,
    max_rows=lambda: MAX_HISTORY_ROWS,
    lock=OPS_STATE_LOCK,
    load_json_object=load_json_object,
    save_json_atomic=save_json_atomic,
    now_iso=lambda: now_iso(),
    parse_iso=lambda value: parse_iso(value),
    storage_data_dir=lambda: Path(RUNTIME_CONFIG.data_dir),
)
LOG_LEVEL_ORDER = bridge_config.LOG_LEVEL_ORDER
SYNC_CONFIG: Any = None
BRIDGE_SERVICES = admin_entrypoint_services_mod.BRIDGE_SERVICES
_SYNC_SERVICE: SyncService | None = None
_SYNC_SERVICE_DATA_DIR: Path | None = None
_SYNC_SERVICE_LOCK = BRIDGE_SERVICES.sync_service_lock
_REGISTRY_SERVICE: RegistryService | None = None
_REGISTRY_SERVICE_PATHS: tuple[Path, Path, Path] | None = None
_REGISTRY_SERVICE_LOCK = BRIDGE_SERVICES.registry_service_lock
_DISCOVERY_SERVICE: DiscoveryService | None = None
_DISCOVERY_SERVICE_PATHS: tuple[Path, Path, Path, Path] | None = None
_DISCOVERY_SERVICE_LOCK = BRIDGE_SERVICES.discovery_service_lock
_PIPELINE_SERVICE: PipelineService | None = None
_PIPELINE_SERVICE_LOCK = BRIDGE_SERVICES.pipeline_service_lock
_DESKTOP_UPDATE_SERVICE: DesktopUpdateService | None = None
_DESKTOP_UPDATE_SERVICE_DATA_DIR: Path | None = None
_DESKTOP_UPDATE_SERVICE_LOCK = BRIDGE_SERVICES.desktop_update_service_lock


_get_sync_service = admin_entrypoint_services_mod.get_sync_service
_get_sync_state = admin_entrypoint_services_mod.get_sync_state
_get_registry_service = admin_entrypoint_services_mod.get_registry_service
_get_discovery_service = admin_entrypoint_services_mod.get_discovery_service
_get_task_launch_api = admin_entrypoint_services_mod.get_task_launch_api
_get_ops_api = admin_entrypoint_services_mod.get_ops_api
_get_pipeline_service = admin_entrypoint_services_mod.get_pipeline_service
_get_desktop_update_service = admin_entrypoint_services_mod.get_desktop_update_service

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

admin_entrypoint_api_mod.root = sys.modules[__name__]
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


_log_enabled = admin_entrypoint_runtime_mod.log_enabled
bridge_log = admin_entrypoint_runtime_mod.bridge_log
configure_runtime_paths = admin_entrypoint_runtime_mod.configure_runtime_paths
startup_banner = admin_entrypoint_runtime_mod.startup_banner


def build_bridge_api(config: RuntimeConfig) -> Any:
    return admin_entrypoint_api_mod.build_bridge_api(config)


append_startup_metric = admin_entrypoint_runtime_mod.append_startup_metric
read_startup_metrics = admin_entrypoint_runtime_mod.read_startup_metrics


def load_saved_sync_settings() -> dict[str, Any]:
    return _get_sync_service().load_saved_sync_settings()


def refresh_sync_config() -> source_sync_module.SyncConfig:
    sync_config = BRIDGE_SERVICES.refresh_sync_config(_get_sync_service().refresh_sync_config)
    sys.modules[__name__].SYNC_CONFIG = sync_config
    return cast(source_sync_module.SyncConfig, sync_config)


normalize_state = admin_registry_api_mod.normalize_state
load_state = admin_registry_api_mod.load_state
summarize_state = admin_registry_api_mod.summarize_state
get_registry_summary_payload = admin_registry_api_mod.get_registry_summary_payload
get_registry_auto_heal_report = admin_registry_api_mod.get_registry_auto_heal_report
persist_state = admin_registry_api_mod.persist_state
persist_state_and_auto_sync = admin_registry_api_mod.persist_state_and_auto_sync
move_entries = admin_registry_api_mod.move_entries
build_manual_candidate = admin_registry_api_mod.build_manual_candidate
add_manual_source = admin_registry_api_mod.add_manual_source
check_static_source = admin_registry_api_mod.check_static_source


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


def normalize_manual_static_studio_fields(row: dict[str, Any]) -> dict[str, Any]:
    return admin_registry_api_mod.normalize_manual_static_studio_fields(row)


def trigger_source_check(source_id: str, timeout_s: int = 12) -> dict[str, Any]:
    return admin_registry_api_mod.trigger_source_check(source_id, timeout_s=timeout_s)


def check_registry_conflicts(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return _registry_conflict_adjudication.run_registry_conflict_adjudication(
        build_bridge_api(RUNTIME_CONFIG),
        payload if isinstance(payload, dict) else {},
    )


def load_registry_conflict_adjudication() -> dict[str, Any]:
    return _registry_conflict_adjudication.load_registry_conflict_adjudication(
        build_bridge_api(RUNTIME_CONFIG)
    )


def run_background_script(
    script_name: str,
    args: list[str] | None = None,
    *,
    extra_env: dict[str, str] | None = None,
    **launch_kwargs: Any,
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
        **admin_task_runtime_mod.background_process_flags(subprocess),
        **launch_kwargs,
    )


def _failed_source_names_from_latest_report(*, allowed_names: set[str] | None = None) -> list[str]:
    return _get_ops_api().failed_source_names_from_latest_report(allowed_names=allowed_names)


def build_fetcher_args_from_payload(payload: dict[str, Any]) -> tuple[list[str], str]:
    return _get_task_launch_api().build_fetcher_args_from_payload(payload)


def mark_desktop_session_activity(path: str) -> None:
    owner_mode = str(getattr(RUNTIME_CONFIG, "owner_mode", "") or "").strip()
    route_path = str(path or "").strip()
    if owner_mode == "desktop-window" and route_path == "/ops/health":
        return
    if not bool(getattr(RUNTIME_CONFIG, "desktop_mode", False)):
        return
    bridge_runtime_state.mark_desktop_session_activity(now_iso=now_iso)


get_desktop_session_payload = admin_entrypoint_runtime_mod.get_desktop_session_payload
update_desktop_session_lifecycle = admin_entrypoint_runtime_mod.update_desktop_session_lifecycle
owner_session_should_exit = admin_entrypoint_runtime_mod.owner_session_should_exit
parse_iso = admin_entrypoint_runtime_mod.parse_iso
pid_is_running = admin_entrypoint_runtime_mod.pid_is_running


load_run_history = _TASK_HISTORY.load
save_run_history = _TASK_HISTORY.save_run_history
prune_started_rows_for_type = _TASK_HISTORY.prune_started_rows_for_type
_clear_task_state_locked = _TASK_HISTORY.clear_task_state_locked
clear_task_state = _TASK_HISTORY.clear_task_state
task_running_from_state = _TASK_HISTORY.task_running_from_state
report_is_stale_in_progress = _TASK_HISTORY.report_is_stale_in_progress
start_lifecycle_run = _TASK_LIFECYCLE.start_run
heartbeat_lifecycle_run = _TASK_LIFECYCLE.heartbeat_run
finish_lifecycle_run = _TASK_LIFECYCLE.finish_run
fail_lifecycle_run = _TASK_LIFECYCLE.fail_run
cancel_lifecycle_run = _TASK_LIFECYCLE.cancel_run
orphan_lifecycle_run = _TASK_LIFECYCLE.orphan_run
attach_lifecycle_child = _TASK_LIFECYCLE.attach_child
get_lifecycle_rows = _TASK_LIFECYCLE.rows
get_lifecycle_current_runs = _TASK_LIFECYCLE.get_current_runs
get_lifecycle_recent_runs = _TASK_LIFECYCLE.get_recent_runs
reconcile_lifecycle_from_legacy = _TASK_LIFECYCLE.reconcile_from_legacy


def append_run_history(row: dict[str, Any]) -> dict[str, Any]:
    entry = _TASK_HISTORY.append(row)
    _TASK_LIFECYCLE.mirror_history_row(entry)
    return entry


def upsert_run_history(entry: dict[str, Any], *, dedupe_fields: tuple[str, ...]) -> dict[str, Any]:
    row = _TASK_HISTORY.upsert(entry, dedupe_fields=dedupe_fields)
    _TASK_LIFECYCLE.mirror_history_row(row)
    return row


_read_tasks_config = admin_task_runtime_mod.read_tasks_config


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


def get_lifecycle_run_history_rows() -> list[dict[str, Any]]:
    return _get_ops_api().get_lifecycle_run_history_rows()


def compute_ops_health() -> dict[str, Any]:
    return _get_ops_api().compute_ops_health()


def compute_ops_dashboard_health() -> dict[str, Any]:
    return _get_ops_api().compute_ops_dashboard_health()


def compute_fetcher_metrics(window_runs: int = 20) -> dict[str, Any]:
    return _get_ops_api().compute_fetcher_metrics(window_runs=window_runs)


_set_sync_status = admin_task_runtime_mod.set_sync_status
get_sync_status_payload = admin_task_runtime_mod.get_sync_status_payload
_sync_guard = admin_task_runtime_mod.sync_guard
sync_pull_sources = admin_task_runtime_mod.sync_pull_sources
sync_push_sources = admin_task_runtime_mod.sync_push_sources
startup_sync_pull = admin_task_runtime_mod.startup_sync_pull
schedule_startup_sync_pull = admin_task_runtime_mod.schedule_startup_sync_pull
sync_task_running = admin_task_runtime_mod.sync_task_running
wait_for_sync_tasks = admin_task_runtime_mod.wait_for_sync_tasks
_mark_discovery_sync_finished = admin_task_runtime_mod.mark_discovery_sync_finished
_maybe_trigger_auto_sync_push = admin_task_runtime_mod.maybe_trigger_auto_sync_push
migrate_legacy_task_state_to_lifecycle = (
    admin_task_runtime_mod.migrate_legacy_task_state_to_lifecycle
)


def cleanup_stale_startup_tasks() -> dict[str, Any]:
    return cleanup_orphaned_startup_tasks(
        Path(RUNTIME_CONFIG.data_dir).resolve(),
        pid_is_running=pid_is_running,
        now_iso=now_iso,
        current_runs=_TASK_LIFECYCLE.get_current_runs,
        orphan_run=_TASK_LIFECYCLE.orphan_run,
        cancel_run=_TASK_LIFECYCLE.cancel_run,
    )


on_bridge_started = admin_task_runtime_mod.on_bridge_started


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


_current_fetch_output_count = admin_task_runtime_mod.current_fetch_output_count
get_jobs_pipeline_status_payload = admin_task_runtime_mod.get_jobs_pipeline_status_payload


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


_wait_for_sync_completion = admin_task_runtime_mod.wait_for_sync_completion
start_jobs_bootstrap_task = admin_task_runtime_mod.start_jobs_bootstrap_task
start_fetcher_task = admin_task_runtime_mod.start_fetcher_task
start_jobs_pipeline_task = admin_task_runtime_mod.start_jobs_pipeline_task
desktop_local_data_store = admin_entrypoint_runtime_mod.desktop_local_data_store


def parse_args(argv: list[str] | None = None) -> RuntimeConfig:
    return resolve_runtime_config(argv)


def main() -> int:
    config = parse_args()
    configure_runtime_paths(config)
    refresh_sync_config()
    ensure_active_registry()
    return bridge_bootstrap.run_bridge_server(
        api=build_bridge_api(config),
        host=config.host,
        port=config.port,
        on_started=on_bridge_started,
    )


if __name__ == "__main__":
    raise SystemExit(main())
