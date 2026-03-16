#!/usr/bin/env python3
"""Local admin bridge for source discovery approval workflows."""

from __future__ import annotations

import argparse
import ast
import html as html_module
import io
import json
import os
import re
import subprocess
import sys
import uuid
import threading
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.jobs.parsers import parse_jobpostings_from_html
from src.jobs.pipeline import default_source_loaders
from src.jobs.registry import DEFAULT_STUDIO_SOURCE_REGISTRY
from src.jobs.transport import normalize_url as normalize_job_url
from src import source_discovery as discovery
from src import fetcher_metrics as fetcher_metrics_module
from src import source_registry as source_registry_module
from src import source_sync as source_sync_module
from src.app_version import get_app_version
from src.baluffo_config import get_bridge_defaults, get_security_defaults, get_storage_defaults
from src.contracts import SCHEMA_VERSION
from src.local_data_store import LocalDataPaths, LocalDataStore
from src.source_registry import (
    ACTIVE_PATH,
    APPROVAL_STATE_PATH,
    DISCOVERY_CANDIDATES_PATH,
    DISCOVERY_REPORT_PATH,
    PENDING_PATH,
    REJECTED_PATH,
    ensure_source_id,
    load_json_array,
    load_json_object,
    normalize_source_url,
    save_json_atomic,
    source_identity,
    source_url_fingerprint,
    unique_sources,
)
# Bridge module imports (sync state extracted from admin_bridge.py)
from src.bridge import SYNC_CONFIG_LOCK, SYNC_STATE_LOCK, SyncService, SyncState
from src.bridge.sync_state import ACTIVE_SYNC_RUNS, ACTIVE_SYNC_THREADS, SYNC_STATUS
from src.bridge.registry_service import RegistryPaths, RegistryService
from src.bridge.discovery_service import DiscoveryDeps, DiscoveryPaths, DiscoveryService
from src.bridge.pipeline_service import PipelineRuntime, PipelineService
from src.bridge import html_extractor as _html_extractor
from src.bridge import source_checker as _source_checker
from src.bridge import task_history as _task_history_module
from src.shared.regex import find_urls_in_text
from src.shared.utils import coerce_port as _coerce_port, now_iso, now_utc

OPS_HISTORY_PATH = ROOT / "data" / "admin-run-history.json"
OPS_ALERT_STATE_PATH = ROOT / "data" / "admin-alert-state.json"
JOBS_FETCH_REPORT_PATH = ROOT / "data" / "jobs-fetch-report.json"
TASKS_CONFIG_PATH = ROOT / ".vscode" / "tasks.json"
TASK_STATE_PATH = ROOT / "data" / "admin-task-state.json"
DISCOVERY_LOG_PATH = ROOT / "data" / "source-discovery.log"
FETCHER_LOG_PATH = ROOT / "data" / "jobs-fetcher.log"
SYNC_CONFIG_PATH = ROOT / "data" / "source-sync-config.json"
SYNC_RUNTIME_PATH = ROOT / "data" / "source-sync-runtime.json"
STARTUP_METRICS_PATH = ROOT / "data" / "desktop-startup-metrics.jsonl"

MAX_HISTORY_ROWS = 240
STALE_FETCH_HOURS = 12
DEGRADED_FAILURE_RATIO = 0.25
OUTPUT_DROP_RATIO = 0.40
SOCIAL_ZERO_MATCH_THRESHOLD = 2
SOCIAL_FAILURE_THRESHOLD = 2
SOCIAL_LOW_CONFIDENCE_SPIKE_THRESHOLD = 120
OPS_SCHEMA_VERSION = 1
OPS_STATE_LOCK = threading.RLock()
_TASK_HISTORY_MANAGER: Optional[Any] = None


def _get_task_history_manager() -> Any:
    global _TASK_HISTORY_MANAGER
    if _TASK_HISTORY_MANAGER is None:
        _TASK_HISTORY_MANAGER = _task_history_module.TaskHistoryManager(
            OPS_HISTORY_PATH,
            TASK_STATE_PATH,
            MAX_HISTORY_ROWS,
            OPS_STATE_LOCK,
            load_json_array=load_json_array,
            save_json_atomic=save_json_atomic,
            load_json_object=load_json_object,
        )
    return _TASK_HISTORY_MANAGER


LOG_LEVEL_ORDER = {"debug": 10, "info": 20, "warn": 30, "error": 40}
PIPELINE_STATE_LOCK = threading.RLock()
ACTIVE_PIPELINE_RUN_ID = ""
ACTIVE_PIPELINE_THREAD: Optional[threading.Thread] = None
PIPELINE_STATUS: Dict[str, Any] = {
    "active": False,
    "runId": "",
    "stage": "idle",
    "progress": {"currentStep": 0, "totalSteps": 3, "percent": 0, "label": "Idle"},
    "startedAt": "",
    "finishedAt": "",
    "error": "",
    "updatesFound": False,
    "refreshRecommended": False,
    "baselineOutputCount": 0,
    "finalOutputCount": 0,
    "jobsPageLoadedCount": 0,
}
DESKTOP_LOCAL_DATA_STORE: LocalDataStore | None = None
STARTUP_METRICS_LOCK = threading.RLock()
DESKTOP_SESSION_ACTIVITY_AT = ""

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
_PIPELINE_RUNTIME = PipelineRuntime()
_PIPELINE_SERVICE: PipelineService | None = None
_PIPELINE_SERVICE_LOCK = threading.RLock()


class _RunHistoryAdapter:
    def append(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return append_run_history(row)

    def upsert(self, entry: Dict[str, Any], *, dedupe_fields: Tuple[str, ...]) -> Dict[str, Any]:
        return upsert_run_history(entry, dedupe_fields=dedupe_fields)

    def load(self) -> List[Dict[str, Any]]:
        return load_run_history()

    def prune_started_rows_for_type(self, entry_type: str, finished_at: str) -> None:
        prune_started_rows_for_type(entry_type, finished_at=finished_at)


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
            run_history=_RunHistoryAdapter(),
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
                paths=RegistryPaths(active=ACTIVE_PATH, pending=PENDING_PATH, rejected=REJECTED_PATH),
                default_active=[dict(row) for row in DEFAULT_STUDIO_SOURCE_REGISTRY],
                normalize_manual_static=normalize_manual_static_studio_fields,
            )
        return _REGISTRY_SERVICE


def _get_discovery_service() -> DiscoveryService:
    global _DISCOVERY_SERVICE, _DISCOVERY_SERVICE_PATHS
    current_paths = (Path(DISCOVERY_REPORT_PATH), Path(DISCOVERY_CANDIDATES_PATH), Path(PENDING_PATH), Path(DISCOVERY_LOG_PATH))
    with _DISCOVERY_SERVICE_LOCK:
        if _DISCOVERY_SERVICE is None or _DISCOVERY_SERVICE_PATHS != current_paths:
            _DISCOVERY_SERVICE_PATHS = current_paths
            _DISCOVERY_SERVICE = DiscoveryService(
                paths=DiscoveryPaths(
                    report=DISCOVERY_REPORT_PATH,
                    candidates=DISCOVERY_CANDIDATES_PATH,
                    pending=PENDING_PATH,
                    log=DISCOVERY_LOG_PATH,
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
                    normalize_discovery_report_contract=normalize_discovery_report_contract,
                    load_sync_runtime_state=load_sync_runtime_state,
                    maybe_trigger_auto_sync_push=_maybe_trigger_auto_sync_push,
                    mark_discovery_sync_finished=_mark_discovery_sync_finished,
                ),
            )
        return _DISCOVERY_SERVICE


def _get_pipeline_service() -> PipelineService:
    global _PIPELINE_SERVICE
    with _PIPELINE_SERVICE_LOCK:
        if _PIPELINE_SERVICE is None:
            _PIPELINE_SERVICE = PipelineService(
                pipeline_state_lock=PIPELINE_STATE_LOCK,
                pipeline_status=PIPELINE_STATUS,
                runtime=_PIPELINE_RUNTIME,
                bridge_log=bridge_log,
                now_iso=now_iso,
                parse_iso=parse_iso,
                append_run_history=append_run_history,
                upsert_run_history=upsert_run_history,
                task_running_from_state=task_running_from_state,
                sync_task_running=sync_task_running,
                current_fetch_output_count=_current_fetch_output_count,
                wait_for_report_completion=_wait_for_report_completion,
                wait_for_sync_completion=_wait_for_sync_completion,
                discovery_report_path=DISCOVERY_REPORT_PATH,
                fetch_report_path=JOBS_FETCH_REPORT_PATH,
                trigger_discovery_task=trigger_discovery_task,
                start_fetcher_task=start_fetcher_task,
                start_sync_task=start_sync_task,
                get_app_version=get_app_version,
            )
        return _PIPELINE_SERVICE


@dataclass
class RuntimeConfig:
    root: Path
    data_dir: Path
    host: str
    port: int
    log_format: str
    log_level: str
    quiet_requests: bool
    desktop_mode: bool = False


RUNTIME_CONFIG = RuntimeConfig(
    root=ROOT,
    data_dir=ROOT / "data",
    host="127.0.0.1",
    port=8877,
    log_format="human",
    log_level="info",
    quiet_requests=False,
    desktop_mode=False,
)


def _normalize_log_level(value: Any, default: str = "info") -> str:
    token = str(value or "").strip().lower()
    return token if token in LOG_LEVEL_ORDER else str(default)


def _normalize_log_format(value: Any, default: str = "human") -> str:
    token = str(value or "").strip().lower()
    return token if token in {"human", "jsonl"} else str(default)


def resolve_runtime_config(
    argv: Optional[List[str]] = None,
    *,
    env: Optional[Dict[str, str]] = None,
) -> RuntimeConfig:
    bridge_defaults = get_bridge_defaults()
    storage_defaults = get_storage_defaults()
    parser = argparse.ArgumentParser(description="Run local admin bridge API.")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--desktop-mode", action="store_true", default=False)
    parser.add_argument("--log-format", choices=("human", "jsonl"), default=None)
    parser.add_argument("--log-level", choices=("info", "debug"), default=None)
    parser.add_argument("--quiet-requests", action="store_true", default=None)
    args = parser.parse_args(argv)
    env_map = env if isinstance(env, dict) else os.environ

    host = str(args.host or env_map.get("BALUFFO_BRIDGE_HOST") or bridge_defaults["host"]).strip() or str(bridge_defaults["host"])
    port = _coerce_port(args.port if args.port is not None else env_map.get("BALUFFO_BRIDGE_PORT"), int(bridge_defaults["port"]))
    data_dir_raw = str(args.data_dir or env_map.get("BALUFFO_DATA_DIR") or storage_defaults["data_dir"]).strip()
    data_dir = Path(data_dir_raw).expanduser().resolve()
    log_format = _normalize_log_format(args.log_format or env_map.get("BALUFFO_BRIDGE_LOG_FORMAT") or bridge_defaults["log_format"])
    log_level = _normalize_log_level(args.log_level or env_map.get("BALUFFO_BRIDGE_LOG_LEVEL") or bridge_defaults["log_level"])
    quiet_requests = bool(
        args.quiet_requests
        if args.quiet_requests is not None
        else str(env_map.get("BALUFFO_BRIDGE_QUIET_REQUESTS") or "").strip().lower() in {"1", "true", "yes", "on"}
        if str(env_map.get("BALUFFO_BRIDGE_QUIET_REQUESTS") or "").strip()
        else bridge_defaults["quiet_requests"]
    )
    desktop_mode = bool(args.desktop_mode) or (
        str(env_map.get("BALUFFO_DESKTOP_MODE") or "").strip().lower() in {"1", "true", "yes", "on"}
    )
    return RuntimeConfig(
        root=ROOT,
        data_dir=data_dir,
        host=host,
        port=port,
        log_format=log_format,
        log_level=log_level,
        quiet_requests=quiet_requests,
        desktop_mode=desktop_mode,
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
        "ts": datetime.now(timezone.utc).isoformat(),
        "level": normalized_level,
        "message": str(message or ""),
        **{key: value for key, value in fields.items() if value is not None and value != ""},
    }
    if _normalize_log_format(RUNTIME_CONFIG.log_format) == "jsonl":
        print(json.dumps(payload, ensure_ascii=False), flush=True)
        return
    field_text = " ".join(f"{key}={value}" for key, value in payload.items() if key not in {"ts", "level", "message"})
    line = f"[admin_bridge][{normalized_level.upper()}] {payload['message']}"
    if field_text:
        line = f"{line} {field_text}"
    print(line, flush=True)


def configure_runtime_paths(config: RuntimeConfig) -> None:
    global RUNTIME_CONFIG
    global _TASK_HISTORY_MANAGER
    global OPS_HISTORY_PATH, OPS_ALERT_STATE_PATH, JOBS_FETCH_REPORT_PATH, TASK_STATE_PATH, DISCOVERY_LOG_PATH, FETCHER_LOG_PATH
    global ACTIVE_PATH, PENDING_PATH, REJECTED_PATH, DISCOVERY_REPORT_PATH, APPROVAL_STATE_PATH
    global TASKS_CONFIG_PATH, SYNC_CONFIG_PATH, SYNC_RUNTIME_PATH, STARTUP_METRICS_PATH
    global DESKTOP_LOCAL_DATA_STORE, DESKTOP_SESSION_ACTIVITY_AT
    global _REGISTRY_SERVICE, _REGISTRY_SERVICE_PATHS
    global _DISCOVERY_SERVICE, _DISCOVERY_SERVICE_PATHS

    RUNTIME_CONFIG = config
    data_dir = Path(config.data_dir).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)

    OPS_HISTORY_PATH = data_dir / "admin-run-history.json"
    OPS_ALERT_STATE_PATH = data_dir / "admin-alert-state.json"
    JOBS_FETCH_REPORT_PATH = data_dir / "jobs-fetch-report.json"
    TASK_STATE_PATH = data_dir / "admin-task-state.json"
    _TASK_HISTORY_MANAGER = None  # force new manager with updated paths
    DISCOVERY_LOG_PATH = data_dir / "source-discovery.log"
    FETCHER_LOG_PATH = data_dir / "jobs-fetcher.log"
    SYNC_CONFIG_PATH = data_dir / "source-sync-config.json"
    SYNC_RUNTIME_PATH = data_dir / "source-sync-runtime.json"
    STARTUP_METRICS_PATH = data_dir / "desktop-startup-metrics.jsonl"
    ACTIVE_PATH = data_dir / "source-registry-active.json"
    PENDING_PATH = data_dir / "source-registry-pending.json"
    REJECTED_PATH = data_dir / "source-registry-rejected.json"
    DISCOVERY_REPORT_PATH = data_dir / "source-discovery-report.json"
    APPROVAL_STATE_PATH = data_dir / "source-approval-state.json"
    TASKS_CONFIG_PATH = Path(config.root) / ".vscode" / "tasks.json"

    source_registry_module.DATA_DIR = data_dir
    source_registry_module.ACTIVE_PATH = ACTIVE_PATH
    source_registry_module.PENDING_PATH = PENDING_PATH
    source_registry_module.REJECTED_PATH = REJECTED_PATH
    source_registry_module.DISCOVERY_REPORT_PATH = DISCOVERY_REPORT_PATH
    source_registry_module.APPROVAL_STATE_PATH = APPROVAL_STATE_PATH
    # Desktop local-data APIs are safe to keep available on localhost and are required
    # for the packaged desktop experience. Treat the LocalDataStore as the feature flag,
    # rather than relying exclusively on environment toggles.
    DESKTOP_LOCAL_DATA_STORE = LocalDataStore(LocalDataPaths.from_data_dir(data_dir))
    DESKTOP_SESSION_ACTIVITY_AT = now_iso()
    with _REGISTRY_SERVICE_LOCK:
        _REGISTRY_SERVICE = None
        _REGISTRY_SERVICE_PATHS = None
    with _DISCOVERY_SERVICE_LOCK:
        _DISCOVERY_SERVICE = None
        _DISCOVERY_SERVICE_PATHS = None


def startup_banner(config: RuntimeConfig) -> None:
    bridge_log(
        "info",
        "admin_bridge_started",
        url=f"http://{config.host}:{config.port}",
        root=str(config.root),
        data_dir=str(config.data_dir),
        log_format=config.log_format,
        log_level=config.log_level,
        pid=os.getpid(),
    )
    bridge_log(
        "info",
        "admin_bridge_endpoints",
        ops="GET /ops/health, GET /ops/history, GET /ops/fetcher-metrics, POST /ops/alerts/ack",
        registry="GET /registry/*, POST /registry/*",
        sync="GET /sync/status, POST /sync/config, POST /sync/test, POST /sync/pull, POST /sync/push",
        tasks="POST /tasks/run-fetcher, POST /tasks/run-discovery, POST /tasks/run-sync-pull, POST /tasks/run-sync-push, POST /tasks/run-jobs-pipeline, GET /tasks/run-jobs-pipeline-status",
    )


def _normalize_sync_settings(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    security_defaults = get_security_defaults()
    data = payload if isinstance(payload, dict) else {}
    enabled_raw = data.get("enabled", bool(security_defaults["github_app_enabled_default"]))
    if isinstance(enabled_raw, bool):
        enabled = enabled_raw
    else:
        enabled = str(enabled_raw or "").strip().lower() not in {"", "0", "false", "no", "off"}
    return {"enabled": bool(enabled)}


def load_saved_sync_settings() -> Dict[str, Any]:
    return _get_sync_service().load_saved_sync_settings()


def append_startup_metric(event: str, payload: Optional[Dict[str, Any]] = None) -> None:
    row = {
        "ts": now_iso(),
        "event": str(event or "").strip() or "unknown",
        "payload": payload if isinstance(payload, dict) else {},
    }
    with STARTUP_METRICS_LOCK:
        try:
            STARTUP_METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
            with STARTUP_METRICS_PATH.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        except OSError:
            return


def read_startup_metrics(limit: int = 200) -> List[Dict[str, Any]]:
    max_rows = max(1, min(1000, int(limit or 200)))
    try:
        text = STARTUP_METRICS_PATH.read_text(encoding="utf-8")
    except OSError:
        return []
    rows: List[Dict[str, Any]] = []
    for line in text.splitlines():
        line = str(line or "").strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows[-max_rows:]


def resolve_effective_sync_config() -> source_sync_module.SyncConfig:
    return _get_sync_service()._resolve_effective_sync_config()  # noqa: SLF001


def refresh_sync_config() -> source_sync_module.SyncConfig:
    global SYNC_CONFIG
    SYNC_CONFIG = _get_sync_service().refresh_sync_config()
    return SYNC_CONFIG


def _mask_sync_token(token: str) -> str:
    candidate = str(token or "").strip()
    if len(candidate) <= 8:
        return candidate
    return f"{candidate[:6]}...{candidate[-4:]}"


def get_saved_sync_config_payload() -> Dict[str, Any]:
    return _get_sync_service().get_saved_sync_config_payload()


def update_saved_sync_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _get_sync_service().update_saved_sync_settings(payload)


def load_sync_runtime_state() -> Dict[str, Any]:
    return _get_sync_state().load_sync_runtime_state()


def save_sync_runtime_state(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _get_sync_state().save_sync_runtime_state(payload)


def test_sync_config() -> Dict[str, Any]:
    return _get_sync_service().test_sync_config()


def read_json_from_request(handler: BaseHTTPRequestHandler) -> Dict[str, Any]:
    length = int(handler.headers.get("Content-Length") or 0)
    if length <= 0:
        return {}
    raw = handler.rfile.read(length)
    try:
        payload = json.loads(raw.decode("utf-8"))
        return payload if isinstance(payload, dict) else {}
    except json.JSONDecodeError:
        return {}


def ensure_active_registry() -> List[Dict[str, Any]]:
    return _get_registry_service().ensure_active_registry()


def normalize_state(state: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
    return _get_registry_service().normalize_state(state)


def load_state() -> Dict[str, List[Dict[str, Any]]]:
    return _get_registry_service().load_state()


def summarize_state(state: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
    return RegistryService.summarize_state(state)


def persist_state(state: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
    return _get_registry_service().persist_state(state)


def persist_state_and_auto_sync(state: Dict[str, List[Dict[str, Any]]], *, reason: str) -> Dict[str, List[Dict[str, Any]]]:
    normalized = persist_state(state)
    _maybe_trigger_auto_sync_push(reason)
    return normalized


def move_entries(pending: List[Dict[str, Any]], selected_ids: List[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    return RegistryService.move_entries(pending, selected_ids)


def infer_studio_name_from_host(url: str) -> str:
    host = (urlparse(url).netloc or "").lower().strip()
    if ":" in host:
        host = host.split(":", 1)[0]
    labels = [part for part in host.split(".") if part]
    while labels and labels[0] in {"www", "w", "ww", "www2", "jobs", "job", "careers", "career", "apply", "join"}:
        labels.pop(0)
    token = labels[0] if labels else ""
    # Fallback if first remaining label still looks like a placeholder.
    if token in {"www", "w", "ww", "www2"} and len(labels) > 1:
        token = labels[1]
    split_token = token
    for marker in ("interactive", "entertainment", "software", "studios", "studio", "games", "game"):
        split_token = re.sub(rf"(?<!\s){marker}(?!\s)", f" {marker} ", split_token)
    token = split_token
    cleaned = re.sub(r"[^a-z0-9]+", " ", token).strip()
    if not cleaned:
        return "Manual Source"
    return " ".join(part.capitalize() for part in cleaned.split())


def build_manual_candidate(normalized_url: str) -> Dict[str, Any] | None:
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


def find_existing_source_by_url(state: Dict[str, List[Dict[str, Any]]], normalized_url: str) -> Dict[str, Any] | None:
    if not normalized_url:
        return None
    for bucket in ("active", "pending", "rejected"):
        for row in state.get(bucket, []):
            if source_url_fingerprint(row) == normalized_url:
                return row
    return None


def _normalized_host_token(raw_url: str) -> str:
    host = (urlparse(str(raw_url or "")).netloc or "").lower().strip()
    if ":" in host:
        host = host.split(":", 1)[0]
    labels = [part for part in host.split(".") if part]
    while labels and labels[0] in {"www", "w", "ww", "www2", "jobs", "job", "careers", "career", "apply", "join"}:
        labels.pop(0)
    return ".".join(labels)


def find_existing_static_source_by_studio_domain(
    state: Dict[str, List[Dict[str, Any]]],
    *,
    studio: str,
    normalized_url: str,
) -> Tuple[str, int, Dict[str, Any]] | None:
    studio_key = str(studio or "").strip().lower()
    host_key = _normalized_host_token(normalized_url)
    if not studio_key or not host_key:
        return None
    for bucket in ("active", "pending", "rejected"):
        rows = state.get(bucket, [])
        if not isinstance(rows, list):
            continue
        for idx, row in enumerate(rows):
            if str(row.get("adapter") or "").strip().lower() != "static":
                continue
            row_studio = str(row.get("studio") or "").strip().lower()
            if row_studio != studio_key:
                continue
            endpoint = str(
                row.get("listing_url")
                or row.get("api_url")
                or row.get("feed_url")
                or row.get("board_url")
                or (row.get("pages")[0] if isinstance(row.get("pages"), list) and row.get("pages") else "")
                or ""
            )
            if _normalized_host_token(endpoint) == host_key:
                return bucket, idx, row
    return None


def add_manual_source(raw_url: str) -> Dict[str, Any]:
    normalized_url = normalize_source_url(raw_url)
    if not normalized_url:
        return {"status": "invalid", "message": "Invalid URL. Use a full http(s) URL."}

    state = load_state()
    duplicate = find_existing_source_by_url(state, normalized_url)
    if duplicate:
        return {
            "status": "duplicate",
            "sourceId": source_identity(duplicate),
            "source": ensure_source_id(duplicate),
            "message": "Source already exists.",
        }

    candidate = build_manual_candidate(normalized_url)
    if not candidate:
        return {
            "status": "invalid",
            "message": "URL is valid but provider is not supported for discovery checks.",
        }

    # Collapse manual static variants by studio+domain (e.g. /careers, /career, /de/karriere).
    if str(candidate.get("adapter") or "").strip().lower() == "static":
        studio = str(candidate.get("studio") or "").strip()
        existing_match = find_existing_static_source_by_studio_domain(state, studio=studio, normalized_url=normalized_url)
        if existing_match is not None:
            bucket, idx, existing = existing_match
            updated = dict(existing)
            pages = list(updated.get("pages") or []) if isinstance(updated.get("pages"), list) else []
            normalized_pages = [normalize_source_url(str(page or "")) for page in pages]
            normalized_pages = [page for page in normalized_pages if page]
            if normalized_url not in normalized_pages:
                normalized_pages.append(normalized_url)
            updated["pages"] = normalized_pages
            if not str(updated.get("listing_url") or "").strip():
                updated["listing_url"] = normalized_pages[0] if normalized_pages else normalized_url
            updated = ensure_source_id(updated)
            state[bucket][idx] = updated
            state = persist_state_and_auto_sync(state, reason="manual_source_variant_added")
            return {
                "status": "duplicate",
                "sourceId": source_identity(updated),
                "source": ensure_source_id(updated),
                "summary": summarize_state(state),
                "message": "Source already exists for this studio/domain. Added URL as page variant.",
            }

    state["pending"] = unique_sources([candidate, *state["pending"]])
    state = persist_state_and_auto_sync(state, reason="manual_source_added")
    added = next((row for row in state["pending"] if source_identity(row) == source_identity(candidate)), candidate)
    return {
        "status": "added",
        "sourceId": source_identity(added),
        "source": ensure_source_id(added),
        "summary": summarize_state(state),
        "message": "Manual source added with generic website scraping fallback."
        if str(added.get("adapter") or "").lower() == "static"
        else "Manual source added.",
    }


def _try_fetch_with_playwright(url: str, timeout_s: int) -> Tuple[str, str]:
    """Best-effort browser fallback for anti-bot pages; returns (html, error)."""
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception:
        return "", "browser fallback unavailable (playwright is not installed)"
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=max(1, int(timeout_s)) * 1000)
            html = page.content() or ""
            browser.close()
            if not html:
                return "", "browser fallback returned empty content"
            return html, ""
    except Exception as exc:  # noqa: BLE001
        return "", str(exc)


def _is_http_forbidden_error(exc: Exception) -> bool:
    return bool(re.search(r"\bHTTP Error 403\b", str(exc), flags=re.I))


def _normalize_error_code(error_text: str) -> str:
    text = str(error_text or "").lower()
    if "browser fallback unavailable" in text or "playwright is not installed" in text:
        return "browser_fallback_unavailable"
    if "http error 404" in text:
        return "not_found"
    if "http error 403" in text:
        return "forbidden"
    if "certificate verify failed" in text or "hostname mismatch" in text or "[ssl:" in text:
        return "ssl_error"
    if "getaddrinfo failed" in text or "name or service not known" in text or "nodename nor servname provided" in text:
        return "dns_error"
    if "timed out" in text:
        return "timeout"
    if "no job postings found" in text:
        return "no_jobs"
    return "probe_failed"


def _suggest_alternate_career_urls(url: str) -> List[str]:
    parsed = urlparse(str(url or "").strip())
    host = (parsed.netloc or "").strip().lower()
    if not host:
        return []
    if ":" in host:
        host = host.split(":", 1)[0]
    labels = [part for part in host.split(".") if part]
    base_host = ".".join(labels[1:]) if labels[:1] == ["www"] and len(labels) > 2 else host
    path = parsed.path or ""
    if path.endswith("/") and path != "/":
        path = path[:-1]
    path = path or "/"
    source_norm = normalize_source_url(url)

    candidates_raw = [
        f"https://careers.{base_host}/",
        f"https://jobs.{base_host}/",
        f"https://{base_host}/careers",
        f"https://{base_host}/jobs",
        f"https://{base_host}/vacancies",
    ]
    if host != base_host:
        candidates_raw.append(f"https://{base_host}{path}")
    else:
        candidates_raw.append(f"https://www.{base_host}{path}")

    out: List[str] = []
    seen = set()
    for raw in candidates_raw:
        normalized = normalize_source_url(raw)
        if not normalized or normalized == source_norm or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out[:5]


def _discover_redirect_career_candidates(source_url: str, timeout_s: int) -> List[str]:
    parsed = urlparse(str(source_url or "").strip())
    host = (parsed.netloc or "").strip().lower()
    if not host:
        return []
    if ":" in host:
        host = host.split(":", 1)[0]
    labels = [part for part in host.split(".") if part]
    base_host = ".".join(labels[1:]) if labels[:1] == ["www"] and len(labels) > 2 else host
    roots = [f"https://{base_host}/"]
    if not base_host.startswith("www."):
        roots.append(f"https://www.{base_host}/")

    out: List[str] = []
    seen = set()
    for root in roots:
        body = ""
        try:
            req = Request(root, headers={"User-Agent": "Mozilla/5.0 Baluffo/1.0"})
            with urlopen(req, timeout=max(4, int(timeout_s))) as resp:
                final_url = normalize_source_url(resp.geturl() or "")
                charset = resp.headers.get_content_charset() or "utf-8"
                body = resp.read().decode(charset, errors="replace")
        except Exception:
            continue
        if final_url and final_url not in seen:
            low = final_url.lower()
            parsed_final = urlparse(final_url)
            path = (parsed_final.path or "").lower()
            if any(token in low for token in ("jobs.", "careers.", "/jobs", "/career", "/careers", "/vacancies")) or path in {"/jobs", "/career", "/careers", "/vacancies"}:
                seen.add(final_url)
                out.append(final_url)
        for href in re.findall(r'(?is)<a[^>]+href=["\']([^"\']+)["\']', str(body or "")):
            candidate = normalize_source_url(urljoin(root, str(href or "").strip()))
            if not candidate or candidate in seen:
                continue
            low_candidate = candidate.lower()
            if not any(
                token in low_candidate
                for token in (
                    "jobs.",
                    "careers.",
                    "apply.workable.com/",
                    "jobs.lever.co/",
                    "boards.greenhouse.io/",
                    "jobs.ashbyhq.com/",
                    "jobs.smartrecruiters.com/",
                    ".jobs.personio.de/",
                    "intervieweb.it/",
                    "/jobs",
                    "/career",
                    "/careers",
                    "/vacancies",
                    "/vacancy",
                )
            ):
                continue
            seen.add(candidate)
            out.append(candidate)
    return out[:6]


def _build_check_failure_details(error_text: str, source_url: str, *, browser_fallback_attempted: bool = False) -> Dict[str, Any]:
    code = _normalize_error_code(error_text)
    details: Dict[str, Any] = {
        "errorCode": code,
        "browserFallbackAttempted": bool(browser_fallback_attempted),
    }
    if code == "not_found":
        details["suggestedUrls"] = _suggest_alternate_career_urls(source_url)
    else:
        details["suggestedUrls"] = []
    return details


def _is_not_found_error_text(error_text: str) -> bool:
    return "http error 404" in str(error_text or "").lower()


def _looks_like_browser_challenge_page(html: str) -> bool:
    low = str(html or "").lower()
    if not low:
        return False
    challenge_tokens = (
        "challenge-platform",
        "/cdn-cgi/challenge-platform/",
        "cf-chl-",
        "cloudflare",
        "just a moment...",
        "enable javascript and cookies to continue",
    )
    return any(token in low for token in challenge_tokens)


def _fetch_html_with_fallback(url: str, timeout_s: int) -> Tuple[str, str, bool, bool]:
    """Return (html, error, browser_attempted, browser_used)."""
    try:
        html = discovery.fetch_text_with_retry(url, timeout_s, adapter="static")
        if not _looks_like_browser_challenge_page(html) or _html_has_extractable_job_data(html, url):
            return html, "", False, False
        browser_html, browser_error = _try_fetch_with_playwright(url, timeout_s)
        if browser_html:
            return browser_html, "", True, True
        if browser_error:
            return "", f"{url}: {browser_error}", True, False
        return html, "", True, False
    except Exception as exc:  # noqa: BLE001
        if not _is_http_forbidden_error(exc):
            return "", f"{url}: {exc}", False, False
        browser_html, browser_error = _try_fetch_with_playwright(url, timeout_s)
        if browser_html:
            return browser_html, "", True, True
        if browser_error:
            return "", f"{url}: {browser_error}", True, False
        return "", f"{url}: {exc}", True, False


def _html_has_extractable_job_data(html: str, page_url: str) -> bool:
    if _html_extractor.extract_job_like_links(html, page_url):
        return True
    if _html_extractor.extract_embedded_job_urls(html, page_url):
        return True
    embedded_links, embedded_signals = _html_extractor.extract_embedded_job_filter_signals(html, page_url)
    return bool(embedded_links or embedded_signals)


def _fetch_static_page_with_alternates(page_url: str, timeout_s: int) -> Tuple[str, str, bool, bool, str]:
    html, fetch_error, attempted, used = _fetch_html_with_fallback(page_url, timeout_s)
    if not fetch_error or not _is_not_found_error_text(fetch_error):
        return html, fetch_error, attempted, used, ""

    alt_candidates = list(_suggest_alternate_career_urls(page_url)[:3])
    for redirect_candidate in _discover_redirect_career_candidates(page_url, timeout_s):
        if redirect_candidate not in alt_candidates:
            alt_candidates.append(redirect_candidate)
    for alt_url in alt_candidates[:6]:
        alt_html, alt_error, alt_attempted, alt_used = _fetch_html_with_fallback(alt_url, timeout_s)
        attempted = attempted or alt_attempted
        used = used or alt_used
        if alt_error:
            continue
        return alt_html, "", attempted, used, alt_url
    return html, fetch_error, attempted, used, ""


def check_static_source(row: Dict[str, Any], timeout_s: int = 12) -> Tuple[bool, int, str, bool, Dict[str, Any]]:
    return _source_checker.check_static_source(
        row,
        timeout_s,
        fetch_page_with_alternates=_fetch_static_page_with_alternates,
        fetch_page=_fetch_html_with_fallback,
        fetch_text=lambda url, timeout: discovery.fetch_text_with_retry(url, timeout, adapter="static"),
        html_extractor=_html_extractor,
        parse_jobpostings_from_html=parse_jobpostings_from_html,
        normalize_job_url=normalize_job_url,
        source_identity=source_identity,
        suggest_alternate_career_urls=_suggest_alternate_career_urls,
    )


def normalize_manual_static_studio_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(row)
    source_url = normalize_source_url(
        str(normalized.get("listing_url") or "")
    ) or normalize_source_url(
        str((normalized.get("pages") or [""])[0] if isinstance(normalized.get("pages"), list) else "")
    )
    if not source_url:
        return normalized
    inferred = infer_studio_name_from_host(source_url)
    current_studio = str(normalized.get("studio") or "").strip().lower()
    # Correct placeholder studio values created by naive host parsing.
    if (
        current_studio in {"", "www", "w", "manual source"}
        or bool(re.search(r"\b(?:game|studio)\s+s\b", current_studio))
    ):
        normalized["studio"] = inferred
        normalized["company"] = inferred
        normalized["name"] = f"{inferred} (Manual Website)"
    return normalized


def trigger_source_check(source_id: str, timeout_s: int = 12) -> Dict[str, Any]:
    token = str(source_id or "").strip().lower()
    if not token:
        return {"started": False, "error": "Missing sourceId."}

    state = load_state()
    run_id = f"check_{uuid.uuid4().hex[:12]}"
    for bucket in ("active", "pending", "rejected"):
        rows = state.get(bucket, [])
        for idx, row in enumerate(rows):
            if source_identity(row) != token:
                continue
            if str(row.get("adapter") or "").strip().lower() == "static":
                row = normalize_manual_static_studio_fields(row)
                ok, jobs_found, error, weak_signal, probe_meta = check_static_source(row, timeout_s=timeout_s)
                updated = dict(row)
                updated["lastProbedAt"] = now_iso()
                if ok:
                    score, reasons = discovery.compute_candidate_score(updated, jobs_found)
                    updated["jobsFound"] = int(jobs_found)
                    updated["sampleCount"] = int(jobs_found)
                    updated["score"] = int(score)
                    updated["reasons"] = reasons
                    updated["confidence"] = "high" if jobs_found >= 10 else ("medium" if jobs_found >= 1 else "low")
                    updated.pop("lastProbeError", None)
                    updated["lastProbeWeakSignal"] = bool(weak_signal)
                    rows[idx] = updated
                    state[bucket] = rows
                    persist_state_and_auto_sync(state, reason="source_check_updated")
                    return {
                        "started": True,
                        "runId": run_id,
                        "sourceId": source_identity(updated),
                        "ok": True,
                        "jobsFound": int(jobs_found),
                        "weakSignal": bool(weak_signal),
                        "browserFallbackAttempted": bool((probe_meta or {}).get("browserFallbackAttempted")),
                        "browserFallbackUsed": bool((probe_meta or {}).get("browserFallbackUsed")),
                    }
                updated["lastProbeError"] = str(error or "probe failed")
                rows[idx] = updated
                state[bucket] = rows
                persist_state_and_auto_sync(state, reason="source_check_updated")
                source_url = normalize_source_url(
                    str(updated.get("listing_url") or "")
                ) or normalize_source_url(
                    str((updated.get("pages") or [""])[0] if isinstance(updated.get("pages"), list) else "")
                ) or ""
                failure_details = _build_check_failure_details(
                    str(error or "probe failed"),
                    source_url,
                    browser_fallback_attempted=bool((probe_meta or {}).get("browserFallbackAttempted")),
                )
                return {
                    "started": True,
                    "runId": run_id,
                    "sourceId": source_identity(updated),
                    "ok": False,
                    "error": str(error or "probe failed"),
                    "errorCode": str(failure_details.get("errorCode") or "probe_failed"),
                    "suggestedUrls": failure_details.get("suggestedUrls") or [],
                    "browserFallbackAttempted": bool(failure_details.get("browserFallbackAttempted")),
                    "browserFallbackUsed": bool((probe_meta or {}).get("browserFallbackUsed")),
                }
            ok, jobs_found, error = discovery.probe_candidate(row, timeout_s=timeout_s)
            if not ok and str(error or "").strip().lower() == "missing adapter or url":
                # Some canonical registry rows only store identity token (e.g. greenhouse slug)
                # and rely on adapter-specific URL fallback patterns.
                reconstructed = dict(row)
                adapter = str(reconstructed.get("adapter") or "").strip().lower()
                if adapter == "greenhouse" and not reconstructed.get("api_url") and reconstructed.get("slug"):
                    reconstructed["api_url"] = f"https://boards-api.greenhouse.io/v1/boards/{reconstructed.get('slug')}/jobs"
                elif adapter == "lever" and not reconstructed.get("api_url") and reconstructed.get("account"):
                    reconstructed["api_url"] = f"https://api.lever.co/v0/postings/{reconstructed.get('account')}?mode=json"
                elif adapter == "workable" and not reconstructed.get("api_url") and reconstructed.get("account"):
                    reconstructed["api_url"] = f"https://apply.workable.com/api/v1/widget/accounts/{reconstructed.get('account')}?details=true"
                elif adapter == "smartrecruiters" and not reconstructed.get("api_url") and reconstructed.get("company_id"):
                    reconstructed["api_url"] = f"https://api.smartrecruiters.com/v1/companies/{reconstructed.get('company_id')}/postings"
                ok, jobs_found, error = discovery.probe_candidate(reconstructed, timeout_s=timeout_s)
            if ok:
                score, reasons = discovery.compute_candidate_score(row, jobs_found)
                updated = discovery.normalize_candidate(row, score, reasons, jobs_found, probed_at=now_iso())
                updated["enabledByDefault"] = bool(row.get("enabledByDefault"))
                updated.pop("lastProbeError", None)
                if row.get("manualAddedAt"):
                    updated["manualAddedAt"] = row.get("manualAddedAt")
                rows[idx] = updated
                state[bucket] = rows
                persist_state_and_auto_sync(state, reason="source_check_updated")
                return {
                    "started": True,
                    "runId": run_id,
                    "sourceId": source_identity(updated),
                    "ok": True,
                    "jobsFound": int(jobs_found),
                }
            updated = dict(row)
            updated["lastProbedAt"] = now_iso()
            updated["lastProbeError"] = str(error or "probe failed")
            rows[idx] = updated
            state[bucket] = rows
            persist_state_and_auto_sync(state, reason="source_check_updated")
            source_url = normalize_source_url(endpoint_url := str(
                row.get("listing_url")
                or row.get("api_url")
                or row.get("feed_url")
                or row.get("board_url")
                or ""
            )) or endpoint_url
            failure_details = _build_check_failure_details(str(error or "probe failed"), str(source_url or ""))
            return {
                "started": True,
                "runId": run_id,
                "sourceId": source_identity(updated),
                "ok": False,
                "error": str(error or "probe failed"),
                "errorCode": str(failure_details.get("errorCode") or "probe_failed"),
                "suggestedUrls": failure_details.get("suggestedUrls") or [],
            }
    return {"started": False, "error": "Source not found."}


def run_background_script(script_name: str, args: List[str] | None = None) -> int:
    if getattr(sys, "frozen", False):
        command = [
            sys.executable,
            "__child_script__",
            "--root",
            str(Path(RUNTIME_CONFIG.root)),
            "--script",
            str(script_name),
            "--",
        ]
        command.extend(args or [])
    else:
        # When running from source, prefer module execution so that the `src`
        # package can be imported reliably regardless of how the script path is
        # resolved on sys.path.
        script_lower = str(script_name).lower()
        module = None
        if script_lower.endswith("jobs_fetcher.py"):
            module = "src.jobs_fetcher"
        elif script_lower.endswith("source_discovery.py"):
            module = "src.source_discovery"

        if module:
            command = [sys.executable, "-m", module]
            command.extend(args or [])
        else:
            command = [sys.executable, str(Path(RUNTIME_CONFIG.root) / "src" / script_name)]
            command.extend(args or [])
    script = Path(script_name).name.lower()
    task_type = "discovery" if "discovery" in script else ("fetch" if "fetcher" in script else script)
    child_env = os.environ.copy()
    child_env["BALUFFO_DATA_DIR"] = str(RUNTIME_CONFIG.data_dir)
    if task_type == "discovery":
        child_env["BALUFFO_DISCOVERY_LOG_PATH"] = str(DISCOVERY_LOG_PATH)
    elif task_type == "fetch":
        child_env["BALUFFO_FETCHER_LOG_PATH"] = str(FETCHER_LOG_PATH)
    popen_kwargs: Dict[str, Any] = {
        "cwd": str(Path(RUNTIME_CONFIG.root)),
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "env": child_env,
    }
    if os.name == "nt":
        # Detach child jobs from admin bridge console streams to avoid Windows
        # stdio initialization failures when terminal handles are unstable/closed.
        popen_kwargs["creationflags"] = int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
    log_handle = None
    try:
        if task_type in {"discovery", "fetch"}:
            log_path = DISCOVERY_LOG_PATH if task_type == "discovery" else FETCHER_LOG_PATH
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_handle = open(log_path, "a", encoding="utf-8")
            popen_kwargs["stdout"] = log_handle
            popen_kwargs["stderr"] = subprocess.STDOUT
        proc = subprocess.Popen(command, **popen_kwargs)
    finally:
        if log_handle is not None:
            log_handle.close()
    with OPS_STATE_LOCK:
        state = load_json_object(TASK_STATE_PATH, {})
        state[str(task_type)] = {
            "pid": int(proc.pid),
            "script": str(script_name),
            "startedAt": now_iso(),
        }
        save_json_atomic(TASK_STATE_PATH, state)
    bridge_log("info", "task_process_spawned", task=task_type, script=script_name, pid=int(proc.pid))
    return int(proc.pid)


def _safe_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(minimum, min(maximum, parsed))


def _safe_schema_version(value: Any) -> int:
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        parsed = 1
    return max(1, parsed)


def _coerce_fetch_report_detail_row(detail: Any) -> Dict[str, Any] | None:
    candidate: Dict[str, Any] | None = None
    if isinstance(detail, dict):
        candidate = detail
    elif isinstance(detail, str):
        raw = str(detail).strip()
        if raw.startswith("{") and raw.endswith("}"):
            parsed: Any = None
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                try:
                    parsed = ast.literal_eval(raw)
                except Exception:  # noqa: BLE001
                    parsed = None
            if isinstance(parsed, dict):
                candidate = parsed
    if not isinstance(candidate, dict):
        return None
    return {
        "name": str(candidate.get("name") or "").strip(),
        "status": str(candidate.get("status") or "").strip().lower(),
        "adapter": str(candidate.get("adapter") or "").strip().lower(),
        "studio": str(candidate.get("studio") or "").strip(),
        "fetchedCount": _safe_int(candidate.get("fetchedCount"), 0, 0, 1_000_000),
        "keptCount": _safe_int(candidate.get("keptCount"), 0, 0, 1_000_000),
        "lowConfidenceDropped": _safe_int(candidate.get("lowConfidenceDropped"), 0, 0, 1_000_000),
        "error": str(candidate.get("error") or "").strip(),
    }


def normalize_fetch_report_contract(payload: Dict[str, Any]) -> Dict[str, Any]:
    src = payload if isinstance(payload, dict) else {}
    summary = src.get("summary") if isinstance(src.get("summary"), dict) else {}
    runtime = src.get("runtime") if isinstance(src.get("runtime"), dict) else {}
    sources = src.get("sources")
    if not isinstance(sources, list):
        sources = []
    normalized_sources: List[Dict[str, Any]] = []
    for row in sources:
        if not isinstance(row, dict):
            continue
        details_raw = row.get("details")
        details = details_raw if isinstance(details_raw, list) else []
        normalized_details: List[Dict[str, Any]] = []
        for detail in details:
            parsed_detail = _coerce_fetch_report_detail_row(detail)
            if parsed_detail:
                normalized_details.append(parsed_detail)
        normalized_sources.append({
            "name": str(row.get("name") or "").strip(),
            "status": str(row.get("status") or "").strip().lower(),
            "adapter": str(row.get("adapter") or "").strip().lower(),
            "studio": str(row.get("studio") or "").strip(),
            "fetchedCount": _safe_int(row.get("fetchedCount"), 0, 0, 1_000_000),
            "keptCount": _safe_int(row.get("keptCount"), 0, 0, 1_000_000),
            "lowConfidenceDropped": _safe_int(row.get("lowConfidenceDropped"), 0, 0, 1_000_000),
            "error": str(row.get("error") or "").strip(),
            "durationMs": _safe_int(row.get("durationMs"), 0, 0, 86_400_000),
            "details": normalized_details,
        })
    return {
        "schemaVersion": _safe_schema_version(src.get("schemaVersion")),
        "startedAt": str(src.get("startedAt") or "").strip(),
        "finishedAt": str(src.get("finishedAt") or "").strip(),
        "runtime": dict(runtime),
        "summary": dict(summary),
        "sources": normalized_sources,
        "outputs": dict(src.get("outputs") or {}),
    }


def _derive_discovery_queued_count(report: Dict[str, Any], summary: Dict[str, Any]) -> int:
    queued = int(summary.get("queuedCandidateCount") or summary.get("newCandidateCount") or 0)
    candidates = report.get("candidates")
    if not isinstance(candidates, list):
        return max(0, queued)
    derived = len([
        row for row in candidates
        if isinstance(row, dict) and not bool(row.get("deferred"))
    ])
    return max(0, max(queued, derived))


def normalize_discovery_report_contract(payload: Dict[str, Any]) -> Dict[str, Any]:
    src = payload if isinstance(payload, dict) else {}
    summary = src.get("summary") if isinstance(src.get("summary"), dict) else {}
    candidates = src.get("candidates")
    failures = src.get("failures")
    top_failures = src.get("topFailures")
    normalized = {
        "schemaVersion": _safe_schema_version(src.get("schemaVersion")),
        "mode": str(src.get("mode") or "").strip(),
        "startedAt": str(src.get("startedAt") or "").strip(),
        "finishedAt": str(src.get("finishedAt") or "").strip(),
        "summary": dict(summary),
        "candidates": list(candidates) if isinstance(candidates, list) else [],
        "failures": list(failures) if isinstance(failures, list) else [],
        "topFailures": list(top_failures) if isinstance(top_failures, list) else [],
        "outputs": dict(src.get("outputs") or {}),
    }
    normalized["summary"]["queuedCandidateCount"] = _derive_discovery_queued_count(normalized, normalized["summary"])
    return normalized


def _failed_source_names_from_latest_report(*, allowed_names: set[str] | None = None) -> List[str]:
    report = normalize_fetch_report_contract(load_json_object(JOBS_FETCH_REPORT_PATH, {}))
    sources = report.get("sources")
    if not isinstance(sources, list):
        return []
    names: List[str] = []
    for row in sources:
        if not isinstance(row, dict):
            continue
        if str(row.get("status") or "").strip().lower() != "error":
            continue
        name = str(row.get("name") or "").strip()
        if allowed_names is not None and name not in allowed_names:
            continue
        if name:
            names.append(name)
    # Keep deterministic order and remove duplicates.
    seen = set()
    out: List[str] = []
    for name in sorted(names, key=lambda item: item.lower()):
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out


def build_fetcher_args_from_payload(payload: Dict[str, Any]) -> Tuple[List[str], str]:
    data = payload if isinstance(payload, dict) else {}
    preset = str(data.get("preset") or "default").strip().lower()
    args: List[str] = []

    # Optional explicit overrides.
    max_workers = _safe_int(data.get("maxWorkers"), 6, 1, 16)
    max_per_domain = _safe_int(data.get("maxPerDomain"), 2, 1, 6)
    fetch_strategy = str(data.get("fetchStrategy") or "auto").strip().lower()
    if fetch_strategy not in {"auto", "http", "browser"}:
        fetch_strategy = "auto"
    adapter_http_concurrency = _safe_int(data.get("adapterHttpConcurrency"), 24, 1, 128)
    source_ttl = _safe_int(data.get("sourceTtlMinutes"), 360, 0, 1440)
    hot_cadence = _safe_int(data.get("hotSourceCadenceMinutes"), 15, 1, 240)
    cold_cadence = _safe_int(data.get("coldSourceCadenceMinutes"), 60, 1, 1440)
    circuit_failures = _safe_int(data.get("circuitBreakerFailures"), 3, 0, 20)
    circuit_cooldown = _safe_int(data.get("circuitBreakerCooldownMinutes"), 180, 0, 24 * 60)

    if preset == "incremental":
        args.extend(["--skip-successful-sources", "--source-ttl-minutes", str(source_ttl), "--quiet"])
    elif preset == "retry_failed":
        available_names = {name for name, _loader in default_source_loaders()}
        failed_names = _failed_source_names_from_latest_report(allowed_names=available_names)
        if failed_names:
            args.extend(["--only-sources", ",".join(failed_names)])
        args.extend(["--ignore-circuit-breaker", "--quiet"])
    elif preset == "force_full":
        args.extend(["--ignore-circuit-breaker", "--quiet"])
    else:
        preset = "default"

    # Apply common overrides (including defaults) so runtime is explicit.
    args.extend(["--max-workers", str(max_workers), "--max-per-domain", str(max_per_domain)])
    args.extend(["--fetch-strategy", fetch_strategy, "--adapter-http-concurrency", str(adapter_http_concurrency)])
    args.extend(["--circuit-breaker-failures", str(circuit_failures)])
    args.extend(["--circuit-breaker-cooldown-minutes", str(circuit_cooldown)])
    args.extend(["--hot-source-cadence-minutes", str(hot_cadence), "--cold-source-cadence-minutes", str(cold_cadence)])

    if bool(data.get("skipSuccessfulSources")) and "--skip-successful-sources" not in args:
        args.append("--skip-successful-sources")
        args.extend(["--source-ttl-minutes", str(source_ttl)])
    if bool(data.get("respectSourceCadence")) and "--respect-source-cadence" not in args:
        args.append("--respect-source-cadence")
    if bool(data.get("ignoreCircuitBreaker")) and "--ignore-circuit-breaker" not in args:
        args.append("--ignore-circuit-breaker")
    if bool(data.get("quiet")) and "--quiet" not in args:
        args.append("--quiet")

    only_sources = data.get("onlySources")
    if isinstance(only_sources, list):
        sanitized = [str(item).strip() for item in only_sources if str(item).strip()]
        if sanitized:
            args.extend(["--only-sources", ",".join(sanitized)])
    return args, preset


def mark_desktop_session_activity(path: str) -> None:
    global DESKTOP_SESSION_ACTIVITY_AT
    if not RUNTIME_CONFIG.desktop_mode:
        return
    normalized = str(path or "").strip()
    if not normalized or normalized == "/ops/health":
        return
    DESKTOP_SESSION_ACTIVITY_AT = now_iso()


def parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def load_run_history() -> List[Dict[str, Any]]:
    return _get_task_history_manager().load_run_history()


def save_run_history(rows: List[Dict[str, Any]]) -> None:
    _get_task_history_manager().save_run_history(rows)


def append_run_history(row: Dict[str, Any]) -> Dict[str, Any]:
    return _get_task_history_manager().append_run_history(row)


def upsert_run_history(entry: Dict[str, Any], *, dedupe_fields: Tuple[str, ...]) -> Dict[str, Any]:
    return _get_task_history_manager().upsert_run_history(entry, dedupe_fields=dedupe_fields)


def prune_started_rows_for_type(
    run_type: str,
    *,
    keep_started_at: str = "",
    finished_at: str = "",
) -> None:
    _get_task_history_manager().prune_started_rows_for_type(
        run_type, keep_started_at=keep_started_at, finished_at=finished_at
    )


def pid_is_running(pid: int) -> bool:
    if int(pid or 0) <= 0:
        return False
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False
    return True


def _clear_task_state_locked(task_type: str) -> None:
    _get_task_history_manager()._clear_task_state_locked(task_type)


def clear_task_state(task_type: str) -> None:
    _get_task_history_manager().clear_task_state(task_type)


def task_running_from_state(task_type: str) -> bool:
    state = load_json_object(TASK_STATE_PATH, {})
    if not isinstance(state, dict):
        return False
    entry = state.get(str(task_type))
    if not isinstance(entry, dict):
        return False
    pid = int(entry.get("pid") or 0)
    return pid_is_running(pid)


def report_is_stale_in_progress(task_type: str, path: Path, report: Dict[str, Any], *, max_age_minutes: int = 5, max_mtime_idle_minutes: float = 0.35) -> bool:
    started_raw = str(report.get("startedAt") or "")
    finished_raw = str(report.get("finishedAt") or "")
    if not started_raw or finished_raw:
        return False
    started_dt = parse_iso(started_raw)
    if not started_dt:
        return False
    age_minutes = (now_utc() - started_dt).total_seconds() / 60.0
    if task_running_from_state(task_type):
        return False
    # If we have explicit task state but process is gone, clear stale quickly.
    state = load_json_object(TASK_STATE_PATH, {})
    if isinstance(state, dict) and isinstance(state.get(task_type), dict):
        return age_minutes >= 0.5
    try:
        mtime_dt = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        idle_minutes = (now_utc() - mtime_dt).total_seconds() / 60.0
        if idle_minutes >= float(max_mtime_idle_minutes):
            return True
    except OSError:
        pass
    return age_minutes >= float(max_age_minutes)


def load_alert_state() -> Dict[str, Any]:
    state = load_json_object(OPS_ALERT_STATE_PATH, {})
    acked = state.get("acked")
    if not isinstance(acked, dict):
        acked = {}
    return {
        "schemaVersion": OPS_SCHEMA_VERSION,
        "acked": {str(k): str(v) for k, v in acked.items()},
    }


def save_alert_state(state: Dict[str, Any]) -> None:
    payload = {
        "schemaVersion": OPS_SCHEMA_VERSION,
        "acked": dict(state.get("acked") or {}),
        "updatedAt": now_iso(),
    }
    save_json_atomic(OPS_ALERT_STATE_PATH, payload)


def detect_task_interval_hours(task: Dict[str, Any]) -> float | None:
    text = " ".join([
        str(task.get("label") or ""),
        str(task.get("command") or ""),
        str(task.get("detail") or ""),
    ]).lower()
    match_hours = re.search(r"every\s+(\d+(?:\.\d+)?)\s*(h|hour|hours)\b", text)
    if match_hours:
        return max(0.1, float(match_hours.group(1)))
    match_minutes = re.search(r"every\s+(\d+(?:\.\d+)?)\s*(m|min|minute|minutes)\b", text)
    if match_minutes:
        return max(1.0, float(match_minutes.group(1))) / 60.0
    match_flag = re.search(r"--every-hours\s+(\d+(?:\.\d+)?)", text)
    if match_flag:
        return max(0.1, float(match_flag.group(1)))
    return None


def parse_schedule_metadata() -> Dict[str, Any]:
    fallback = {
        "fetcher": {"intervalHours": None, "nextRunAt": "", "note": "unknown"},
        "discovery": {"intervalHours": None, "nextRunAt": "", "note": "unknown"},
    }
    try:
        payload = json.loads(TASKS_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fallback
    tasks = payload.get("tasks")
    if not isinstance(tasks, list):
        return fallback

    by_type: Dict[str, Dict[str, Any]] = {
        "fetcher": dict(fallback["fetcher"]),
        "discovery": dict(fallback["discovery"]),
    }
    for task in tasks:
        if not isinstance(task, dict):
            continue
        command = str(task.get("command") or "").lower()
        label = str(task.get("label") or "").lower()
        interval = detect_task_interval_hours(task)
        if "jobs_fetcher.py" in command or "run jobs fetcher" in label:
            by_type["fetcher"]["intervalHours"] = interval
            by_type["fetcher"]["note"] = "inferred" if interval else "manual_task"
        if "source_discovery.py" in command or "run source discovery" in label:
            by_type["discovery"]["intervalHours"] = interval
            by_type["discovery"]["note"] = "inferred" if interval else "manual_task"
    return by_type


def median(values: List[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[mid])
    return float((ordered[mid - 1] + ordered[mid]) / 2.0)


def summarize_fetch_report(report: Dict[str, Any]) -> Dict[str, Any]:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    output = int(summary.get("outputCount") or summary.get("uniqueOutputCount") or 0)
    failed = int(summary.get("failedSources") or 0)
    source_count = int(summary.get("sourceCount") or 0)
    duration_ms = 0
    sources = report.get("sources")
    if isinstance(sources, list):
        duration_ms = sum(int(item.get("durationMs") or 0) for item in sources if isinstance(item, dict))
    status = "ok"
    if source_count > 0 and failed >= source_count:
        status = "error"
    elif failed > 0:
        status = "warning"
    return {
        "outputCount": output,
        "failedSources": failed,
        "sourceCount": source_count,
        "durationMs": duration_ms,
        "failedRatio": (failed / source_count) if source_count > 0 else 0.0,
    }


def summarize_discovery_report(report: Dict[str, Any]) -> Dict[str, Any]:
    normalized = normalize_discovery_report_contract(report)
    summary = normalized.get("summary") if isinstance(normalized.get("summary"), dict) else {}
    queued = int(summary.get("queuedCandidateCount") or 0)
    failed = int(summary.get("failedProbeCount") or 0)
    probed = int(summary.get("probedCandidateCount") or summary.get("probedCount") or 0)
    duration_ms = 0
    started = parse_iso(report.get("startedAt"))
    finished = parse_iso(report.get("finishedAt"))
    if started and finished:
        duration_ms = int(max(0.0, (finished - started).total_seconds() * 1000))
    status = "ok"
    if probed > 0 and failed >= probed:
        status = "error"
    elif failed > 0:
        status = "warning"
    return {
        "queuedCandidateCount": queued,
        "failedProbeCount": failed,
        "probedCandidateCount": probed,
        "durationMs": duration_ms,
    }, status


def sync_history_from_reports() -> List[Dict[str, Any]]:
    with OPS_STATE_LOCK:
        _reconcile_sync_history_locked()
        _reconcile_started_task_history_locked("fetch")
        _reconcile_started_task_history_locked("discovery")
        fetch_report = normalize_fetch_report_contract(load_json_object(JOBS_FETCH_REPORT_PATH, {}))
        fetch_started_at = str(fetch_report.get("startedAt") or "")
        fetch_finished_at = str(fetch_report.get("finishedAt") or "")
        if report_is_stale_in_progress("fetch", JOBS_FETCH_REPORT_PATH, fetch_report):
            prune_started_rows_for_type("fetch")
            clear_task_state("fetch")
            fetch_started_at = ""
        if fetch_started_at and not fetch_finished_at:
            prune_started_rows_for_type("fetch", keep_started_at=fetch_started_at)
            fetch_summary = summarize_fetch_report(fetch_report)
            upsert_run_history({
                "type": "fetch",
                "status": "started",
                "startedAt": fetch_started_at,
                "finishedAt": "",
                "durationMs": int(fetch_summary["durationMs"]),
                "summary": {
                    "outputCount": int(fetch_summary["outputCount"]),
                    "failedSources": int(fetch_summary["failedSources"]),
                    "sourceCount": int(fetch_summary["sourceCount"]),
                },
            }, dedupe_fields=("type", "status", "startedAt"))
        if fetch_report.get("finishedAt"):
            fetch_summary = summarize_fetch_report(fetch_report)
            prune_started_rows_for_type("fetch", finished_at=str(fetch_report.get("finishedAt") or ""))
            clear_task_state("fetch")
            upsert_run_history({
                "type": "fetch",
                "status": "ok" if fetch_summary["failedSources"] == 0 else ("error" if fetch_summary["failedRatio"] >= 1 else "warning"),
                "startedAt": str(fetch_report.get("startedAt") or ""),
                "finishedAt": str(fetch_report.get("finishedAt") or ""),
                "durationMs": int(fetch_summary["durationMs"]),
                "summary": {
                    "outputCount": int(fetch_summary["outputCount"]),
                    "failedSources": int(fetch_summary["failedSources"]),
                    "sourceCount": int(fetch_summary["sourceCount"]),
                },
            }, dedupe_fields=("type", "finishedAt"))
        discovery_report = normalize_discovery_report_contract(load_json_object(DISCOVERY_REPORT_PATH, {}))
        discovery_started_at = str(discovery_report.get("startedAt") or "")
        discovery_finished_at = str(discovery_report.get("finishedAt") or "")
        if report_is_stale_in_progress("discovery", DISCOVERY_REPORT_PATH, discovery_report):
            prune_started_rows_for_type("discovery")
            clear_task_state("discovery")
            discovery_started_at = ""
        if discovery_started_at and not discovery_finished_at:
            prune_started_rows_for_type("discovery", keep_started_at=discovery_started_at)
            discovery_summary, _status = summarize_discovery_report(discovery_report)
            upsert_run_history({
                "type": "discovery",
                "status": "started",
                "startedAt": discovery_started_at,
                "finishedAt": "",
                "durationMs": int(discovery_summary["durationMs"]),
                "summary": {
                    "queuedCandidateCount": int(discovery_summary["queuedCandidateCount"]),
                    "failedProbeCount": int(discovery_summary["failedProbeCount"]),
                    "probedCandidateCount": int(discovery_summary["probedCandidateCount"]),
                },
            }, dedupe_fields=("type", "status", "startedAt"))
        if discovery_report.get("finishedAt"):
            discovery_summary, status = summarize_discovery_report(discovery_report)
            prune_started_rows_for_type("discovery", finished_at=str(discovery_report.get("finishedAt") or ""))
            clear_task_state("discovery")
            upsert_run_history({
                "type": "discovery",
                "status": status,
                "startedAt": str(discovery_report.get("startedAt") or ""),
                "finishedAt": str(discovery_report.get("finishedAt") or ""),
                "durationMs": int(discovery_summary["durationMs"]),
                "summary": {
                    "queuedCandidateCount": int(discovery_summary["queuedCandidateCount"]),
                    "failedProbeCount": int(discovery_summary["failedProbeCount"]),
                    "probedCandidateCount": int(discovery_summary["probedCandidateCount"]),
                },
            }, dedupe_fields=("type", "finishedAt"))
        return load_run_history()


def evaluate_alerts(*, history: List[Dict[str, Any]], latest_fetch_report: Dict[str, Any], pending_count: int) -> Dict[str, Any]:
    alert_state = load_alert_state()
    acked = dict(alert_state.get("acked") or {})
    active_conditions: List[Dict[str, Any]] = []
    now = now_utc()
    fetch_rows = [row for row in history if str(row.get("type")) == "fetch" and row.get("finishedAt")]
    latest_fetch = fetch_rows[-1] if fetch_rows else None
    last_success_fetch = next(
        (row for row in reversed(fetch_rows) if str(row.get("status")) in {"ok", "warning"}),
        None
    )
    stale_hours = None
    if last_success_fetch:
        finished = parse_iso(last_success_fetch.get("finishedAt"))
        if finished:
            stale_hours = (now - finished).total_seconds() / 3600.0
    if stale_hours is None or stale_hours > STALE_FETCH_HOURS:
        active_conditions.append({
            "id": "stale_fetch",
            "severity": "critical",
            "message": f"No successful fetch in the last {STALE_FETCH_HOURS}h.",
            "value": None if stale_hours is None else round(stale_hours, 2),
            "triggeredAt": now_iso(),
        })

    fetch_summary = summarize_fetch_report(latest_fetch_report)
    failed_ratio = float(fetch_summary["failedRatio"])
    if failed_ratio > DEGRADED_FAILURE_RATIO:
        active_conditions.append({
            "id": "degraded_reliability",
            "severity": "warning" if failed_ratio < 0.5 else "critical",
            "message": f"Failed source ratio is {failed_ratio:.0%} (threshold {DEGRADED_FAILURE_RATIO:.0%}).",
            "value": round(failed_ratio, 4),
            "triggeredAt": now_iso(),
        })

    outputs = [int((row.get("summary") or {}).get("outputCount") or 0) for row in fetch_rows if int((row.get("summary") or {}).get("outputCount") or 0) > 0]
    if len(outputs) >= 4 and latest_fetch:
        baseline_values = outputs[:-1] if len(outputs) > 1 else outputs
        baseline = median([float(v) for v in baseline_values[-10:]])
        latest_output = float(outputs[-1])
        if baseline > 0 and latest_output < baseline * (1.0 - OUTPUT_DROP_RATIO):
            drop_ratio = 1.0 - (latest_output / baseline)
            active_conditions.append({
                "id": "output_drop",
                "severity": "warning" if drop_ratio < 0.6 else "critical",
                "message": f"Output dropped {drop_ratio:.0%} vs rolling median.",
                "value": round(drop_ratio, 4),
                "triggeredAt": now_iso(),
            })

    source_rows = latest_fetch_report.get("sources") if isinstance(latest_fetch_report.get("sources"), list) else []
    social_rows = [
        row for row in source_rows
        if isinstance(row, dict) and str(row.get("name") or "").strip().lower().startswith("social_")
    ]
    if social_rows:
        social_failures = [
            row for row in social_rows
            if str(row.get("status") or "").strip().lower() == "error"
        ]
        if len(social_failures) >= SOCIAL_FAILURE_THRESHOLD:
            active_conditions.append({
                "id": "social_sources_failing",
                "severity": "warning" if len(social_failures) < 3 else "critical",
                "message": f"{len(social_failures)} social sources failed in the latest run.",
                "value": int(len(social_failures)),
                "triggeredAt": now_iso(),
            })

        zero_rows = [
            row for row in social_rows
            if str(row.get("status") or "").strip().lower() in {"ok", "error"}
            and int(row.get("keptCount") or 0) == 0
        ]
        if len(zero_rows) >= SOCIAL_ZERO_MATCH_THRESHOLD:
            active_conditions.append({
                "id": "social_zero_matches",
                "severity": "warning",
                "message": f"{len(zero_rows)} social sources produced zero matches in the latest run.",
                "value": int(len(zero_rows)),
                "triggeredAt": now_iso(),
            })

        low_conf_dropped = sum(int(row.get("lowConfidenceDropped") or 0) for row in social_rows)
        if low_conf_dropped >= SOCIAL_LOW_CONFIDENCE_SPIKE_THRESHOLD:
            active_conditions.append({
                "id": "social_low_confidence_spike",
                "severity": "warning",
                "message": "Social ingestion dropped an unusually high number of low-confidence posts.",
                "value": int(low_conf_dropped),
                "triggeredAt": now_iso(),
            })

    # Clear ack for alerts no longer active.
    active_ids = {row["id"] for row in active_conditions}
    for key in list(acked.keys()):
        if key not in active_ids:
            acked.pop(key, None)

    visible_alerts = [row for row in active_conditions if row["id"] not in acked]
    save_alert_state({"acked": acked})
    return {
        "alerts": visible_alerts,
        "suppressedCount": max(0, len(active_conditions) - len(visible_alerts)),
        "pendingApprovals": int(pending_count),
    }


def format_age(finished_at: str) -> str:
    dt = parse_iso(finished_at)
    if not dt:
        return "unknown"
    delta = now_utc() - dt
    total_minutes = int(max(0.0, delta.total_seconds() // 60))
    if total_minutes < 60:
        return f"{total_minutes}m"
    hours = total_minutes // 60
    if hours < 48:
        return f"{hours}h"
    return f"{hours // 24}d"


def _collect_fetch_history_metrics(history: List[Dict[str, Any]]) -> Dict[str, Any]:
    now = now_utc()
    seven_days_ago = now - timedelta(days=7)
    fetch_rows = [row for row in history if str(row.get("type")) == "fetch" and row.get("finishedAt")]
    fetch_7d = [
        row for row in fetch_rows
        if (parse_iso(row.get("finishedAt")) or datetime.min.replace(tzinfo=timezone.utc)) >= seven_days_ago
    ]
    success_7d = [row for row in fetch_7d if str(row.get("status")) in {"ok", "warning"}]
    success_rate = (len(success_7d) / len(fetch_7d)) if fetch_7d else 0.0
    avg_duration = int(sum(int(row.get("durationMs") or 0) for row in fetch_7d) / len(fetch_7d)) if fetch_7d else 0
    latest_fetch = fetch_rows[-1] if fetch_rows else None
    last_success = next((row for row in reversed(fetch_rows) if str(row.get("status")) in {"ok", "warning"}), None)
    return {
        "fetchRows": fetch_rows,
        "successRate7d": success_rate,
        "avgDurationMs7d": avg_duration,
        "latestFetch": latest_fetch,
        "lastSuccessFetch": last_success,
    }


def _populate_schedule_next_run(schedule: Dict[str, Any], history: List[Dict[str, Any]]) -> Dict[str, Any]:
    for run_type, key in (("fetch", "fetcher"), ("discovery", "discovery")):
        interval_hours = schedule[key].get("intervalHours")
        if not interval_hours:
            schedule[key]["nextRunAt"] = ""
            continue
        last_type_row = next((row for row in reversed(history) if str(row.get("type")) == run_type and row.get("finishedAt")), None)
        last_finished = parse_iso(last_type_row.get("finishedAt")) if last_type_row else None
        if last_finished:
            schedule[key]["nextRunAt"] = (last_finished + timedelta(hours=float(interval_hours))).isoformat()
        else:
            schedule[key]["nextRunAt"] = ""
    return schedule


def _derive_ops_severity(alerts: List[Dict[str, Any]]) -> str:
    if any(alert.get("severity") == "critical" for alert in alerts):
        return "critical"
    if alerts:
        return "warning"
    return "healthy"


def compute_ops_health() -> Dict[str, Any]:
    history = sync_history_from_reports()
    latest_fetch_report = normalize_fetch_report_contract(load_json_object(JOBS_FETCH_REPORT_PATH, {}))
    state = load_state()
    schedule = _populate_schedule_next_run(parse_schedule_metadata(), history)
    alerts_meta = evaluate_alerts(history=history, latest_fetch_report=latest_fetch_report, pending_count=len(state["pending"]))

    metrics = _collect_fetch_history_metrics(history)
    last_success = metrics["lastSuccessFetch"]
    latest_fetch_summary = summarize_fetch_report(latest_fetch_report)
    failed_ratio_latest = latest_fetch_summary["failedRatio"]

    latest_run = history[-1] if history else {}
    severity = _derive_ops_severity(alerts_meta["alerts"])

    return {
        "service": "baluffo-bridge",
        "desktopMode": bool(RUNTIME_CONFIG.desktop_mode),
        "generatedAt": now_iso(),
        "desktopLastActivityAt": str(DESKTOP_SESSION_ACTIVITY_AT or ""),
        "status": severity,
        "kpis": {
            "lastSuccessfulFetchAge": format_age(last_success.get("finishedAt") if last_success else ""),
            "sevenDayFetchSuccessRate": round(float(metrics["successRate7d"]), 4),
            "avgFetchDurationMs7d": int(metrics["avgDurationMs7d"]),
            "failedSourceRatioLatest": round(float(failed_ratio_latest), 4),
            "pendingApprovalsCount": len(state["pending"]),
            "lastRunResult": {
                "type": str(latest_run.get("type") or ""),
                "status": str(latest_run.get("status") or "unknown"),
                "finishedAt": str(latest_run.get("finishedAt") or latest_run.get("startedAt") or ""),
            },
        },
        "schedule": schedule,
        "alerts": alerts_meta["alerts"],
        "suppressedAlertsCount": int(alerts_meta["suppressedCount"]),
        "historyCount": len(history),
    }


def compute_fetcher_metrics(window_runs: int = 20) -> Dict[str, Any]:
    latest_fetch_report = normalize_fetch_report_contract(load_json_object(JOBS_FETCH_REPORT_PATH, {}))
    history = sync_history_from_reports()
    return fetcher_metrics_module.build_metrics(
        latest_fetch_report,
        history,
        window=max(1, int(window_runs or 1)),
    )


def _set_sync_status(*, action: str = "", result: str = "", error: str = "", pulled: bool = False, pushed: bool = False) -> None:
    _get_sync_state().set_sync_status(
        action=action,
        result=result,
        error=error,
        pulled=bool(pulled),
        pushed=bool(pushed),
    )


def get_sync_status_payload() -> Dict[str, Any]:
    return _get_sync_service().get_sync_status_payload()


def _sync_guard() -> Optional[Dict[str, Any]]:
    return _get_sync_service()._sync_guard()  # noqa: SLF001


def sync_pull_sources() -> Dict[str, Any]:
    return _get_sync_service().sync_pull_sources()


def sync_push_sources() -> Dict[str, Any]:
    return _get_sync_service().sync_push_sources()


def startup_sync_pull() -> None:
    _get_sync_service().startup_sync_pull()


def _reconcile_sync_history_locked() -> None:
    history = load_run_history()
    active_runs = SyncState.get_active_sync_runs()
    next_rows: List[Dict[str, Any]] = []
    changed = False
    for row in history:
        if str(row.get("type") or "").strip().lower() != "sync":
            next_rows.append(row)
            continue
        if str(row.get("status") or "").strip().lower() != "started":
            next_rows.append(row)
            continue
        if str(row.get("finishedAt") or "").strip():
            next_rows.append(row)
            continue
        run_id = str(row.get("id") or "").strip()
        if run_id and run_id in active_runs:
            next_rows.append(row)
            continue
        changed = True
    if changed:
        save_run_history(next_rows)


def _reconcile_started_task_history_locked(run_type: str) -> None:
    """Drop or finalize stale started rows when no task process is running.

    Fetch/discovery tasks can exit early (or be killed) without always updating their
    report `finishedAt` field. We use `admin-task-state.json` as the authoritative
    "is a process running" signal and finalize stale started placeholders.
    """
    history = load_run_history()
    now_dt = now_utc()
    next_rows: List[Dict[str, Any]] = []
    changed = False
    for row in history:
        if str(row.get("type") or "").strip().lower() != str(run_type or "").strip().lower():
            next_rows.append(row)
            continue
        if str(row.get("status") or "").strip().lower() != "started":
            next_rows.append(row)
            continue
        if str(row.get("finishedAt") or "").strip():
            next_rows.append(row)
            continue
        started_dt = parse_iso(row.get("startedAt"))
        if task_running_from_state(run_type):
            next_rows.append(row)
            continue
        if not started_dt:
            changed = True
            continue
        age_minutes = (now_dt - started_dt).total_seconds() / 60.0
        if age_minutes < 0.5:
            next_rows.append(row)
            continue
        changed = True
        finished_at = now_iso()
        next_rows.append(
            {
                **dict(row),
                "status": "error",
                "finishedAt": finished_at,
                "summary": {**(row.get("summary") if isinstance(row.get("summary"), dict) else {}), "error": "stale_started_run_pruned"},
            }
        )
        _clear_task_state_locked(run_type)
    if changed:
        save_run_history(next_rows)


def sync_task_running() -> bool:
    with OPS_STATE_LOCK:
        _reconcile_sync_history_locked()
    return _get_sync_service().sync_task_running()


def wait_for_sync_tasks(timeout_s: float = 5.0) -> None:
    _get_sync_service().wait_for_sync_tasks(timeout_s=float(timeout_s))


def _mark_discovery_sync_finished(finished_at: str) -> None:
    with SYNC_STATE_LOCK:
        _get_sync_state().save_sync_runtime_state({"lastDiscoverySyncFinishedAt": str(finished_at or "")})


def _maybe_trigger_auto_sync_push(reason: str) -> bool:
    guard = _sync_guard()
    if guard:
        return False
    if sync_task_running():
        return False
    result = start_sync_task("push", reason=reason, automatic=True)
    return bool(result.get("started"))


def _watch_discovery_run_for_auto_sync(run_id: str, pid: int, started_at: str) -> None:
    _get_discovery_service().watch_discovery_run_for_auto_sync(run_id, pid, started_at)


def _run_sync_task_worker(run_id: str, action: str, started_at: str, *, reason: str = "", automatic: bool = False) -> None:
    started_dt = parse_iso(started_at) or now_utc()
    status = "ok"
    summary: Dict[str, Any] = {"action": action}
    try:
        if action == "pull":
            result = sync_pull_sources()
            if not bool(result.get("ok")):
                status = "warning"
                summary["error"] = str(result.get("error") or "sync pull not executed")
            summary.update({
                "changed": bool(result.get("changed")),
                "remoteFound": bool(result.get("remoteFound")),
                "remoteSha": str(result.get("remoteSha") or ""),
                "remoteGeneratedAt": str(result.get("remoteGeneratedAt") or ""),
            })
            state_summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
            summary.update({
                "activeCount": int(state_summary.get("activeCount") or 0),
                "pendingCount": int(state_summary.get("pendingCount") or 0),
                "rejectedCount": int(state_summary.get("rejectedCount") or 0),
            })
        else:
            result = sync_push_sources()
            if not bool(result.get("ok")):
                status = "warning"
                summary["error"] = str(result.get("error") or "sync push not executed")
            summary.update({
                "remoteSha": str(result.get("remoteSha") or ""),
                "remotePreviouslyExisted": bool(result.get("remotePreviouslyExisted")),
            })
            counts = result.get("counts") if isinstance(result.get("counts"), dict) else {}
            summary.update({
                "activeCount": int(counts.get("active") or 0),
                "pendingCount": int(counts.get("pending") or 0),
                "rejectedCount": int(counts.get("rejected") or 0),
            })
    except Exception as exc:  # noqa: BLE001
        status = "error"
        summary["error"] = str(exc)
        _set_sync_status(action=action, result="error", error=str(exc))
    finally:
        SyncState.remove_active_sync_run(str(run_id or ""))
        SyncState.remove_active_sync_thread(str(run_id or ""))
    finished_dt = now_utc()
    duration_ms = int(max(0.0, (finished_dt - started_dt).total_seconds() * 1000))
    prune_started_rows_for_type("sync", finished_at=finished_dt.isoformat())
    upsert_run_history(
        {
            "id": run_id,
            "type": "sync",
            "status": status,
            "startedAt": started_at,
            "finishedAt": finished_dt.isoformat(),
            "durationMs": duration_ms,
            "summary": summary,
        },
        dedupe_fields=("type", "finishedAt"),
    )
    bridge_log(
        "info" if status != "error" else "error",
        "sync_task_finished",
        runId=run_id,
        action=action,
        reason=reason,
        automatic=automatic,
        status=status,
        durationMs=duration_ms,
        error=str(summary.get("error") or ""),
    )


def start_sync_task(action: str, *, reason: str = "", automatic: bool = False) -> Dict[str, Any]:
    return _get_sync_service().start_sync_task(action, reason=reason, automatic=bool(automatic))


def trigger_discovery_task(*, route_name: str, enable_auto_sync_watch: bool = True) -> Tuple[int, Dict[str, Any]]:
    return _get_discovery_service().trigger_discovery_task(
        route_name=route_name, enable_auto_sync_watch=enable_auto_sync_watch
    )


def _current_fetch_output_count() -> int:
    report = normalize_fetch_report_contract(load_json_object(JOBS_FETCH_REPORT_PATH, {}))
    summary = summarize_fetch_report(report)
    return int(summary.get("outputCount") or 0)


def _pipeline_progress(current_step: int, total_steps: int, label: str) -> Dict[str, Any]:
    safe_total = max(1, int(total_steps or 1))
    safe_current = max(0, min(int(current_step or 0), safe_total))
    return {
        "currentStep": safe_current,
        "totalSteps": safe_total,
        "percent": int(round((safe_current / safe_total) * 100)),
        "label": str(label or ""),
    }


def _pipeline_mark_stage(*, stage: str, current_step: int, total_steps: int, label: str, error: str = "") -> None:
    with PIPELINE_STATE_LOCK:
        PIPELINE_STATUS["stage"] = str(stage or "unknown")
        PIPELINE_STATUS["progress"] = _pipeline_progress(current_step, total_steps, label)
        if error:
            PIPELINE_STATUS["error"] = str(error)


def _pipeline_set_completed(*, status: str, final_output_count: int = 0, error: str = "") -> None:
    with PIPELINE_STATE_LOCK:
        run_id = str(PIPELINE_STATUS.get("runId") or "")
        started_at = str(PIPELINE_STATUS.get("startedAt") or "")
        baseline = int(PIPELINE_STATUS.get("baselineOutputCount") or 0)
        loaded = int(PIPELINE_STATUS.get("jobsPageLoadedCount") or 0)
        compare_base = max(baseline, loaded)
        updates_found = int(final_output_count or 0) > compare_base
        PIPELINE_STATUS.update({
            "active": False,
            "stage": "completed" if status != "error" else "error",
            "progress": _pipeline_progress(3, 3, "Pipeline completed" if status != "error" else "Pipeline failed"),
            "finishedAt": now_iso(),
            "error": str(error or ""),
            "finalOutputCount": int(final_output_count or 0),
            "updatesFound": bool(updates_found),
            "refreshRecommended": bool(updates_found),
        })
        finished_at = str(PIPELINE_STATUS.get("finishedAt") or "")
        if run_id:
            upsert_run_history({
                "id": run_id,
                "type": "pipeline",
                "status": "error" if status == "error" else "ok",
                "startedAt": started_at,
                "finishedAt": finished_at,
                "durationMs": int(max(0.0, (parse_iso(finished_at) - parse_iso(started_at)).total_seconds() * 1000)) if parse_iso(finished_at) and parse_iso(started_at) else 0,
                "summary": {
                    "error": str(error or ""),
                    "baselineOutputCount": baseline,
                    "jobsPageLoadedCount": loaded,
                    "finalOutputCount": int(final_output_count or 0),
                    "updatesFound": bool(updates_found),
                },
            }, dedupe_fields=("id",))
        global ACTIVE_PIPELINE_RUN_ID
        ACTIVE_PIPELINE_RUN_ID = ""


def get_jobs_pipeline_status_payload() -> Dict[str, Any]:
    return _get_pipeline_service().get_status_payload()


def _wait_for_report_completion(
    *,
    report_path: Path,
    started_at: str,
    timeout_s: float,
    report_name: str,
    fail_on_stale: bool = False,
) -> Dict[str, Any]:
    deadline = datetime.now(timezone.utc) + timedelta(seconds=max(10.0, float(timeout_s)))
    started_dt = parse_iso(started_at)
    while datetime.now(timezone.utc) < deadline:
        report = load_json_object(report_path, {})
        report_started = parse_iso(report.get("startedAt"))
        report_finished = parse_iso(report.get("finishedAt"))
        if started_dt and report_started and report_started >= (started_dt - timedelta(seconds=1)):
            if report_finished and report_finished >= report_started:
                return report if isinstance(report, dict) else {}
        if fail_on_stale and report_is_stale_in_progress(
            "fetch" if "fetch" in report_name else "discovery",
            report_path,
            report if isinstance(report, dict) else {},
        ):
            raise RuntimeError(f"{report_name} became stale before completion")
        threading.Event().wait(1.0)
    raise TimeoutError(f"{report_name} did not finish within timeout")


def _wait_for_sync_completion(run_id: str, timeout_s: float = 900.0) -> Dict[str, Any]:
    deadline = datetime.now(timezone.utc) + timedelta(seconds=max(10.0, float(timeout_s)))
    while datetime.now(timezone.utc) < deadline:
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


def start_fetcher_task(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    run_id = f"fetch_{uuid.uuid4().hex[:10]}"
    started_at = now_iso()
    fetcher_args, preset = build_fetcher_args_from_payload(payload if isinstance(payload, dict) else {})
    FETCHER_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    FETCHER_LOG_PATH.write_text(f"[{started_at}] Launching jobs fetcher task...\n", encoding="utf-8")
    append_run_history({
        "id": run_id,
        "type": "fetch",
        "status": "started",
        "startedAt": started_at,
        "finishedAt": "",
        "durationMs": 0,
        "summary": {},
    })
    spawn_args = list(fetcher_args)
    if "--output-dir" not in spawn_args:
        spawn_args.extend(["--output-dir", str(RUNTIME_CONFIG.data_dir)])
    pid = run_background_script("jobs_fetcher.py", spawn_args)
    approval = load_json_object(APPROVAL_STATE_PATH, {"approvedSinceLastRun": 0})
    approval["approvedSinceLastRun"] = 0
    save_json_atomic(APPROVAL_STATE_PATH, approval)
    bridge_log(
        "info",
        "task_started",
        runId=run_id,
        task="jobs_fetcher",
        preset=preset,
        pid=pid,
        args=" ".join(spawn_args),
    )
    return {
        "started": True,
        "runId": run_id,
        "task": "jobs_fetcher",
        "preset": preset,
        "args": spawn_args,
        "pid": int(pid),
        "startedAt": started_at,
    }


def start_jobs_pipeline_task(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _get_pipeline_service().start_task(payload)


def desktop_local_data_store() -> LocalDataStore:
    if DESKTOP_LOCAL_DATA_STORE is None:
        raise RuntimeError("Desktop local data API is unavailable.")
    return DESKTOP_LOCAL_DATA_STORE


class Handler(BaseHTTPRequestHandler):
    def _route_path(self) -> str:
        return urlparse(self.path).path

    def _route_query(self) -> Dict[str, List[str]]:
        return parse_qs(urlparse(self.path).query)

    def _send_json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_bytes(
        self,
        body: bytes,
        *,
        content_type: str,
        filename: str = "",
        disposition: str = "inline",
        status: int = 200,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        if filename:
            safe_filename = str(filename).replace('"', "")
            safe_disposition = "attachment" if str(disposition).lower() == "attachment" else "inline"
            self.send_header("Content-Disposition", f'{safe_disposition}; filename="{safe_filename}"')
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        if RUNTIME_CONFIG.quiet_requests:
            return
        try:
            message = format % args
        except Exception:  # noqa: BLE001
            message = format
        bridge_log("debug", "http_request", method=getattr(self, "command", ""), path=self.path, detail=message)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._send_json({"ok": True})

    def do_GET(self) -> None:  # noqa: N802
        path = self._route_path()
        query = self._route_query()
        mark_desktop_session_activity(path)
        from src.bridge.routes.get_routes import handle_get
        import sys

        if handle_get(self, api=sys.modules[__name__], path=path, query=query):
            return
        self._send_json({"error": "Not found"}, status=404)

    def do_POST(self) -> None:  # noqa: N802
        path = self._route_path()
        payload = read_json_from_request(self)
        mark_desktop_session_activity(path)
        from src.bridge.routes.post_routes import handle_post
        import sys
        import traceback

        try:
            if handle_post(self, api=sys.modules[__name__], path=path, payload=payload):
                return
            self._send_json({"error": "Not found"}, status=404)
        except Exception as exc:  # noqa: BLE001
            bridge_log("error", "http_post_handler_failed", path=path, error=str(exc))
            self._send_json(
                {
                    "error": "Internal server error",
                    "detail": str(exc),
                    "traceback": traceback.format_exc(),
                },
                status=500,
            )


def parse_args(argv: Optional[List[str]] = None) -> RuntimeConfig:
    return resolve_runtime_config(argv)


def main() -> int:
    config = parse_args()
    configure_runtime_paths(config)
    refresh_sync_config()
    ensure_active_registry()
    startup_sync_pull()
    try:
        server = ThreadingHTTPServer((config.host, config.port), Handler)
    except OSError as exc:
        bridge_log(
            "error",
            "admin_bridge_start_failed",
            host=config.host,
            port=config.port,
            error=str(exc),
        )
        return 1
    startup_banner(config)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        bridge_log("info", "admin_bridge_shutdown_requested", signal="keyboard_interrupt")
    finally:
        server.server_close()
        bridge_log("info", "admin_bridge_stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

