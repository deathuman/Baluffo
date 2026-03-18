#!/usr/bin/env python3
"""Local admin bridge for source discovery approval workflows."""

from __future__ import annotations

import argparse
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

# Allow running via `py src/admin_bridge.py` from repo root (or elsewhere).
# When executed as a script, Python puts `.../Baluffo/src` on sys.path, not the repo root,
# so absolute imports like `import src.jobs...` would fail without this.
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
from src.bridge.request_utils import read_json_from_request
from src.bridge.server import make_handler, run_http_server
from src.bridge.api import BridgeApi
from src.bridge import config as bridge_config
from src.bridge.server import runtime_state as bridge_runtime_state
from src.bridge.source_helpers import (
    find_existing_source_by_url,
    find_existing_static_source_by_studio_domain,
    infer_studio_name_from_host,
)
from src.bridge import report_normalizer
from src.bridge import ops_health as _ops_health
from src.bridge import registry_sync_flow as _registry_sync_flow
from src.bridge import run_history_api as _run_history_api
from src.bridge import source_check_fetch as _source_check_fetch
from src.bridge import source_check_http as _source_check_http
from src.bridge import sync_task_flow as _sync_task_flow
from src.shared.regex import find_urls_in_text

normalize_fetch_report_contract = report_normalizer.normalize_fetch_report_contract
normalize_discovery_report_contract = report_normalizer.normalize_discovery_report_contract
_safe_int = report_normalizer.safe_int
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
)


def _normalize_log_level(value: Any, default: str = "info") -> str:
    return bridge_config._normalize_log_level(value, default)


def _normalize_log_format(value: Any, default: str = "human") -> str:
    return bridge_config._normalize_log_format(value, default)


def resolve_runtime_config(
    argv: Optional[List[str]] = None,
    *,
    env: Optional[Dict[str, str]] = None,
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
    bridge_runtime_state.configure_runtime_paths(
        startup_metrics_path=STARTUP_METRICS_PATH,
        desktop_local_data_store=LocalDataStore(LocalDataPaths.from_data_dir(data_dir)),
        now_iso=now_iso,
    )
    with _REGISTRY_SERVICE_LOCK:
        _REGISTRY_SERVICE = None
        _REGISTRY_SERVICE_PATHS = None
    with _DISCOVERY_SERVICE_LOCK:
        _DISCOVERY_SERVICE = None
        _DISCOVERY_SERVICE_PATHS = None


def startup_banner(config: RuntimeConfig) -> None:
    bridge_config.startup_banner(config=config, bridge_log=bridge_log)


def build_bridge_api(config: RuntimeConfig) -> BridgeApi:
    # BridgeApi is dependency-injected to avoid importing `src.admin_bridge` from bridge modules.
    return BridgeApi(
        runtime_config=config,
        registry=_get_registry_service(),
        sync=_get_sync_service(),
        pipeline=_get_pipeline_service(),
        discovery=_get_discovery_service(),
        normalize_fetch_report_contract=normalize_fetch_report_contract,
        normalize_discovery_report_contract=normalize_discovery_report_contract,
        DISCOVERY_REPORT_PATH=DISCOVERY_REPORT_PATH,
        JOBS_FETCH_REPORT_PATH=JOBS_FETCH_REPORT_PATH,
        APPROVAL_STATE_PATH=APPROVAL_STATE_PATH,
        DISCOVERY_LOG_PATH=DISCOVERY_LOG_PATH,
        FETCHER_LOG_PATH=FETCHER_LOG_PATH,
        STARTUP_METRICS_PATH=STARTUP_METRICS_PATH,
        DESKTOP_SESSION_ACTIVITY_AT=bridge_runtime_state.DESKTOP_SESSION_ACTIVITY_AT,
        bridge_log=bridge_log,
        now_iso=now_iso,
        _mark_desktop_session_activity=mark_desktop_session_activity,
        desktop_local_data_store=desktop_local_data_store,
        read_startup_metrics=read_startup_metrics,
        load_json_object=load_json_object,
        save_json_atomic=save_json_atomic,
        start_fetcher_task=start_fetcher_task,
        start_sync_task=start_sync_task,
        compute_ops_health=compute_ops_health,
        compute_fetcher_metrics=compute_fetcher_metrics,
        sync_history_from_reports=sync_history_from_reports,
        load_alert_state=load_alert_state,
        save_alert_state=save_alert_state,
    )


def load_saved_sync_settings() -> Dict[str, Any]:
    return _get_sync_service().load_saved_sync_settings()


def append_startup_metric(event: str, payload: Optional[Dict[str, Any]] = None) -> None:
    bridge_runtime_state.append_startup_metric(event, payload, now_iso=now_iso)


def read_startup_metrics(limit: int = 200) -> List[Dict[str, Any]]:
    return bridge_runtime_state.read_startup_metrics(limit)


def resolve_effective_sync_config() -> source_sync_module.SyncConfig:
    return _get_sync_service()._resolve_effective_sync_config()  # noqa: SLF001


def refresh_sync_config() -> source_sync_module.SyncConfig:
    global SYNC_CONFIG
    SYNC_CONFIG = _get_sync_service().refresh_sync_config()
    return SYNC_CONFIG


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
    return _registry_sync_flow.persist_state_and_auto_sync(
        state,
        reason=reason,
        persist_state=persist_state,
        maybe_trigger_auto_sync_push=_maybe_trigger_auto_sync_push,
    )


def move_entries(pending: List[Dict[str, Any]], selected_ids: List[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    return RegistryService.move_entries(pending, selected_ids)


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


def _fetch_html_with_fallback_bound(url: str, timeout_s: int) -> Tuple[str, str, bool, bool]:
    return _source_check_fetch.fetch_html_with_fallback(
        url,
        timeout_s,
        fetch_text=lambda u, t: discovery.fetch_text_with_retry(u, t, adapter="static"),
        looks_like_challenge=_source_check_http.looks_like_browser_challenge_page,
        has_extractable_job_data=lambda html, page_url: _source_check_fetch.html_has_extractable_job_data(
            html, page_url, html_extractor=_html_extractor
        ),
        try_playwright=_source_check_http.try_fetch_with_playwright,
        is_http_forbidden=_source_check_http.is_http_forbidden_error,
    )


def _fetch_static_page_with_alternates_bound(page_url: str, timeout_s: int) -> Tuple[str, str, bool, bool, str]:
    return _source_check_fetch.fetch_static_page_with_alternates(
        page_url,
        timeout_s,
        fetch_html_with_fallback_fn=_fetch_html_with_fallback_bound,
        suggest_alternate_urls=_source_check_http.suggest_alternate_career_urls,
        discover_redirect_career_candidates=_source_check_http.discover_redirect_career_candidates,
        is_not_found_error_text=_source_check_http.is_not_found_error_text,
    )


def check_static_source(row: Dict[str, Any], timeout_s: int = 12) -> Tuple[bool, int, str, bool, Dict[str, Any]]:
    return _source_checker.check_static_source(
        row,
        timeout_s,
        fetch_page_with_alternates=_fetch_static_page_with_alternates_bound,
        fetch_page=_fetch_html_with_fallback_bound,
        fetch_text=lambda url, timeout: discovery.fetch_text_with_retry(url, timeout, adapter="static"),
        html_extractor=_html_extractor,
        parse_jobpostings_from_html=parse_jobpostings_from_html,
        normalize_job_url=normalize_job_url,
        source_identity=source_identity,
        suggest_alternate_career_urls=_source_check_http.suggest_alternate_career_urls,
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
                failure_details = _source_check_http.build_check_failure_details(
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
            failure_details = _source_check_http.build_check_failure_details(str(error or "probe failed"), str(source_url or ""))
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


def _failed_source_names_from_latest_report(*, allowed_names: set[str] | None = None) -> List[str]:
    report = normalize_fetch_report_contract(load_json_object(JOBS_FETCH_REPORT_PATH, {}))
    return report_normalizer.failed_source_names_from_report(
        report, allowed_names=allowed_names
    )


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
    bridge_runtime_state.mark_desktop_session_activity(
        path,
        now_iso=now_iso,
        desktop_mode=RUNTIME_CONFIG.desktop_mode,
    )


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


def load_run_history() -> List[Dict[str, Any]]:
    return _run_history_api.load_run_history(_get_task_history_manager())


def save_run_history(rows: List[Dict[str, Any]]) -> None:
    _run_history_api.save_run_history(_get_task_history_manager(), rows)


def append_run_history(row: Dict[str, Any]) -> Dict[str, Any]:
    return _run_history_api.append_run_history(_get_task_history_manager(), row)


def upsert_run_history(entry: Dict[str, Any], *, dedupe_fields: Tuple[str, ...]) -> Dict[str, Any]:
    return _run_history_api.upsert_run_history(
        _get_task_history_manager(), entry, dedupe_fields=dedupe_fields
    )


def prune_started_rows_for_type(
    run_type: str,
    *,
    keep_started_at: str = "",
    finished_at: str = "",
) -> None:
    _run_history_api.prune_started_rows_for_type(
        _get_task_history_manager(),
        run_type,
        keep_started_at=keep_started_at,
        finished_at=finished_at,
    )


def _clear_task_state_locked(task_type: str) -> None:
    _get_task_history_manager()._clear_task_state_locked(task_type)


def clear_task_state(task_type: str) -> None:
    _run_history_api.clear_task_state(_get_task_history_manager(), task_type)


def task_running_from_state(task_type: str) -> bool:
    return _run_history_api.task_running_from_state(
        task_type, load_json_object, TASK_STATE_PATH, pid_is_running
    )


def report_is_stale_in_progress(task_type: str, path: Path, report: Dict[str, Any], *, max_age_minutes: int = 5, max_mtime_idle_minutes: float = 0.35) -> bool:
    return _run_history_api.report_is_stale_in_progress(
        task_type,
        path,
        report,
        load_json_object=load_json_object,
        task_state_path=TASK_STATE_PATH,
        parse_iso=parse_iso,
        now_utc=now_utc,
        pid_is_running=pid_is_running,
        max_age_minutes=max_age_minutes,
        max_mtime_idle_minutes=max_mtime_idle_minutes,
    )


def _read_tasks_config() -> Dict[str, Any]:
    try:
        return json.loads(TASKS_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def load_alert_state() -> Dict[str, Any]:
    return _ops_health.load_alert_state(load_json_object, OPS_ALERT_STATE_PATH, OPS_SCHEMA_VERSION)


def save_alert_state(state: Dict[str, Any]) -> None:
    _ops_health.save_alert_state(
        save_json_atomic, OPS_ALERT_STATE_PATH, state, OPS_SCHEMA_VERSION, now_iso
    )


def detect_task_interval_hours(task: Dict[str, Any]) -> float | None:
    return _ops_health.detect_task_interval_hours(task)


def parse_schedule_metadata() -> Dict[str, Any]:
    return _ops_health.parse_schedule_metadata(_read_tasks_config)


def summarize_fetch_report(report: Dict[str, Any]) -> Dict[str, Any]:
    return _ops_health.summarize_fetch_report(report)


def summarize_discovery_report(report: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    return _ops_health.summarize_discovery_report(
        report, normalize_discovery_report_contract, parse_iso
    )


def sync_history_from_reports() -> List[Dict[str, Any]]:
    return _run_history_api.sync_history_from_reports(
        _run_history_api.SyncHistoryDeps(
            ops_state_lock=OPS_STATE_LOCK,
            load_run_history=load_run_history,
            save_run_history=save_run_history,
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
            discovery_report_path=DISCOVERY_REPORT_PATH,
            get_active_sync_runs=SyncState.get_active_sync_runs,
            parse_iso=parse_iso,
            now_iso=now_iso,
            now_utc=now_utc,
        )
    )


def _build_ops_health_deps() -> Any:
    deps = type("OpsHealthDeps", (), {})()
    deps.get_history = lambda: sync_history_from_reports()
    deps.get_fetch_report = lambda: normalize_fetch_report_contract(load_json_object(JOBS_FETCH_REPORT_PATH, {}))
    deps.get_state = load_state
    deps.now_iso = now_iso
    deps.desktop_mode = RUNTIME_CONFIG.desktop_mode
    deps.desktop_last_activity_at = bridge_runtime_state.DESKTOP_SESSION_ACTIVITY_AT
    deps.load_alert_state_fn = load_alert_state
    deps.save_alert_state_fn = save_alert_state
    deps.parse_schedule_metadata_fn = parse_schedule_metadata
    deps.parse_iso = parse_iso
    deps.now_utc = now_utc
    return deps


def compute_ops_health() -> Dict[str, Any]:
    return _ops_health.compute_ops_health(_build_ops_health_deps())


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


def sync_task_running() -> bool:
    with OPS_STATE_LOCK:
        _run_history_api.reconcile_sync_history_locked(
            _run_history_api.SyncHistoryDeps(
                ops_state_lock=OPS_STATE_LOCK,
                load_run_history=load_run_history,
                save_run_history=save_run_history,
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
                discovery_report_path=DISCOVERY_REPORT_PATH,
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
        _get_sync_state().save_sync_runtime_state({"lastDiscoverySyncFinishedAt": str(finished_at or "")})


def _maybe_trigger_auto_sync_push(reason: str) -> bool:
    return _registry_sync_flow.maybe_trigger_auto_sync_push(
        reason=reason,
        sync_guard=_sync_guard,
        sync_task_running=sync_task_running,
        start_sync_task=start_sync_task,
    )


def _watch_discovery_run_for_auto_sync(run_id: str, pid: int, started_at: str) -> None:
    _get_discovery_service().watch_discovery_run_for_auto_sync(run_id, pid, started_at)


def _run_sync_task_worker(run_id: str, action: str, started_at: str, *, reason: str = "", automatic: bool = False) -> None:
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
        upsert_run_history=lambda entry: upsert_run_history(entry, dedupe_fields=("type", "finishedAt")),
        bridge_log=bridge_log,
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
    return bridge_runtime_state.get_desktop_local_data_store()


def parse_args(argv: Optional[List[str]] = None) -> RuntimeConfig:
    return resolve_runtime_config(argv)


def main() -> int:
    config = parse_args()
    configure_runtime_paths(config)
    refresh_sync_config()
    ensure_active_registry()
    startup_sync_pull()
    api = build_bridge_api(config)
    handler_cls = make_handler(api=api)
    return run_http_server(api=api, host=config.host, port=config.port, handler_cls=handler_cls)


if __name__ == "__main__":
    raise SystemExit(main())

