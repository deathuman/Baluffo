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

from scripts.jobs.parsers import parse_jobpostings_from_html
from scripts.jobs.pipeline import default_source_loaders
from scripts.jobs.registry import DEFAULT_STUDIO_SOURCE_REGISTRY
from scripts.jobs.transport import normalize_url as normalize_job_url
from scripts import source_discovery as discovery
from scripts import fetcher_metrics as fetcher_metrics_module
from scripts import source_registry as source_registry_module
from scripts import source_sync as source_sync_module
from scripts.baluffo_config import get_bridge_defaults, get_security_defaults, get_storage_defaults
from scripts.contracts import SCHEMA_VERSION
from scripts.local_data_store import LocalDataPaths, LocalDataStore
from scripts.source_registry import (
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

OPS_HISTORY_PATH = ROOT / "data" / "admin-run-history.json"
OPS_ALERT_STATE_PATH = ROOT / "data" / "admin-alert-state.json"
JOBS_FETCH_REPORT_PATH = ROOT / "data" / "jobs-fetch-report.json"
TASKS_CONFIG_PATH = ROOT / ".vscode" / "tasks.json"
TASK_STATE_PATH = ROOT / "data" / "admin-task-state.json"
DISCOVERY_LOG_PATH = ROOT / "data" / "source-discovery.log"
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
LOG_LEVEL_ORDER = {"debug": 10, "info": 20, "warn": 30, "error": 40}
SYNC_STATE_LOCK = threading.RLock()
ACTIVE_SYNC_RUNS: set[str] = set()
ACTIVE_SYNC_THREADS: Dict[str, threading.Thread] = {}
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
SYNC_STATUS: Dict[str, Any] = {
    "lastPullAt": "",
    "lastPushAt": "",
    "lastError": "",
    "lastAction": "",
    "lastResult": "",
}
SYNC_CONFIG = source_sync_module.resolve_sync_config()
SYNC_CONFIG_LOCK = threading.RLock()
DESKTOP_LOCAL_DATA_STORE: LocalDataStore | None = None
STARTUP_METRICS_LOCK = threading.RLock()
DESKTOP_SESSION_ACTIVITY_AT = ""


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


def _coerce_port(value: Any, default: int = 8877) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(1, min(65535, parsed))


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
    desktop_mode = str(env_map.get("BALUFFO_DESKTOP_MODE") or "").strip().lower() in {"1", "true", "yes", "on"}
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
    global OPS_HISTORY_PATH, OPS_ALERT_STATE_PATH, JOBS_FETCH_REPORT_PATH, TASK_STATE_PATH, DISCOVERY_LOG_PATH
    global ACTIVE_PATH, PENDING_PATH, REJECTED_PATH, DISCOVERY_REPORT_PATH, APPROVAL_STATE_PATH
    global TASKS_CONFIG_PATH, SYNC_CONFIG_PATH, SYNC_RUNTIME_PATH, STARTUP_METRICS_PATH
    global DESKTOP_LOCAL_DATA_STORE, DESKTOP_SESSION_ACTIVITY_AT

    RUNTIME_CONFIG = config
    data_dir = Path(config.data_dir).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)

    OPS_HISTORY_PATH = data_dir / "admin-run-history.json"
    OPS_ALERT_STATE_PATH = data_dir / "admin-alert-state.json"
    JOBS_FETCH_REPORT_PATH = data_dir / "jobs-fetch-report.json"
    TASK_STATE_PATH = data_dir / "admin-task-state.json"
    DISCOVERY_LOG_PATH = data_dir / "source-discovery.log"
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
    DESKTOP_LOCAL_DATA_STORE = LocalDataStore(LocalDataPaths.from_data_dir(data_dir)) if config.desktop_mode else None
    DESKTOP_SESSION_ACTIVITY_AT = now_iso() if config.desktop_mode else ""


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
    raw = load_json_object(SYNC_CONFIG_PATH, {})
    if isinstance(raw, dict) and "enabled" in raw:
        return _normalize_sync_settings(raw)
    return {}


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
    return source_sync_module.resolve_sync_config(settings=load_saved_sync_settings(), env=os.environ)


def refresh_sync_config() -> source_sync_module.SyncConfig:
    global SYNC_CONFIG
    with SYNC_CONFIG_LOCK:
        SYNC_CONFIG = resolve_effective_sync_config()
        return SYNC_CONFIG


def _mask_sync_token(token: str) -> str:
    candidate = str(token or "").strip()
    if len(candidate) <= 8:
        return candidate
    return f"{candidate[:6]}...{candidate[-4:]}"


def get_saved_sync_config_payload() -> Dict[str, Any]:
    settings = load_saved_sync_settings()
    if "enabled" in settings:
        return {"enabled": bool(settings.get("enabled"))}
    return {"enabled": bool(source_sync_module.config_status(refresh_sync_config()).get("enabled"))}


def update_saved_sync_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_sync_settings(payload)
    save_json_atomic(SYNC_CONFIG_PATH, normalized)
    refresh_sync_config()
    return normalized


def load_sync_runtime_state() -> Dict[str, Any]:
    payload = load_json_object(SYNC_RUNTIME_PATH, {})
    raw = payload if isinstance(payload, dict) else {}
    return {
        "lastPullAt": str(raw.get("lastPullAt") or ""),
        "lastPushAt": str(raw.get("lastPushAt") or ""),
        "lastError": str(raw.get("lastError") or ""),
        "lastAction": str(raw.get("lastAction") or ""),
        "lastResult": str(raw.get("lastResult") or ""),
        "lastDiscoverySyncFinishedAt": str(raw.get("lastDiscoverySyncFinishedAt") or ""),
    }


def save_sync_runtime_state(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = load_sync_runtime_state()
    normalized.update({key: value for key, value in payload.items() if key in normalized})
    save_json_atomic(SYNC_RUNTIME_PATH, normalized)
    return normalized


def test_sync_config() -> Dict[str, Any]:
    cfg = refresh_sync_config()
    guard = _sync_guard()
    if guard:
        return guard
    remote = source_sync_module.read_remote_snapshot(cfg)
    return {
        "ok": True,
        "remoteFound": bool(remote.get("exists")),
        "remoteSha": str(remote.get("sha") or ""),
        "message": "GitHub sync connection verified.",
    }


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
    active = load_json_array(ACTIVE_PATH, [])
    if active:
        return active
    active = [dict(row) for row in DEFAULT_STUDIO_SOURCE_REGISTRY]
    save_json_atomic(ACTIVE_PATH, active)
    return active


def normalize_state(state: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
    # Precedence is explicit: active > pending > rejected.
    seen = set()
    normalized: Dict[str, List[Dict[str, Any]]] = {"active": [], "pending": [], "rejected": []}
    for bucket in ("active", "pending", "rejected"):
        for row in state.get(bucket, []):
            if not isinstance(row, dict):
                continue
            if str(row.get("adapter") or "").strip().lower() == "static":
                row = normalize_manual_static_studio_fields(row)
            key = source_identity(row)
            if key in seen:
                continue
            seen.add(key)
            normalized[bucket].append(ensure_source_id(row))
    return normalized


def load_state() -> Dict[str, List[Dict[str, Any]]]:
    return normalize_state({
        "active": ensure_active_registry(),
        "pending": load_json_array(PENDING_PATH, []),
        "rejected": load_json_array(REJECTED_PATH, []),
    })


def summarize_state(state: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
    return {
        "activeCount": len(state["active"]),
        "pendingCount": len(state["pending"]),
        "rejectedCount": len(state["rejected"]),
    }


def persist_state(state: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
    normalized = normalize_state(state)
    save_json_atomic(ACTIVE_PATH, normalized["active"])
    save_json_atomic(PENDING_PATH, normalized["pending"])
    save_json_atomic(REJECTED_PATH, normalized["rejected"])
    return normalized


def persist_state_and_auto_sync(state: Dict[str, List[Dict[str, Any]]], *, reason: str) -> Dict[str, List[Dict[str, Any]]]:
    normalized = persist_state(state)
    _maybe_trigger_auto_sync_push(reason)
    return normalized


def move_entries(pending: List[Dict[str, Any]], selected_ids: List[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    selected = set(str(item) for item in selected_ids)
    moved: List[Dict[str, Any]] = []
    remaining: List[Dict[str, Any]] = []
    for row in pending:
        if source_identity(row) in selected:
            moved.append(row)
        else:
            remaining.append(row)
    return moved, remaining


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


def _extract_job_like_links(html: str, base_url: str) -> List[str]:
    links: List[str] = []
    seen = set()
    for href in re.findall(r'(?is)<a[^>]+href=["\']([^"\']+)["\']', html):
        absolute = ""
        try:
            absolute = urljoin(base_url, str(href or "").strip())
        except Exception:  # noqa: BLE001
            absolute = str(href or "").strip()
        parsed = urlparse(absolute)
        path = (parsed.path or "").lower()
        is_job_path = "/job/" in path or "/jobs/" in path or "/career/posting/" in path
        if not is_job_path and path.startswith("/requisitions/view/"):
            is_job_path = bool(re.search(r"/requisitions/view/\d+/?$", path))
        # Many studio websites expose role pages under /careers/<role>/ rather than /jobs/.
        if not is_job_path and "/careers/" in path:
            tail = path.rstrip("/")
            is_job_path = not (
                tail == "/careers"
                or tail.endswith("/careers-category")
                or "/careers-category/" in tail
            )
        # Some sites use singular /career/<role>/ paths.
        if not is_job_path and "/career/" in path:
            tail = path.rstrip("/")
            is_job_path = tail != "/career"
        if not is_job_path and path.startswith("/open-positions/"):
            # WordPress-style careers listing/detail pages.
            is_job_path = True
        if not is_job_path and ("/job-offers/" in path or path.rstrip("/") == "/job-offers"):
            # Some studios publish opportunities under /job-offers.
            is_job_path = True
        if not is_job_path and path.startswith("/vacancy/"):
            # Vacancy detail pages used by some studio sites.
            is_job_path = bool(re.search(r"/vacancy/\d+/?$", path))
        if not is_job_path and path.startswith("/vacancies/"):
            # Some sites expose listings/details under /vacancies.
            is_job_path = True
        if not is_job_path and path.rstrip("/") == "/vacancies":
            is_job_path = True
        if not is_job_path and path.startswith("/join/"):
            # Some studios expose job pages under /join/<slug>/<numeric-id>.
            is_job_path = bool(re.search(r"/join/[^/]+/\d+/?$", path))
        if not is_job_path:
            continue
        normalized = normalize_job_url(absolute)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        links.append(normalized)
    return links


def _extract_embedded_job_urls(html: str, base_url: str) -> List[str]:
    links: List[str] = []
    seen = set()
    for raw in re.findall(r'https?://[^\s"\'<>]+', html, flags=re.I):
        absolute = normalize_job_url(raw)
        if not absolute or absolute in seen:
            continue
        low = absolute.lower()
        if any(token in low for token in ("jobs.lever.co/", "boards.greenhouse.io/", "jobs.ashbyhq.com/")):
            seen.add(absolute)
            links.append(absolute)
            continue
        if ".jobs.personio.de/" in low:
            seen.add(absolute)
            links.append(absolute)
            if not low.endswith("/search.json"):
                search_url = normalize_job_url(absolute.rstrip("/") + "/search.json")
                if search_url and search_url not in seen:
                    seen.add(search_url)
                    links.append(search_url)
            continue
        if "jobs.smartrecruiters.com/" in low:
            seen.add(absolute)
            links.append(absolute)
            continue
        if "apply.workable.com/" in low:
            seen.add(absolute)
            links.append(absolute)
            continue
        parsed = urlparse(absolute)
        path = (parsed.path or "").lower()
        if "/career/posting/" in path or "/jobs/" in path or "/job/" in path:
            seen.add(absolute)
            links.append(absolute)
    for raw in re.findall(r'(?is)href=["\']([^"\']+)["\']', html):
        absolute = normalize_job_url(urljoin(base_url, str(raw or "").strip()))
        if not absolute or absolute in seen:
            continue
        path = (urlparse(absolute).path or "").lower()
        if "/career/posting/" in path:
            seen.add(absolute)
            links.append(absolute)
    # JSON-heavy pages can embed relative detail URLs not exposed as <a href=...>.
    for raw in re.findall(r'(?is)["\'](/[^"\']{3,260})["\']', html):
        absolute = normalize_job_url(urljoin(base_url, str(raw or "").strip()))
        if not absolute or absolute in seen:
            continue
        path = (urlparse(absolute).path or "").lower()
        is_job_path = "/job/" in path or "/jobs/" in path or "/career/posting/" in path
        if not is_job_path and "/careers/" in path:
            tail = path.rstrip("/")
            is_job_path = tail != "/careers" and "/careers-category/" not in tail
        if not is_job_path and "/career/" in path:
            is_job_path = path.rstrip("/") != "/career"
        if not is_job_path and any(token in path for token in ("/vacancy/", "/open-positions/", "/join/")):
            is_job_path = True
        if not is_job_path and ("/vacancies/" in path or path.rstrip("/") == "/vacancies"):
            is_job_path = True
        if not is_job_path and ("/job-offers/" in path or path.rstrip("/") == "/job-offers"):
            is_job_path = True
        if not is_job_path:
            continue
        seen.add(absolute)
        links.append(absolute)
    return links


def _extract_workable_account(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    if "apply.workable.com" not in (parsed.netloc or "").lower():
        return ""
    parts = [part for part in (parsed.path or "").split("/") if part]
    if not parts:
        return ""
    account = str(parts[0] or "").strip()
    return account if re.match(r"^[a-z0-9][a-z0-9-]{1,80}$", account, flags=re.I) else ""


def _count_workable_jobs(account: str, timeout_s: int) -> int:
    token = str(account or "").strip()
    if not token:
        return 0
    api_url = f"https://apply.workable.com/api/v1/widget/accounts/{token}?details=true"
    payload_text = discovery.fetch_text_with_retry(api_url, timeout_s, adapter="static")
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError:
        return 0
    if not isinstance(payload, dict):
        return 0
    jobs = payload.get("jobs")
    return len(jobs) if isinstance(jobs, list) else 0


def _parse_personio_search_count(text: str) -> int:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return 0
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        for key in ("data", "positions", "items", "results"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return len(rows)
        if isinstance(payload.get("jobs"), list):
            return len(payload.get("jobs") or [])
    return 0


def _extract_jobylon_embed_urls(html: str) -> List[str]:
    out: List[str] = []
    seen = set()
    if "cdn.jobylon.com/embedder.js" not in html.lower():
        return out
    company_ids = re.findall(r"jbl_company_id\s*=\s*([0-9]+)", html, flags=re.I)
    versions = re.findall(r"jbl_version\s*=\s*['\"]([^'\"]+)['\"]", html, flags=re.I)
    page_sizes = re.findall(r"jbl_page_size\s*=\s*([0-9]+)", html, flags=re.I)
    version = versions[0].strip() if versions else "v2"
    page_size = page_sizes[0].strip() if page_sizes else "30"
    for company_id in company_ids:
        company = str(company_id or "").strip()
        if not company:
            continue
        url = (
            f"https://cdn.jobylon.com/jobs/companies/{company}/embed/{version}/"
            f"?target=jobylon-jobs-widget&page_size={page_size}"
        )
        normalized = normalize_job_url(url)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _extract_script_sources(html: str, base_url: str) -> List[str]:
    out: List[str] = []
    seen = set()
    for src in re.findall(r'(?is)<script[^>]+src=["\']([^"\']+)["\']', html):
        absolute = normalize_job_url(urljoin(base_url, str(src or "").strip()))
        if not absolute or absolute in seen:
            continue
        seen.add(absolute)
        out.append(absolute)
    return out


def _build_intervieweb_iframe_url(script_url: str, page_url: str) -> str:
    parsed = urlparse(script_url)
    if "intervieweb.it" not in (parsed.netloc or "").lower():
        return ""
    if "announces_js.php" not in (parsed.path or "").lower():
        return ""
    query = parse_qs(parsed.query, keep_blank_values=True)
    k = (query.get("k") or [""])[0]
    lac = (query.get("LAC") or [""])[0]
    lang = (query.get("lang") or ["en"])[0] or "en"
    ann_type = (query.get("annType") or ["published"])[0] or "published"
    type_view = (query.get("typeView") or ["large"])[0] or "large"
    d_value = (query.get("d") or [""])[0] or (urlparse(page_url).netloc or "")
    if not k or not lac or not d_value:
        return ""
    params = {
        "module": "iframeAnnunci",
        "lang": lang,
        "k": k,
        "d": d_value,
        "LAC": lac,
        "utype": (query.get("utype") or [""])[0],
        "act1": "23",
        "defgroup": (query.get("defgroup") or ["name"])[0],
        "gnavenable": (query.get("gnavenable") or ["1"])[0],
        "desc": (query.get("desc") or ["1"])[0],
        "annType": ann_type,
        "h": (query.get("h") or [""])[0],
        "typeView": type_view,
    }
    return f"{parsed.scheme}://{parsed.netloc}/app.php?{urlencode(params)}"


def _extract_intervieweb_job_links(html: str, base_url: str) -> List[str]:
    links: List[str] = []
    seen = set()
    for href in re.findall(r'(?is)href=["\']([^"\']+)["\']', html):
        absolute = normalize_job_url(urljoin(base_url, str(href or "").strip()))
        if not absolute or absolute in seen:
            continue
        lower = absolute.lower()
        if "idannuncio=" in lower or ("module=iframeannunci" in lower and "act1=1" in lower):
            seen.add(absolute)
            links.append(absolute)
    return links


def _extract_external_job_links_from_scripts(html: str, page_url: str, timeout_s: int) -> Tuple[List[str], List[str]]:
    job_links: List[str] = []
    errors: List[str] = []
    seen = set()
    script_sources = _extract_script_sources(html, page_url)
    for script_url in script_sources:
        lower = script_url.lower()
        intervieweb_iframe = _build_intervieweb_iframe_url(script_url, page_url)
        if intervieweb_iframe:
            try:
                iframe_html = discovery.fetch_text_with_retry(intervieweb_iframe, timeout_s, adapter="static")
                for link in _extract_intervieweb_job_links(iframe_html, intervieweb_iframe):
                    if link in seen:
                        continue
                    seen.add(link)
                    job_links.append(link)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{intervieweb_iframe}: {exc}")
            continue
        if not any(token in lower for token in ("career", "job", "vacanc", "recruit", "announc")):
            continue
        try:
            script_text = discovery.fetch_text_with_retry(script_url, timeout_s, adapter="static")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{script_url}: {exc}")
            continue
        for raw in re.findall(r'https?://[^\s"\'<>]+', script_text, flags=re.I):
            absolute = normalize_job_url(raw)
            if not absolute or absolute in seen:
                continue
            low_abs = absolute.lower()
            if not any(token in low_abs for token in ("job", "career", "vacanc", "recruit", "annunci")):
                continue
            seen.add(absolute)
            job_links.append(absolute)
    return job_links, errors


def _extract_text_job_signals(html: str, page_url: str) -> List[str]:
    """Fallback weak signals for pages that render role text without stable job links."""
    # Remove script/style blocks so token counts come from visible content.
    sanitized = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", html)
    sanitized = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", sanitized)
    text = re.sub(r"(?is)<[^>]+>", " ", sanitized)
    text = re.sub(r"\s+", " ", text).strip().lower()
    if not text:
        return []

    parsed = urlparse(page_url)
    path = (parsed.path or "").lower()
    on_careers_page = "/career" in path or "/careers" in path
    apply_count = len(re.findall(r"\bapply(?:\s+now)?\b", text))
    role_keywords = (
        "programmer",
        "engineer",
        "designer",
        "artist",
        "animator",
        "producer",
        "director",
        "qa",
        "tester",
        "technical",
    )
    role_count = sum(len(re.findall(rf"\b{re.escape(token)}\b", text)) for token in role_keywords)
    if not on_careers_page or apply_count < 4 or role_count < 4:
        return []
    signal_count = max(1, min(24, role_count // 2))
    page_norm = normalize_source_url(page_url) or page_url
    return [f"signal:text_jobs:{page_norm}:{idx}" for idx in range(signal_count)]


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


def _looks_like_not_found_page(html: str) -> bool:
    low = str(html or "").lower()
    if not low:
        return False
    if "<title>404" in low or "404 not found" in low:
        return True
    if "/404.json?index=" in low:
        return True
    if '"notfound":true' in low or '"not_found":true' in low:
        return True
    return False


def _extract_static_module_signals(html: str, page_url: str) -> List[str]:
    low = str(html or "").lower()
    signals: List[str] = []
    if "job_openings_module" in low or '"slice_type":"job_openings_module"' in low:
        signals.append(f"signal:job_openings_module:{normalize_source_url(page_url) or page_url}")
    if "sumo-lever-integration" in low or "sumo_lever_filter" in low:
        signals.append(f"signal:sumo_lever_module:{normalize_source_url(page_url) or page_url}")
    if "apply.workable.com/" in low:
        signals.append(f"signal:workable_embed:{normalize_source_url(page_url) or page_url}")
    return signals


def _extract_embedded_job_filter_signals(html: str, page_url: str) -> Tuple[List[str], List[str]]:
    structured_links: List[str] = []
    weak_signals: List[str] = []
    seen_links = set()
    page_norm = normalize_source_url(page_url) or page_url
    matches = re.findall(r'(?is)<job-filter\b[^>]+:raw-data=["\'](.*?)["\']', html)
    for raw_payload in matches:
        payload_text = html_module.unescape(str(raw_payload or "").strip())
        if not payload_text:
            continue
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError:
            continue
        jobs = payload.get("jobs") if isinstance(payload, dict) else []
        if not isinstance(jobs, list):
            continue
        for idx, job in enumerate(jobs):
            if not isinstance(job, dict):
                continue
            raw_link = str(job.get("link") or job.get("url") or "").strip()
            if raw_link:
                absolute = normalize_job_url(urljoin(page_url, raw_link))
                if absolute and absolute not in seen_links:
                    seen_links.add(absolute)
                    structured_links.append(absolute)
                    continue
            job_id = str(job.get("id") or idx).strip() or str(idx)
            weak_signals.append(f"signal:embedded_job_filter:{page_norm}:{job_id}")
    return structured_links, weak_signals


def _html_has_extractable_job_data(html: str, page_url: str) -> bool:
    if _extract_job_like_links(html, page_url):
        return True
    if _extract_embedded_job_urls(html, page_url):
        return True
    embedded_links, embedded_signals = _extract_embedded_job_filter_signals(html, page_url)
    return bool(embedded_links or embedded_signals)


def _resolve_static_source_pages(row: Dict[str, Any]) -> List[str]:
    pages_raw = row.get("pages") if isinstance(row.get("pages"), list) else []
    pages = [normalize_source_url(page) for page in pages_raw if normalize_source_url(page)]
    if pages:
        return pages
    listing_url = normalize_source_url(str(row.get("listing_url") or ""))
    return [listing_url] if listing_url else []


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


def _collect_embedded_signals(
    html: str,
    page_url: str,
    timeout_s: int,
    *,
    weak_links: set[str],
    errors: List[str],
) -> None:
    for embedded_link in _extract_embedded_job_urls(html, page_url):
        weak_links.add(embedded_link)
        low_embedded = str(embedded_link or "").lower()
        if low_embedded.endswith("/search.json") and ".jobs.personio.de/" in low_embedded:
            try:
                personio_json = discovery.fetch_text_with_retry(embedded_link, timeout_s, adapter="static")
                personio_count = _parse_personio_search_count(personio_json)
                for idx in range(max(0, personio_count)):
                    weak_links.add(f"signal:personio_search:{embedded_link}:{idx}")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{embedded_link}: {exc}")
        workable_account = _extract_workable_account(embedded_link)
        if workable_account:
            try:
                workable_count = _count_workable_jobs(workable_account, timeout_s)
                for idx in range(max(0, workable_count)):
                    weak_links.add(f"signal:workable_jobs:{workable_account}:{idx}")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"workable:{workable_account}: {exc}")


def _collect_detail_page_structured_links(
    html: str,
    page_url: str,
    timeout_s: int,
    *,
    company: str,
    source_id: str,
    structured_links: set[str],
    weak_links: set[str],
    errors: List[str],
) -> Tuple[bool, bool]:
    browser_fallback_attempted = False
    browser_fallback_used = False
    detail_links = _extract_job_like_links(html, page_url)
    for link in detail_links:
        weak_links.add(link)
        detail_html, detail_error, attempted, used = _fetch_html_with_fallback(link, timeout_s)
        browser_fallback_attempted = browser_fallback_attempted or attempted
        browser_fallback_used = browser_fallback_used or used
        if detail_error:
            errors.append(detail_error)
            continue
        detail_rows = parse_jobpostings_from_html(
            detail_html,
            base_url=link,
            fallback_company=company,
            fallback_source_id_prefix=f"static:{source_id}",
        )
        for parsed in detail_rows:
            parsed_link = normalize_job_url(parsed.get("jobLink"))
            if parsed_link:
                structured_links.add(parsed_link)
    return browser_fallback_attempted, browser_fallback_used


def _expand_static_alt_pages(
    *,
    page_url: str,
    pages_to_visit: List[str],
    seen_pages: set[str],
    max_pages_to_visit: int,
) -> None:
    low_page = str(page_url or "").lower()
    if not any(token in low_page for token in ("/career", "/careers", "/jobs", "/job", "/vacancies", "/vacancy")):
        return
    for alt_url in _suggest_alternate_career_urls(page_url):
        if len(pages_to_visit) >= max_pages_to_visit:
            break
        alt_normalized = normalize_source_url(alt_url)
        if not alt_normalized or alt_normalized in seen_pages:
            continue
        seen_pages.add(alt_normalized)
        pages_to_visit.append(alt_normalized)


def check_static_source(row: Dict[str, Any], timeout_s: int = 12) -> Tuple[bool, int, str, bool, Dict[str, Any]]:
    pages = _resolve_static_source_pages(row)
    if not pages:
        return False, 0, "missing source pages", False, {
            "browserFallbackAttempted": False,
            "browserFallbackUsed": False,
        }

    company = str(row.get("company") or row.get("studio") or row.get("name") or "Unknown")
    structured_links = set()
    weak_links = set()
    errors: List[str] = []
    browser_fallback_attempted = False
    browser_fallback_used = False
    pages_to_visit = list(pages)
    seen_pages = set(pages_to_visit)
    max_pages_to_visit = 18
    idx = 0
    while idx < len(pages_to_visit):
        page_url = pages_to_visit[idx]
        idx += 1
        before_structured_count = len(structured_links)
        before_weak_count = len(weak_links)
        html, fetch_error, attempted, used, redirected_url = _fetch_static_page_with_alternates(page_url, timeout_s)
        browser_fallback_attempted = browser_fallback_attempted or attempted
        browser_fallback_used = browser_fallback_used or used
        if redirected_url:
            weak_links.add(redirected_url)
        if fetch_error:
            errors.append(fetch_error)
            continue
        if _looks_like_not_found_page(html):
            errors.append(f"{page_url}: HTTP Error 404: Not Found")
            continue

        _collect_embedded_signals(
            html,
            page_url,
            timeout_s,
            weak_links=weak_links,
            errors=errors,
        )
        embedded_structured_links, embedded_weak_signals = _extract_embedded_job_filter_signals(html, page_url)
        for link in embedded_structured_links:
            structured_links.add(link)
        for signal in embedded_weak_signals:
            weak_links.add(signal)
        for signal in _extract_static_module_signals(html, page_url):
            weak_links.add(signal)
        for signal in _extract_text_job_signals(html, page_url):
            weak_links.add(signal)
        for jobylon_link in _extract_jobylon_embed_urls(html):
            weak_links.add(jobylon_link)
            try:
                jobylon_html = discovery.fetch_text_with_retry(jobylon_link, timeout_s, adapter="static")
                for embedded_job_link in _extract_embedded_job_urls(jobylon_html, jobylon_link):
                    weak_links.add(embedded_job_link)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{jobylon_link}: {exc}")

        parsed_rows = parse_jobpostings_from_html(
            html,
            base_url=page_url,
            fallback_company=company,
            fallback_source_id_prefix=f"static:{source_identity(row)}",
        )
        for parsed in parsed_rows:
            link = normalize_job_url(parsed.get("jobLink"))
            if link:
                structured_links.add(link)

        external_links, external_errors = _extract_external_job_links_from_scripts(html, page_url, timeout_s)
        for err in external_errors:
            errors.append(err)
        for ext_link in external_links:
            weak_links.add(ext_link)

        detail_attempted, detail_used = _collect_detail_page_structured_links(
            html,
            page_url,
            timeout_s,
            company=company,
            source_id=source_identity(row),
            structured_links=structured_links,
            weak_links=weak_links,
            errors=errors,
        )
        browser_fallback_attempted = browser_fallback_attempted or detail_attempted
        browser_fallback_used = browser_fallback_used or detail_used
        page_has_signals = len(structured_links) > before_structured_count or len(weak_links) > before_weak_count
        if not page_has_signals:
            _expand_static_alt_pages(
                page_url=page_url,
                pages_to_visit=pages_to_visit,
                seen_pages=seen_pages,
                max_pages_to_visit=max_pages_to_visit,
            )

    if structured_links:
        return True, len(structured_links), "", False, {
            "browserFallbackAttempted": browser_fallback_attempted,
            "browserFallbackUsed": browser_fallback_used,
        }
    if weak_links:
        return True, len(weak_links), "", True, {
            "browserFallbackAttempted": browser_fallback_attempted,
            "browserFallbackUsed": browser_fallback_used,
        }
    if errors:
        return False, 0, "; ".join(errors[:4]), False, {
            "browserFallbackAttempted": browser_fallback_attempted,
            "browserFallbackUsed": browser_fallback_used,
        }
    return False, 0, "no job postings found", False, {
        "browserFallbackAttempted": browser_fallback_attempted,
        "browserFallbackUsed": browser_fallback_used,
    }


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
        command = [sys.executable, str(Path(RUNTIME_CONFIG.root) / "scripts" / script_name)]
        command.extend(args or [])
    script = Path(script_name).name.lower()
    task_type = "discovery" if "discovery" in script else ("fetch" if "fetcher" in script else script)
    child_env = os.environ.copy()
    child_env["BALUFFO_DATA_DIR"] = str(RUNTIME_CONFIG.data_dir)
    if task_type == "discovery":
        child_env["BALUFFO_DISCOVERY_LOG_PATH"] = str(DISCOVERY_LOG_PATH)
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
        if task_type == "discovery":
            DISCOVERY_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
            log_handle = open(DISCOVERY_LOG_PATH, "a", encoding="utf-8")
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


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_utc().isoformat()


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
    rows = load_json_array(OPS_HISTORY_PATH, [])
    cleaned: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if not row.get("type"):
            continue
        cleaned.append(dict(row))
    cleaned.sort(key=lambda item: str(item.get("finishedAt") or item.get("startedAt") or ""))
    return cleaned[-MAX_HISTORY_ROWS:]


def save_run_history(rows: List[Dict[str, Any]]) -> None:
    save_json_atomic(OPS_HISTORY_PATH, rows[-MAX_HISTORY_ROWS:])


def append_run_history(row: Dict[str, Any]) -> Dict[str, Any]:
    with OPS_STATE_LOCK:
        history = load_run_history()
        entry = dict(row)
        entry.setdefault("id", f"run_{uuid.uuid4().hex[:12]}")
        history.append(entry)
        save_run_history(history)
        return entry


def upsert_run_history(entry: Dict[str, Any], *, dedupe_fields: Tuple[str, ...]) -> Dict[str, Any]:
    with OPS_STATE_LOCK:
        history = load_run_history()
        match_idx = -1
        for idx, row in enumerate(history):
            if all(str(row.get(field) or "") == str(entry.get(field) or "") for field in dedupe_fields):
                match_idx = idx
                break
        if match_idx >= 0:
            merged = {**history[match_idx], **entry}
            merged.setdefault("id", history[match_idx].get("id") or f"run_{uuid.uuid4().hex[:12]}")
            history[match_idx] = merged
            save_run_history(history)
            return merged
        history.append({**entry, "id": str(entry.get("id") or f"run_{uuid.uuid4().hex[:12]}")})
        save_run_history(history)
        return history[-1]


def prune_started_rows_for_type(
    run_type: str,
    *,
    keep_started_at: str = "",
    finished_at: str = "",
) -> None:
    with OPS_STATE_LOCK:
        history = load_run_history()
        keep_started_token = str(keep_started_at or "")
        finished_dt = parse_iso(finished_at) if finished_at else None
        next_rows: List[Dict[str, Any]] = []
        for row in history:
            if str(row.get("type") or "") != run_type:
                next_rows.append(row)
                continue
            if str(row.get("status") or "").lower() != "started":
                next_rows.append(row)
                continue
            row_started = str(row.get("startedAt") or "")
            if keep_started_token and row_started == keep_started_token:
                next_rows.append(row)
                continue
            if finished_dt:
                row_started_dt = parse_iso(row_started)
                # If a run of this type has finished, older/parallel started placeholders are stale.
                if not row_started_dt or row_started_dt <= finished_dt:
                    continue
                next_rows.append(row)
                continue
            if not keep_started_token:
                # Explicit prune-all mode for this type.
                continue
            # While a run is active, keep only the current startedAt marker.
            if keep_started_token and row_started and row_started < keep_started_token:
                continue
            next_rows.append(row)
        save_run_history(next_rows)


def pid_is_running(pid: int) -> bool:
    if int(pid or 0) <= 0:
        return False
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False
    return True


def clear_task_state(task_type: str) -> None:
    with OPS_STATE_LOCK:
        state = load_json_object(TASK_STATE_PATH, {})
        if not isinstance(state, dict):
            return
        if str(task_type) in state:
            state.pop(str(task_type), None)
            save_json_atomic(TASK_STATE_PATH, state)


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
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    queued = int(summary.get("queuedCandidateCount") or summary.get("newCandidateCount") or 0)
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
        discovery_report = load_json_object(DISCOVERY_REPORT_PATH, {})
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
    with SYNC_STATE_LOCK:
        runtime_state = load_sync_runtime_state()
        if action:
            SYNC_STATUS["lastAction"] = str(action)
            runtime_state["lastAction"] = str(action)
        if result:
            SYNC_STATUS["lastResult"] = str(result)
            runtime_state["lastResult"] = str(result)
        if error:
            SYNC_STATUS["lastError"] = str(error)
            runtime_state["lastError"] = str(error)
        elif action:
            SYNC_STATUS["lastError"] = ""
            runtime_state["lastError"] = ""
        stamp = now_iso()
        if pulled:
            SYNC_STATUS["lastPullAt"] = stamp
            runtime_state["lastPullAt"] = stamp
        if pushed:
            SYNC_STATUS["lastPushAt"] = stamp
            runtime_state["lastPushAt"] = stamp
        save_sync_runtime_state(runtime_state)


def get_sync_status_payload() -> Dict[str, Any]:
    cfg = source_sync_module.config_status(refresh_sync_config())
    with SYNC_STATE_LOCK:
        runtime_state = {**dict(SYNC_STATUS), **load_sync_runtime_state()}
    return {
        "ok": True,
        "config": cfg,
        "savedConfig": get_saved_sync_config_payload(),
        "runtime": runtime_state,
    }


def _sync_guard() -> Optional[Dict[str, Any]]:
    cfg = source_sync_module.config_status(refresh_sync_config())
    if not cfg.get("enabled"):
        return {"ok": False, "error": "Sync is disabled", "config": cfg}
    if not cfg.get("ready"):
        return {"ok": False, "error": "Sync is not configured", "config": cfg}
    return None


def sync_pull_sources() -> Dict[str, Any]:
    guard = _sync_guard()
    if guard:
        return guard
    local_state = load_state()
    result = source_sync_module.pull_and_merge_sources(SYNC_CONFIG, local_state)
    merged_state = result.get("mergedState") if isinstance(result.get("mergedState"), dict) else local_state
    if bool(result.get("changed")):
        persist_state(merged_state)
    _set_sync_status(
        action="pull",
        result="ok",
        pulled=True,
        error="",
    )
    summary = summarize_state(load_state())
    return {
        "ok": True,
        "changed": bool(result.get("changed")),
        "remoteFound": bool(result.get("remoteFound")),
        "remoteSha": str(result.get("remoteSha") or ""),
        "remoteGeneratedAt": str(result.get("remoteGeneratedAt") or ""),
        "summary": summary,
    }


def sync_push_sources() -> Dict[str, Any]:
    guard = _sync_guard()
    if guard:
        return guard
    state = load_state()
    result = source_sync_module.push_sources_snapshot(SYNC_CONFIG, state)
    snapshot = result.get("snapshot") if isinstance(result.get("snapshot"), dict) else {}
    _set_sync_status(
        action="push",
        result="ok",
        pushed=True,
        error="",
    )
    return {
        "ok": True,
        "remoteSha": str(result.get("remoteSha") or ""),
        "remotePreviouslyExisted": bool(result.get("remotePreviouslyExisted")),
        "counts": {
            "active": len(snapshot.get("active") or []),
            "pending": len(snapshot.get("pending") or []),
            "rejected": len(snapshot.get("rejected") or []),
        },
    }


def startup_sync_pull() -> None:
    cfg = source_sync_module.config_status(refresh_sync_config())
    if not cfg.get("enabled"):
        return
    if not cfg.get("ready"):
        missing = ",".join(cfg.get("missing") or [])
        bridge_log("warn", "sync_startup_skipped", reason="misconfigured", missing=missing)
        return
    try:
        result = sync_pull_sources()
        bridge_log(
            "info",
            "sync_startup_pull_done",
            changed=bool(result.get("changed")),
            remoteFound=bool(result.get("remoteFound")),
            active=int((result.get("summary") or {}).get("activeCount") or 0),
            pending=int((result.get("summary") or {}).get("pendingCount") or 0),
            rejected=int((result.get("summary") or {}).get("rejectedCount") or 0),
        )
    except Exception as exc:  # noqa: BLE001
        _set_sync_status(action="pull", result="error", error=str(exc), pulled=False)
        bridge_log("warn", "sync_startup_pull_failed", error=str(exc))


def _reconcile_sync_history_locked() -> None:
    history = load_run_history()
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
        if run_id and run_id in ACTIVE_SYNC_RUNS:
            next_rows.append(row)
            continue
        changed = True
    if changed:
        save_run_history(next_rows)


def sync_task_running() -> bool:
    with OPS_STATE_LOCK:
        _reconcile_sync_history_locked()
        history = load_run_history()
        return any(
            str(row.get("type") or "").strip().lower() == "sync"
            and str(row.get("status") or "").strip().lower() == "started"
            and not str(row.get("finishedAt") or "").strip()
            for row in history
        )


def wait_for_sync_tasks(timeout_s: float = 5.0) -> None:
    deadline = datetime.now(timezone.utc).timestamp() + max(0.0, float(timeout_s))
    while True:
        with OPS_STATE_LOCK:
            items = list(ACTIVE_SYNC_THREADS.items())
        pending = False
        for run_id, worker in items:
            remaining = max(0.0, deadline - datetime.now(timezone.utc).timestamp())
            is_alive = getattr(worker, "is_alive", None)
            join = getattr(worker, "join", None)
            alive = bool(is_alive()) if callable(is_alive) else False
            if alive and callable(join) and remaining > 0.0:
                join(timeout=min(0.2, remaining))
                alive = bool(is_alive()) if callable(is_alive) else False
            if alive:
                pending = True
                continue
            with OPS_STATE_LOCK:
                ACTIVE_SYNC_THREADS.pop(run_id, None)
        if not pending or datetime.now(timezone.utc).timestamp() >= deadline:
            return


def _mark_discovery_sync_finished(finished_at: str) -> None:
    with SYNC_STATE_LOCK:
        save_sync_runtime_state({"lastDiscoverySyncFinishedAt": str(finished_at or "")})


def _maybe_trigger_auto_sync_push(reason: str) -> bool:
    guard = _sync_guard()
    if guard:
        return False
    if sync_task_running():
        return False
    result = start_sync_task("push", reason=reason, automatic=True)
    return bool(result.get("started"))


def _watch_discovery_run_for_auto_sync(run_id: str, pid: int, started_at: str) -> None:
    started_dt = parse_iso(started_at) or now_utc()
    while pid_is_running(pid):
        threading.Event().wait(0.8)
    try:
        report = load_json_object(DISCOVERY_REPORT_PATH, {})
        finished_at = str(report.get("finishedAt") or "")
        finished_dt = parse_iso(finished_at)
        if not finished_dt or finished_dt < started_dt:
            return
        summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
        queued = int(summary.get("queuedCandidateCount") or summary.get("newCandidateCount") or 0)
        if queued <= 0:
            _mark_discovery_sync_finished(finished_at)
            return
        runtime_state = load_sync_runtime_state()
        if str(runtime_state.get("lastDiscoverySyncFinishedAt") or "") == finished_at:
            return
        if _maybe_trigger_auto_sync_push("discovery_completed"):
            _mark_discovery_sync_finished(finished_at)
            bridge_log("info", "sync_auto_push_started", runId=run_id, reason="discovery_completed", queued=queued)
    except Exception as exc:  # noqa: BLE001
        bridge_log("warn", "sync_auto_push_skipped", runId=run_id, reason="discovery_completed", error=str(exc))


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
        with OPS_STATE_LOCK:
            ACTIVE_SYNC_RUNS.discard(str(run_id or ""))
            ACTIVE_SYNC_THREADS.pop(str(run_id or ""), None)
    finished_dt = now_utc()
    duration_ms = int(max(0.0, (finished_dt - started_dt).total_seconds() * 1000))
    prune_started_rows_for_type("sync", finished_at=finished_dt.isoformat())
    upsert_run_history({
        "id": run_id,
        "type": "sync",
        "status": status,
        "startedAt": started_at,
        "finishedAt": finished_dt.isoformat(),
        "durationMs": duration_ms,
        "summary": summary,
    }, dedupe_fields=("type", "finishedAt"))
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
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"pull", "push"}:
        raise ValueError("Invalid sync action")
    if sync_task_running():
        return {"started": False, "task": "source_sync", "action": normalized_action, "error": "Sync task already running"}
    run_id = f"sync_{uuid.uuid4().hex[:10]}"
    started_at = now_iso()
    append_run_history({
        "id": run_id,
        "type": "sync",
        "status": "started",
        "startedAt": started_at,
        "finishedAt": "",
        "durationMs": 0,
        "summary": {"action": normalized_action, "reason": str(reason or ""), "automatic": bool(automatic)},
    })
    with OPS_STATE_LOCK:
        ACTIVE_SYNC_RUNS.add(run_id)
    worker = threading.Thread(
        target=_run_sync_task_worker,
        args=(run_id, normalized_action, started_at),
        kwargs={"reason": str(reason or ""), "automatic": bool(automatic)},
        name=f"sync-task-{normalized_action}-{run_id}",
        daemon=True,
    )
    with OPS_STATE_LOCK:
        ACTIVE_SYNC_THREADS[run_id] = worker
    worker.start()
    bridge_log("info", "sync_task_started", runId=run_id, action=normalized_action, reason=reason, automatic=automatic)
    return {"started": True, "runId": run_id, "task": "source_sync", "action": normalized_action, "automatic": bool(automatic), "reason": str(reason or "")}


def trigger_discovery_task(*, route_name: str, enable_auto_sync_watch: bool = True) -> Tuple[int, Dict[str, Any]]:
    run_id = f"discovery_{uuid.uuid4().hex[:10]}"
    started_at = now_iso()
    save_json_atomic(
        DISCOVERY_REPORT_PATH,
        {
            "schemaVersion": SCHEMA_VERSION,
            "mode": "dynamic",
            "startedAt": started_at,
            "finishedAt": "",
            "summary": {
                "foundEndpointCount": 0,
                "probedCandidateCount": 0,
                "queuedCandidateCount": 0,
                "failedProbeCount": 0,
                "skippedDuplicateCount": 0,
                "skippedLowEvidenceProbeCount": 0,
            },
            "candidates": [],
            "failures": [],
            "topFailures": [],
            "outputs": {
                "report": str(DISCOVERY_REPORT_PATH),
                "candidates": str(DISCOVERY_CANDIDATES_PATH),
                "pending": str(PENDING_PATH),
            },
        },
    )
    DISCOVERY_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    DISCOVERY_LOG_PATH.write_text(f"[{started_at}] Launching source discovery task...\n", encoding="utf-8")
    append_run_history({
        "id": run_id,
        "type": "discovery",
        "status": "started",
        "startedAt": started_at,
        "finishedAt": "",
        "durationMs": 0,
        "summary": {},
    })
    try:
        pid = run_background_script("source_discovery.py", ["--mode", "dynamic"])
    except Exception as exc:  # noqa: BLE001
        save_json_atomic(
            DISCOVERY_REPORT_PATH,
            {
                "schemaVersion": SCHEMA_VERSION,
                "mode": "dynamic",
                "startedAt": started_at,
                "finishedAt": now_iso(),
                "summary": {
                    "foundEndpointCount": 0,
                    "probedCandidateCount": 0,
                    "queuedCandidateCount": 0,
                    "failedProbeCount": 1,
                },
                "candidates": [],
                "failures": [{"name": "source_discovery.py", "adapter": "bridge", "error": str(exc), "stage": "launch"}],
                "topFailures": [{"key": "bridge:launch", "count": 1}],
                "outputs": {
                    "report": str(DISCOVERY_REPORT_PATH),
                    "candidates": str(DISCOVERY_CANDIDATES_PATH),
                    "pending": str(PENDING_PATH),
                },
            },
        )
        try:
            with DISCOVERY_LOG_PATH.open("a", encoding="utf-8") as handle:
                handle.write(f"[{now_iso()}] Launch failed: {str(exc)}\n")
        except OSError:
            pass
        bridge_log("error", "task_start_failed", runId=run_id, task="source_discovery", mode="dynamic", route=route_name, error=str(exc))
        return 500, {"started": False, "task": "source_discovery", "mode": "dynamic", "route": route_name, "error": str(exc)}
    if enable_auto_sync_watch:
        watcher = threading.Thread(
            target=_watch_discovery_run_for_auto_sync,
            args=(run_id, pid, started_at),
            name=f"discovery-sync-watch-{run_id}",
            daemon=True,
        )
        watcher.start()
    bridge_log("info", "task_started", runId=run_id, task="source_discovery", mode="dynamic", route=route_name, pid=pid)
    return 200, {
        "started": True,
        "runId": run_id,
        "task": "source_discovery",
        "mode": "dynamic",
        "route": route_name,
        "startedAt": started_at,
        "pid": int(pid),
    }


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
    with PIPELINE_STATE_LOCK:
        payload = dict(PIPELINE_STATUS)
        progress = payload.get("progress")
        payload["progress"] = dict(progress) if isinstance(progress, dict) else _pipeline_progress(0, 3, "Idle")
        payload["active"] = bool(payload.get("active"))
        return payload


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


def _run_jobs_pipeline_worker(run_id: str) -> None:
    try:
        _pipeline_mark_stage(stage="discovery", current_step=1, total_steps=3, label="Running discovery...")
        discovery_status, discovery_result = trigger_discovery_task(
            route_name="/tasks/run-jobs-pipeline",
            enable_auto_sync_watch=False,
        )
        if int(discovery_status) >= 300 or not bool(discovery_result.get("started")):
            raise RuntimeError(str(discovery_result.get("error") or "discovery start failed"))
        discovery_started_at = str(discovery_result.get("startedAt") or now_iso())
        _wait_for_report_completion(
            report_path=DISCOVERY_REPORT_PATH,
            started_at=discovery_started_at,
            timeout_s=900.0,
            report_name="discovery report",
        )

        _pipeline_mark_stage(stage="fetch", current_step=2, total_steps=3, label="Running fetch...")
        fetch_result = start_fetcher_task({"preset": "default"})
        fetch_started_at = str(fetch_result.get("startedAt") or now_iso())
        _wait_for_report_completion(
            report_path=JOBS_FETCH_REPORT_PATH,
            started_at=fetch_started_at,
            timeout_s=1200.0,
            report_name="fetch report",
        )

        _pipeline_mark_stage(stage="sync_push", current_step=3, total_steps=3, label="Running sync push...")
        sync_result = start_sync_task("push", reason="jobs_pipeline", automatic=False)
        if not bool(sync_result.get("started")):
            raise RuntimeError(str(sync_result.get("error") or "sync push failed to start"))
        sync_row = _wait_for_sync_completion(str(sync_result.get("runId") or ""), timeout_s=900.0)
        sync_status = str(sync_row.get("status") or "").strip().lower()
        if sync_status == "error":
            sync_error = str((sync_row.get("summary") or {}).get("error") or "sync push failed")
            raise RuntimeError(sync_error)

        final_output_count = _current_fetch_output_count()
        _pipeline_set_completed(status="ok", final_output_count=final_output_count)
    except Exception as exc:  # noqa: BLE001
        bridge_log("error", "jobs_pipeline_failed", runId=run_id, error=str(exc))
        _pipeline_set_completed(status="error", final_output_count=_current_fetch_output_count(), error=str(exc))


def start_jobs_pipeline_task(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    global ACTIVE_PIPELINE_RUN_ID, ACTIVE_PIPELINE_THREAD
    with PIPELINE_STATE_LOCK:
        if bool(PIPELINE_STATUS.get("active")) and str(PIPELINE_STATUS.get("runId") or ""):
            return {
                "started": False,
                "error": "Jobs pipeline already running",
                "runId": str(PIPELINE_STATUS.get("runId") or ""),
                "stage": str(PIPELINE_STATUS.get("stage") or "running"),
            }
        if task_running_from_state("fetch") or task_running_from_state("discovery") or sync_task_running():
            return {
                "started": False,
                "error": "Another fetch/discovery/sync task is already running",
                "runId": "",
                "stage": "blocked",
            }

        run_id = f"pipeline_{uuid.uuid4().hex[:10]}"
        started_at = now_iso()
        jobs_page_loaded_count = int((payload or {}).get("jobsPageLoadedCount") or 0)
        baseline_output_count = _current_fetch_output_count()
        PIPELINE_STATUS.update({
            "active": True,
            "runId": run_id,
            "stage": "starting",
            "progress": _pipeline_progress(0, 3, "Starting pipeline..."),
            "startedAt": started_at,
            "finishedAt": "",
            "error": "",
            "updatesFound": False,
            "refreshRecommended": False,
            "baselineOutputCount": int(baseline_output_count),
            "finalOutputCount": 0,
            "jobsPageLoadedCount": int(max(0, jobs_page_loaded_count)),
        })
        append_run_history({
            "id": run_id,
            "type": "pipeline",
            "status": "started",
            "startedAt": started_at,
            "finishedAt": "",
            "durationMs": 0,
            "summary": {
                "baselineOutputCount": int(baseline_output_count),
                "jobsPageLoadedCount": int(max(0, jobs_page_loaded_count)),
                "stage": "starting",
            },
        })
        worker = threading.Thread(
            target=_run_jobs_pipeline_worker,
            args=(run_id,),
            name=f"jobs-pipeline-{run_id}",
            daemon=True,
        )
        ACTIVE_PIPELINE_RUN_ID = run_id
        ACTIVE_PIPELINE_THREAD = worker
        worker.start()
        bridge_log("info", "jobs_pipeline_started", runId=run_id, baseline=baseline_output_count, jobsPageLoadedCount=jobs_page_loaded_count)
        return {
            "started": True,
            "runId": run_id,
            "stage": "starting",
            "progress": dict(PIPELINE_STATUS.get("progress") or {}),
        }


def desktop_local_data_store() -> LocalDataStore:
    if not RUNTIME_CONFIG.desktop_mode or DESKTOP_LOCAL_DATA_STORE is None:
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
        if path == "/desktop-local-data/session":
            try:
                self._send_json({"ok": True, "user": desktop_local_data_store().get_current_user(), "lastActivityAt": str(DESKTOP_SESSION_ACTIVITY_AT or "")})
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if path == "/desktop-local-data/saved-jobs":
            try:
                uid = (query.get("uid") or [""])[0]
                self._send_json({"ok": True, "rows": desktop_local_data_store().list_saved_jobs(uid)})
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if path == "/desktop-local-data/saved-job-keys":
            try:
                uid = (query.get("uid") or [""])[0]
                self._send_json({"ok": True, "keys": desktop_local_data_store().get_saved_job_keys(uid)})
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if path == "/desktop-local-data/attachments":
            try:
                uid = (query.get("uid") or [""])[0]
                job_key = (query.get("jobKey") or [""])[0]
                self._send_json({"ok": True, "rows": desktop_local_data_store().list_attachments_for_job(uid, job_key)})
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if path == "/desktop-local-data/attachments/content":
            try:
                uid = (query.get("uid") or [""])[0]
                job_key = (query.get("jobKey") or [""])[0]
                attachment_id = (query.get("attachmentId") or [""])[0]
                download_flag = str((query.get("download") or [""])[0]).strip().lower()
                body, content_type, filename = desktop_local_data_store().get_attachment_blob(uid, job_key, attachment_id)
                self._send_bytes(
                    body,
                    content_type=content_type,
                    filename=filename,
                    disposition="attachment" if download_flag in {"1", "true", "yes"} else "inline",
                )
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if path == "/desktop-local-data/backup/export-file":
            try:
                uid = (query.get("uid") or [""])[0]
                include_files_raw = str((query.get("includeFiles") or ["0"])[0]).strip().lower()
                include_files = include_files_raw in {"1", "true", "yes", "on"}
                payload = desktop_local_data_store().export_profile_data(uid, include_files=include_files)
                date_token = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                safe_uid = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(uid or "profile")).strip("_") or "profile"
                if include_files:
                    backup_json = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
                    buffer = io.BytesIO()
                    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_STORED) as zf:
                        zf.writestr("backup.json", backup_json)
                    body = buffer.getvalue()
                    filename = f"baluffo-backup-{safe_uid}-{date_token}.zip"
                    self._send_bytes(body, content_type="application/zip", filename=filename, disposition="attachment")
                else:
                    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
                    filename = f"baluffo-backup-{safe_uid}-{date_token}.json"
                    self._send_bytes(body, content_type="application/json; charset=utf-8", filename=filename, disposition="attachment")
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if path == "/desktop-local-data/activity":
            try:
                uid = (query.get("uid") or [""])[0]
                limit = int((query.get("limit") or ["300"])[0])
                self._send_json({"ok": True, "rows": desktop_local_data_store().list_activity_for_user(uid, limit)})
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if path == "/desktop-local-data/startup-metrics":
            try:
                limit_raw = (query.get("limit") or ["200"])[0]
                try:
                    limit = int(limit_raw)
                except ValueError:
                    limit = 200
                self._send_json({"ok": True, "rows": read_startup_metrics(limit)})
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if path == "/registry/active":
            state = load_state()
            self._send_json({"sources": state["active"], "summary": summarize_state(state)})
            return
        if path == "/registry/pending":
            state = load_state()
            self._send_json({"sources": state["pending"], "summary": summarize_state(state)})
            return
        if path == "/registry/rejected":
            state = load_state()
            self._send_json({"sources": state["rejected"], "summary": summarize_state(state)})
            return
        if path == "/discovery/report":
            report = load_json_object(DISCOVERY_REPORT_PATH, {})
            self._send_json(report or {"summary": {}, "candidates": [], "failures": []})
            return
        if path == "/discovery/log":
            offset_raw = (query.get("offset") or ["0"])[0]
            try:
                offset = max(0, int(offset_raw))
            except ValueError:
                offset = 0
            try:
                text = DISCOVERY_LOG_PATH.read_text(encoding="utf-8")
            except OSError:
                text = ""
            next_offset = min(len(text), offset)
            chunk = text[offset:]
            next_offset = len(text)
            self._send_json({"text": chunk, "offset": offset, "nextOffset": next_offset, "hasMore": False})
            return
        if path == "/registry/summary":
            state = load_state()
            self._send_json({"summary": summarize_state(state)})
            return
        if path == "/ops/health":
            self._send_json(compute_ops_health())
            return
        if path == "/ops/history":
            limit_raw = (query.get("limit") or ["30"])[0]
            try:
                limit = max(1, min(200, int(limit_raw)))
            except ValueError:
                limit = 30
            rows = sync_history_from_reports()
            self._send_json({"runs": rows[-limit:], "count": len(rows)})
            return
        if path == "/ops/fetcher-metrics":
            window_raw = (query.get("windowRuns") or ["20"])[0]
            try:
                window_runs = max(1, min(200, int(window_raw)))
            except ValueError:
                window_runs = 20
            self._send_json(compute_fetcher_metrics(window_runs=window_runs))
            return
        if path == "/ops/fetch-report":
            self._send_json(normalize_fetch_report_contract(load_json_object(JOBS_FETCH_REPORT_PATH, {})))
            return
        if path == "/sync/status":
            self._send_json(get_sync_status_payload())
            return
        if path == "/tasks/run-jobs-pipeline-status":
            self._send_json(get_jobs_pipeline_status_payload())
            return
        self._send_json({"error": "Not found"}, status=404)

    def do_POST(self) -> None:  # noqa: N802
        path = self._route_path()
        payload = read_json_from_request(self)
        mark_desktop_session_activity(path)
        if path == "/desktop-local-data/sign-in":
            try:
                user = desktop_local_data_store().sign_in(str(payload.get("name") or ""))
                self._send_json({"ok": True, "user": user})
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if path == "/desktop-local-data/sign-out":
            try:
                desktop_local_data_store().sign_out()
                self._send_json({"ok": True})
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if path == "/desktop-local-data/saved-jobs/save":
            try:
                job_key = desktop_local_data_store().save_job_for_user(
                    str(payload.get("uid") or ""),
                    payload.get("job") if isinstance(payload.get("job"), dict) else {},
                    payload.get("options") if isinstance(payload.get("options"), dict) else {},
                )
                self._send_json({"ok": True, "jobKey": job_key})
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if path == "/desktop-local-data/saved-jobs/remove":
            try:
                desktop_local_data_store().remove_saved_job_for_user(str(payload.get("uid") or ""), str(payload.get("jobKey") or ""))
                self._send_json({"ok": True})
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if path == "/desktop-local-data/saved-jobs/status":
            try:
                desktop_local_data_store().update_application_status(
                    str(payload.get("uid") or ""),
                    str(payload.get("jobKey") or ""),
                    str(payload.get("status") or ""),
                    payload.get("options") if isinstance(payload.get("options"), dict) else {},
                )
                self._send_json({"ok": True})
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if path == "/desktop-local-data/saved-jobs/notes":
            try:
                desktop_local_data_store().update_job_notes(str(payload.get("uid") or ""), str(payload.get("jobKey") or ""), str(payload.get("notes") or ""))
                self._send_json({"ok": True})
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if path == "/desktop-local-data/attachments/add":
            try:
                attachment_id = desktop_local_data_store().add_attachment_for_job(
                    str(payload.get("uid") or ""),
                    str(payload.get("jobKey") or ""),
                    payload.get("fileMeta") if isinstance(payload.get("fileMeta"), dict) else {},
                    str(payload.get("blobDataUrl") or ""),
                )
                self._send_json({"ok": True, "attachmentId": attachment_id})
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if path == "/desktop-local-data/attachments/delete":
            try:
                desktop_local_data_store().delete_attachment_for_job(str(payload.get("uid") or ""), str(payload.get("jobKey") or ""), str(payload.get("attachmentId") or ""))
                self._send_json({"ok": True})
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if path == "/desktop-local-data/backup/export":
            try:
                result = desktop_local_data_store().export_profile_data(str(payload.get("uid") or ""), bool((payload.get("options") or {}).get("includeFiles")))
                self._send_json({"ok": True, "payload": result})
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if path == "/desktop-local-data/backup/import":
            try:
                result = desktop_local_data_store().import_profile_data(str(payload.get("uid") or ""), payload.get("payload") if isinstance(payload.get("payload"), dict) else {})
                self._send_json({"ok": True, "result": result})
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if path == "/desktop-local-data/admin/overview":
            try:
                self._send_json({"ok": True, "overview": desktop_local_data_store().get_admin_overview(str(payload.get("pin") or ""))})
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if path == "/desktop-local-data/admin/wipe":
            try:
                desktop_local_data_store().wipe_account_admin(str(payload.get("pin") or ""), str(payload.get("uid") or ""))
                self._send_json({"ok": True, "user": desktop_local_data_store().get_current_user()})
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if path == "/desktop-local-data/startup-metric":
            try:
                event = str(payload.get("event") or "").strip() or "unknown"
                details = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
                append_startup_metric(event, details)
                self._send_json({"ok": True})
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        state = load_state()

        if path == "/sources/manual":
            result = add_manual_source(str(payload.get("url") or ""))
            self._send_json(result)
            return

        if path == "/discovery/check-source":
            result = trigger_source_check(str(payload.get("sourceId") or ""))
            status = 200 if bool(result.get("started")) else 400
            self._send_json(result, status=status)
            return

        if path == "/registry/approve":
            ids = payload.get("ids") if isinstance(payload.get("ids"), list) else []
            moved, remaining = move_entries(state["pending"], [str(item) for item in ids])
            for row in moved:
                row["enabledByDefault"] = True
            state["pending"] = remaining
            state["active"] = unique_sources([*state["active"], *moved])
            state = persist_state_and_auto_sync(state, reason="registry_approve")
            approval = load_json_object(APPROVAL_STATE_PATH, {"approvedSinceLastRun": 0})
            approval["approvedSinceLastRun"] = int(approval.get("approvedSinceLastRun") or 0) + len(moved)
            save_json_atomic(APPROVAL_STATE_PATH, approval)
            self._send_json({"approved": len(moved), "summary": summarize_state(state)})
            return

        if path == "/registry/reject":
            ids = payload.get("ids") if isinstance(payload.get("ids"), list) else []
            moved, remaining = move_entries(state["pending"], [str(item) for item in ids])
            state["pending"] = remaining
            state["rejected"] = unique_sources([*state["rejected"], *moved])
            state = persist_state_and_auto_sync(state, reason="registry_reject")
            self._send_json({"rejected": len(moved), "summary": summarize_state(state)})
            return

        if path == "/registry/rollback":
            ids = payload.get("ids") if isinstance(payload.get("ids"), list) else []
            selected = set(str(item) for item in ids)
            moved: List[Dict[str, Any]] = []
            active_remaining: List[Dict[str, Any]] = []
            for row in state["active"]:
                if source_identity(row) in selected:
                    moved.append(row)
                else:
                    active_remaining.append(row)
            state["active"] = active_remaining
            state["pending"] = unique_sources([*state["pending"], *moved])
            state = persist_state_and_auto_sync(state, reason="registry_rollback")
            self._send_json({"rolledBack": len(moved), "summary": summarize_state(state)})
            return

        if path == "/registry/restore-rejected":
            ids = payload.get("ids") if isinstance(payload.get("ids"), list) else []
            moved, remaining = move_entries(state["rejected"], [str(item) for item in ids])
            state["rejected"] = remaining
            for row in moved:
                row["enabledByDefault"] = False
            state["pending"] = unique_sources([*state["pending"], *moved])
            state = persist_state_and_auto_sync(state, reason="registry_restore_rejected")
            self._send_json({"restored": len(moved), "summary": summarize_state(state)})
            return

        if path == "/registry/delete":
            ids = payload.get("ids") if isinstance(payload.get("ids"), list) else []
            urls = payload.get("urls") if isinstance(payload.get("urls"), list) else []
            selected = {str(item).strip().lower() for item in ids if str(item).strip()}
            selected_urls = {
                normalize_source_url(str(item))
                for item in urls
                if normalize_source_url(str(item))
            }
            if not selected:
                selected = set()
            if not selected and not selected_urls:
                self._send_json({"deleted": 0, "summary": summarize_state(state)})
                return
            before = (
                len(state.get("active", []))
                + len(state.get("pending", []))
                + len(state.get("rejected", []))
            )
            def keep_row(row: Dict[str, Any]) -> bool:
                row_id = source_identity(row)
                row_url = source_url_fingerprint(row)
                if row_id in selected:
                    return False
                if row_url and row_url in selected_urls:
                    return False
                return True

            state["active"] = [row for row in state["active"] if keep_row(row)]
            state["pending"] = [row for row in state["pending"] if keep_row(row)]
            state["rejected"] = [row for row in state["rejected"] if keep_row(row)]
            state = persist_state_and_auto_sync(state, reason="registry_delete")
            after = (
                len(state.get("active", []))
                + len(state.get("pending", []))
                + len(state.get("rejected", []))
            )
            self._send_json({"deleted": max(0, before - after), "summary": summarize_state(state)})
            return

        if path == "/tasks/run-discovery":
            status_code, result = trigger_discovery_task(route_name=path)
            self._send_json(result, status=status_code)
            return

        if path == "/tasks/run-jobs-pipeline":
            result = start_jobs_pipeline_task(payload if isinstance(payload, dict) else {})
            status_code = 200 if bool(result.get("started")) else 409
            self._send_json(result, status=status_code)
            return

        if path == "/tasks/run-sync-pull":
            try:
                result = start_sync_task("pull", reason="manual_pull", automatic=False)
                status_code = 200 if bool(result.get("started")) else 409
                self._send_json(result, status=status_code)
            except Exception as exc:  # noqa: BLE001
                self._send_json({"started": False, "task": "source_sync", "action": "pull", "error": str(exc)}, status=500)
            return

        if path == "/tasks/run-sync-push":
            try:
                result = start_sync_task("push", reason="manual_push", automatic=False)
                status_code = 200 if bool(result.get("started")) else 409
                self._send_json(result, status=status_code)
            except Exception as exc:  # noqa: BLE001
                self._send_json({"started": False, "task": "source_sync", "action": "push", "error": str(exc)}, status=500)
            return

        if path == "/tasks/run-fetcher":
            try:
                result = start_fetcher_task(payload if isinstance(payload, dict) else {})
                self._send_json(result)
            except Exception as exc:  # noqa: BLE001
                self._send_json({"started": False, "task": "jobs_fetcher", "error": str(exc)}, status=500)
            return

        if path == "/ops/alerts/ack":
            alert_id = str(payload.get("id") or "").strip()
            if not alert_id:
                self._send_json({"error": "Missing alert id"}, status=400)
                return
            state_alert = load_alert_state()
            acked = dict(state_alert.get("acked") or {})
            acked[alert_id] = now_iso()
            save_alert_state({"acked": acked})
            self._send_json({"acked": alert_id, "ok": True})
            return

        if path == "/sync/config":
            try:
                update_saved_sync_settings(payload if isinstance(payload, dict) else {})
                self._send_json(get_sync_status_payload())
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": str(exc), "config": source_sync_module.config_status(refresh_sync_config())}, status=400)
            return

        if path == "/sync/test":
            try:
                self._send_json(test_sync_config())
            except Exception as exc:  # noqa: BLE001
                _set_sync_status(action="test", result="error", error=str(exc), pulled=False, pushed=False)
                self._send_json({"ok": False, "error": str(exc), "config": source_sync_module.config_status(refresh_sync_config())}, status=500)
            return

        if path == "/sync/pull":
            try:
                self._send_json(sync_pull_sources())
            except Exception as exc:  # noqa: BLE001
                _set_sync_status(action="pull", result="error", error=str(exc), pulled=False)
                self._send_json({"ok": False, "error": str(exc), "config": source_sync_module.config_status(refresh_sync_config())}, status=500)
            return

        if path == "/sync/push":
            try:
                self._send_json(sync_push_sources())
            except Exception as exc:  # noqa: BLE001
                _set_sync_status(action="push", result="error", error=str(exc), pushed=False)
                self._send_json({"ok": False, "error": str(exc), "config": source_sync_module.config_status(refresh_sync_config())}, status=500)
            return

        self._send_json({"error": "Not found"}, status=404)


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
