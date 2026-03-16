#!/usr/bin/env python3
"""Shared Baluffo config loader."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from src.shared.utils import coerce_bool as _coerce_bool, coerce_int as _coerce_int, coerce_str as _coerce_str

ROOT = Path(__file__).resolve().parents[1]
BASE_CONFIG_PATH = ROOT / "baluffo.config.json"
LOCAL_CONFIG_PATH = ROOT / "baluffo.config.local.json"

CODE_FALLBACK_CONFIG: Dict[str, Any] = {
    "bridge": {
        "host": "127.0.0.1",
        "port": 8877,
        "log_format": "human",
        "log_level": "info",
        "quiet_requests": False,
        "max_history_rows": 240,
    },
    "storage": {
        "data_dir": "data",
        "source_discovery_config_path": "data/source-discovery-config.json",
        "source_discovery_log_path": "data/source-discovery.log",
        "social_sources_config_path": "data/social-sources-config.json",
    },
    "security": {
        "admin_pin_default": "1234",
        "github_app_enabled_default": True,
    },
    "sync": {
        "packaged_config_path": "packaging/github-app-sync-config.json",
        "local_enabled_default": True,
        "default_repo": "",
        "default_branch": "main",
        "default_path": "baluffo/source-sync.json",
        "default_allowed_repo": "",
        "default_allowed_branch": "main",
        "default_allowed_path_prefix": "baluffo/source-sync.json",
        "build_key_derivation_default": "embedded",
        "build_passphrase_env": "BALUFFO_SYNC_KEY_PASSPHRASE",
        "build_embedded_key_version": "v1",
    },
    "desktop": {
        "site_port": 8080,
        "bridge_port": 8877,
        "bridge_host": "127.0.0.1",
        "open_path": "jobs.html",
        "title": "Baluffo",
    },
}


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config() -> Dict[str, Any]:
    config = _merge_dicts(CODE_FALLBACK_CONFIG, _read_json(BASE_CONFIG_PATH))
    if LOCAL_CONFIG_PATH.exists():
        config = _merge_dicts(config, _read_json(LOCAL_CONFIG_PATH))
    return config


def resolve_path(value: Any, default: str) -> Path:
    raw = _coerce_str(value, default)
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    return path.resolve()


def get_bridge_defaults() -> Dict[str, Any]:
    cfg = dict(load_config().get("bridge") or {})
    return {
        "host": _coerce_str(cfg.get("host"), CODE_FALLBACK_CONFIG["bridge"]["host"]),
        "port": _coerce_int(cfg.get("port"), CODE_FALLBACK_CONFIG["bridge"]["port"]),
        "log_format": _coerce_str(cfg.get("log_format"), CODE_FALLBACK_CONFIG["bridge"]["log_format"]).lower(),
        "log_level": _coerce_str(cfg.get("log_level"), CODE_FALLBACK_CONFIG["bridge"]["log_level"]).lower(),
        "quiet_requests": _coerce_bool(cfg.get("quiet_requests"), CODE_FALLBACK_CONFIG["bridge"]["quiet_requests"]),
        "max_history_rows": _coerce_int(
            cfg.get("max_history_rows"), CODE_FALLBACK_CONFIG["bridge"]["max_history_rows"], minimum=0, maximum=1000
        ),
    }


def get_storage_defaults() -> Dict[str, Any]:
    cfg = dict(load_config().get("storage") or {})
    return {
        "data_dir": resolve_path(cfg.get("data_dir"), CODE_FALLBACK_CONFIG["storage"]["data_dir"]),
        "source_discovery_config_path": resolve_path(
            cfg.get("source_discovery_config_path"),
            CODE_FALLBACK_CONFIG["storage"]["source_discovery_config_path"],
        ),
        "source_discovery_log_path": resolve_path(
            cfg.get("source_discovery_log_path"),
            CODE_FALLBACK_CONFIG["storage"]["source_discovery_log_path"],
        ),
        "social_sources_config_path": resolve_path(
            cfg.get("social_sources_config_path"),
            CODE_FALLBACK_CONFIG["storage"]["social_sources_config_path"],
        ),
    }


def get_security_defaults() -> Dict[str, Any]:
    cfg = dict(load_config().get("security") or {})
    return {
        "admin_pin_default": _coerce_str(cfg.get("admin_pin_default"), CODE_FALLBACK_CONFIG["security"]["admin_pin_default"]),
        "github_app_enabled_default": _coerce_bool(
            cfg.get("github_app_enabled_default"),
            CODE_FALLBACK_CONFIG["security"]["github_app_enabled_default"],
        ),
    }


def get_sync_defaults() -> Dict[str, Any]:
    cfg = dict(load_config().get("sync") or {})
    return {
        "packaged_config_path": resolve_path(
            cfg.get("packaged_config_path"),
            CODE_FALLBACK_CONFIG["sync"]["packaged_config_path"],
        ),
        "local_enabled_default": _coerce_bool(
            cfg.get("local_enabled_default"),
            CODE_FALLBACK_CONFIG["sync"]["local_enabled_default"],
        ),
        "default_repo": str(cfg.get("default_repo") or "").strip(),
        "default_branch": _coerce_str(cfg.get("default_branch"), CODE_FALLBACK_CONFIG["sync"]["default_branch"]),
        "default_path": _coerce_str(cfg.get("default_path"), CODE_FALLBACK_CONFIG["sync"]["default_path"]),
        "default_allowed_repo": str(cfg.get("default_allowed_repo") or "").strip(),
        "default_allowed_branch": _coerce_str(
            cfg.get("default_allowed_branch"),
            CODE_FALLBACK_CONFIG["sync"]["default_allowed_branch"],
        ),
        "default_allowed_path_prefix": _coerce_str(
            cfg.get("default_allowed_path_prefix"),
            CODE_FALLBACK_CONFIG["sync"]["default_allowed_path_prefix"],
        ),
        "build_key_derivation_default": _coerce_str(
            cfg.get("build_key_derivation_default"),
            CODE_FALLBACK_CONFIG["sync"]["build_key_derivation_default"],
        ).lower(),
        "build_passphrase_env": _coerce_str(
            cfg.get("build_passphrase_env"),
            CODE_FALLBACK_CONFIG["sync"]["build_passphrase_env"],
        ),
        "build_embedded_key_version": _coerce_str(
            cfg.get("build_embedded_key_version"),
            CODE_FALLBACK_CONFIG["sync"]["build_embedded_key_version"],
        ),
    }


def get_desktop_defaults() -> Dict[str, Any]:
    cfg = dict(load_config().get("desktop") or {})
    return {
        "site_port": _coerce_int(cfg.get("site_port"), CODE_FALLBACK_CONFIG["desktop"]["site_port"]),
        "bridge_port": _coerce_int(cfg.get("bridge_port"), CODE_FALLBACK_CONFIG["desktop"]["bridge_port"]),
        "bridge_host": _coerce_str(cfg.get("bridge_host"), CODE_FALLBACK_CONFIG["desktop"]["bridge_host"]),
        "open_path": _coerce_str(cfg.get("open_path"), CODE_FALLBACK_CONFIG["desktop"]["open_path"]).lstrip("/"),
        "title": _coerce_str(cfg.get("title"), CODE_FALLBACK_CONFIG["desktop"]["title"]),
    }


# ---------------------------------------------------------------------------
# Structured config objects (salvaged from refactor, additive-only)
# ---------------------------------------------------------------------------

@dataclass
class BridgeConfig:
    host: str
    port: int
    log_format: str
    log_level: str
    quiet_requests: bool
    max_history_rows: int


@dataclass
class StorageConfig:
    data_dir: Path
    source_discovery_config_path: Path
    source_discovery_log_path: Path
    social_sources_config_path: Path
    ops_history_path: Path
    ops_alert_state_path: Path
    jobs_fetch_report_path: Path
    discovery_report_path: Path
    active_path: Path
    pending_path: Path
    rejected_path: Path
    tasks_config_path: Path
    task_state_path: Path
    sync_config_path: Path
    sync_runtime_path: Path
    run_history_path: Path


@dataclass
class AdminBridgeConfig:
    root: Path
    bridge: BridgeConfig
    storage: StorageConfig
    desktop_mode: bool


def resolve_admin_bridge_config(
    args: argparse.Namespace,
    *,
    env: Optional[Dict[str, str]] = None,
) -> AdminBridgeConfig:
    """Resolve admin bridge config from CLI args, env vars, and config files.

    Layers (highest to lowest priority): CLI args > env vars > config files > code fallback.
    The existing ``get_bridge_defaults()`` / ``get_storage_defaults()`` functions are
    used as the source-of-truth for resolved scalar values so this function stays in sync
    automatically whenever those functions evolve.
    """
    env_map = env if isinstance(env, dict) else os.environ
    bridge_defaults = get_bridge_defaults()
    storage_defaults = get_storage_defaults()

    host = _coerce_str(
        getattr(args, "host", None) or env_map.get("BALUFFO_BRIDGE_HOST") or bridge_defaults["host"],
        bridge_defaults["host"],
    )
    port = _coerce_int(
        getattr(args, "port", None) or env_map.get("BALUFFO_BRIDGE_PORT"),
        bridge_defaults["port"],
    )
    log_format = _coerce_str(
        getattr(args, "log_format", None) or env_map.get("BALUFFO_BRIDGE_LOG_FORMAT") or bridge_defaults["log_format"],
        bridge_defaults["log_format"],
    ).lower()
    log_level = _coerce_str(
        getattr(args, "log_level", None) or env_map.get("BALUFFO_BRIDGE_LOG_LEVEL") or bridge_defaults["log_level"],
        bridge_defaults["log_level"],
    ).lower()
    quiet_requests_env = str(env_map.get("BALUFFO_BRIDGE_QUIET_REQUESTS") or "").strip().lower()
    quiet_requests = _coerce_bool(
        getattr(args, "quiet_requests", None)
        if getattr(args, "quiet_requests", None) is not None
        else quiet_requests_env
        if quiet_requests_env
        else bridge_defaults["quiet_requests"],
        bridge_defaults["quiet_requests"],
    )
    max_history_rows = _coerce_int(
        getattr(args, "max_history_rows", None) or env_map.get("BALUFFO_BRIDGE_MAX_HISTORY_ROWS"),
        bridge_defaults["max_history_rows"],
        minimum=0,
        maximum=1000,
    )
    desktop_mode = _coerce_bool(env_map.get("BALUFFO_DESKTOP_MODE"), False)

    data_dir = resolve_path(
        getattr(args, "data_dir", None) or env_map.get("BALUFFO_DATA_DIR") or storage_defaults["data_dir"],
        str(storage_defaults["data_dir"]),
    )

    bridge = BridgeConfig(
        host=host,
        port=port,
        log_format=log_format,
        log_level=log_level,
        quiet_requests=quiet_requests,
        max_history_rows=max_history_rows,
    )

    storage = StorageConfig(
        data_dir=data_dir,
        source_discovery_config_path=data_dir / "source-discovery-config.json",
        source_discovery_log_path=data_dir / "source-discovery.log",
        social_sources_config_path=data_dir / "social-sources-config.json",
        ops_history_path=data_dir / "admin-run-history.json",
        ops_alert_state_path=data_dir / "admin-alert-state.json",
        jobs_fetch_report_path=data_dir / "jobs-fetch-report.json",
        discovery_report_path=data_dir / "source-discovery-report.json",
        active_path=data_dir / "source-registry-active.json",
        pending_path=data_dir / "source-registry-pending.json",
        rejected_path=data_dir / "source-registry-rejected.json",
        tasks_config_path=ROOT / ".vscode" / "tasks.json",
        task_state_path=data_dir / "admin-task-state.json",
        sync_config_path=data_dir / "source-sync-config.json",
        sync_runtime_path=data_dir / "source-sync-runtime.json",
        run_history_path=data_dir / "admin-run-history.json",
    )

    return AdminBridgeConfig(
        root=ROOT,
        bridge=bridge,
        storage=storage,
        desktop_mode=desktop_mode,
    )
