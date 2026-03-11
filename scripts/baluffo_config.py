#!/usr/bin/env python3
"""Shared Baluffo config loader."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

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
        "webview2_runtime_path": "",
        "webview2_disable_gpu": False,
        "webview2_additional_browser_arguments": "",
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


def _coerce_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _coerce_int(value: Any, default: int, *, minimum: int = 1, maximum: int = 65535) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(minimum, min(maximum, parsed))


def _coerce_str(value: Any, default: str) -> str:
    text = str(value or "").strip()
    return text or str(default)


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
        "webview2_runtime_path": str(cfg.get("webview2_runtime_path") or "").strip(),
        "webview2_disable_gpu": _coerce_bool(
            cfg.get("webview2_disable_gpu"),
            CODE_FALLBACK_CONFIG["desktop"]["webview2_disable_gpu"],
        ),
        "webview2_additional_browser_arguments": str(
            cfg.get("webview2_additional_browser_arguments") or ""
        ).strip(),
    }
