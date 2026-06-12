"""Bridge configuration helpers for runtime initialization.

This module provides RuntimeConfig dataclass and path resolution.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.baluffo_config import resolve_path as _resolve_path

LOG_LEVEL_ORDER: dict[str, int] = {"debug": 10, "info": 20, "warn": 30, "error": 40}


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
    owner_mode: str = ""
    owner_token: str = ""
    desktop_session_id: str = ""
    started_by: str = ""
    owner_idle_timeout_s: float = 0.0
    container_mode: bool = False


def _normalize_log_level(value: Any, default: str = "info") -> str:
    token = str(value or "").strip().lower()
    return token if token in LOG_LEVEL_ORDER else str(default)


def _normalize_log_format(value: Any, default: str = "human") -> str:
    token = str(value or "").strip().lower()
    return token if token in {"human", "jsonl"} else str(default)


def _coerce_port(value: Any, default: int) -> int:
    try:
        port = int(value)
    except (TypeError, ValueError):
        return int(default)
    return port if 1 <= port <= 65535 else int(default)


def resolve_runtime_config(
    *,
    root: Path,
    get_bridge_defaults: Any,
    get_storage_defaults: Any,
    argv: list[str] | None = None,
    env: dict[str, str] | None = None,
) -> RuntimeConfig:
    bridge_defaults = get_bridge_defaults()
    storage_defaults = get_storage_defaults()
    parser = argparse.ArgumentParser(description="Run local admin bridge API.")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--desktop-mode", action="store_true", default=False)
    parser.add_argument("--owner-mode", default=None)
    parser.add_argument("--owner-token", default=None)
    parser.add_argument("--desktop-session-id", default=None)
    parser.add_argument("--started-by", default=None)
    parser.add_argument("--owner-idle-timeout-s", type=float, default=None)
    parser.add_argument("--log-format", choices=("human", "jsonl"), default=None)
    parser.add_argument("--log-level", choices=tuple(LOG_LEVEL_ORDER.keys()), default=None)
    parser.add_argument("--quiet-requests", action="store_true", default=None)
    args = parser.parse_args(argv)
    env_map = env if isinstance(env, dict) else os.environ

    host = str(
        args.host or env_map.get("BALUFFO_BRIDGE_HOST") or bridge_defaults["host"]
    ).strip() or str(bridge_defaults["host"])
    port = _coerce_port(
        args.port if args.port is not None else env_map.get("BALUFFO_BRIDGE_PORT"),
        int(bridge_defaults["port"]),
    )
    data_dir_value = (
        args.data_dir
        if args.data_dir is not None
        else (env_map.get("BALUFFO_DATA_DIR") or storage_defaults["data_dir"])
    )
    # Keep relative `BALUFFO_DATA_DIR` resolution consistent with `src/baluffo_config.py`.
    data_dir = _resolve_path(data_dir_value, str(storage_defaults["data_dir"]))
    log_format = _normalize_log_format(
        args.log_format or env_map.get("BALUFFO_BRIDGE_LOG_FORMAT") or bridge_defaults["log_format"]
    )
    log_level = _normalize_log_level(
        args.log_level or env_map.get("BALUFFO_BRIDGE_LOG_LEVEL") or bridge_defaults["log_level"]
    )
    quiet_requests = bool(
        args.quiet_requests
        if args.quiet_requests is not None
        else str(env_map.get("BALUFFO_BRIDGE_QUIET_REQUESTS") or "").strip().lower()
        in {"1", "true", "yes", "on"}
        if str(env_map.get("BALUFFO_BRIDGE_QUIET_REQUESTS") or "").strip()
        else bridge_defaults["quiet_requests"]
    )
    desktop_mode = bool(args.desktop_mode) or (
        str(env_map.get("BALUFFO_DESKTOP_MODE") or "").strip().lower() in {"1", "true", "yes", "on"}
    )
    owner_mode = str(args.owner_mode or env_map.get("BALUFFO_BRIDGE_OWNER_MODE") or "").strip()
    owner_token = str(args.owner_token or env_map.get("BALUFFO_BRIDGE_OWNER_TOKEN") or "").strip()
    desktop_session_id = str(
        args.desktop_session_id or env_map.get("BALUFFO_BRIDGE_SESSION_ID") or ""
    ).strip()
    started_by = str(args.started_by or env_map.get("BALUFFO_BRIDGE_STARTED_BY") or "").strip()
    try:
        owner_idle_timeout_s = float(
            args.owner_idle_timeout_s
            if args.owner_idle_timeout_s is not None
            else env_map.get("BALUFFO_BRIDGE_OWNER_IDLE_TIMEOUT_S") or 0.0
        )
    except (TypeError, ValueError):
        owner_idle_timeout_s = 0.0
    return RuntimeConfig(
        root=Path(root),
        data_dir=data_dir,
        host=host,
        port=port,
        log_format=log_format,
        log_level=log_level,
        quiet_requests=quiet_requests,
        desktop_mode=desktop_mode,
        owner_mode=owner_mode,
        owner_token=owner_token,
        desktop_session_id=desktop_session_id,
        started_by=started_by,
        owner_idle_timeout_s=max(0.0, owner_idle_timeout_s),
        container_mode=False,
    )


def startup_banner(*, config: RuntimeConfig, bridge_log: Any) -> None:
    bridge_log(
        "info",
        "admin_bridge_started",
        url=f"http://{config.host}:{config.port}",
        root=str(config.root),
        data_dir=str(config.data_dir),
        log_format=config.log_format,
        log_level=config.log_level,
        owner_mode=config.owner_mode,
        owner_token=config.owner_token,
        desktop_session_id=config.desktop_session_id,
        started_by=config.started_by,
        owner_idle_timeout_s=config.owner_idle_timeout_s,
        pid=os.getpid(),
    )
    bridge_log(
        "info",
        "admin_bridge_endpoints",
        ops="GET /ops/health, GET /ops/dashboard-health, GET /ops/fetch-kpis, GET /ops/history, GET /ops/fetcher-metrics, POST /ops/alerts/ack",
        registry="GET /registry/*, POST /registry/*",
        sync="GET /sync/status, POST /sync/config, POST /sync/test, POST /sync/pull, POST /sync/push",
        tasks="POST /tasks/run-jobs-bootstrap, POST /tasks/run-fetcher, POST /tasks/run-discovery, POST /tasks/run-sync-pull, POST /tasks/run-sync-push, POST /tasks/run-jobs-pipeline, GET/POST /tasks/jobs-pipeline-schedule, POST /tasks/abort, GET /tasks/run-jobs-pipeline-status",
    )


__all__ = [
    "LOG_LEVEL_ORDER",
    "RuntimeConfig",
    "_normalize_log_format",
    "_normalize_log_level",
    "resolve_runtime_config",
    "startup_banner",
]
