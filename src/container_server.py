#!/usr/bin/env python3
"""Container entrypoint for same-origin Baluffo UI and bridge API serving."""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any

from src import admin_bridge
from src.app_version import get_app_version
from src.bridge.config import (
    ContainerConfigLike,
    RuntimeConfig,
    _normalize_log_format,
    _normalize_log_level,
)
from src.bridge.pipeline_control_files import inactive_pipeline_status, write_pipeline_status
from src.bridge.server.handler import make_handler
from src.bridge.server.httpd import run_http_server
from src.bridge.server.static_files import StaticFileService
from src.python_version_guard import ensure_required_python
from src.runtime_seed import seed_runtime_data

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONTAINER_PORT = 8080
DEFAULT_CONTAINER_HOST = "0.0.0.0"
DEFAULT_CONTAINER_DATA_DIR = Path("/data")


def _coerce_port(value: Any, default: int = DEFAULT_CONTAINER_PORT) -> int:
    try:
        port = int(str(value or "").strip())
    except (TypeError, ValueError):
        return int(default)
    return port if 1 <= port <= 65535 else int(default)


def _coerce_bool(value: Any, default: bool = False) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return bool(default)
    return text in {"1", "true", "yes", "on"}


def parse_args(
    argv: list[str] | None = None, *, env: dict[str, str] | None = None
) -> RuntimeConfig:
    env_map = env if isinstance(env, dict) else os.environ
    parser = argparse.ArgumentParser(
        description="Run Baluffo as one same-origin container HTTP service."
    )
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--log-format", choices=("human", "jsonl"), default=None)
    parser.add_argument(
        "--log-level", choices=tuple(admin_bridge.LOG_LEVEL_ORDER.keys()), default=None
    )
    parser.add_argument("--quiet-requests", action="store_true", default=None)
    args = parser.parse_args(argv)

    data_dir_value = (
        args.data_dir or env_map.get("BALUFFO_DATA_DIR") or str(DEFAULT_CONTAINER_DATA_DIR)
    )
    return RuntimeConfig(
        root=ROOT,
        data_dir=Path(data_dir_value).expanduser().resolve(),
        host=str(args.host or env_map.get("BALUFFO_CONTAINER_HOST") or DEFAULT_CONTAINER_HOST),
        port=_coerce_port(
            args.port if args.port is not None else env_map.get("BALUFFO_CONTAINER_PORT"),
            DEFAULT_CONTAINER_PORT,
        ),
        log_format=_normalize_log_format(
            args.log_format or env_map.get("BALUFFO_BRIDGE_LOG_FORMAT") or "human"
        ),
        log_level=_normalize_log_level(
            args.log_level or env_map.get("BALUFFO_BRIDGE_LOG_LEVEL") or "info"
        ),
        quiet_requests=bool(
            args.quiet_requests
            if args.quiet_requests is not None
            else _coerce_bool(env_map.get("BALUFFO_BRIDGE_QUIET_REQUESTS"), False)
        ),
        desktop_mode=False,
        owner_mode="",
        owner_token="",
        desktop_session_id="",
        started_by="container",
        owner_idle_timeout_s=0.0,
        container_mode=True,
    )


def build_container_handler(config: ContainerConfigLike):
    admin_bridge.configure_runtime_paths(config)
    admin_bridge.refresh_sync_config()
    api = admin_bridge.build_bridge_api(config)
    static_service = StaticFileService(
        static_root=Path(config.root).resolve(),
        data_dir=Path(config.data_dir).resolve(),
    )
    return make_handler(api=api, static_service=static_service), api


def main(argv: list[str] | None = None) -> int:
    ensure_required_python()
    config = parse_args(argv)
    seed_runtime_data(Path(config.data_dir), source_root=Path(config.root), overwrite=False)
    write_pipeline_status(
        Path(config.data_dir),
        inactive_pipeline_status(
            app_version=get_app_version(),
            now_iso=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        ),
    )
    handler_cls, api = build_container_handler(config)
    return run_http_server(
        api=api,
        host=config.host,
        port=config.port,
        handler_cls=handler_cls,
        on_started=admin_bridge.on_bridge_started,
    )


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
