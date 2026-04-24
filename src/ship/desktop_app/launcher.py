from __future__ import annotations

import argparse
from pathlib import Path

from ._compat import desktop_api
from .config import DEFAULT_OPEN_PATH, ROOT, WINDOW_TITLE
from .launcher_diagnostics import (
    _recoverable_browser_launch_result,
    _write_launch_diagnostics,
)
from .launcher_flow import launch_desktop_app
from .launcher_recovery import _runtime_ports_need_retry, _should_retry_runtime_launch

__all__ = [
    "_recoverable_browser_launch_result",
    "_runtime_ports_need_retry",
    "_should_retry_runtime_launch",
    "_write_launch_diagnostics",
    "ensure_desktop_prerequisites",
    "launch_desktop_app",
    "main",
    "parse_args",
]


def ensure_desktop_prerequisites() -> None:
    return None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    api = desktop_api()
    parser = argparse.ArgumentParser(description="Launch Baluffo in a dedicated desktop window.")
    parser.add_argument("child_mode", nargs="?", default="")
    parser.add_argument("--root", default="")
    parser.add_argument("--site-port", type=int, default=0)
    parser.add_argument("--bridge-port", type=int, default=0)
    parser.add_argument("--bridge-host", default=str(api.DESKTOP_DEFAULTS["bridge_host"]))
    parser.add_argument("--data-dir", default="")
    parser.add_argument("--open-path", default=DEFAULT_OPEN_PATH)
    parser.add_argument("--title", default=WINDOW_TITLE)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--bind-host", default="127.0.0.1")
    parser.add_argument("--owner-mode", default="")
    parser.add_argument("--owner-token", default="")
    parser.add_argument("--desktop-session-id", default="")
    parser.add_argument("--started-by", default="")
    parser.add_argument("--owner-idle-timeout-s", type=float, default=0.0)
    parser.add_argument("--script", default="")
    parser.add_argument("--desktop-runtime", action="store_true")
    parser.add_argument("--startup-probe", action="store_true")
    args, extra = parser.parse_known_args(argv)
    if str(getattr(args, "child_mode", "") or "") == "__child_script__":
        args.script_args = list(extra)
        return args
    if extra:
        parser.error(f"unrecognized arguments: {' '.join(extra)}")
    args.script_args = []
    return args


def main(argv: list[str] | None = None) -> int:
    api = desktop_api()
    args = api.parse_args(argv)
    if args.child_mode == "__child_site__":
        from src.ship.runtime_launcher import run_site_server

        run_site_server(
            args.root or None,
            port=int(args.port),
            desktop_bridge_host=str(args.bridge_host or ""),
            desktop_bridge_port=int(args.bridge_port or 0),
        )
        return 0
    if args.child_mode == "__child_bridge__":
        from src.ship.runtime_launcher import run_bridge_server

        desktop_mode = bool(args.desktop_runtime) or api._truthy_env(
            api.os.environ.get("BALUFFO_DESKTOP_MODE")
        )
        run_bridge_server(
            args.root or None,
            bind_host=str(args.bind_host),
            port=int(args.port),
            data_dir=args.data_dir or None,
            desktop_mode=desktop_mode,
            owner_mode=str(args.owner_mode or ""),
            owner_token=str(args.owner_token or ""),
            desktop_session_id=str(args.desktop_session_id or ""),
            started_by=str(args.started_by or ""),
            owner_idle_timeout_s=float(args.owner_idle_timeout_s or 0.0),
        )
        return 0
    if args.child_mode == "__child_script__":
        runtime_root = (
            Path(args.root).expanduser().resolve() if str(args.root or "").strip() else ROOT
        )
        script_name = str(args.script or "").strip()
        if not script_name:
            raise RuntimeError("Missing --script for __child_script__ mode.")
        script_path = runtime_root / "src" / script_name
        if not script_path.exists():
            raise RuntimeError(f"Child script not found: {script_path}")
        script_argv = list(args.script_args or [])
        if script_argv and script_argv[0] == "--":
            script_argv = script_argv[1:]
        original_argv = list(api.sys.argv)
        try:
            api.sys.argv = [str(script_path), *script_argv]
            with (
                api._pushd(runtime_root),
                api._patched_syspath(runtime_root),
                api._isolated_src_package(),
            ):
                api.runpy.run_path(str(script_path), run_name="__main__")
            return 0
        finally:
            api.sys.argv = original_argv
    config = api.create_runtime_config(args)
    try:
        api.ensure_desktop_prerequisites()
        launch_desktop_app(config)
        return 0
    except Exception as exc:  # noqa: BLE001
        message = str(exc).strip() or "The Baluffo desktop app could not start."
        api.show_native_message(WINDOW_TITLE, message)
        return 1
