#!/usr/bin/env python3
"""Platform-neutral runtime launcher for Baluffo ship bundles."""

from __future__ import annotations

import argparse
import contextlib
import functools
import json
import os
import runpy
import socket
import sys
import time
from dataclasses import dataclass
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator
from urllib.error import URLError
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ship import update_manager
from scripts.app_version import get_app_version
from scripts.baluffo_config import get_bridge_defaults, get_desktop_defaults

BRIDGE_DEFAULTS = get_bridge_defaults()
DESKTOP_DEFAULTS = get_desktop_defaults()

@dataclass(frozen=True)
class RuntimeLayout:
    root: Path
    current_version: str
    active_root: Path
    data_dir: Path


class QuietSimpleHTTPRequestHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return


def _append_startup_trace(data_dir: Path, event: str, **fields: object) -> None:
    row = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + f".{int((time.time() % 1) * 1_000_000):06d}+00:00",
        "event": str(event or "").strip() or "unknown",
        "fields": {key: value for key, value in fields.items()},
    }
    path = Path(data_dir) / "desktop-startup-metrics.jsonl"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    except OSError:
        return


def build_site_request_handler(directory: Path, *, data_dir: Path | None = None, startup_probe: bool = False):
    class ProbeAwareSimpleHTTPRequestHandler(QuietSimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(directory), **kwargs)

        def end_headers(self):  # noqa: N802
            # Desktop runtime should always load the latest local bundle assets.
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")
            return super().end_headers()

        def do_GET(self):  # noqa: N802
            trace_enabled = bool(startup_probe and data_dir)
            path_only = str(getattr(self, "path", "") or "").split("?", 1)[0]
            trace_path = path_only.lstrip("/")
            request_started = time.perf_counter()
            if trace_enabled and trace_path in {"jobs.html", "saved.html", "admin.html"}:
                _append_startup_trace(
                    Path(data_dir),
                    "desktop_site_request_start",
                    path=trace_path,
                )
            try:
                return super().do_GET()
            finally:
                if trace_enabled and trace_path in {"jobs.html", "saved.html", "admin.html"}:
                    _append_startup_trace(
                        Path(data_dir),
                        "desktop_site_request_complete",
                        path=trace_path,
                        durationMs=int((time.perf_counter() - request_started) * 1000),
                    )

    return ProbeAwareSimpleHTTPRequestHandler


def resolve_root(root: str | Path | None = None) -> Path:
    return Path(root).expanduser().resolve() if root else ROOT


def resolve_runtime_layout(root: str | Path | None = None, *, data_dir: str | Path | None = None) -> RuntimeLayout:
    bundle_root = resolve_root(root)
    paths = update_manager.ShipPaths.from_root(bundle_root)
    state = update_manager.ensure_state(paths)
    current_version = str(state.get("current_version") or "").strip()
    if not current_version:
        raise RuntimeError("Current version pointer is empty.")
    active_root = paths.versions / current_version
    if not active_root.exists():
        raise RuntimeError(f"Active version directory not found: {active_root}")
    resolved_data_dir = Path(data_dir).expanduser().resolve() if data_dir else paths.data
    return RuntimeLayout(
        root=bundle_root,
        current_version=current_version,
        active_root=active_root,
        data_dir=resolved_data_dir,
    )


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def wait_for_url(url: str, *, timeout_s: float = 20.0, interval_s: float = 0.25) -> None:
    deadline = time.monotonic() + max(0.1, timeout_s)
    last_error = ""
    while time.monotonic() < deadline:
        try:
            with urlopen(url, timeout=max(1.0, interval_s * 4)) as response:  # noqa: S310
                status = int(getattr(response, "status", 200) or 200)
                if 200 <= status < 500:
                    return
        except URLError as exc:
            last_error = str(exc)
        except OSError as exc:
            last_error = str(exc)
        time.sleep(interval_s)
    raise TimeoutError(f"Timed out waiting for {url}. Last error: {last_error or 'no response'}")


@contextlib.contextmanager
def _pushd(path: Path) -> Iterator[None]:
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


@contextlib.contextmanager
def _patched_argv(argv: list[str]) -> Iterator[None]:
    previous = list(sys.argv)
    sys.argv = argv
    try:
        yield
    finally:
        sys.argv = previous


@contextlib.contextmanager
def _patched_syspath(path: Path) -> Iterator[None]:
    token = str(path)
    inserted = token not in sys.path
    if inserted:
        sys.path.insert(0, token)
    try:
        yield
    finally:
        if inserted:
            with contextlib.suppress(ValueError):
                sys.path.remove(token)


@contextlib.contextmanager
def _isolated_scripts_package() -> Iterator[None]:
    saved = {name: module for name, module in sys.modules.items() if name == "scripts" or name.startswith("scripts.")}
    for name in list(saved):
        sys.modules.pop(name, None)
    try:
        yield
    finally:
        for name in list(sys.modules):
            if name == "scripts" or name.startswith("scripts."):
                sys.modules.pop(name, None)
        sys.modules.update(saved)


def run_site_server(root: str | Path | None = None, *, port: int = int(DESKTOP_DEFAULTS["site_port"])) -> None:
    layout = resolve_runtime_layout(root)
    print(
        json.dumps(
            {
                "ok": True,
                "mode": "site",
                "appVersion": get_app_version(),
                "root": str(layout.root),
                "activeRoot": str(layout.active_root),
                "currentVersion": layout.current_version,
                "port": int(port),
            }
        )
    )
    handler = build_site_request_handler(
        layout.active_root,
        data_dir=Path(str(os.environ.get("BALUFFO_DATA_DIR") or "")).expanduser().resolve() if str(os.environ.get("BALUFFO_DATA_DIR") or "").strip() else None,
        startup_probe=str(os.environ.get("BALUFFO_STARTUP_PROBE") or "").strip().lower() in {"1", "true", "yes", "on"},
    )
    server = ThreadingHTTPServer(("127.0.0.1", int(port)), handler)
    with server:
        server.serve_forever()


def run_bridge_server(
    root: str | Path | None = None,
    *,
    bind_host: str = "127.0.0.1",
    port: int = int(BRIDGE_DEFAULTS["port"]),
    data_dir: str | Path | None = None,
    desktop_mode: bool = False,
) -> None:
    layout = resolve_runtime_layout(root, data_dir=data_dir)
    update_manager.startup_check(layout.root, layout.data_dir)
    bridge_script = layout.active_root / "scripts" / "admin_bridge.py"
    if not bridge_script.exists():
        raise RuntimeError(f"Admin bridge entrypoint not found: {bridge_script}")
    os.environ["BALUFFO_DATA_DIR"] = str(layout.data_dir)
    if desktop_mode:
        os.environ["BALUFFO_DESKTOP_MODE"] = "1"
    else:
        os.environ.pop("BALUFFO_DESKTOP_MODE", None)
    with _pushd(layout.active_root), _patched_syspath(layout.active_root), _isolated_scripts_package(), _patched_argv(
        [
            str(bridge_script),
            "--host",
            str(bind_host),
            "--port",
            str(port),
            "--data-dir",
            str(layout.data_dir),
            "--log-format",
            "human",
            "--log-level",
            "info",
        ]
    ):
        runpy.run_path(str(bridge_script), run_name="__main__")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Baluffo ship runtime launcher.")
    sub = parser.add_subparsers(dest="command", required=True)

    site_parser = sub.add_parser("site", help="Run the static site from the active app version.")
    site_parser.add_argument("--root", default="")
    site_parser.add_argument("--port", type=int, default=int(DESKTOP_DEFAULTS["site_port"]))

    bridge_parser = sub.add_parser("bridge", help="Run the admin bridge from the active app version.")
    bridge_parser.add_argument("--root", default="")
    bridge_parser.add_argument("--bind-host", default=str(BRIDGE_DEFAULTS["host"]))
    bridge_parser.add_argument("--port", type=int, default=int(BRIDGE_DEFAULTS["port"]))
    bridge_parser.add_argument("--data-dir", default="")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.command == "site":
            run_site_server(args.root or None, port=int(args.port))
            return 0
        if args.command == "bridge":
            run_bridge_server(
            args.root or None,
            bind_host=str(args.bind_host),
            port=int(args.port),
            data_dir=args.data_dir or None,
            desktop_mode=str(os.environ.get("BALUFFO_DESKTOP_MODE") or "").strip().lower() in {"1", "true", "yes", "on"},
        )
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"ok": False, "error": str(exc)}))
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
