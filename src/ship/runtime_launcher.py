#!/usr/bin/env python3
"""Platform-neutral runtime launcher for Baluffo ship bundles."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import runpy
import shutil
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.app_version import get_app_version
from src.baluffo_config import get_bridge_defaults, get_desktop_defaults
from src.ship import update_manager

BRIDGE_DEFAULTS = get_bridge_defaults()
DESKTOP_DEFAULTS = get_desktop_defaults()


def _is_expected_client_disconnect(exc: BaseException) -> bool:
    current: BaseException | None = exc
    while current is not None:
        if isinstance(current, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
            return True
        winerror = getattr(current, "winerror", None)
        if isinstance(winerror, int) and winerror in {10053, 10054}:
            return True
        errno = getattr(current, "errno", None)
        if isinstance(errno, int) and errno in {32, 104}:
            return True
        current = current.__cause__ or current.__context__
    return False


@dataclass(frozen=True)
class RuntimeLayout:
    root: Path
    current_version: str
    active_root: Path
    data_dir: Path


class QuietSimpleHTTPRequestHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return

    def handle_one_request(self) -> None:
        try:
            return super().handle_one_request()
        except Exception as exc:  # noqa: BLE001
            if _is_expected_client_disconnect(exc):
                self.close_connection = True
                return
            raise


def _append_startup_trace(data_dir: Path, event: str, **fields: object) -> None:
    row = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
        + f".{int((time.time() % 1) * 1_000_000):06d}+00:00",
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


def build_site_request_handler(
    directory: Path, *, data_dir: Path | None = None, startup_probe: bool = False
):
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


def resolve_runtime_layout(
    root: str | Path | None = None, *, data_dir: str | Path | None = None
) -> RuntimeLayout:
    bundle_root = resolve_root(root)
    paths = update_manager.ShipPaths.from_root(bundle_root)
    update_manager.ensure_state(paths)
    current_version = paths.current.read_text(encoding="utf-8").strip()
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


def _try_heal_required_files_from_repo(layout: RuntimeLayout) -> int:
    """Dev checkout: fill missing ``REQUIRED_VERSION_FILES`` from the repo beside ``src/ship``."""
    if getattr(sys, "frozen", False):
        return 0
    repo = Path(__file__).resolve().parents[2]
    copied = 0
    for rel in update_manager.REQUIRED_VERSION_FILES:
        dest = layout.active_root / rel
        if dest.exists():
            continue
        if rel.startswith("src/"):
            src = repo / rel
        else:
            src = repo / Path(rel).name
        if not src.is_file():
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        copied += 1
    return copied


def _try_heal_required_files_from_meipass(layout: RuntimeLayout) -> int:
    """Frozen exe: copy missing required files from PyInstaller ``baluffo_embed`` payload."""
    if not getattr(sys, "frozen", False):
        return 0
    meipass = getattr(sys, "_MEIPASS", None)
    if not meipass:
        return 0
    base = Path(meipass) / "baluffo_embed"
    if not base.is_dir():
        return 0
    copied = 0
    for rel in update_manager.REQUIRED_VERSION_FILES:
        dest = layout.active_root / rel
        if dest.exists():
            continue
        name = Path(rel).name
        src = (base / "src" / name) if rel.startswith("src/") else (base / name)
        if not src.is_file():
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        copied += 1
    return copied


def heal_active_ship_version(layout: RuntimeLayout) -> None:
    """Best-effort restore of missing critical files before health checks or static serving."""
    paths = update_manager.ShipPaths.from_root(layout.root)
    update_manager.repair_version_from_runtime_bootstrap(
        paths, layout.active_root, layout.current_version
    )
    _try_heal_required_files_from_repo(layout)
    _try_heal_required_files_from_meipass(layout)


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
def _isolated_src_package() -> Iterator[None]:
    saved = {
        name: module
        for name, module in sys.modules.items()
        if name == "src" or name.startswith("src.")
    }
    for name in list(saved):
        sys.modules.pop(name, None)
    try:
        yield
    finally:
        for name in list(sys.modules):
            if name == "src" or name.startswith("src."):
                sys.modules.pop(name, None)
        sys.modules.update(saved)


def run_site_server(
    root: str | Path | None = None, *, port: int = int(DESKTOP_DEFAULTS["site_port"])
) -> None:
    layout = resolve_runtime_layout(root)
    heal_active_ship_version(layout)
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
        data_dir=Path(str(os.environ.get("BALUFFO_DATA_DIR") or "")).expanduser().resolve()
        if str(os.environ.get("BALUFFO_DATA_DIR") or "").strip()
        else None,
        startup_probe=str(os.environ.get("BALUFFO_STARTUP_PROBE") or "").strip().lower()
        in {"1", "true", "yes", "on"},
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
    owner_mode: str = "",
    owner_token: str = "",
    desktop_session_id: str = "",
    started_by: str = "",
    owner_idle_timeout_s: float = 0.0,
) -> None:
    layout = resolve_runtime_layout(root, data_dir=data_dir)
    heal_active_ship_version(layout)
    update_manager.startup_check(layout.root, layout.data_dir)
    bridge_script = layout.active_root / "src" / "admin_bridge.py"
    if not bridge_script.exists():
        raise RuntimeError(f"Admin bridge entrypoint not found: {bridge_script}")
    os.environ["BALUFFO_DATA_DIR"] = str(layout.data_dir)
    if desktop_mode:
        os.environ["BALUFFO_DESKTOP_MODE"] = "1"
    else:
        os.environ.pop("BALUFFO_DESKTOP_MODE", None)
    if str(desktop_session_id or "").strip():
        os.environ["BALUFFO_BRIDGE_SESSION_ID"] = str(desktop_session_id)
    else:
        os.environ.pop("BALUFFO_BRIDGE_SESSION_ID", None)
    argv = [
        str(bridge_script),
        "--host",
        str(bind_host),
        "--port",
        str(port),
        "--data-dir",
        str(layout.data_dir),
        "--desktop-mode" if desktop_mode else "",
        "--log-format",
        "human",
        "--log-level",
        "info",
    ]
    if str(owner_mode or "").strip():
        argv.extend(["--owner-mode", str(owner_mode)])
    if str(owner_token or "").strip():
        argv.extend(["--owner-token", str(owner_token)])
    if str(desktop_session_id or "").strip():
        argv.extend(["--desktop-session-id", str(desktop_session_id)])
    if str(started_by or "").strip():
        argv.extend(["--started-by", str(started_by)])
    if float(owner_idle_timeout_s or 0.0) > 0.0:
        argv.extend(["--owner-idle-timeout-s", str(float(owner_idle_timeout_s))])
    argv = [item for item in argv if str(item).strip()]
    with (
        _pushd(layout.active_root),
        _patched_syspath(layout.active_root),
        _isolated_src_package(),
        _patched_argv(argv),
    ):
        runpy.run_path(str(bridge_script), run_name="__main__")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Baluffo ship runtime launcher.")
    sub = parser.add_subparsers(dest="command", required=True)

    site_parser = sub.add_parser("site", help="Run the static site from the active app version.")
    site_parser.add_argument("--root", default="")
    site_parser.add_argument("--port", type=int, default=int(DESKTOP_DEFAULTS["site_port"]))

    bridge_parser = sub.add_parser(
        "bridge", help="Run the admin bridge from the active app version."
    )
    bridge_parser.add_argument("--root", default="")
    bridge_parser.add_argument("--bind-host", default=str(BRIDGE_DEFAULTS["host"]))
    bridge_parser.add_argument("--port", type=int, default=int(BRIDGE_DEFAULTS["port"]))
    bridge_parser.add_argument("--data-dir", default="")
    bridge_parser.add_argument("--owner-mode", default="")
    bridge_parser.add_argument("--owner-token", default="")
    bridge_parser.add_argument("--desktop-session-id", default="")
    bridge_parser.add_argument("--started-by", default="")
    bridge_parser.add_argument("--owner-idle-timeout-s", type=float, default=0.0)

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
                desktop_mode=str(os.environ.get("BALUFFO_DESKTOP_MODE") or "").strip().lower()
                in {"1", "true", "yes", "on"},
                owner_mode=str(args.owner_mode or ""),
                owner_token=str(args.owner_token or ""),
                desktop_session_id=str(args.desktop_session_id or ""),
                started_by=str(args.started_by or ""),
                owner_idle_timeout_s=float(args.owner_idle_timeout_s or 0.0),
            )
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"ok": False, "error": str(exc)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
