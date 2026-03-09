#!/usr/bin/env python3
"""Desktop entrypoint for portable Baluffo executable builds."""

from __future__ import annotations

import argparse
import contextlib
import html
import os
import runpy
import subprocess
import sys
import time
import webbrowser
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence
import json

if os.name == "nt":
    import ctypes
    import winreg

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ship.runtime_launcher import wait_for_url

WINDOW_TITLE = "Baluffo"
DEFAULT_OPEN_PATH = "jobs.html"
DEFAULT_SITE_PORT = 8080
DEFAULT_BRIDGE_PORT = 8877
READY_TIMEOUT_S = 25.0
WEBVIEW2_INSTALL_URL = "https://go.microsoft.com/fwlink/p/?LinkId=2124703"
MB_OK = 0x00000000
MB_ICONERROR = 0x00000010
MB_YESNO = 0x00000004
IDYES = 6


@dataclass(frozen=True)
class DesktopRuntimeConfig:
    ship_root: Path
    site_port: int
    bridge_port: int
    bridge_host: str
    data_dir: Path
    open_path: str
    title: str


def _append_startup_trace(data_dir: Path, event: str, **fields: object) -> None:
    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
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


def _default_ship_root() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent / "ship"
    return ROOT / "dist" / "baluffo-ship"


def resolve_ship_root(root: str | Path | None = None) -> Path:
    resolved = Path(root).expanduser().resolve() if root else _default_ship_root()
    if not resolved.exists():
        raise RuntimeError(f"Ship root not found: {resolved}")
    return resolved


def _entry_command() -> list[str]:
    if getattr(sys, "frozen", False):
        return [sys.executable]
    return [sys.executable, str(Path(__file__).resolve())]


def build_child_command(
    mode: str,
    *,
    root: Path,
    port: int,
    bridge_host: str = "127.0.0.1",
    data_dir: Path | None = None,
    desktop_runtime: bool = False,
) -> list[str]:
    normalized = str(mode or "").strip().lower()
    if normalized not in {"site", "bridge"}:
        raise ValueError("Invalid child mode")
    command = _entry_command()
    if normalized == "site":
        child_command = command + ["__child_site__", "--root", str(root), "--port", str(port)]
        if desktop_runtime:
            child_command.append("--desktop-runtime")
        return child_command
    child_command = command + [
        "__child_bridge__",
        "--root",
        str(root),
        "--bind-host",
        str(bridge_host),
        "--port",
        str(port),
        "--data-dir",
        str(data_dir or (root / "data")),
    ]
    if desktop_runtime:
        child_command.append("--desktop-runtime")
    return child_command


def start_child_process(command: Sequence[str]) -> subprocess.Popen[str]:
    popen_kwargs: dict[str, object] = {
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "text": True,
    }
    if os.name == "nt":
        popen_kwargs["creationflags"] = int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
    return subprocess.Popen(list(command), **popen_kwargs)


def terminate_process(process: subprocess.Popen[str] | None) -> None:
    if process is None or process.poll() is not None:
        return
    with contextlib.suppress(Exception):  # noqa: BLE001
        process.terminate()
        process.wait(timeout=5)
        return
    with contextlib.suppress(Exception):  # noqa: BLE001
        process.kill()


@contextlib.contextmanager
def _pushd(path: Path):
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


@contextlib.contextmanager
def _patched_syspath(path: Path):
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
def _isolated_scripts_package():
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


def create_runtime_config(args: argparse.Namespace) -> DesktopRuntimeConfig:
    ship_root = resolve_ship_root(args.root or None)
    site_port = int(args.site_port) if int(args.site_port) > 0 else DEFAULT_SITE_PORT
    bridge_port = int(args.bridge_port) if int(args.bridge_port) > 0 else DEFAULT_BRIDGE_PORT
    data_dir = Path(args.data_dir).expanduser().resolve() if str(args.data_dir or "").strip() else ship_root / "data"
    return DesktopRuntimeConfig(
        ship_root=ship_root,
        site_port=site_port,
        bridge_port=bridge_port,
        bridge_host=str(args.bridge_host or "127.0.0.1"),
        data_dir=data_dir,
        open_path=str(args.open_path or DEFAULT_OPEN_PATH).lstrip("/") or DEFAULT_OPEN_PATH,
        title=str(args.title or WINDOW_TITLE).strip() or WINDOW_TITLE,
    )


def _port_is_available(host: str, port: int) -> bool:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, int(port)))
            return True
        except OSError:
            return False


def ensure_runtime_ports(config: DesktopRuntimeConfig) -> None:
    if not _port_is_available("127.0.0.1", config.site_port):
        raise RuntimeError(
            f"Baluffo desktop site port {config.site_port} is already in use. Close the other process or choose a different --site-port."
        )
    if not _port_is_available(config.bridge_host, config.bridge_port):
        raise RuntimeError(
            f"Baluffo desktop bridge port {config.bridge_port} is already in use. Close the other process or choose a different --bridge-port."
        )


def build_open_url(config: DesktopRuntimeConfig) -> str:
    separator = "&" if "?" in config.open_path else "?"
    return f"http://127.0.0.1:{config.site_port}/{config.open_path}{separator}desktop=1"


def build_loading_html(title: str) -> str:
    safe_title = html.escape(str(title or WINDOW_TITLE))
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{safe_title}</title>
  <style>
    :root {{
      color-scheme: dark;
    }}
    html, body {{
      width: 100%;
      height: 100%;
      margin: 0;
      background: radial-gradient(circle at 20% 20%, #2f3b57 0%, #0f131d 58%, #0b0e15 100%);
      color: #e9eefc;
      font-family: "Segoe UI", Arial, sans-serif;
    }}
    .shell {{
      height: 100%;
      display: grid;
      place-items: center;
    }}
    .card {{
      display: grid;
      gap: 10px;
      justify-items: center;
      text-align: center;
      padding: 22px 26px;
      border: 1px solid rgba(193, 210, 255, 0.26);
      border-radius: 14px;
      background: rgba(13, 18, 28, 0.64);
      box-shadow: 0 14px 34px rgba(0, 0, 0, 0.4);
      backdrop-filter: blur(2px);
    }}
    .spinner {{
      width: 28px;
      height: 28px;
      border: 3px solid rgba(193, 210, 255, 0.26);
      border-top-color: #8eb5ff;
      border-radius: 50%;
      animation: spin 0.8s linear infinite;
    }}
    .title {{
      font-weight: 700;
      letter-spacing: 0.2px;
    }}
    .hint {{
      color: #b8c6e9;
      font-size: 13px;
    }}
    @keyframes spin {{
      to {{
        transform: rotate(360deg);
      }}
    }}
  </style>
</head>
<body>
  <main class="shell">
    <section class="card" aria-live="polite">
      <div class="spinner" aria-hidden="true"></div>
      <div class="title">Starting {safe_title}</div>
      <div class="hint">Loading interface...</div>
    </section>
  </main>
</body>
</html>
"""


def _load_window_url(window: object, url: str) -> None:
    try:
        load_url = getattr(window, "load_url", None)
        if callable(load_url):
            load_url(url)
    except Exception:
        # Best effort only; startup fallback remains the initial html shell.
        return


def _load_window_url_with_trace(data_dir: Path, started_mono: float, window: object, url: str) -> None:
    _append_startup_trace(
        data_dir,
        "desktop_window_load_url",
        elapsedMs=int((time.perf_counter() - started_mono) * 1000),
        url=str(url),
    )
    _load_window_url(window, url)


def is_webview2_available() -> bool:
    if os.name != "nt":
        return True
    if os.environ.get("BALUFFO_WEBVIEW2_RUNTIME_PATH"):
        return Path(os.environ["BALUFFO_WEBVIEW2_RUNTIME_PATH"]).expanduser().exists()

    def read_edge_build(root_name: str, key_guid: str) -> str:
        base = getattr(winreg, root_name)
        subkey = rf"SOFTWARE\Microsoft\EdgeUpdate\Clients\{key_guid}"
        if root_name == "HKEY_LOCAL_MACHINE":
            for candidate in (
                rf"SOFTWARE\WOW6432Node\Microsoft\EdgeUpdate\Clients\{key_guid}",
                subkey,
            ):
                with contextlib.suppress(OSError):
                    with winreg.OpenKey(base, candidate) as key:
                        value, _ = winreg.QueryValueEx(key, "pv")
                        return str(value)
            return "0"
        with contextlib.suppress(OSError):
            with winreg.OpenKey(base, subkey) as key:
                value, _ = winreg.QueryValueEx(key, "pv")
                return str(value)
        return "0"

    def version_at_least(current: str, minimum: str) -> bool:
        try:
            current_parts = [int(part) for part in current.split(".")]
            minimum_parts = [int(part) for part in minimum.split(".")]
        except ValueError:
            return False
        length = max(len(current_parts), len(minimum_parts))
        current_parts.extend([0] * (length - len(current_parts)))
        minimum_parts.extend([0] * (length - len(minimum_parts)))
        return current_parts >= minimum_parts

    try:
        with winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\NET Framework Setup\NDP\v4\Full"
        ) as net_key:
            release, _ = winreg.QueryValueEx(net_key, "Release")
        if int(release) < 394802:
            return False
    except OSError:
        return False

    runtime_guids = (
        "{F3017226-FE2A-4295-8BDF-00C3A9A7E4C5}",
        "{2CD8A007-E189-409D-A2C8-9AF4EF3C72AA}",
        "{0D50BFEC-CD6A-4F9A-964C-C7416E3ACB10}",
        "{65C35B14-6C1D-4122-AC46-7148CC9D6497}",
    )
    for guid in runtime_guids:
        for root_name in ("HKEY_CURRENT_USER", "HKEY_LOCAL_MACHINE"):
            if version_at_least(read_edge_build(root_name, guid), "86.0.622.0"):
                return True
    return False


def show_native_message(title: str, message: str, *, ask_to_install: bool = False) -> bool:
    if os.name == "nt":
        flags = MB_ICONERROR | (MB_YESNO if ask_to_install else MB_OK)
        dialog_message = message
        if ask_to_install:
            dialog_message = f"{message}\n\nOpen the Microsoft WebView2 Runtime installer page now?"
        result = ctypes.windll.user32.MessageBoxW(None, dialog_message, title, flags)
        return bool(ask_to_install and result == IDYES)
    print(f"{title}: {message}", file=sys.stderr)
    return False


def ensure_desktop_prerequisites() -> None:
    try:
        import webview  # type: ignore  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "Baluffo desktop components are missing. Rebuild or reinstall the packaged executable."
        ) from exc

    if os.name == "nt" and not is_webview2_available():
        raise RuntimeError(
            "Microsoft Edge WebView2 Runtime is required to launch the Baluffo desktop app on Windows."
        )


def launch_desktop_app(config: DesktopRuntimeConfig) -> None:
    site_process: subprocess.Popen[str] | None = None
    bridge_process: subprocess.Popen[str] | None = None
    started_mono = time.perf_counter()
    _append_startup_trace(
        config.data_dir,
        "desktop_launch_start",
        sitePort=int(config.site_port),
        bridgePort=int(config.bridge_port),
        shipRoot=str(config.ship_root),
    )
    try:
        ensure_runtime_ports(config)
        _append_startup_trace(config.data_dir, "desktop_ports_available", elapsedMs=int((time.perf_counter() - started_mono) * 1000))
        site_process = start_child_process(
            build_child_command("site", root=config.ship_root, port=config.site_port, desktop_runtime=True)
        )
        _append_startup_trace(
            config.data_dir,
            "desktop_site_spawned",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            pid=int(site_process.pid) if site_process else 0,
        )
        bridge_process = start_child_process(
            build_child_command(
                "bridge",
                root=config.ship_root,
                port=config.bridge_port,
                bridge_host=config.bridge_host,
                data_dir=config.data_dir,
                desktop_runtime=True,
            )
        )
        _append_startup_trace(
            config.data_dir,
            "desktop_bridge_spawned",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            pid=int(bridge_process.pid) if bridge_process else 0,
        )
        open_url = build_open_url(config)
        wait_for_url(open_url, timeout_s=READY_TIMEOUT_S)
        _append_startup_trace(
            config.data_dir,
            "desktop_site_ready",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            url=str(open_url),
        )
        import webview  # type: ignore
        window = webview.create_window(
            config.title,
            html=build_loading_html(config.title),
            min_size=(1100, 720),
        )
        _append_startup_trace(config.data_dir, "desktop_window_created", elapsedMs=int((time.perf_counter() - started_mono) * 1000))
        webview.start(_load_window_url_with_trace, (config.data_dir, started_mono, window, open_url))
        _append_startup_trace(config.data_dir, "desktop_window_closed", elapsedMs=int((time.perf_counter() - started_mono) * 1000))
        if window is None:
            raise RuntimeError("Failed to create desktop window.")
    except Exception as exc:
        _append_startup_trace(
            config.data_dir,
            "desktop_launch_error",
            elapsedMs=int((time.perf_counter() - started_mono) * 1000),
            error=str(exc),
        )
        raise
    finally:
        terminate_process(bridge_process)
        terminate_process(site_process)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launch Baluffo in a dedicated desktop window.")
    parser.add_argument("child_mode", nargs="?", default="")
    parser.add_argument("--root", default="")
    parser.add_argument("--site-port", type=int, default=0)
    parser.add_argument("--bridge-port", type=int, default=0)
    parser.add_argument("--bridge-host", default="127.0.0.1")
    parser.add_argument("--data-dir", default="")
    parser.add_argument("--open-path", default=DEFAULT_OPEN_PATH)
    parser.add_argument("--title", default=WINDOW_TITLE)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--bind-host", default="127.0.0.1")
    parser.add_argument("--script", default="")
    parser.add_argument("--desktop-runtime", action="store_true")
    args, extra = parser.parse_known_args(argv)
    if str(getattr(args, "child_mode", "") or "") == "__child_script__":
        args.script_args = list(extra)
        return args
    if extra:
        parser.error(f"unrecognized arguments: {' '.join(extra)}")
    args.script_args = []
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.child_mode == "__child_site__":
        from scripts.ship.runtime_launcher import run_site_server

        run_site_server(args.root or None, port=int(args.port))
        return 0
    if args.child_mode == "__child_bridge__":
        from scripts.ship.runtime_launcher import run_bridge_server

        run_bridge_server(
            args.root or None,
            bind_host=str(args.bind_host),
            port=int(args.port),
            data_dir=args.data_dir or None,
            desktop_mode=bool(args.desktop_runtime),
        )
        return 0
    if args.child_mode == "__child_script__":
        runtime_root = Path(args.root).expanduser().resolve() if str(args.root or "").strip() else ROOT
        script_name = str(args.script or "").strip()
        if not script_name:
            raise RuntimeError("Missing --script for __child_script__ mode.")
        script_path = runtime_root / "scripts" / script_name
        if not script_path.exists():
            raise RuntimeError(f"Child script not found: {script_path}")
        script_argv = list(args.script_args or [])
        if script_argv and script_argv[0] == "--":
            script_argv = script_argv[1:]
        original_argv = list(sys.argv)
        try:
            sys.argv = [str(script_path), *script_argv]
            with _pushd(runtime_root), _patched_syspath(runtime_root), _isolated_scripts_package():
                runpy.run_path(str(script_path), run_name="__main__")
            return 0
        finally:
            sys.argv = original_argv
    config = create_runtime_config(args)
    try:
        ensure_desktop_prerequisites()
        launch_desktop_app(config)
        return 0
    except Exception as exc:  # noqa: BLE001
        message = str(exc).strip() or "The Baluffo desktop app could not start."
        should_offer_install = "WebView2" in message
        if show_native_message(WINDOW_TITLE, message, ask_to_install=should_offer_install):
            with contextlib.suppress(Exception):  # noqa: BLE001
                webbrowser.open(WEBVIEW2_INSTALL_URL)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
