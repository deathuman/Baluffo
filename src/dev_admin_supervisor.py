#!/usr/bin/env python3
"""Local admin workspace supervisor for bridge + site + browser ownership."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.contracts import SCHEMA_VERSION
from src.ship.desktop_app.browser import launch_browser_for_url
from src.ship.desktop_app.process import terminate_process
from src.ship.desktop_app.startup import watch_browser_session
from src.ship.startup_telemetry import wait_for_url

DEFAULT_SITE_PORT = 8080
DEFAULT_BRIDGE_PORT = 8877
DEFAULT_BRIDGE_HOST = "127.0.0.1"
DEFAULT_OPEN_PATH = "jobs.html"
DEFAULT_OWNER_IDLE_TIMEOUT_S = 30.0
LOCAL_BROWSER_EXIT_POLL_INTERVAL_S = 0.25
LOCAL_BROWSER_EXIT_SETTLE_S = 1.0
SESSION_FILENAME = "admin-dev-session.json"
TASK_STATE_FILENAME = "admin-task-state.json"
FETCH_REPORT_FILENAME = "jobs-fetch-report.json"
FETCH_TASKS_FILENAME = "jobs-fetch-tasks.json"


@dataclass(frozen=True)
class DevAdminConfig:
    root: Path
    data_dir: Path
    site_port: int
    bridge_port: int
    bridge_host: str
    open_path: str
    owner_idle_timeout_s: float
    open_browser: bool


def _session_path(data_dir: Path) -> Path:
    return Path(data_dir) / SESSION_FILENAME


def _task_state_path(data_dir: Path) -> Path:
    return Path(data_dir) / TASK_STATE_FILENAME


def _fetch_report_path(data_dir: Path) -> Path:
    return Path(data_dir) / FETCH_REPORT_FILENAME


def _fetch_tasks_path(data_dir: Path) -> Path:
    return Path(data_dir) / FETCH_TASKS_FILENAME


def _schema_version_int() -> int:
    try:
        return int(SCHEMA_VERSION)
    except (TypeError, ValueError):
        return int(float(str(SCHEMA_VERSION or 1)))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _python_command() -> list[str]:
    return [sys.executable]


def build_site_command(config: DevAdminConfig) -> list[str]:
    return [
        *_python_command(),
        "-m",
        "http.server",
        str(int(config.site_port)),
        "--directory",
        str(config.root),
    ]


def build_bridge_command(config: DevAdminConfig, *, owner_token: str) -> list[str]:
    return [
        *_python_command(),
        str(config.root / "src" / "admin_bridge.py"),
        "--host",
        str(config.bridge_host),
        "--port",
        str(int(config.bridge_port)),
        "--data-dir",
        str(config.data_dir),
        "--owner-mode",
        "dev-supervisor",
        "--owner-token",
        str(owner_token),
        "--started-by",
        "dev_admin_supervisor",
        "--owner-idle-timeout-s",
        str(float(config.owner_idle_timeout_s)),
    ]


def _spawn(command: list[str]) -> subprocess.Popen[str]:
    kwargs: dict[str, Any] = {
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "text": True,
    }
    if os.name == "nt":
        kwargs["creationflags"] = int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
    return subprocess.Popen(command, **kwargs)


def _terminate_pid(pid: int) -> None:
    if int(pid or 0) <= 0:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(int(pid)), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=10,
        )
        return
    with subprocess.Popen(
        ["kill", "-TERM", str(int(pid))],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    ) as process:
        process.wait(timeout=5)


def load_session_state(data_dir: Path) -> dict[str, Any]:
    path = _session_path(data_dir)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def save_session_state(data_dir: Path, payload: dict[str, Any]) -> None:
    path = _session_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def clear_session_state(data_dir: Path, *, owner_token: str = "") -> None:
    path = _session_path(data_dir)
    current = load_session_state(data_dir)
    if owner_token and str(current.get("ownerToken") or "").strip() != str(owner_token).strip():
        return
    try:
        path.unlink()
    except OSError:
        return


def _load_task_state(data_dir: Path) -> dict[str, Any]:
    path = _task_state_path(data_dir)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _clear_task_state(data_dir: Path) -> None:
    try:
        _task_state_path(data_dir).unlink()
    except OSError:
        return


def _reset_fetch_report(data_dir: Path) -> None:
    path = _fetch_report_path(data_dir)
    _write_json(
        path,
        {
            "schemaVersion": _schema_version_int(),
            "runId": "",
            "startedAt": "",
            "finishedAt": "",
            "runtime": {"lifecycle": {"owner": "fetch_report", "heartbeatAt": ""}},
            "summary": {"outputCount": 0, "failedSources": 0, "sourceCount": 0},
            "taskProgress": {
                "active": False,
                "phaseKey": "",
                "phaseLabel": "",
                "mode": "indeterminate",
                "ratio": 0.0,
                "counts": {},
            },
            "sources": [],
            "outputs": {"report": str(path)},
        },
    )


def _reset_fetch_tasks(data_dir: Path) -> None:
    path = _fetch_tasks_path(data_dir)
    _write_json(
        path,
        {
            "schemaVersion": _schema_version_int(),
            "runId": "",
            "startedAt": "",
            "finishedAt": "",
            "heartbeatAt": "",
            "summary": {"queued": 0, "running": 0, "ok": 0, "error": 0},
            "taskProgress": {
                "active": False,
                "phaseKey": "",
                "phaseLabel": "",
                "mode": "indeterminate",
                "ratio": 0.0,
                "counts": {},
            },
            "tasks": [],
            "outputs": {"report": str(_fetch_report_path(data_dir))},
        },
    )


def _reset_fetch_artifacts(data_dir: Path) -> None:
    _reset_fetch_report(data_dir)
    _reset_fetch_tasks(data_dir)


def _terminate_recorded_pids(state: dict[str, Any]) -> list[int]:
    killed: list[int] = []
    seen: set[int] = set()
    for key in ("bridgePid", "sitePid", "supervisorPid"):
        pid = int(state.get(key) or 0)
        if pid > 0 and pid not in seen:
            _terminate_pid(pid)
            seen.add(pid)
            killed.append(pid)
    return killed


def reclaim_previous_dev_session(
    data_dir: Path, *, kill_recorded_pids: bool = True
) -> dict[str, Any]:
    session_state = load_session_state(data_dir)
    task_state = _load_task_state(data_dir)
    killed: list[int] = []
    if kill_recorded_pids and session_state:
        killed.extend(_terminate_recorded_pids(session_state))
    if kill_recorded_pids and task_state:
        for task_type, entry in task_state.items():
            if not isinstance(entry, dict):
                continue
            if str(task_type) not in {"discovery", "fetch"}:
                continue
            pid = int(entry.get("pid") or 0)
            if pid > 0 and pid not in killed:
                _terminate_pid(pid)
                killed.append(pid)
    clear_session_state(data_dir)
    _clear_task_state(data_dir)
    _reset_fetch_artifacts(data_dir)
    return {"stopped": bool(killed), "killedPids": killed}


def stop_owned_session(data_dir: Path) -> dict[str, Any]:
    return reclaim_previous_dev_session(data_dir)


def _health_url(config: DevAdminConfig) -> str:
    return f"http://{config.bridge_host}:{int(config.bridge_port)}/ops/health"


def _admin_url(config: DevAdminConfig) -> str:
    return f"http://127.0.0.1:{int(config.site_port)}/{str(config.open_path).lstrip('/')}"


def _ensure_previous_owned_session_stopped(data_dir: Path) -> None:
    state = load_session_state(data_dir)
    task_state = _load_task_state(data_dir)
    if not state and not task_state:
        _reset_fetch_artifacts(data_dir)
        return
    reclaim_previous_dev_session(data_dir)
    _reset_fetch_artifacts(data_dir)


def wait_for_local_browser_exit(
    browser_process: subprocess.Popen[str] | None,
    *,
    settle_s: float = LOCAL_BROWSER_EXIT_SETTLE_S,
    poll_interval_s: float = LOCAL_BROWSER_EXIT_POLL_INTERVAL_S,
) -> None:
    if browser_process is None:
        return
    interval = max(0.05, float(poll_interval_s))
    while browser_process.poll() is None:
        time.sleep(interval)
    time.sleep(max(0.0, float(settle_s)))


def run_supervised_admin_session(config: DevAdminConfig) -> int:
    _ensure_previous_owned_session_stopped(config.data_dir)
    owner_token = uuid.uuid4().hex
    site_process: subprocess.Popen[str] | None = None
    bridge_process: subprocess.Popen[str] | None = None
    browser_process: subprocess.Popen[str] | None = None
    try:
        site_process = _spawn(build_site_command(config))
        wait_for_url(_admin_url(config), timeout_s=20.0, interval_s=0.25)
        bridge_process = _spawn(build_bridge_command(config, owner_token=owner_token))
        wait_for_url(_health_url(config), timeout_s=20.0, interval_s=0.25)
        save_session_state(
            config.data_dir,
            {
                "ownerMode": "dev-supervisor",
                "ownerToken": owner_token,
                "supervisorPid": int(os.getpid()),
                "sitePid": int(site_process.pid),
                "bridgePid": int(bridge_process.pid),
                "sitePort": int(config.site_port),
                "bridgePort": int(config.bridge_port),
                "bridgeHost": str(config.bridge_host),
                "openPath": str(config.open_path),
                "startedAt": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            },
        )
        print(
            json.dumps(
                {
                    "message": "dev_admin_supervisor_started",
                    "siteUrl": _admin_url(config),
                    "bridgeUrl": _health_url(config),
                    "ownerToken": owner_token,
                    "sitePid": int(site_process.pid),
                    "bridgePid": int(bridge_process.pid),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        if not config.open_browser:
            while True:
                if site_process.poll() is not None or bridge_process.poll() is not None:
                    break
                time.sleep(1.0)
            return 0
        launch = launch_browser_for_url(_admin_url(config))
        browser_process = launch.get("process") if isinstance(launch, dict) else None
        started_mono = float(launch.get("windowShownAtMonotonic") or time.perf_counter())
        if browser_process is not None:
            wait_for_local_browser_exit(browser_process)
        else:
            watch_browser_session(
                config.data_dir,
                started_mono,
                bridge_port=int(config.bridge_port),
                browser_process=browser_process,
                heartbeat_idle_timeout_s=float(config.owner_idle_timeout_s),
            )
        return 0
    finally:
        if browser_process is not None:
            terminate_process(browser_process)
        terminate_process(bridge_process)
        terminate_process(site_process)
        reclaim_previous_dev_session(config.data_dir, kill_recorded_pids=False)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Baluffo local admin supervisor.")
    sub = parser.add_subparsers(dest="command", required=False)

    start_parser = sub.add_parser("start", help="Start site, bridge, and optionally browser.")
    start_parser.add_argument("--data-dir", default=str(ROOT / "data"))
    start_parser.add_argument("--site-port", type=int, default=DEFAULT_SITE_PORT)
    start_parser.add_argument("--bridge-port", type=int, default=DEFAULT_BRIDGE_PORT)
    start_parser.add_argument("--bridge-host", default=DEFAULT_BRIDGE_HOST)
    start_parser.add_argument("--open-path", default=DEFAULT_OPEN_PATH)
    start_parser.add_argument(
        "--owner-idle-timeout-s",
        type=float,
        default=DEFAULT_OWNER_IDLE_TIMEOUT_S,
    )
    start_parser.add_argument("--no-browser", action="store_true", default=False)

    stop_parser = sub.add_parser("stop", help="Stop the owned local admin session.")
    stop_parser.add_argument("--data-dir", default=str(ROOT / "data"))

    return parser.parse_args(
        ["start", *(argv or [])] if not argv or str(argv[0]).startswith("--") else argv
    )


def create_config(args: argparse.Namespace) -> DevAdminConfig:
    return DevAdminConfig(
        root=ROOT,
        data_dir=Path(args.data_dir).expanduser().resolve(),
        site_port=int(args.site_port),
        bridge_port=int(args.bridge_port),
        bridge_host=str(args.bridge_host),
        open_path=str(args.open_path),
        owner_idle_timeout_s=max(5.0, float(args.owner_idle_timeout_s)),
        open_browser=not bool(args.no_browser),
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if str(args.command or "start").strip().lower() == "stop":
        result = stop_owned_session(Path(args.data_dir).expanduser().resolve())
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return 0
    config = create_config(args)
    try:
        return run_supervised_admin_session(config)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
