"""Side effects: process ownership, src-module isolation. Verify: npm run test:frontend:packaged:desktop-lifecycle-rehearsal."""

from __future__ import annotations

import contextlib
import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import cast

from ._compat import desktop_api

_EXPECTED_TERMINATE_EXCEPTIONS = (OSError, subprocess.SubprocessError)


def _entry_command() -> list[str]:
    api = desktop_api()
    if getattr(api.sys, "frozen", False):
        return [api.sys.executable]
    return [api.sys.executable, str(Path(str(api.__file__ or "")).resolve())]


def build_child_command(
    mode: str,
    *,
    root: Path,
    port: int,
    bridge_host: str = "127.0.0.1",
    bridge_port: int | None = None,
    data_dir: Path | None = None,
    desktop_runtime: bool = False,
    owner_mode: str = "",
    owner_token: str = "",
    desktop_session_id: str = "",
    started_by: str = "",
    owner_idle_timeout_s: float = 0.0,
) -> list[str]:
    normalized = str(mode or "").strip().lower()
    if normalized not in {"site", "bridge"}:
        raise ValueError("Invalid child mode")
    command = _entry_command()
    if normalized == "site":
        child_command = command + ["__child_site__", "--root", str(root), "--port", str(port)]
        if desktop_runtime:
            child_command.append("--desktop-runtime")
            child_command.extend(["--bridge-host", str(bridge_host)])
            if bridge_port is not None:
                child_command.extend(["--bridge-port", str(int(bridge_port))])
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
    if str(owner_mode or "").strip():
        child_command.extend(["--owner-mode", str(owner_mode)])
    if str(owner_token or "").strip():
        child_command.extend(["--owner-token", str(owner_token)])
    if str(desktop_session_id or "").strip():
        child_command.extend(["--desktop-session-id", str(desktop_session_id)])
    if str(started_by or "").strip():
        child_command.extend(["--started-by", str(started_by)])
    if float(owner_idle_timeout_s or 0.0) > 0.0:
        child_command.extend(["--owner-idle-timeout-s", str(float(owner_idle_timeout_s))])
    return child_command


def start_child_process(
    command: Sequence[str],
    *,
    extra_env: dict[str, str] | None = None,
    job_handle: int | None = None,
) -> subprocess.Popen[str]:
    api = desktop_api()
    env = None
    if extra_env:
        env = api.os.environ.copy()
        env.update({key: str(value) for key, value in extra_env.items()})
    if api.os.name == "nt":
        # CREATE_NO_WINDOW suppresses console; CREATE_NEW_PROCESS_GROUP enables targeted terminate.
        proc = api.subprocess.Popen(
            list(command),
            stdout=api.subprocess.DEVNULL,
            stderr=api.subprocess.DEVNULL,
            text=True,
            env=env,
            creationflags=int(getattr(api.subprocess, "CREATE_NO_WINDOW", 0))
            | int(getattr(api.subprocess, "CREATE_NEW_PROCESS_GROUP", 0)),
            close_fds=True,
        )
    else:
        proc = api.subprocess.Popen(
            list(command),
            stdout=api.subprocess.DEVNULL,
            stderr=api.subprocess.DEVNULL,
            text=True,
            env=env,
        )
    if job_handle and proc.pid:
        # Attach child to kill-on-close job for guaranteed cleanup on launcher exit.
        try:
            api._windows_try_assign_pid_to_job(job_handle, int(proc.pid))
        except OSError:
            api.terminate_process(proc)
            raise
    return cast(subprocess.Popen[str], proc)


def terminate_process(process: subprocess.Popen[str] | None) -> None:
    api = desktop_api()
    if process is None or process.poll() is not None:
        return
    if api.os.name == "nt":
        with contextlib.suppress(*_EXPECTED_TERMINATE_EXCEPTIONS):
            api.subprocess.run(
                ["taskkill", "/PID", str(int(process.pid)), "/T", "/F"],
                stdout=api.subprocess.DEVNULL,
                stderr=api.subprocess.DEVNULL,
                check=False,
                timeout=10,
            )
            process.wait(timeout=5)
            return
    with contextlib.suppress(*_EXPECTED_TERMINATE_EXCEPTIONS):
        process.terminate()
        process.wait(timeout=5)


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
def _isolate_src_package_modules():
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
