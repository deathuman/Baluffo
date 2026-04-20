from __future__ import annotations

import contextlib
import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

from ._compat import desktop_api


def _entry_command() -> list[str]:
    api = desktop_api()
    if getattr(api.sys, "frozen", False):
        return [api.sys.executable]
    return [api.sys.executable, str(Path(api.__file__).resolve())]


def build_child_command(
    mode: str,
    *,
    root: Path,
    port: int,
    bridge_host: str = "127.0.0.1",
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
    popen_kwargs: dict[str, object] = {
        "stdout": api.subprocess.DEVNULL,
        "stderr": api.subprocess.DEVNULL,
        "text": True,
    }
    if extra_env:
        env = api.os.environ.copy()
        env.update({key: str(value) for key, value in extra_env.items()})
        popen_kwargs["env"] = env
    if api.os.name == "nt":
        popen_kwargs["creationflags"] = int(getattr(api.subprocess, "CREATE_NO_WINDOW", 0)) | int(
            getattr(api.subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        )
        popen_kwargs["close_fds"] = True
    proc = api.subprocess.Popen(list(command), **popen_kwargs)
    if job_handle and proc.pid:
        try:
            api._windows_try_assign_pid_to_job(job_handle, int(proc.pid))
        except OSError:
            api.terminate_process(proc)
            raise
    return proc


def terminate_process(process: subprocess.Popen[str] | None) -> None:
    api = desktop_api()
    if process is None or process.poll() is not None:
        return
    if api.os.name == "nt":
        with contextlib.suppress(Exception):  # noqa: BLE001
            api.subprocess.run(
                ["taskkill", "/PID", str(int(process.pid)), "/T", "/F"],
                stdout=api.subprocess.DEVNULL,
                stderr=api.subprocess.DEVNULL,
                check=False,
                timeout=10,
            )
            process.wait(timeout=5)
            return
    with contextlib.suppress(Exception):  # noqa: BLE001
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
def _isolated_src_package():
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
