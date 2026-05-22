from __future__ import annotations

"""Process and port helpers for packaged-smoke runtime checks."""

import subprocess
from pathlib import Path
from typing import Any, cast


def launch_packaged_exe(
    exe_path: Path,
    *,
    site_port: int,
    bridge_port: int,
    data_dir: Path,
    stdout_path: Path,
    stderr_path: Path,
    open_path: str = "jobs.html",
    startup_probe: bool = False,
    owner_idle_timeout_s: float = 0.0,
    env: dict[str, str] | None = None,
) -> tuple[subprocess.Popen[Any], Any, Any]:
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    stdout_handle = stdout_path.open("wb")
    stderr_handle = stderr_path.open("wb")
    command = [
        str(exe_path),
        "--site-port",
        str(int(site_port)),
        "--bridge-port",
        str(int(bridge_port)),
        "--data-dir",
        str(data_dir),
        "--open-path",
        str(open_path or "jobs.html"),
    ]
    if startup_probe:
        command.append("--startup-probe")
    if float(owner_idle_timeout_s or 0.0) > 0.0:
        command.extend(["--owner-idle-timeout-s", str(float(owner_idle_timeout_s))])
    process = subprocess.Popen(
        command,
        cwd=exe_path.parent,
        stdout=stdout_handle,
        stderr=stderr_handle,
        env=env,
    )
    return process, stdout_handle, stderr_handle


def launch_packaged_command(
    exe_path: Path,
    *,
    args: list[str],
    stdout_path: Path,
    stderr_path: Path,
    env: dict[str, str] | None = None,
) -> tuple[subprocess.Popen[Any], Any, Any]:
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    stdout_handle = stdout_path.open("wb")
    stderr_handle = stderr_path.open("wb")
    process = subprocess.Popen(
        [str(exe_path), *list(args)],
        cwd=exe_path.parent,
        stdout=stdout_handle,
        stderr=stderr_handle,
        env=env,
    )
    return process, stdout_handle, stderr_handle


def launch_packaged_desktop_child(
    deps: Any,
    exe_path: Path,
    *,
    mode: str,
    port: int,
    data_dir: Path | None = None,
    owner_token: str = "",
    desktop_session_id: str = "",
    stdout_path: Path,
    stderr_path: Path,
    env: dict[str, str] | None = None,
) -> tuple[subprocess.Popen[Any], Any, Any]:
    normalized = str(mode or "").strip().lower()
    portable_root = exe_path.parent.resolve()
    ship_root = portable_root / "ship"
    if normalized == "site":
        args = [
            "__child_site__",
            "--root",
            str(ship_root),
            "--port",
            str(int(port)),
            "--desktop-runtime",
        ]
    elif normalized == "bridge":
        args = [
            "__child_bridge__",
            "--root",
            str(ship_root),
            "--bind-host",
            "127.0.0.1",
            "--port",
            str(int(port)),
            "--data-dir",
            str(data_dir or (ship_root / "data")),
            "--desktop-runtime",
            "--owner-mode",
            "desktop-window",
            "--owner-token",
            str(owner_token or ""),
            "--desktop-session-id",
            str(desktop_session_id or ""),
            "--started-by",
            "packaged-orphan-reclaim",
            "--owner-idle-timeout-s",
            "600.0",
        ]
    else:
        raise ValueError(f"Unsupported packaged child mode: {mode}")
    return cast(
        tuple[subprocess.Popen[Any], Any, Any],
        deps.launch_packaged_command(
            exe_path,
            args=args,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            env=env,
        ),
    )


def local_address_matches_listen_port(local_addr: str, port: int) -> bool:
    token = str(local_addr or "").strip()
    if not token:
        return False
    suffix = f":{int(port)}"
    return token.endswith(suffix)


def pids_listening_on_tcp_port_windows(deps: Any, port: int) -> set[int]:
    pids: set[int] = set()
    if deps.os.name != "nt":
        return pids
    try:
        completed = deps.subprocess.run(
            ["netstat", "-ano", "-p", "tcp"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except OSError:
        return pids
    text = str(completed.stdout or "")
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 5 or str(parts[0]).upper() != "TCP":
            continue
        local_field = parts[1]
        state = str(parts[3]).upper()
        if state != "LISTENING":
            continue
        pid_field = parts[-1]
        if not deps._local_address_matches_listen_port(local_field, port):
            continue
        try:
            pid = int(pid_field)
        except ValueError:
            continue
        if pid > 0:
            pids.add(pid)
    return pids


def cleanup_orphaned_desktop_ports_nt(deps: Any, *ports: int) -> None:
    if deps.os.name != "nt":
        return
    own = int(deps.os.getpid())
    seen: set[int] = set()
    for raw in ports:
        port = int(raw)
        if port <= 0 or port > 65535:
            continue
        for pid in deps.pids_listening_on_tcp_port_windows(port):
            if pid == own or pid in seen:
                continue
            seen.add(pid)
            deps.subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                stdout=deps.subprocess.DEVNULL,
                stderr=deps.subprocess.DEVNULL,
                check=False,
            )


def terminate_process_tree(deps: Any, process: subprocess.Popen[Any] | None) -> None:
    if process is None or process.poll() is not None:
        return
    if deps.os.name == "nt":
        deps.subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            stdout=deps.subprocess.DEVNULL,
            stderr=deps.subprocess.DEVNULL,
            check=False,
        )
    else:
        process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def terminate_process_only(deps: Any, process: subprocess.Popen[Any] | None) -> None:
    if process is None or process.poll() is not None:
        return
    if deps.os.name == "nt":
        deps.subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/F"],
            stdout=deps.subprocess.DEVNULL,
            stderr=deps.subprocess.DEVNULL,
            check=False,
        )
    else:
        process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(
            "Packaged launcher did not exit after launcher-only termination."
        ) from exc
