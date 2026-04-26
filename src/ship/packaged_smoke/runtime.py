"""Runtime smoke helpers behind the root packaged-smoke facade."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

from . import runtime_node_smoke, runtime_process, runtime_snapshot, runtime_wait

root: Any | None = None


def _root() -> Any:
    if root is None:
        raise RuntimeError("packaged_smoke.runtime.root is not configured")
    return root


def _as_dict(value: Any) -> dict[str, Any]:
    return runtime_wait.as_dict(value)


def _as_list(value: Any) -> list[Any]:
    return runtime_wait.as_list(value)


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
    env: dict[str, str] | None = None,
) -> tuple[subprocess.Popen[Any], Any, Any]:
    return runtime_process.launch_packaged_exe(
        exe_path,
        site_port=site_port,
        bridge_port=bridge_port,
        data_dir=data_dir,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        open_path=open_path,
        startup_probe=startup_probe,
        env=env,
    )


def launch_packaged_command(
    exe_path: Path,
    *,
    args: list[str],
    stdout_path: Path,
    stderr_path: Path,
    env: dict[str, str] | None = None,
) -> tuple[subprocess.Popen[Any], Any, Any]:
    return runtime_process.launch_packaged_command(
        exe_path,
        args=args,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        env=env,
    )


def launch_packaged_desktop_child(
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
    return runtime_process.launch_packaged_desktop_child(
        _root(),
        exe_path,
        mode=mode,
        port=port,
        data_dir=data_dir,
        owner_token=owner_token,
        desktop_session_id=desktop_session_id,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        env=env,
    )


def _local_address_matches_listen_port(local_addr: str, port: int) -> bool:
    return runtime_process.local_address_matches_listen_port(local_addr, port)


def pids_listening_on_tcp_port_windows(port: int) -> set[int]:
    return runtime_process.pids_listening_on_tcp_port_windows(_root(), port)


def cleanup_orphaned_desktop_ports_nt(*ports: int) -> None:
    runtime_process.cleanup_orphaned_desktop_ports_nt(_root(), *ports)


def terminate_process_tree(process: subprocess.Popen[Any] | None) -> None:
    runtime_process.terminate_process_tree(_root(), process)


def terminate_process_only(process: subprocess.Popen[Any] | None) -> None:
    runtime_process.terminate_process_only(_root(), process)


def _packaged_runtime_page_ready(site_base_url: str, open_path: str) -> bool:
    return runtime_wait.packaged_runtime_page_ready(_root(), site_base_url, open_path)


def wait_for_packaged_runtime(
    process: subprocess.Popen[Any],
    *,
    site_base_url: str,
    bridge_base_url: str,
    timeout_s: float,
    open_path: str = "jobs.html",
    required_events: list[str] | tuple[str, ...] = (),
    require_managed_window: bool = False,
    require_page_ready: bool = True,
) -> dict[str, Any]:
    return runtime_wait.wait_for_packaged_runtime(
        _root(),
        process,
        site_base_url=site_base_url,
        bridge_base_url=bridge_base_url,
        timeout_s=timeout_s,
        open_path=open_path,
        required_events=required_events,
        require_managed_window=require_managed_window,
        require_page_ready=require_page_ready,
    )


def wait_for_packaged_runtime_with_port_pivot(
    process: subprocess.Popen[Any],
    *,
    requested_site_port: int,
    requested_bridge_port: int,
    expected_data_dir: Path,
    timeout_s: float,
    open_path: str = "jobs.html",
    required_events: list[str] | tuple[str, ...] = (),
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    return runtime_wait.wait_for_packaged_runtime_with_port_pivot(
        _root(),
        process,
        requested_site_port=requested_site_port,
        requested_bridge_port=requested_bridge_port,
        expected_data_dir=expected_data_dir,
        timeout_s=timeout_s,
        open_path=open_path,
        required_events=required_events,
        env=env,
    )


def wait_for_packaged_child_runtime(
    site_process: subprocess.Popen[Any],
    bridge_process: subprocess.Popen[Any],
    *,
    site_base_url: str,
    bridge_base_url: str,
    owner_token: str,
    timeout_s: float,
) -> dict[str, Any]:
    return runtime_wait.wait_for_packaged_child_runtime(
        _root(),
        site_process,
        bridge_process,
        site_base_url=site_base_url,
        bridge_base_url=bridge_base_url,
        owner_token=owner_token,
        timeout_s=timeout_s,
    )


def capture_runtime_snapshot(bridge_base_url: str, artifacts_dir: Path) -> dict[str, str]:
    return runtime_snapshot.capture_runtime_snapshot(_root(), bridge_base_url, artifacts_dir)


def wait_for_runtime_events(
    bridge_base_url: str, required_events: list[str] | tuple[str, ...], timeout_s: float
) -> list[dict[str, Any]]:
    return runtime_wait.wait_for_runtime_events(
        _root(), bridge_base_url, required_events, timeout_s
    )


def run_embedded_runtime_probe(
    *,
    exe_path: Path,
    probe: dict[str, Any],
    artifacts_root: Path,
    runtime_timeout_s: float,
    startup_probe: bool,
    profile_mode: str,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    return runtime_snapshot.run_embedded_runtime_probe(
        _root(),
        exe_path=exe_path,
        probe=probe,
        artifacts_root=artifacts_root,
        runtime_timeout_s=runtime_timeout_s,
        startup_probe=startup_probe,
        profile_mode=profile_mode,
        env=env,
    )


def parse_packaged_node_smoke_report(path: Path) -> list[dict[str, Any]]:
    return runtime_node_smoke.parse_packaged_node_smoke_report(path)


def read_packaged_node_smoke_payload(path: Path) -> dict[str, Any]:
    return runtime_node_smoke.read_packaged_node_smoke_payload(path)


def run_packaged_node_smoke(
    *,
    requested_exe_path: Path,
    exe_path: Path,
    site_base_url: str,
    bridge_base_url: str,
    artifacts_dir: Path,
    node_smoke_script: Path,
    headed: bool,
    pause_on_failure: bool,
    timeout_s: float,
) -> dict[str, Any]:
    return runtime_node_smoke.run_packaged_node_smoke(
        _root(),
        requested_exe_path=requested_exe_path,
        exe_path=exe_path,
        site_base_url=site_base_url,
        bridge_base_url=bridge_base_url,
        artifacts_dir=artifacts_dir,
        node_smoke_script=node_smoke_script,
        headed=headed,
        pause_on_failure=pause_on_failure,
        timeout_s=timeout_s,
    )


def build_failure_payload(
    step: str, error: Exception | str, *, category: str = ""
) -> dict[str, Any]:
    return runtime_snapshot.build_failure_payload(step, error, category=category)


def run_warmup_launch(
    exe_path: Path,
    *,
    artifacts_root: Path,
    open_path: str,
    runtime_timeout_s: float,
    startup_probe: bool,
    env: dict[str, str] | None = None,
) -> None:
    runtime_snapshot.run_warmup_launch(
        _root(),
        exe_path,
        artifacts_root=artifacts_root,
        open_path=open_path,
        runtime_timeout_s=runtime_timeout_s,
        startup_probe=startup_probe,
        env=env,
    )
