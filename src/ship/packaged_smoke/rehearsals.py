"""Compatibility shim over packaged smoke rehearsal leaves."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from . import rehearsal_browser as rehearsal_browser_mod
from . import rehearsal_sync as rehearsal_sync_mod
from . import rehearsal_update as rehearsal_update_mod

root: Any | None = None


def _bind_subroots() -> None:
    rehearsal_sync_mod.root = root
    rehearsal_update_mod.root = root
    rehearsal_browser_mod.root = root


def _root() -> Any:
    if root is None:
        raise RuntimeError("packaged_smoke.rehearsals.root is not configured")
    _bind_subroots()
    return root


def _archive_portable_dir(portable_dir: Path, target_zip: Path) -> Path:
    _root()
    return rehearsal_update_mod._archive_portable_dir(portable_dir, target_zip)


def _inject_desktop_update_public_keys(portable_root: Path, public_keys: dict[str, str]) -> None:
    _root()
    rehearsal_update_mod._inject_desktop_update_public_keys(portable_root, public_keys)


def _portable_current_version(portable_root: Path) -> str:
    _root()
    return rehearsal_sync_mod._portable_current_version(portable_root)


def _portable_packaged_sync_config_path(portable_root: Path) -> Path:
    _root()
    return rehearsal_sync_mod._portable_packaged_sync_config_path(portable_root)


def _load_portable_packaged_sync_rehearsal_config(
    portable_root: Path,
) -> tuple[Path, dict[str, Any], Any]:
    _root()
    return rehearsal_sync_mod._load_portable_packaged_sync_rehearsal_config(portable_root)


def _seed_rehearsal_local_data(data_dir: Path) -> dict[str, Any]:
    _root()
    return rehearsal_update_mod._seed_rehearsal_local_data(data_dir)


class _PackagedSyncRehearsalHandler(rehearsal_sync_mod._PackagedSyncRehearsalHandler):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        _root()
        super().__init__(*args, **kwargs)


def _start_packaged_sync_rehearsal_server(
    *,
    packaged_config: Any,
    snapshot_payload: dict[str, Any],
) -> tuple[str, dict[str, Any], Any, Any]:
    _root()
    return rehearsal_sync_mod._start_packaged_sync_rehearsal_server(
        packaged_config=packaged_config,
        snapshot_payload=snapshot_payload,
    )


class _DesktopUpdateReleaseHandler(rehearsal_update_mod._DesktopUpdateReleaseHandler):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        _root()
        super().__init__(*args, **kwargs)


def _start_desktop_update_release_server(
    *,
    manifest: dict[str, Any],
    portable_zip: Path,
) -> tuple[str, Any, Any]:
    _root()
    return rehearsal_update_mod._start_desktop_update_release_server(
        manifest=manifest,
        portable_zip=portable_zip,
    )


def _wait_for_process_exit(process: Any, *, timeout_s: float) -> None:
    _root()
    rehearsal_update_mod._wait_for_process_exit(process, timeout_s=timeout_s)


def _wait_for_install_handoff_confirmation(
    *,
    bridge_port: int,
    paths: Any,
    process: Any,
    timeout_s: float,
) -> dict[str, Any]:
    _root()
    return rehearsal_update_mod._wait_for_install_handoff_confirmation(
        bridge_port=bridge_port,
        paths=paths,
        process=process,
        timeout_s=timeout_s,
    )


def _wait_for_pid_exit(pid: int, *, timeout_s: float) -> None:
    _root()
    rehearsal_browser_mod._wait_for_pid_exit(pid, timeout_s=timeout_s)


def _wait_for_launcher_exit(process: Any, *, timeout_s: float) -> None:
    _root()
    rehearsal_browser_mod._wait_for_launcher_exit(process, timeout_s=timeout_s)


def _terminate_launcher_process_only(process: Any) -> None:
    _root()
    rehearsal_browser_mod._terminate_launcher_process_only(process)


def _terminate_pid(pid: int, *, label: str, graceful_timeout_s: float = 5.0) -> None:
    _root()
    rehearsal_browser_mod._terminate_pid(
        pid,
        label=label,
        graceful_timeout_s=graceful_timeout_s,
    )


def _wait_for_desktop_ports_released(*ports: int, timeout_s: float) -> None:
    _root()
    rehearsal_browser_mod._wait_for_desktop_ports_released(*ports, timeout_s=timeout_s)


def _run_desktop_lifecycle_node_probe(
    *,
    site_base_url: str,
    bridge_base_url: str,
    artifacts_dir: Path,
    owner_idle_timeout_s: float,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    _root()
    return rehearsal_browser_mod._run_desktop_lifecycle_node_probe(
        site_base_url=site_base_url,
        bridge_base_url=bridge_base_url,
        artifacts_dir=artifacts_dir,
        owner_idle_timeout_s=owner_idle_timeout_s,
        runtime_timeout_s=runtime_timeout_s,
    )


def _run_active_task_close_node_probe(
    *,
    site_base_url: str,
    bridge_base_url: str,
    cdp_port: int,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    _root()
    return rehearsal_browser_mod._run_active_task_close_node_probe(
        site_base_url=site_base_url,
        bridge_base_url=bridge_base_url,
        cdp_port=cdp_port,
        artifacts_dir=artifacts_dir,
        runtime_timeout_s=runtime_timeout_s,
    )


def _run_desktop_lifecycle_close_node_probe(
    *,
    site_base_url: str,
    bridge_base_url: str,
    cdp_port: int,
    browser_pid: int,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    _root()
    return rehearsal_browser_mod._run_desktop_lifecycle_close_node_probe(
        site_base_url=site_base_url,
        bridge_base_url=bridge_base_url,
        cdp_port=cdp_port,
        browser_pid=browser_pid,
        artifacts_dir=artifacts_dir,
        runtime_timeout_s=runtime_timeout_s,
    )


def _wait_for_relaunched_runtime(
    *,
    expected_data_dir: Path,
    expected_version: str,
    timeout_s: float,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    _root()
    return rehearsal_update_mod._wait_for_relaunched_runtime(
        expected_data_dir=expected_data_dir,
        expected_version=expected_version,
        timeout_s=timeout_s,
        env=env,
    )


def _verify_rehearsal_local_data(data_dir: Path, expected: dict[str, Any]) -> None:
    _root()
    rehearsal_update_mod._verify_rehearsal_local_data(data_dir, expected)


def _preferred_desktop_browser_env() -> dict[str, str]:
    _root()
    return rehearsal_browser_mod._preferred_desktop_browser_env()


def _select_packaged_browser_job_browser(
    env: dict[str, str] | None = None,
) -> tuple[dict[str, str], dict[str, str]]:
    _root()
    return rehearsal_browser_mod._select_packaged_browser_job_browser(env)


def _select_browser_shutdown_proof(rows: list[dict[str, Any]]) -> dict[str, Any]:
    _root()
    return rehearsal_browser_mod._select_browser_shutdown_proof(rows)


def _assert_desktop_update_helper_succeeded(
    *,
    paths: Any,
    relaunch_bridge_port: int,
) -> None:
    _root()
    rehearsal_update_mod._assert_desktop_update_helper_succeeded(
        paths=paths,
        relaunch_bridge_port=relaunch_bridge_port,
    )


def run_packaged_sync_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    _root()
    return rehearsal_sync_mod.run_packaged_sync_rehearsal(
        exe_path=exe_path,
        artifacts_dir=artifacts_dir,
        runtime_timeout_s=runtime_timeout_s,
    )


def run_desktop_update_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    _root()
    return rehearsal_update_mod.run_desktop_update_rehearsal(
        exe_path=exe_path,
        artifacts_dir=artifacts_dir,
        runtime_timeout_s=runtime_timeout_s,
    )


def run_packaged_browser_job_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    _root()
    return rehearsal_browser_mod.run_packaged_browser_job_rehearsal(
        exe_path=exe_path,
        artifacts_dir=artifacts_dir,
        runtime_timeout_s=runtime_timeout_s,
    )


def run_packaged_desktop_lifecycle_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    _root()
    return rehearsal_browser_mod.run_packaged_desktop_lifecycle_rehearsal(
        exe_path=exe_path,
        artifacts_dir=artifacts_dir,
        runtime_timeout_s=runtime_timeout_s,
    )


def run_packaged_active_task_close_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    _root()
    return rehearsal_browser_mod.run_packaged_active_task_close_rehearsal(
        exe_path=exe_path,
        artifacts_dir=artifacts_dir,
        runtime_timeout_s=runtime_timeout_s,
    )


def run_packaged_orphan_reclaim_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    _root()
    return rehearsal_browser_mod.run_packaged_orphan_reclaim_rehearsal(
        exe_path=exe_path,
        artifacts_dir=artifacts_dir,
        runtime_timeout_s=runtime_timeout_s,
    )
