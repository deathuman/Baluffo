from __future__ import annotations

from pathlib import Path

from ._compat import desktop_api
from .config import ALREADY_RUNNING_ERROR, DesktopRuntimeConfig


def _runtime_ports_need_retry(config: DesktopRuntimeConfig) -> bool:
    return (not bool(config.site_port_explicit)) or (not bool(config.bridge_port_explicit))


def _should_retry_runtime_launch(
    config: DesktopRuntimeConfig,
    exc: Exception,
    *,
    site_process=None,
    bridge_process=None,
) -> bool:
    if not _runtime_ports_need_retry(config):
        return False
    if "already in use" in str(exc).strip().lower():
        return True
    if site_process is not None and site_process.poll() is not None:
        return True
    if bridge_process is not None and bridge_process.poll() is not None:
        return True
    return False


def _lock_reclaim_callback(data_dir: Path):
    api = desktop_api()
    return lambda reason: api._append_startup_trace(
        data_dir,
        "desktop_lock_reclaimed",
        reason=api._truncate_reason(reason),
    )


def acquire_runtime_instance_lock(config: DesktopRuntimeConfig, *, launcher_token: str):
    api = desktop_api()
    instance_lock = api.acquire_instance_lock(
        launcher_token=launcher_token,
        on_reclaim=_lock_reclaim_callback(config.data_dir),
    )
    if instance_lock is not None:
        return instance_lock

    diagnosis = api.diagnose_instance_conflict(data_dir=config.data_dir)
    action = str(diagnosis.get("action") or "")
    if action == "active":
        existing_session = diagnosis.get("session") if isinstance(diagnosis.get("session"), dict) else {}
        api._append_startup_trace(
            config.data_dir,
            "desktop_session_reused",
            bridgePort=int(existing_session.get("bridgePort") or 0),
            reason="instance_lock_contended",
        )
        api._trace_already_running_rejection(
            data_dir=config.data_dir,
            detection="instance_lock_contended",
            launcher_token=launcher_token,
            existing_session=existing_session,
        )
        raise RuntimeError(ALREADY_RUNNING_ERROR)
    if action == "blocked":
        target = str(diagnosis.get("target") or "desktop runtime").strip() or "desktop runtime"
        raise RuntimeError(
            f"Baluffo found a stale {target} process but could not terminate it. "
            "Please close it manually and retry."
        )
    if action in {"reclaimed", "retry"}:
        instance_lock = api.acquire_instance_lock(
            launcher_token=launcher_token,
            on_reclaim=_lock_reclaim_callback(config.data_dir),
        )
    if instance_lock is None:
        raise RuntimeError(
            "Baluffo is already starting in another process. Please retry in a few seconds."
        )
    return instance_lock


def reconcile_session_state_before_launch(
    config: DesktopRuntimeConfig,
    *,
    launcher_token: str,
    instance_lock,
) -> None:
    api = desktop_api()
    existing_session = api.get_valid_session_state(
        expected_launcher_token=str(instance_lock.launcher_token or launcher_token),
        clear_invalid=False,
    )
    if existing_session:
        api._append_startup_trace(
            config.data_dir,
            "desktop_session_reused",
            bridgePort=int(existing_session.get("bridgePort") or 0),
        )
        api._trace_already_running_rejection(
            data_dir=config.data_dir,
            detection="valid_session_state",
            launcher_token=launcher_token,
            existing_session=existing_session,
        )
        raise RuntimeError(ALREADY_RUNNING_ERROR)

    raw_session_state = api.load_session_state()
    if not raw_session_state:
        return

    session_ok, reason = api.validate_session_state(
        raw_session_state,
        expected_launcher_token=str(instance_lock.launcher_token or launcher_token),
    )
    if session_ok:
        return

    api._append_startup_trace(
        config.data_dir,
        "desktop_session_invalid_reason",
        reason=api._truncate_reason(reason),
    )
    reclaim_result = api._reclaim_stale_instance_artifacts(
        data_dir=config.data_dir,
        stale_state=raw_session_state,
    )
    if bool(reclaim_result.get("blocked")):
        target = str(reclaim_result.get("target") or "desktop runtime").strip()
        blocked_reason = str(reclaim_result.get("reason") or "stale_runtime_cleanup_failed")
        api._append_startup_trace(
            config.data_dir,
            "desktop_lock_reclaim_failed",
            reason=api._truncate_reason(blocked_reason),
            target=target,
        )
        raise RuntimeError(
            f"Baluffo found a stale {target or 'desktop runtime'} process but could not terminate it. "
            "Please close it manually and retry."
        )


def cleanup_runtime_launch(
    *,
    instance_lock,
    session_state_written: bool,
    desktop_job: int | None,
    browser_process,
    bridge_process,
    site_process,
) -> None:
    api = desktop_api()
    api.terminate_process(bridge_process)
    api.terminate_process(site_process)
    api.terminate_process(browser_process)
    api._windows_close_desktop_job(desktop_job)
    api.release_instance_lock(instance_lock)
    if session_state_written:
        api.clear_session_state()
