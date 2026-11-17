#!/usr/bin/env python3
"""Install, recovery, and startup verification helpers.

Side effects: install mutation, rollback snapshot, relaunch verification.

AI boundary owns: desktop updater install, rollback, marker handling, and post-install startup verification.
AI boundary implement in: this file for install mutation; update service state and helper launch stay in desktop update siblings.
AI boundary search before contracts: desktop update service, updater entrypoint, packaged update rehearsal, and installer tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused desktop updater install tests.
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import signal
import subprocess
import time as _time
import urllib.error
import uuid
import zipfile as _zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from src.shared.coerce import as_dict as _as_dict
from src.ship.desktop_artifact_paths import ARTIFACT_KIND_APPIMAGE
from src.ship.desktop_update_shared import (
    DesktopUpdatePaths,
)
from src.ship.desktop_update_shared import (
    fetch_json as _fetch_json,
)
from src.ship.desktop_update_shared import (
    install_stage_label as _install_stage_label,
)
from src.ship.desktop_update_shared import (
    iso_now as _iso_now,
)
from src.ship.desktop_update_shared import (
    pid_is_running as _pid_is_running,
)
from src.ship.desktop_update_shared import (
    terminal_install_stage as _terminal_install_stage,
)
from src.ship.desktop_update_state import (
    clear_handoff_request as _clear_handoff_request,
)
from src.ship.desktop_update_state import (
    clear_success_marker as _clear_success_marker,
)
from src.ship.desktop_update_state import (
    load_status as _load_status,
)
from src.ship.desktop_update_state import (
    save_status as _save_status,
)
from src.ship.desktop_update_state import (
    validate_install_plan as _validate_install_plan,
)
from src.ship.desktop_updater_release import (
    _classify_install_failure as _classify_install_failure_impl,
)
from src.ship.desktop_updater_release import (
    _ensure_verified_zip_for_install as _ensure_verified_zip_for_install_impl,
)
from src.ship.desktop_updater_release import (
    _recover_manifest_for_install as _recover_manifest_for_install_impl,
)
from src.ship.desktop_updater_ui import (
    NullProgressWindow as _NullProgressWindow,
)
from src.ship.desktop_updater_ui import (
    _helper_relaunch_verify_timeout_s as _helper_relaunch_verify_timeout_s_impl,
)
from src.ship.desktop_updater_ui import (
    _launch_executable as _launch_executable_impl,
)
from src.ship.update_manager_apply import (
    create_data_backup as _create_data_backup,
)
from src.ship.update_manager_apply import (
    restore_data_backup as _restore_data_backup,
)
from src.ship.update_manager_apply import (
    run_migrations as _run_migrations,
)
from src.ship.update_manager_paths import ShipPaths as _ShipPaths

MUTATING_INSTALL_STAGES = frozenset(
    {
        "replacing",
        "migrating",
        "relaunching",
        "verifying",
        "rolling_back",
    }
)
SUCCESS_RECOVERY_STAGES = frozenset({"relaunching", "verifying"})
TERMINAL_INSTALL_REPAIR_MODES = ("restore-snapshot", "accept-install")

# Preserve these names on the module root for desktop_updater.py facade compatibility.
time = _time
zipfile = _zipfile
update_manager = SimpleNamespace(
    ShipPaths=_ShipPaths,
    create_data_backup=_create_data_backup,
    restore_data_backup=_restore_data_backup,
    run_migrations=_run_migrations,
)
fetch_json = _fetch_json
install_stage_label = _install_stage_label
iso_now = _iso_now
pid_is_running = _pid_is_running
clear_handoff_request = _clear_handoff_request
clear_success_marker = _clear_success_marker
load_status = _load_status
save_status = _save_status
validate_install_plan = _validate_install_plan
_classify_install_failure = _classify_install_failure_impl
_ensure_verified_zip_for_install = _ensure_verified_zip_for_install_impl
_recover_manifest_for_install = _recover_manifest_for_install_impl
NullProgressWindow = _NullProgressWindow
_helper_relaunch_verify_timeout_s = _helper_relaunch_verify_timeout_s_impl
_launch_executable = _launch_executable_impl


def _save_install_stage_status(
    paths: DesktopUpdatePaths,
    *,
    install_state: str,
    install_stage: str,
    **extra: Any,
) -> dict[str, Any]:
    status = load_status(paths)
    status.update(
        {
            "installState": str(install_state or "").strip().lower() or "idle",
            "installStage": str(install_stage or "").strip().lower() or "idle",
            "installStageLabel": install_stage_label(install_state, install_stage),
            "helperUpdatedAt": iso_now(),
        }
    )
    status.update(extra)
    return _as_dict(save_status(paths, status))


def _wait_for_launcher_exit(plan: dict[str, Any], *, timeout_s: float = 120.0) -> None:
    deadline = _time.monotonic() + max(5.0, float(timeout_s))
    launcher_pid = int(plan.get("launcherPid") or 0)
    session_root = Path(str(plan.get("desktopSessionRoot") or "")).expanduser().resolve()
    session_state_path = session_root / "desktop-session.json"
    while _time.monotonic() < deadline:
        launcher_alive = pid_is_running(launcher_pid)
        if launcher_pid > 0 and launcher_alive:
            _time.sleep(0.5)
            continue
        if session_state_path.exists():
            _time.sleep(0.5)
            continue
        return
    raise RuntimeError("Timed out waiting for the desktop launcher to exit.")


def _files_under(root: Path) -> list[Path]:
    return [path for path in root.rglob("*") if path.is_file()]


def _copy_tree_files(source_root: Path, target_root: Path, *, skip_data: bool = False) -> int:
    copied = 0
    for source in _files_under(source_root):
        rel = source.relative_to(source_root)
        if skip_data and rel.as_posix().startswith("ship/data/"):
            continue
        target = target_root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        copied += 1
    return copied


def _copy_install_snapshot(install_root: Path, rollback_root: Path) -> None:
    snapshot_root = rollback_root / "runtime"
    if snapshot_root.exists():
        shutil.rmtree(snapshot_root)
    snapshot_root.mkdir(parents=True, exist_ok=True)
    _copy_tree_files(install_root, snapshot_root, skip_data=True)


def _restore_install_snapshot(
    install_root: Path,
    rollback_root: Path,
    *,
    expected_version: str = "",
) -> dict[str, Any]:
    """Put the rollback snapshot back and report what came back.

    Locked files are not suppressed: restoring over a live install has to fail
    loudly instead of leaving a half-restored tree marked as rolled back.
    """
    snapshot_root = rollback_root / "runtime"
    if not snapshot_root.is_dir():
        raise RuntimeError(f"Install rollback snapshot is missing: {snapshot_root}")
    pointer_rel = Path("ship") / "app" / "current.txt"
    snapshot_pointer = snapshot_root / pointer_rel
    if not snapshot_pointer.is_file():
        raise RuntimeError(
            f"Install rollback snapshot is incomplete: {snapshot_pointer} is missing"
        )
    snapshot_version = snapshot_pointer.read_text(encoding="utf-8").strip()
    if not snapshot_version:
        raise RuntimeError(f"Install rollback snapshot is incomplete: {snapshot_pointer} is empty")
    required_version = str(expected_version or "").strip()
    if required_version and snapshot_version != required_version:
        raise RuntimeError(
            "Install rollback snapshot version does not match the requested repair "
            f"(expected {required_version!r}, found {snapshot_version!r})"
        )
    if not (snapshot_root / "Baluffo.exe").is_file():
        raise RuntimeError(
            f"Install rollback snapshot is incomplete: {snapshot_root / 'Baluffo.exe'} is missing"
        )
    for path in _files_under(install_root):
        rel = path.relative_to(install_root)
        if rel.as_posix().startswith("ship/data/"):
            continue
        path.unlink(missing_ok=True)
    restored_files = _copy_tree_files(snapshot_root, install_root)
    restored_version = (install_root / pointer_rel).read_text(encoding="utf-8").strip()
    if restored_version != snapshot_version:
        raise RuntimeError(
            "Install rollback did not restore ship/app/current.txt "
            f"(expected {snapshot_version!r}, found {restored_version!r})"
        )
    return {"files": restored_files, "currentVersion": restored_version}


def _session_launcher_pid(plan: dict[str, Any]) -> int:
    session_root_text = str(plan.get("desktopSessionRoot") or "").strip()
    if not session_root_text:
        return 0
    session_path = Path(session_root_text).expanduser() / "desktop-session.json"
    try:
        payload = _as_dict(json.loads(session_path.read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError):
        return 0
    try:
        return int(payload.get("launcherPid") or 0)
    except (TypeError, ValueError):
        return 0


def _terminate_process_tree(pid: int) -> None:
    """Kill a launcher and its children; a live install locks the files we restore."""
    if os.name == "nt":
        with contextlib.suppress(OSError, subprocess.SubprocessError):
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                check=False,
                timeout=20,
            )
        return
    with contextlib.suppress(OSError, ProcessLookupError, PermissionError):
        os.kill(pid, signal.SIGTERM)


def _stop_relaunched_app(plan: dict[str, Any], *, timeout_s: float = 20.0) -> dict[str, Any]:
    """Stop the app an install just launched before a rollback touches its files.

    Readiness timeouts fire while the new app is still running. Restoring over a
    live install hits locked files and leaves the tree half-restored, which is
    how a failed update became a gutted install.
    """
    pid = _session_launcher_pid(plan)
    result: dict[str, Any] = {"pid": pid, "terminated": False, "aliveAfter": False}
    if pid <= 0:
        return result
    _terminate_process_tree(pid)
    deadline = _time.monotonic() + max(1.0, float(timeout_s))
    while _time.monotonic() < deadline and pid_is_running(pid):
        _time.sleep(0.2)
    result["aliveAfter"] = pid_is_running(pid)
    result["terminated"] = not result["aliveAfter"]
    return result


# install mutation: deletes files not in new extract, copies extracted content
def _sync_extract_to_install(install_root: Path, extracted_root: Path) -> None:
    target_paths = {
        path.relative_to(extracted_root).as_posix() for path in _files_under(extracted_root)
    }
    for path in _files_under(install_root):
        rel_text = path.relative_to(install_root).as_posix()
        if rel_text.startswith("ship/data/"):
            continue
        if rel_text not in target_paths:
            path.unlink(missing_ok=True)
    for path in extracted_root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(extracted_root)
        if rel.as_posix().startswith("ship/data/"):
            continue
        target = install_root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)


def _verify_target_startup(plan: dict[str, Any], *, timeout_s: float = 90.0) -> None:
    session_root = Path(str(plan.get("desktopSessionRoot") or "")).expanduser().resolve()
    session_state_path = session_root / "desktop-session.json"
    install_root = Path(str(plan.get("installRoot") or "")).expanduser().resolve()
    data_dir = Path(str(plan.get("dataDir") or "")).expanduser().resolve()
    success_marker = DesktopUpdatePaths.from_data_dir(
        data_dir,
        install_root=install_root,
        ship_root=install_root / "ship",
    ).success_marker_path
    target_version = str(plan.get("targetVersion") or "").strip()
    deadline = _time.monotonic() + max(10.0, float(timeout_s))
    while _time.monotonic() < deadline:
        if not session_state_path.exists():
            _time.sleep(1.0)
            continue
        try:
            session_state = _as_dict(json.loads(session_state_path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError):
            _time.sleep(1.0)
            continue
        bridge_port = int(session_state.get("bridgePort") or 0)
        if bridge_port <= 0:
            _time.sleep(1.0)
            continue
        try:
            health = fetch_json(f"http://127.0.0.1:{bridge_port}/ops/health", timeout_s=5.0)
        except (OSError, ValueError, urllib.error.URLError, json.JSONDecodeError):
            _time.sleep(1.0)
            continue
        if (
            isinstance(health, dict)
            and str(health.get("service") or "") == "baluffo-bridge"
            and bool(health.get("desktopMode"))
            and bool(health.get("startupReady"))
            and str(health.get("appVersion") or "").strip() == target_version
            and success_marker.exists()
        ):
            return
        _time.sleep(1.0)
    raise RuntimeError("Updated desktop app did not report startup readiness in time.")


def _migration_backup_ref(status: dict[str, Any]) -> Path | None:
    backup_ref_text = str(status.get("migrationBackupPath") or "").strip()
    if not backup_ref_text:
        return None
    backup_ref = Path(backup_ref_text).expanduser().resolve()
    if not backup_ref.exists():
        raise RuntimeError(f"Migration backup is missing: {backup_ref}")
    return backup_ref


def _restore_previous_install(
    *,
    plan: dict[str, Any],
    paths: DesktopUpdatePaths,
    rollback_root: Path,
    backup_ref: Path | None,
) -> dict[str, Any]:
    """Stop the relaunched app, then put the data and install files back.

    Both restores run after the stop: the readiness timeout fires while the new
    app is still running, and a live install holds the files it restores.
    """
    stopped = _stop_relaunched_app(plan)
    if bool(stopped.get("aliveAfter")):
        raise RuntimeError(
            "Relaunched app did not stop "
            f"(pid {int(stopped.get('pid') or 0)}); refusing to restore over a live install."
        )
    if backup_ref is not None:
        try:
            _restore_data_backup(
                _ShipPaths.from_root(paths.ship_root, data_dir=paths.data_dir),
                backup_ref,
            )
        except (OSError, _zipfile.BadZipFile, _zipfile.LargeZipFile) as exc:
            raise RuntimeError(f"Data backup restore failed: {exc}") from exc
    restored = _restore_install_snapshot(paths.install_root, rollback_root)
    return {
        **restored,
        "stoppedPid": int(stopped.get("pid") or 0),
        "stopVerified": bool(stopped.get("terminated")),
    }


def remove_rollback_path(rollback_path: Path) -> None:
    """Drop a rollback snapshot, whichever shape the platform keeps it in."""
    with contextlib.suppress(OSError, shutil.Error):
        if rollback_path.is_dir():
            shutil.rmtree(rollback_path)
        elif rollback_path.exists():
            rollback_path.unlink()


def _finalize_success(
    paths: DesktopUpdatePaths, plan: dict[str, Any], rollback_root: Path
) -> dict[str, Any]:
    remove_rollback_path(rollback_root)
    clear_success_marker(paths)
    return _as_dict(
        _save_install_stage_status(
            paths,
            install_state="installed",
            install_stage="installed",
            downloadState="idle",
            downloadedBytes=0,
            totalBytes=0,
            downloadPercent=0,
            lastError="",
            lastCheckedAt=iso_now(),
            migrationBackupPath="",
            rollbackPath="",
            targetVersion=str(plan.get("targetVersion") or ""),
        )
    )


def _recover_interrupted_install(
    plan: dict[str, Any],
    *,
    paths: DesktopUpdatePaths,
    rollback_root: Path,
) -> bool:
    status = load_status(paths)
    stage = str(status.get("installStage") or "").strip().lower()
    if not stage or stage in {
        "idle",
        "preparing",
        "waiting_for_exit",
        "extracting",
        "snapshotting",
        "backup",
    }:
        return False
    if stage in SUCCESS_RECOVERY_STAGES:
        try:
            _verify_target_startup(plan, timeout_s=5.0)
        except (OSError, RuntimeError, ValueError):
            pass
        else:
            _finalize_success(paths, plan, rollback_root)
            return True
    if stage not in MUTATING_INSTALL_STAGES:
        return False
    _save_install_stage_status(
        paths,
        install_state="installing",
        install_stage="recovering",
        lastError="",
    )
    try:
        _restore_previous_install(
            plan=plan,
            paths=paths,
            rollback_root=rollback_root,
            backup_ref=_migration_backup_ref(status),
        )
    except (OSError, RuntimeError, ValueError, shutil.Error) as exc:
        _save_install_stage_status(
            paths,
            install_state="failed",
            install_stage="rollback_failed",
            lastError=f"install_restore_failed: {exc}",
            rollbackPath=str(rollback_root),
            rollbackVerified=False,
        )
        raise RuntimeError(f"Install recovery failed: {exc}") from exc
    remove_rollback_path(rollback_root)
    _save_install_stage_status(
        paths,
        install_state="idle",
        install_stage="idle",
        lastError="",
        migrationBackupPath="",
        rollbackPath="",
    )
    return False


def _rollback_failed_install(
    *,
    plan: dict[str, Any],
    paths: DesktopUpdatePaths,
    rollback_root: Path,
    backup_ref: Path | None,
    failure: BaseException,
    mutation_started: bool,
) -> None:
    """Undo an install that failed, and say so when the undo itself fails."""
    clear_handoff_request(paths)
    classified_error = _classify_install_failure_impl(failure)
    backup_text = str(backup_ref) if backup_ref is not None else ""
    if not mutation_started:
        _save_install_stage_status(
            paths,
            install_state="failed",
            install_stage="failed",
            lastError=classified_error,
            rollbackPath=str(rollback_root),
            migrationBackupPath=backup_text,
        )
        return
    _save_install_stage_status(
        paths,
        install_state="failed",
        install_stage="rolling_back",
        lastError=classified_error,
        rollbackPath=str(rollback_root),
        migrationBackupPath=backup_text,
    )
    try:
        restored = _restore_previous_install(
            plan=plan,
            paths=paths,
            rollback_root=rollback_root,
            backup_ref=backup_ref,
        )
    except (OSError, RuntimeError, ValueError, shutil.Error) as exc:
        _save_install_stage_status(
            paths,
            install_state="failed",
            install_stage="rollback_failed",
            lastError=f"{classified_error}; install_restore_failed: {exc}",
            rollbackPath=str(rollback_root),
            migrationBackupPath=backup_text,
            rollbackVerified=False,
        )
        return
    relaunch_error = ""
    try:
        _launch_executable(paths.install_root / "Baluffo.exe", data_dir=paths.data_dir)
    except (OSError, RuntimeError) as exc:
        relaunch_error = str(exc)
    _save_install_stage_status(
        paths,
        install_state="failed",
        install_stage="rolled_back",
        lastError=classified_error,
        rollbackPath=str(rollback_root),
        migrationBackupPath=backup_text,
        rollbackVerified=True,
        rollbackFiles=int(restored.get("files") or 0),
        rollbackVersion=str(restored.get("currentVersion") or ""),
        rollbackStoppedPid=int(restored.get("stoppedPid") or 0),
        rollbackStopVerified=bool(restored.get("stopVerified")),
        rollbackRelaunchError=relaunch_error,
    )


def _refuse_terminal_install(paths: DesktopUpdatePaths, rollback_root: Path) -> None:
    """A terminal failure is an install-entry barrier, not a recovery detail.

    It has to run before the AppImage dispatch and before staging, so no backend
    installs over the failed tree or replaces the retained snapshot.
    """
    status = load_status(paths)
    stage = _terminal_install_stage(status.get("installState"), status.get("installStage"))
    if not stage:
        return
    retained_text = str(status.get("rollbackPath") or "").strip()
    retained_root = Path(retained_text).expanduser().resolve() if retained_text else rollback_root
    raise RuntimeError(
        "Previous install rollback failed; refusing to retry. "
        f"The failed install and rollback snapshot at {retained_root} are left untouched. "
        "Repair the desktop install before trying again."
    )


def _save_terminal_repair_status(
    paths: DesktopUpdatePaths,
    *,
    mode: str,
    previous_stage: str,
    previous_error: str,
    version: str,
    snapshot_path: Path,
    snapshot_removed: bool,
    verified: bool,
) -> dict[str, Any]:
    return _as_dict(
        _save_install_stage_status(
            paths,
            install_state="idle",
            install_stage="idle",
            lastError="",
            migrationBackupPath="",
            rollbackPath="",
            rollbackVerified=bool(verified),
            rollbackVersion=str(version or ""),
            repairState="repaired",
            repairAvailable=False,
            repairAction=str(mode or ""),
            repairError="",
            repairCompletedAt=iso_now(),
            lastRepair={
                "at": iso_now(),
                "action": mode,
                "previousInstallStage": previous_stage,
                "previousError": previous_error,
                "version": str(version or ""),
                "snapshotPath": str(snapshot_path),
                "snapshotRemoved": bool(snapshot_removed),
            },
        )
    )


def save_repair_failure_status(paths: DesktopUpdatePaths, error: str) -> dict[str, Any]:
    """Keep the terminal rollback barrier while reporting why the repair failed."""
    status = load_status(paths)
    status.update(
        {
            "repairState": "repair_failed",
            "repairError": str(error or "").strip() or "desktop_install_repair_failed",
            "repairFailedAt": iso_now(),
            "helperUpdatedAt": iso_now(),
        }
    )
    return _as_dict(save_status(paths, status))


_REPAIR_OPERATIONAL_ERRORS = (
    RuntimeError,
    OSError,
    TypeError,
    ValueError,
    json.JSONDecodeError,
    shutil.Error,
    urllib.error.URLError,
    _zipfile.BadZipFile,
    _zipfile.LargeZipFile,
)


def _repair_plan_settings(args: Any) -> tuple[Path | None, DesktopUpdatePaths, dict[str, Any]]:
    plan_text = str(getattr(args, "repair_plan", "") or "").strip()
    plan_path = Path(plan_text).expanduser().resolve() if plan_text else None
    plan = (
        _as_dict(json.loads(plan_path.read_text(encoding="utf-8"))) if plan_path is not None else {}
    )
    data_dir_text = str(plan.get("dataDir") or getattr(args, "data_dir", "") or "").strip()
    install_root_text = str(
        plan.get("installRoot") or getattr(args, "install_root", "") or ""
    ).strip()
    if not data_dir_text or not install_root_text:
        raise RuntimeError("Repair needs --data-dir and --install-root.")
    paths = DesktopUpdatePaths.from_data_dir(
        Path(data_dir_text),
        install_root=Path(install_root_text),
    )
    return plan_path, paths, plan


def _clear_repair_handoff(paths: DesktopUpdatePaths, plan_path: Path | None) -> None:
    if plan_path is None:
        return
    with contextlib.suppress(OSError):
        plan_path.unlink()
    with contextlib.suppress(OSError):
        paths.repair_request_path.unlink()
    _clear_handoff_request(paths)


def _relaunch_repaired_app(paths: DesktopUpdatePaths, launch_func: Any) -> str:
    try:
        launch_func(
            paths.install_root / "Baluffo.exe",
            clear_app_version_override=True,
            data_dir=paths.data_dir,
        )
    except (OSError, RuntimeError) as exc:
        return str(exc)
    return ""


def _repair_failure_result(
    exc: Exception,
    *,
    paths: DesktopUpdatePaths | None,
    from_panel: bool,
    relaunch: bool,
    launcher_stopped: bool,
    launch_func: Any,
    show_func: Any,
    failure_status_func: Any,
) -> int:
    request_path = getattr(paths, "repair_request_path", None)
    plan_path = getattr(paths, "repair_plan_path", None)
    if request_path is not None:
        with contextlib.suppress(OSError):
            request_path.unlink()
    if plan_path is not None:
        with contextlib.suppress(OSError):
            plan_path.unlink()
    if paths is not None and from_panel:
        with contextlib.suppress(OSError, RuntimeError, ValueError):
            failure_status_func(paths, str(exc))
    relaunch_error = ""
    if paths is not None and relaunch and launcher_stopped:
        relaunch_error = _relaunch_repaired_app(paths, launch_func)
    message = str(exc)
    if relaunch_error:
        message += f"\n\nBaluffo could not reopen automatically: {relaunch_error}"
    if not from_panel or relaunch_error:
        show_func("Baluffo Update Repair Failed", message)
    print(json.dumps({"ok": False, "error": str(exc)}, indent=2))
    return 1


def _repair_success_result(
    result: dict[str, Any],
    *,
    paths: DesktopUpdatePaths,
    relaunch: bool,
    from_panel: bool,
    launch_func: Any,
    show_func: Any,
) -> int:
    version = str(result.get("version") or "")
    if str(result.get("action") or "") == "restore-snapshot":
        message = f"Restored Baluffo {version} from the retained rollback snapshot."
    else:
        message = f"Accepted the Baluffo {version} install and cleared the failed rollback."
    if not bool(result.get("snapshotRemoved")):
        message += f"\n\nThe rollback snapshot remains at {result.get('snapshotPath')}."
    relaunch_error = _relaunch_repaired_app(paths, launch_func) if relaunch else ""
    if relaunch_error:
        message += f"\n\nBaluffo could not reopen automatically: {relaunch_error}"
    if not from_panel or relaunch_error:
        show_func("Baluffo Update Repaired", message)
    print(json.dumps(result, indent=2))
    return 0


def run_desktop_install_repair(
    args: Any,
    *,
    repair_func: Any,
    wait_func: Any,
    launch_func: Any,
    show_func: Any,
    failure_status_func: Any,
) -> int:
    """Run the helper repair mode, waiting for the launcher before restoring."""
    paths: DesktopUpdatePaths | None = None
    from_panel = False
    relaunch = False
    launcher_stopped = False
    try:
        plan_path, paths, plan = _repair_plan_settings(args)
        mode = str(plan.get("mode") or getattr(args, "mode", "") or "").strip()
        expected_version = str(
            plan.get("expectedVersion") or getattr(args, "expect_version", "") or ""
        ).strip()
        discard_snapshot = bool(
            plan.get("discardSnapshot", getattr(args, "discard_snapshot", False))
        )
        launcher_pid = int(plan.get("launcherPid") or getattr(args, "launcher_pid", 0) or 0)
        session_root = str(
            plan.get("desktopSessionRoot") or getattr(args, "desktop_session_root", "") or ""
        ).strip()
        relaunch = bool(plan.get("relaunch", getattr(args, "relaunch", False)))
        from_panel = bool(plan.get("panel", getattr(args, "from_panel", False)))
        _clear_repair_handoff(paths, plan_path)
        if launcher_pid > 0:
            wait_func({"launcherPid": launcher_pid, "desktopSessionRoot": session_root})
        launcher_stopped = True
        result = repair_func(
            paths,
            mode=mode,
            expected_version=expected_version,
            discard_snapshot=discard_snapshot,
        )
    except _REPAIR_OPERATIONAL_ERRORS as exc:
        return _repair_failure_result(
            exc,
            paths=paths,
            from_panel=from_panel,
            relaunch=relaunch,
            launcher_stopped=launcher_stopped,
            launch_func=launch_func,
            show_func=show_func,
            failure_status_func=failure_status_func,
        )
    if paths is None:  # pragma: no cover - the try block sets it before any success
        return 1
    return _repair_success_result(
        _as_dict(result),
        paths=paths,
        relaunch=relaunch,
        from_panel=from_panel,
        launch_func=launch_func,
        show_func=show_func,
    )


def _restore_terminal_install(
    paths: DesktopUpdatePaths,
    *,
    repair_mode: str,
    stage: str,
    previous_error: str,
    rollback_root: Path,
    required_version: str,
) -> dict[str, Any]:
    restored = _restore_install_snapshot(
        paths.install_root,
        rollback_root,
        expected_version=required_version,
    )
    version = str(restored.get("currentVersion") or "").strip()
    # Clear the barrier before deleting the snapshot: if the status write
    # fails, the retained tree is still there to retry the repair.
    saved = _save_terminal_repair_status(
        paths,
        mode=repair_mode,
        previous_stage=stage,
        previous_error=previous_error,
        version=version,
        snapshot_path=rollback_root,
        snapshot_removed=False,
        verified=True,
    )
    remove_rollback_path(rollback_root)
    snapshot_removed = not rollback_root.exists()
    if snapshot_removed:
        saved = _save_terminal_repair_status(
            paths,
            mode=repair_mode,
            previous_stage=stage,
            previous_error=previous_error,
            version=version,
            snapshot_path=rollback_root,
            snapshot_removed=True,
            verified=True,
        )
    return {
        "ok": True,
        "action": repair_mode,
        "version": version,
        "files": int(restored.get("files") or 0),
        "snapshotPath": str(rollback_root),
        "snapshotRemoved": snapshot_removed,
        "status": saved,
    }


def _accept_terminal_install(
    paths: DesktopUpdatePaths,
    *,
    repair_mode: str,
    stage: str,
    previous_error: str,
    rollback_root: Path,
    required_version: str,
    discard_snapshot: bool,
) -> dict[str, Any]:
    install_exe = paths.install_root / "Baluffo.exe"
    pointer_path = paths.ship_root / "app" / "current.txt"
    if not install_exe.is_file():
        raise RuntimeError(f"The install is not usable: {install_exe} is missing.")
    version = pointer_path.read_text(encoding="utf-8").strip() if pointer_path.is_file() else ""
    if not version:
        raise RuntimeError(f"The install is not usable: {pointer_path} is missing or empty.")
    if required_version and version != required_version:
        raise RuntimeError(
            "The install version does not match the requested repair "
            f"(expected {required_version!r}, found {version!r})."
        )
    if rollback_root.exists() and not discard_snapshot:
        raise RuntimeError(
            f"The retained rollback snapshot is still on disk: {rollback_root}. "
            "Confirm the accepted install first, then repeat the repair with "
            "--discard-snapshot."
        )
    if discard_snapshot:
        remove_rollback_path(rollback_root)
    snapshot_removed = not rollback_root.exists()
    saved = _save_terminal_repair_status(
        paths,
        mode=repair_mode,
        previous_stage=stage,
        previous_error=previous_error,
        version=version,
        snapshot_path=rollback_root,
        snapshot_removed=snapshot_removed,
        verified=False,
    )
    return {
        "ok": True,
        "action": repair_mode,
        "version": version,
        "files": 0,
        "snapshotPath": str(rollback_root),
        "snapshotRemoved": snapshot_removed,
        "status": saved,
    }


def repair_terminal_install(
    paths: DesktopUpdatePaths,
    *,
    mode: str,
    expected_version: str = "",
    discard_snapshot: bool = False,
) -> dict[str, Any]:
    """Clear a terminal rollback failure only after an explicit operator repair.

    ``restore-snapshot`` puts the retained last-good tree back and removes the
    snapshot. ``accept-install`` clears the barrier for a tree the operator has
    already replaced, and refuses while the old snapshot is still on disk unless
    they also confirm discarding it. ``ship/data`` is never touched.
    """
    repair_mode = str(mode or "").strip().lower()
    if repair_mode not in TERMINAL_INSTALL_REPAIR_MODES:
        raise ValueError(
            "Unknown desktop install repair mode: "
            f"{mode!r}. Use one of {', '.join(TERMINAL_INSTALL_REPAIR_MODES)}."
        )
    status = load_status(paths)
    stage = _terminal_install_stage(status.get("installState"), status.get("installStage"))
    if not stage:
        raise RuntimeError(
            "No terminal desktop install failure is recorded; there is nothing to repair."
        )
    previous_error = str(status.get("lastError") or "").strip()
    rollback_text = str(status.get("rollbackPath") or "").strip()
    if not rollback_text:
        raise RuntimeError(
            "The terminal install failure does not record a rollback snapshot path; "
            "refusing to guess which tree to repair."
        )
    rollback_root = Path(rollback_text).expanduser().resolve()
    required_version = str(expected_version or "").strip()
    if repair_mode == "restore-snapshot":
        return _restore_terminal_install(
            paths,
            repair_mode=repair_mode,
            stage=stage,
            previous_error=previous_error,
            rollback_root=rollback_root,
            required_version=required_version,
        )
    return _accept_terminal_install(
        paths,
        repair_mode=repair_mode,
        stage=stage,
        previous_error=previous_error,
        rollback_root=rollback_root,
        required_version=required_version,
        discard_snapshot=discard_snapshot,
    )


def run_install(plan_path: Path, progress: Any | None = None) -> dict[str, Any]:
    plan = validate_install_plan(json.loads(plan_path.read_text(encoding="utf-8")))
    install_root = Path(str(plan.get("installRoot") or "")).expanduser().resolve()
    ship_root = install_root / "ship"
    data_dir = Path(str(plan.get("dataDir") or "")).expanduser().resolve()
    paths = DesktopUpdatePaths.from_data_dir(
        data_dir,
        install_root=install_root,
        ship_root=ship_root,
    )
    rollback_root = Path(str(plan.get("rollbackPath") or "")).expanduser().resolve()
    _refuse_terminal_install(paths, rollback_root)
    if str(plan.get("artifactKind") or "").strip().lower() == ARTIFACT_KIND_APPIMAGE:
        # Imported here: the AppImage backend calls back into this module for its
        # shared stages, so a module-level import would be a cycle.
        from src.ship import desktop_updater_install_appimage as appimage_mod

        return appimage_mod.run_appimage_install(plan, progress=progress)
    existing_status = load_status(paths)
    progress = progress if progress is not None else _NullProgressWindow()
    progress.start(
        str(existing_status.get("installStageLabel") or "").strip()
        or install_stage_label("handoff_requested", "preparing")
    )
    staging_root = paths.updater_dir / "staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    temp_extract = (staging_root / f"baluffo-desktop-update-{uuid.uuid4().hex}").resolve()
    if temp_extract.exists():
        shutil.rmtree(temp_extract)
    temp_extract.mkdir(parents=True, exist_ok=True)
    backup_ref: Path | None = None
    mutation_started = False
    recovery_failed = False
    try:
        try:
            recovered_as_complete = _recover_interrupted_install(
                plan,
                paths=paths,
                rollback_root=rollback_root,
            )
        except RuntimeError:
            recovery_failed = True
            raise
        if recovered_as_complete:
            return {"ok": True, "installedVersion": str(plan.get("targetVersion") or "")}
        manifest = _recover_manifest_for_install(
            plan,
            install_root=install_root,
            ship_root=ship_root,
            paths=paths,
        )
        zip_path = _ensure_verified_zip_for_install(
            plan,
            manifest=manifest,
            zip_path=Path(str(plan.get("downloadedZipPath") or "")).expanduser().resolve(),
        )
        _save_install_stage_status(
            paths,
            install_state="handoff_requested",
            install_stage="preparing",
            lastError="",
            rollbackPath=str(rollback_root),
        )
        progress.update(install_stage_label("waiting_for_exit", "waiting_for_exit"))
        _save_install_stage_status(
            paths,
            install_state="waiting_for_exit",
            install_stage="waiting_for_exit",
            lastError="",
            rollbackPath=str(rollback_root),
        )
        _wait_for_launcher_exit(plan)
        clear_handoff_request(paths)

        progress.update(install_stage_label("installing", "extracting"))
        _save_install_stage_status(
            paths,
            install_state="installing",
            install_stage="extracting",
            rollbackPath=str(rollback_root),
        )
        # extractall is not atomic; rollback snapshot is taken before this call.
        with _zipfile.ZipFile(zip_path, "r") as archive:
            archive.extractall(temp_extract)
        clear_success_marker(paths)
        rollback_root.mkdir(parents=True, exist_ok=True)
        _save_install_stage_status(
            paths,
            install_state="installing",
            install_stage="snapshotting",
            rollbackPath=str(rollback_root),
        )
        _copy_install_snapshot(install_root, rollback_root)
        migration_plan = list(manifest.get("migration_plan") or [])
        if migration_plan:
            _save_install_stage_status(
                paths,
                install_state="installing",
                install_stage="backup",
                rollbackPath=str(rollback_root),
            )
            backup_ref = _create_data_backup(_ShipPaths.from_root(ship_root, data_dir=data_dir))
            _save_install_stage_status(
                paths,
                install_state="installing",
                install_stage="backup",
                rollbackPath=str(rollback_root),
                migrationBackupPath=str(backup_ref),
            )
        _save_install_stage_status(
            paths,
            install_state="installing",
            install_stage="replacing",
            rollbackPath=str(rollback_root),
            migrationBackupPath=str(backup_ref) if backup_ref is not None else "",
        )
        mutation_started = True
        _sync_extract_to_install(install_root, temp_extract)
        if migration_plan and backup_ref is not None:
            _save_install_stage_status(
                paths,
                install_state="installing",
                install_stage="migrating",
                rollbackPath=str(rollback_root),
                migrationBackupPath=str(backup_ref),
            )
            _run_migrations(
                _ShipPaths.from_root(ship_root, data_dir=data_dir),
                migration_plan,
                backup_ref,
            )
        progress.update(install_stage_label("verifying", "relaunching"))
        _save_install_stage_status(
            paths,
            install_state="verifying",
            install_stage="relaunching",
            rollbackPath=str(rollback_root),
            migrationBackupPath=str(backup_ref) if backup_ref is not None else "",
        )
        _launch_executable(
            install_root / "Baluffo.exe",
            clear_app_version_override=True,
            data_dir=data_dir,
        )
        _save_install_stage_status(
            paths,
            install_state="verifying",
            install_stage="verifying",
            rollbackPath=str(rollback_root),
            migrationBackupPath=str(backup_ref) if backup_ref is not None else "",
        )
        _verify_target_startup(plan, timeout_s=_helper_relaunch_verify_timeout_s_impl())
        _finalize_success(paths, plan, rollback_root)
        return {"ok": True, "installedVersion": str(plan.get("targetVersion") or "")}
    except BaseException as exc:
        # A failed recovery already left a rollback_failed status and the only
        # good snapshot on disk; continuing would re-snapshot over it.
        if recovery_failed:
            raise
        # Treat install mutation as a transactional cleanup boundary, then re-raise.
        progress.update(install_stage_label("installing", "rolling_back"))
        _rollback_failed_install(
            plan=plan,
            paths=paths,
            rollback_root=rollback_root,
            backup_ref=backup_ref,
            failure=exc,
            mutation_started=mutation_started,
        )
        raise
    finally:
        clear_handoff_request(paths)
        progress.close()
        with contextlib.suppress(OSError):
            shutil.rmtree(temp_extract)
