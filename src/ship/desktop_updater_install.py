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
import shutil
import time as _time
import urllib.error
import uuid
import zipfile as _zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

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


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


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


def _copy_install_snapshot(install_root: Path, rollback_root: Path) -> None:
    snapshot_root = rollback_root / "runtime"
    if snapshot_root.exists():
        shutil.rmtree(snapshot_root)
    snapshot_root.mkdir(parents=True, exist_ok=True)
    for path in install_root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(install_root)
        rel_text = rel.as_posix()
        if rel_text.startswith("ship/data/"):
            continue
        target = snapshot_root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)


def _restore_install_snapshot(install_root: Path, rollback_root: Path) -> None:
    snapshot_root = rollback_root / "runtime"
    if not snapshot_root.is_dir():
        return
    for path in install_root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(install_root)
        if rel.as_posix().startswith("ship/data/"):
            continue
        with contextlib.suppress(OSError):
            path.unlink()
    for path in snapshot_root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(snapshot_root)
        target = install_root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)


# install mutation: deletes files not in new extract, copies extracted content
def _sync_extract_to_install(install_root: Path, extracted_root: Path) -> None:
    target_paths = {
        path.relative_to(extracted_root).as_posix()
        for path in extracted_root.rglob("*")
        if path.is_file()
    }
    for path in install_root.rglob("*"):
        if not path.is_file():
            continue
        rel_text = path.relative_to(install_root).as_posix()
        if rel_text.startswith("ship/data/"):
            continue
        if rel_text not in target_paths:
            with contextlib.suppress(OSError):
                path.unlink()
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


def _restore_data_backup_if_needed(ship_root: Path, data_dir: Path, status: dict[str, Any]) -> None:
    backup_ref_text = str(status.get("migrationBackupPath") or "").strip()
    if not backup_ref_text:
        return
    backup_ref = Path(backup_ref_text).expanduser().resolve()
    if not backup_ref.exists():
        return
    _restore_data_backup(
        _ShipPaths.from_root(ship_root, data_dir=data_dir),
        backup_ref,
    )


def _finalize_success(
    paths: DesktopUpdatePaths, plan: dict[str, Any], rollback_root: Path
) -> dict[str, Any]:
    with contextlib.suppress(OSError):
        shutil.rmtree(rollback_root)
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
    install_root: Path,
    ship_root: Path,
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
    _restore_data_backup_if_needed(ship_root, paths.data_dir, status)
    _restore_install_snapshot(install_root, rollback_root)
    with contextlib.suppress(OSError):
        shutil.rmtree(rollback_root)
    _save_install_stage_status(
        paths,
        install_state="idle",
        install_stage="idle",
        lastError="",
        migrationBackupPath="",
        rollbackPath="",
    )
    return False


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
    try:
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
        recovered_as_complete = _recover_interrupted_install(
            plan,
            install_root=install_root,
            ship_root=ship_root,
            paths=paths,
            rollback_root=rollback_root,
        )
        if recovered_as_complete:
            return {"ok": True, "installedVersion": str(plan.get("targetVersion") or "")}
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
        _sync_extract_to_install(install_root, temp_extract)
        if migration_plan:
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
        # Treat install mutation as a transactional cleanup boundary, then re-raise.
        clear_handoff_request(paths)
        progress.update(install_stage_label("installing", "rolling_back"))
        if backup_ref is not None:
            with contextlib.suppress(OSError, _zipfile.BadZipFile, _zipfile.LargeZipFile):
                _restore_data_backup(
                    _ShipPaths.from_root(ship_root, data_dir=data_dir),
                    backup_ref,
                )
        current_status = _save_install_stage_status(
            paths,
            install_state="failed",
            install_stage="rolling_back",
            lastError=_classify_install_failure_impl(exc),
            rollbackPath=str(rollback_root),
            migrationBackupPath=str(backup_ref) if backup_ref is not None else "",
        )
        with contextlib.suppress(OSError, RuntimeError, ValueError, shutil.Error):
            _restore_install_snapshot(install_root, rollback_root)
        with contextlib.suppress(OSError, RuntimeError):
            _launch_executable(install_root / "Baluffo.exe", data_dir=data_dir)
        _save_install_stage_status(
            paths,
            install_state="failed",
            install_stage="failed",
            lastError=str(current_status.get("lastError") or "desktop_install_failed"),
            rollbackPath=str(rollback_root),
            migrationBackupPath=str(backup_ref) if backup_ref is not None else "",
        )
        raise
    finally:
        clear_handoff_request(paths)
        progress.close()
        with contextlib.suppress(OSError):
            shutil.rmtree(temp_extract)
