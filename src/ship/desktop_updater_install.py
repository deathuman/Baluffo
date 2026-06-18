#!/usr/bin/env python3
"""Install, recovery, and startup verification helpers. Side effects: install mutation, rollback snapshot, relaunch verification. Verify: npm run test:frontend:packaged:update-rehearsal."""

from __future__ import annotations

import contextlib
import json
import shutil
import sys
import time as _time
import urllib.error
import uuid
import zipfile as _zipfile
from pathlib import Path
from typing import Any

from src.ship import update_manager as _update_manager
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
from src.ship.desktop_updater_ui import (
    NullProgressWindow as _NullProgressWindow,
)
from src.ship.desktop_updater_ui import (
    _helper_relaunch_verify_timeout_s as _helper_relaunch_verify_timeout_s_impl,
)
from src.ship.desktop_updater_ui import (
    _launch_executable as _launch_executable_impl,
)

root: Any | None = None

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

# Preserve these names on the module root because the updater flow patches/accesses them through
# `_module()` indirection rather than direct local references.
time = _time
zipfile = _zipfile
update_manager = _update_manager
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
NullProgressWindow = _NullProgressWindow
_helper_relaunch_verify_timeout_s = _helper_relaunch_verify_timeout_s_impl
_launch_executable = _launch_executable_impl


def _module() -> Any:
    return root if root is not None else sys.modules[__name__]


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _save_install_stage_status(
    paths: DesktopUpdatePaths,
    *,
    install_state: str,
    install_stage: str,
    **extra: Any,
) -> dict[str, Any]:
    module = _module()
    status = module.load_status(paths)
    status.update(
        {
            "installState": str(install_state or "").strip().lower() or "idle",
            "installStage": str(install_stage or "").strip().lower() or "idle",
            "installStageLabel": module.install_stage_label(install_state, install_stage),
            "helperUpdatedAt": module.iso_now(),
        }
    )
    status.update(extra)
    return _as_dict(module.save_status(paths, status))


def _wait_for_launcher_exit(plan: dict[str, Any], *, timeout_s: float = 120.0) -> None:
    module = _module()
    deadline = module.time.monotonic() + max(5.0, float(timeout_s))
    launcher_pid = int(plan.get("launcherPid") or 0)
    session_root = Path(str(plan.get("desktopSessionRoot") or "")).expanduser().resolve()
    session_state_path = session_root / "desktop-session.json"
    while module.time.monotonic() < deadline:
        launcher_alive = module.pid_is_running(launcher_pid)
        if launcher_pid > 0 and launcher_alive:
            module.time.sleep(0.5)
            continue
        if session_state_path.exists():
            module.time.sleep(0.5)
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
    module = _module()
    session_root = Path(str(plan.get("desktopSessionRoot") or "")).expanduser().resolve()
    session_state_path = session_root / "desktop-session.json"
    install_root = Path(str(plan.get("installRoot") or "")).expanduser().resolve()
    data_dir = Path(str(plan.get("dataDir") or "")).expanduser().resolve()
    success_marker = module.DesktopUpdatePaths.from_data_dir(
        data_dir,
        install_root=install_root,
        ship_root=install_root / "ship",
    ).success_marker_path
    target_version = str(plan.get("targetVersion") or "").strip()
    deadline = module.time.monotonic() + max(10.0, float(timeout_s))
    while module.time.monotonic() < deadline:
        if not session_state_path.exists():
            module.time.sleep(1.0)
            continue
        try:
            session_state = _as_dict(json.loads(session_state_path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError):
            module.time.sleep(1.0)
            continue
        bridge_port = int(session_state.get("bridgePort") or 0)
        if bridge_port <= 0:
            module.time.sleep(1.0)
            continue
        try:
            health = module.fetch_json(f"http://127.0.0.1:{bridge_port}/ops/health", timeout_s=5.0)
        except (OSError, ValueError, urllib.error.URLError, json.JSONDecodeError):
            module.time.sleep(1.0)
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
        module.time.sleep(1.0)
    raise RuntimeError("Updated desktop app did not report startup readiness in time.")


def _restore_data_backup_if_needed(ship_root: Path, data_dir: Path, status: dict[str, Any]) -> None:
    module = _module()
    backup_ref_text = str(status.get("migrationBackupPath") or "").strip()
    if not backup_ref_text:
        return
    backup_ref = Path(backup_ref_text).expanduser().resolve()
    if not backup_ref.exists():
        return
    module.update_manager.restore_data_backup(
        module.update_manager.ShipPaths.from_root(ship_root, data_dir=data_dir),
        backup_ref,
    )


def _finalize_success(
    paths: DesktopUpdatePaths, plan: dict[str, Any], rollback_root: Path
) -> dict[str, Any]:
    module = _module()
    with contextlib.suppress(OSError):
        shutil.rmtree(rollback_root)
    module.clear_success_marker(paths)
    return _as_dict(
        module._save_install_stage_status(
            paths,
            install_state="installed",
            install_stage="installed",
            downloadState="idle",
            downloadedBytes=0,
            totalBytes=0,
            downloadPercent=0,
            lastError="",
            lastCheckedAt=module.iso_now(),
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
    module = _module()
    status = module.load_status(paths)
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
            module._verify_target_startup(plan, timeout_s=5.0)
        except (OSError, RuntimeError, ValueError):
            pass
        else:
            module._finalize_success(paths, plan, rollback_root)
            return True
    if stage not in MUTATING_INSTALL_STAGES:
        return False
    module._save_install_stage_status(
        paths,
        install_state="installing",
        install_stage="recovering",
        lastError="",
    )
    module._restore_data_backup_if_needed(ship_root, paths.data_dir, status)
    module._restore_install_snapshot(install_root, rollback_root)
    with contextlib.suppress(OSError):
        shutil.rmtree(rollback_root)
    module._save_install_stage_status(
        paths,
        install_state="idle",
        install_stage="idle",
        lastError="",
        migrationBackupPath="",
        rollbackPath="",
    )
    return False


def run_install(plan_path: Path, progress: Any | None = None) -> dict[str, Any]:
    module = _module()
    plan = module.validate_install_plan(json.loads(plan_path.read_text(encoding="utf-8")))
    install_root = Path(str(plan.get("installRoot") or "")).expanduser().resolve()
    ship_root = install_root / "ship"
    data_dir = Path(str(plan.get("dataDir") or "")).expanduser().resolve()
    paths = module.DesktopUpdatePaths.from_data_dir(
        data_dir,
        install_root=install_root,
        ship_root=ship_root,
    )
    rollback_root = Path(str(plan.get("rollbackPath") or "")).expanduser().resolve()
    existing_status = module.load_status(paths)
    progress = progress if progress is not None else module.NullProgressWindow()
    progress.start(
        str(existing_status.get("installStageLabel") or "").strip()
        or module.install_stage_label("handoff_requested", "preparing")
    )
    staging_root = paths.updater_dir / "staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    temp_extract = (staging_root / f"baluffo-desktop-update-{uuid.uuid4().hex}").resolve()
    if temp_extract.exists():
        shutil.rmtree(temp_extract)
    temp_extract.mkdir(parents=True, exist_ok=True)
    backup_ref: Path | None = None
    try:
        manifest = module._recover_manifest_for_install(
            plan,
            install_root=install_root,
            ship_root=ship_root,
            paths=paths,
        )
        zip_path = module._ensure_verified_zip_for_install(
            plan,
            manifest=manifest,
            zip_path=Path(str(plan.get("downloadedZipPath") or "")).expanduser().resolve(),
        )
        recovered_as_complete = module._recover_interrupted_install(
            plan,
            install_root=install_root,
            ship_root=ship_root,
            paths=paths,
            rollback_root=rollback_root,
        )
        if recovered_as_complete:
            return {"ok": True, "installedVersion": str(plan.get("targetVersion") or "")}
        module._save_install_stage_status(
            paths,
            install_state="handoff_requested",
            install_stage="preparing",
            lastError="",
            rollbackPath=str(rollback_root),
        )
        progress.update(module.install_stage_label("waiting_for_exit", "waiting_for_exit"))
        module._save_install_stage_status(
            paths,
            install_state="waiting_for_exit",
            install_stage="waiting_for_exit",
            lastError="",
            rollbackPath=str(rollback_root),
        )
        module._wait_for_launcher_exit(plan)
        module.clear_handoff_request(paths)

        progress.update(module.install_stage_label("installing", "extracting"))
        module._save_install_stage_status(
            paths,
            install_state="installing",
            install_stage="extracting",
            rollbackPath=str(rollback_root),
        )
        # extractall is not atomic; rollback snapshot is taken before this call.
        with module.zipfile.ZipFile(zip_path, "r") as archive:
            archive.extractall(temp_extract)
        module.clear_success_marker(paths)
        rollback_root.mkdir(parents=True, exist_ok=True)
        module._save_install_stage_status(
            paths,
            install_state="installing",
            install_stage="snapshotting",
            rollbackPath=str(rollback_root),
        )
        module._copy_install_snapshot(install_root, rollback_root)
        migration_plan = list(manifest.get("migration_plan") or [])
        if migration_plan:
            module._save_install_stage_status(
                paths,
                install_state="installing",
                install_stage="backup",
                rollbackPath=str(rollback_root),
            )
            backup_ref = module.update_manager.create_data_backup(
                module.update_manager.ShipPaths.from_root(ship_root, data_dir=data_dir)
            )
            module._save_install_stage_status(
                paths,
                install_state="installing",
                install_stage="backup",
                rollbackPath=str(rollback_root),
                migrationBackupPath=str(backup_ref),
            )
        module._save_install_stage_status(
            paths,
            install_state="installing",
            install_stage="replacing",
            rollbackPath=str(rollback_root),
            migrationBackupPath=str(backup_ref) if backup_ref is not None else "",
        )
        module._sync_extract_to_install(install_root, temp_extract)
        if migration_plan:
            module._save_install_stage_status(
                paths,
                install_state="installing",
                install_stage="migrating",
                rollbackPath=str(rollback_root),
                migrationBackupPath=str(backup_ref),
            )
            module.update_manager.run_migrations(
                module.update_manager.ShipPaths.from_root(ship_root, data_dir=data_dir),
                migration_plan,
                backup_ref,
            )
        progress.update(module.install_stage_label("verifying", "relaunching"))
        module._save_install_stage_status(
            paths,
            install_state="verifying",
            install_stage="relaunching",
            rollbackPath=str(rollback_root),
            migrationBackupPath=str(backup_ref) if backup_ref is not None else "",
        )
        module._launch_executable(
            install_root / "Baluffo.exe",
            clear_app_version_override=True,
            data_dir=data_dir,
        )
        module._save_install_stage_status(
            paths,
            install_state="verifying",
            install_stage="verifying",
            rollbackPath=str(rollback_root),
            migrationBackupPath=str(backup_ref) if backup_ref is not None else "",
        )
        module._verify_target_startup(plan, timeout_s=module._helper_relaunch_verify_timeout_s())
        module._finalize_success(paths, plan, rollback_root)
        return {"ok": True, "installedVersion": str(plan.get("targetVersion") or "")}
    except Exception as exc:
        module.clear_handoff_request(paths)
        progress.update(module.install_stage_label("installing", "rolling_back"))
        if backup_ref is not None:
            with contextlib.suppress(
                OSError, module.zipfile.BadZipFile, module.zipfile.LargeZipFile
            ):
                module.update_manager.restore_data_backup(
                    module.update_manager.ShipPaths.from_root(ship_root, data_dir=data_dir),
                    backup_ref,
                )
        current_status = module._save_install_stage_status(
            paths,
            install_state="failed",
            install_stage="rolling_back",
            lastError=module._classify_install_failure(exc),
            rollbackPath=str(rollback_root),
            migrationBackupPath=str(backup_ref) if backup_ref is not None else "",
        )
        with contextlib.suppress(OSError, RuntimeError, ValueError, shutil.Error):
            module._restore_install_snapshot(install_root, rollback_root)
        with contextlib.suppress(Exception):
            module._launch_executable(install_root / "Baluffo.exe", data_dir=data_dir)
        module._save_install_stage_status(
            paths,
            install_state="failed",
            install_stage="failed",
            lastError=str(current_status.get("lastError") or "desktop_install_failed"),
            rollbackPath=str(rollback_root),
            migrationBackupPath=str(backup_ref) if backup_ref is not None else "",
        )
        raise
    finally:
        module.clear_handoff_request(paths)
        progress.close()
        with contextlib.suppress(OSError):
            shutil.rmtree(temp_extract)
