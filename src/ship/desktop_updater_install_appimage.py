"""Install a downloaded AppImage in place of the running one.

A Linux build is a single file, so an update is a file swap rather than the
Windows directory-tree replacement: stage the verified download beside the
target, snapshot the running build, rename the staged file over it, relaunch and
verify. The old mount keeps serving the process that requested the update, which
is why the helper itself runs from a temporary copy outside the AppImage.

AI boundary owns: AppImage staging, snapshot, atomic replace, relaunch, rollback.
AI boundary implement in: this file; shared install stages stay in desktop_updater_install.
AI boundary verify: `python -m pytest tests/test_desktop_update_appimage_install.py`.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import uuid
from pathlib import Path
from typing import Any

STAGED_SUFFIX = ".staged"


def _save_stage(
    install_mod: Any,
    paths: Any,
    *,
    install_state: str,
    install_stage: str,
    **extra: Any,
) -> dict[str, Any]:
    return dict(
        install_mod._save_install_stage_status(
            paths,
            install_state=install_state,
            install_stage=install_stage,
            **extra,
        )
    )


def stage_appimage(source: Path, staged: Path) -> Path:
    """Copy the verified download beside the target, so the rename stays atomic."""
    staged.parent.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(OSError):
        staged.unlink()
    try:
        shutil.copyfile(source, staged)
        staged.chmod(0o755)
    except OSError as exc:
        with contextlib.suppress(OSError):
            staged.unlink()
        raise RuntimeError(
            f"Could not stage the downloaded AppImage next to the current one: {exc}"
        ) from exc
    return staged


def snapshot_appimage(target: Path, rollback_path: Path) -> Path:
    """Keep the build being replaced, so a failed start can put it back."""
    try:
        with contextlib.suppress(OSError):
            rollback_path.unlink()
        shutil.copyfile(target, rollback_path)
        rollback_path.chmod(0o755)
    except OSError as exc:
        raise RuntimeError(f"Could not snapshot the current AppImage: {exc}") from exc
    return rollback_path


def replace_appimage(staged: Path, target: Path) -> None:
    try:
        os.replace(staged, target)
    except OSError as exc:
        raise RuntimeError(f"Could not replace the AppImage: {exc}") from exc


def restore_appimage(target: Path, rollback_path: Path) -> bool:
    """Put the snapshot back. Returns whether anything was restored."""
    if not rollback_path.is_file():
        return False
    try:
        os.replace(rollback_path, target)
        return True
    except OSError:
        with contextlib.suppress(OSError, shutil.Error):
            shutil.copyfile(rollback_path, target)
            target.chmod(0o755)
            rollback_path.unlink()
            return True
    return False


def restore_missing_target(target: Path, rollback_path: Path) -> bool:
    """A crash may have removed the target before the rename landed."""
    if target.exists() or not rollback_path.is_file():
        return False
    return restore_appimage(target, rollback_path)


def _relaunch_target(
    install_mod: Any,
    target: Path,
    *,
    data_dir: Path,
    relaunch_args: tuple[str, ...],
) -> None:
    try:
        install_mod._launch_executable(
            target,
            clear_app_version_override=True,
            data_dir=data_dir,
            args=relaunch_args,
        )
    except OSError as exc:
        raise RuntimeError(f"Could not start the updated AppImage: {exc}") from exc


def run_appimage_install(plan: dict[str, Any], *, progress: Any | None = None) -> dict[str, Any]:
    # Imported here so the shared installer can dispatch back into this module
    # without an import cycle, and so tests can patch the shared stages.
    from src.ship import desktop_updater_install as install_mod

    target_raw = str(plan.get("targetArtifactPath") or "").strip()
    if not target_raw:
        raise RuntimeError("The install plan does not name the AppImage to replace.")
    install_root = Path(str(plan.get("installRoot") or "")).expanduser().resolve()
    data_dir = Path(str(plan.get("dataDir") or "")).expanduser().resolve()
    target = Path(target_raw).expanduser().resolve()
    rollback_path = Path(str(plan.get("rollbackPath") or "")).expanduser().resolve()
    artifact_path = Path(str(plan.get("downloadedZipPath") or "")).expanduser().resolve()
    relaunch_args = tuple(str(item) for item in list(plan.get("relaunchArgs") or []))
    paths = install_mod.DesktopUpdatePaths.from_data_dir(
        data_dir,
        install_root=install_root,
        ship_root=install_root / "ship",
    )
    progress = progress if progress is not None else install_mod.NullProgressWindow()
    existing_status = install_mod.load_status(paths)
    progress.start(
        str(existing_status.get("installStageLabel") or "").strip()
        or install_mod.install_stage_label("handoff_requested", "preparing")
    )
    staged = target.with_name(f".{target.name}.{uuid.uuid4().hex}{STAGED_SUFFIX}")
    try:
        manifest = install_mod._recover_manifest_for_install(
            plan,
            install_root=install_root,
            ship_root=install_root / "ship",
            paths=paths,
        )
        artifact_path = install_mod._ensure_verified_zip_for_install(
            plan,
            manifest=manifest,
            zip_path=artifact_path,
        )
        restore_missing_target(target, rollback_path)
        _save_stage(
            install_mod,
            paths,
            install_state="handoff_requested",
            install_stage="preparing",
            lastError="",
            rollbackPath=str(rollback_path),
        )
        progress.update(install_mod.install_stage_label("waiting_for_exit", "waiting_for_exit"))
        _save_stage(
            install_mod,
            paths,
            install_state="waiting_for_exit",
            install_stage="waiting_for_exit",
            lastError="",
            rollbackPath=str(rollback_path),
        )
        install_mod._wait_for_launcher_exit(plan)
        install_mod.clear_handoff_request(paths)

        progress.update(install_mod.install_stage_label("installing", "extracting"))
        _save_stage(
            install_mod,
            paths,
            install_state="installing",
            install_stage="extracting",
            rollbackPath=str(rollback_path),
        )
        stage_appimage(artifact_path, staged)
        _save_stage(
            install_mod,
            paths,
            install_state="installing",
            install_stage="snapshotting",
            rollbackPath=str(rollback_path),
        )
        snapshot_appimage(target, rollback_path)
        _save_stage(
            install_mod,
            paths,
            install_state="installing",
            install_stage="replacing",
            rollbackPath=str(rollback_path),
        )
        replace_appimage(staged, target)

        progress.update(install_mod.install_stage_label("verifying", "relaunching"))
        _save_stage(
            install_mod,
            paths,
            install_state="verifying",
            install_stage="relaunching",
            rollbackPath=str(rollback_path),
        )
        _relaunch_target(install_mod, target, data_dir=data_dir, relaunch_args=relaunch_args)
        _save_stage(
            install_mod,
            paths,
            install_state="verifying",
            install_stage="verifying",
            rollbackPath=str(rollback_path),
        )
        install_mod._verify_target_startup(
            plan,
            timeout_s=install_mod._helper_relaunch_verify_timeout_s_impl(),
        )
        install_mod._finalize_success(paths, plan, rollback_path)
        return {"ok": True, "installedVersion": str(plan.get("targetVersion") or "")}
    except BaseException as exc:
        install_mod.clear_handoff_request(paths)
        progress.update(install_mod.install_stage_label("installing", "rolling_back"))
        restore_appimage(target, rollback_path)
        failed_status = _save_stage(
            install_mod,
            paths,
            install_state="failed",
            install_stage="rolling_back",
            lastError=install_mod._classify_install_failure(exc),
            rollbackPath=str(rollback_path),
        )
        with contextlib.suppress(OSError, RuntimeError):
            _relaunch_target(install_mod, target, data_dir=data_dir, relaunch_args=relaunch_args)
        _save_stage(
            install_mod,
            paths,
            install_state="failed",
            install_stage="failed",
            lastError=str(failed_status.get("lastError") or "desktop_install_failed"),
            rollbackPath=str(rollback_path),
        )
        raise
    finally:
        install_mod.clear_handoff_request(paths)
        progress.close()
        with contextlib.suppress(OSError):
            staged.unlink()


__all__ = [
    "replace_appimage",
    "restore_appimage",
    "restore_missing_target",
    "run_appimage_install",
    "snapshot_appimage",
    "stage_appimage",
]
