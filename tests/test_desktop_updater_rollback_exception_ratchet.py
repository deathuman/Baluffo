import json
from pathlib import Path
from unittest import mock

import pytest

from src.ship import desktop_update_shared as du_shared
from src.ship import desktop_updater_install as updater
from tests.helpers.temp_paths import workspace_tmpdir


def _write_install_plan(plan_path: Path, install_root: Path, rollback_root: Path) -> None:
    data_dir = install_root / "ship" / "data"
    plan = {
        "planVersion": 1,
        "installRoot": str(install_root),
        "dataDir": str(data_dir),
        "tempHelperPath": str(install_root / "BaluffoUpdater.exe"),
        "targetVersion": "1.4.0",
        "currentVersion": "0.1.0",
        "manifestPath": str(
            du_shared.DesktopUpdatePaths.from_data_dir(data_dir).manifest_cache_path
        ),
        "downloadedZipPath": str(
            du_shared.DesktopUpdatePaths.from_data_dir(data_dir).downloads_dir
            / "baluffo-portable-1.4.0.zip"
        ),
        "expectedZipSha256": "expected-zip-sha",
        "manifestKeyId": "desktop-ed25519-test",
        "rollbackPath": str(rollback_root),
        "updaterWorkingDir": str(du_shared.DesktopUpdatePaths.from_data_dir(data_dir).updater_dir),
        "createdAt": "2026-04-14T12:00:00Z",
        "launcherPid": 1234,
        "launcherToken": "token-1",
        "desktopSessionRoot": str(install_root / "session"),
    }
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_text(json.dumps(plan), encoding="utf-8")


def _run_install_with_backup_restore_failure(
    *,
    tmp: Path,
    restore_failure: BaseException,
) -> None:
    install_root = tmp / "portable"
    data_dir = install_root / "ship" / "data"
    paths = du_shared.DesktopUpdatePaths.from_data_dir(data_dir)
    rollback_root = paths.rollback_root / "1.4.0-20260414-120000"
    backup_ref = tmp / "data-backup.zip"
    _write_install_plan(paths.install_plan_path, install_root, rollback_root)

    class _Archive:
        def __enter__(self) -> "_Archive":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def extractall(self, _target: Path) -> None:
            return None

    with (
        mock.patch.object(updater, "root", None),
        mock.patch.object(
            updater,
            "_recover_manifest_for_install",
            return_value={"migration_plan": ["data-migration"]},
            create=True,
        ),
        mock.patch.object(
            updater,
            "_ensure_verified_zip_for_install",
            return_value=tmp / "update.zip",
            create=True,
        ),
        mock.patch.object(updater, "_recover_interrupted_install", return_value=False),
        mock.patch.object(updater, "_wait_for_launcher_exit"),
        mock.patch.object(updater.zipfile, "ZipFile", return_value=_Archive()),
        mock.patch.object(updater, "_copy_install_snapshot"),
        mock.patch.object(updater.update_manager, "create_data_backup", return_value=backup_ref),
        mock.patch.object(
            updater,
            "_sync_extract_to_install",
            side_effect=RuntimeError("install replacement failed"),
        ),
        mock.patch.object(
            updater.update_manager,
            "restore_data_backup",
            side_effect=restore_failure,
        ),
        mock.patch.object(updater, "_restore_install_snapshot"),
        mock.patch.object(updater, "_launch_executable"),
    ):
        updater.run_install(paths.install_plan_path)


def test_run_install_suppresses_expected_data_backup_restore_failures() -> None:
    with (
        workspace_tmpdir("desktop-updater") as tmp,
        pytest.raises(RuntimeError, match="install replacement failed"),
    ):
        _run_install_with_backup_restore_failure(
            tmp=Path(tmp),
            restore_failure=updater.zipfile.BadZipFile("corrupt backup"),
        )


def test_run_install_does_not_suppress_unexpected_data_backup_restore_failures() -> None:
    with (
        workspace_tmpdir("desktop-updater") as tmp,
        pytest.raises(AssertionError, match="unexpected data backup bug"),
    ):
        _run_install_with_backup_restore_failure(
            tmp=Path(tmp),
            restore_failure=AssertionError("unexpected data backup bug"),
        )


def test_run_install_suppresses_expected_rollback_snapshot_failures() -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        install_root = Path(tmp) / "portable"
        data_dir = install_root / "ship" / "data"
        paths = du_shared.DesktopUpdatePaths.from_data_dir(data_dir)
        rollback_root = paths.rollback_root / "1.4.0-20260414-120000"
        _write_install_plan(paths.install_plan_path, install_root, rollback_root)

        with (
            mock.patch.object(updater, "root", None),
            mock.patch.object(
                updater,
                "_recover_manifest_for_install",
                side_effect=RuntimeError("No cached desktop manifest is available."),
                create=True,
            ),
            mock.patch.object(
                updater,
                "_restore_install_snapshot",
                side_effect=OSError("rollback snapshot unavailable"),
            ),
            pytest.raises(RuntimeError, match="No cached desktop manifest is available"),
        ):
            updater.run_install(paths.install_plan_path)


def test_run_install_does_not_suppress_unexpected_rollback_snapshot_failures() -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        install_root = Path(tmp) / "portable"
        data_dir = install_root / "ship" / "data"
        paths = du_shared.DesktopUpdatePaths.from_data_dir(data_dir)
        rollback_root = paths.rollback_root / "1.4.0-20260414-120000"
        _write_install_plan(paths.install_plan_path, install_root, rollback_root)

        with (
            mock.patch.object(updater, "root", None),
            mock.patch.object(
                updater,
                "_recover_manifest_for_install",
                side_effect=RuntimeError("No cached desktop manifest is available."),
                create=True,
            ),
            mock.patch.object(
                updater,
                "_restore_install_snapshot",
                side_effect=AssertionError("unexpected rollback bug"),
            ),
            pytest.raises(AssertionError, match="unexpected rollback bug"),
        ):
            updater.run_install(paths.install_plan_path)
