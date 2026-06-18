from pathlib import Path
from unittest import mock

import pytest

from src.ship import desktop_update_shared as du_shared
from src.ship import desktop_update_state as update_state
from src.ship import desktop_updater_install as updater
from tests.helpers.temp_paths import workspace_tmpdir


def _verifying_recovery_context(
    tmp: str,
) -> tuple[Path, Path, du_shared.DesktopUpdatePaths, Path, dict]:
    install_root = Path(tmp) / "portable"
    ship_root = install_root / "ship"
    data_dir = ship_root / "data"
    paths = du_shared.DesktopUpdatePaths.from_data_dir(data_dir)
    rollback_root = paths.rollback_root / "1.4.0-20260414-120000"
    zip_path = paths.downloads_dir / "baluffo-portable-1.4.0.zip"
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    zip_path.write_text("zip", encoding="utf-8")
    update_state.save_status(
        paths,
        {
            **update_state.default_status_payload(current_version="0.1.0"),
            "installState": "verifying",
            "installStage": "verifying",
            "downloadState": "downloaded",
            "downloadedZipPath": str(zip_path),
            "rollbackPath": str(rollback_root),
            "targetVersion": "1.4.0",
        },
    )
    return install_root, ship_root, paths, rollback_root, {"targetVersion": "1.4.0"}


def test_recover_interrupted_install_continues_after_expected_startup_failure() -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        install_root, ship_root, paths, rollback_root, plan = _verifying_recovery_context(tmp)

        with (
            mock.patch.object(updater, "root", None),
            mock.patch.object(
                updater,
                "_verify_target_startup",
                side_effect=RuntimeError("startup still unavailable"),
            ),
            mock.patch.object(updater, "_restore_install_snapshot") as restore_snapshot,
        ):
            completed = updater._recover_interrupted_install(
                plan,
                install_root=install_root,
                ship_root=ship_root,
                paths=paths,
                rollback_root=rollback_root,
            )

        assert completed is False
        restore_snapshot.assert_called_once_with(install_root, rollback_root)


def test_recover_interrupted_install_does_not_suppress_unexpected_startup_failure() -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        install_root, ship_root, paths, rollback_root, plan = _verifying_recovery_context(tmp)

        with (
            mock.patch.object(updater, "root", None),
            mock.patch.object(
                updater,
                "_verify_target_startup",
                side_effect=AssertionError("unexpected startup bug"),
            ),
            pytest.raises(AssertionError, match="unexpected startup bug"),
        ):
            updater._recover_interrupted_install(
                plan,
                install_root=install_root,
                ship_root=ship_root,
                paths=paths,
                rollback_root=rollback_root,
            )
