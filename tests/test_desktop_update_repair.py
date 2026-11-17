"""Explicit repair for a terminal desktop update rollback failure."""

from pathlib import Path

import pytest

from src.ship import desktop_update_shared as du_shared
from src.ship import desktop_update_state as update_state
from src.ship import desktop_updater_install as updater_install
from tests.helpers.temp_paths import workspace_tmpdir


def _terminal_install(tmp: str) -> tuple[du_shared.DesktopUpdatePaths, Path, Path]:
    install_root = Path(tmp) / "portable"
    data_dir = install_root / "ship" / "data"
    paths = du_shared.DesktopUpdatePaths.from_data_dir(data_dir)
    (install_root / "ship" / "app").mkdir(parents=True, exist_ok=True)
    (install_root / "Baluffo.exe").write_text("updated", encoding="utf-8")
    (install_root / "ship" / "app" / "current.txt").write_text("1.4.0\n", encoding="utf-8")
    (install_root / "ship" / "app" / "only-in-update.txt").write_text("x", encoding="utf-8")
    (install_root / "ship" / "data").mkdir(parents=True, exist_ok=True)
    (install_root / "ship" / "data" / "runtime.db").write_text("data", encoding="utf-8")

    rollback_root = paths.rollback_root / "1.4.0-20260414-120000"
    snapshot_root = rollback_root / "runtime"
    (snapshot_root / "ship" / "app").mkdir(parents=True)
    (snapshot_root / "Baluffo.exe").write_text("previous", encoding="utf-8")
    (snapshot_root / "ship" / "app" / "current.txt").write_text("0.1.0\n", encoding="utf-8")
    (snapshot_root / "keep.txt").write_text("keep", encoding="utf-8")

    update_state.save_status(
        paths,
        {
            **update_state.default_status_payload(current_version="1.4.0"),
            "installState": "failed",
            "installStage": "rollback_failed",
            "lastError": "install_restore_failed: install is locked",
            "rollbackPath": str(rollback_root),
            "rollbackVerified": False,
        },
    )
    return paths, install_root, rollback_root


def test_repair_restores_the_retained_snapshot_and_clears_the_terminal_stage() -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        paths, install_root, rollback_root = _terminal_install(tmp)

        result = updater_install.repair_terminal_install(
            paths,
            mode="restore-snapshot",
            expected_version="0.1.0",
        )

        assert result["ok"] is True
        assert result["version"] == "0.1.0"
        assert (install_root / "Baluffo.exe").read_text(encoding="utf-8") == "previous"
        assert (install_root / "ship" / "app" / "current.txt").read_text(
            encoding="utf-8"
        ).strip() == "0.1.0"
        assert not (install_root / "ship" / "app" / "only-in-update.txt").exists()
        assert (install_root / "ship" / "data" / "runtime.db").read_text(encoding="utf-8") == "data"
        assert not rollback_root.exists()

        status = update_state.load_status(paths, current_version="0.1.0")
        assert status["installState"] == "idle"
        assert status["installStage"] == "idle"
        assert status["lastError"] == ""
        assert status["rollbackPath"] == ""
        assert status["rollbackVerified"] is True
        assert status["rollbackVersion"] == "0.1.0"
        assert status["lastRepair"]["action"] == "restore-snapshot"
        assert status["lastRepair"]["previousInstallStage"] == "rollback_failed"
        assert status["lastRepair"]["previousError"] == "install_restore_failed: install is locked"


def test_repair_refuses_a_snapshot_with_the_wrong_version_before_touching_the_install() -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        paths, install_root, rollback_root = _terminal_install(tmp)

        with pytest.raises(RuntimeError, match="does not match the requested repair"):
            updater_install.repair_terminal_install(
                paths,
                mode="restore-snapshot",
                expected_version="9.9.9",
            )

        assert (install_root / "Baluffo.exe").read_text(encoding="utf-8") == "updated"
        assert (rollback_root / "runtime" / "keep.txt").read_text(encoding="utf-8") == "keep"
        status = update_state.load_status(paths, current_version="1.4.0")
        assert status["installStage"] == "rollback_failed"


def test_accept_install_requires_discarding_the_retained_snapshot() -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        paths, _install_root, rollback_root = _terminal_install(tmp)

        with pytest.raises(RuntimeError, match="--discard-snapshot"):
            updater_install.repair_terminal_install(paths, mode="accept-install")

        status = update_state.load_status(paths, current_version="1.4.0")
        assert status["installStage"] == "rollback_failed"
        assert (rollback_root / "runtime" / "keep.txt").exists()

        result = updater_install.repair_terminal_install(
            paths,
            mode="accept-install",
            expected_version="1.4.0",
            discard_snapshot=True,
        )

        assert result["version"] == "1.4.0"
        assert not rollback_root.exists()
        status = update_state.load_status(paths, current_version="1.4.0")
        assert status["installState"] == "idle"
        assert status["installStage"] == "idle"
        assert status["lastRepair"]["action"] == "accept-install"
        assert status["lastRepair"]["snapshotRemoved"] is True


def test_repair_refuses_when_no_terminal_failure_is_recorded() -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du_shared.DesktopUpdatePaths.from_data_dir(data_dir)
        update_state.save_status(
            paths,
            {
                **update_state.default_status_payload(current_version="1.4.0"),
                "installState": "idle",
                "installStage": "idle",
            },
        )

        with pytest.raises(RuntimeError, match="No terminal desktop install failure"):
            updater_install.repair_terminal_install(paths, mode="restore-snapshot")
