"""Explicit repair for a terminal desktop update rollback failure."""

import shutil
import zipfile
from pathlib import Path
from unittest import mock

import pytest

from src.ship import desktop_update_service as update_service
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


def test_repair_offer_uses_the_verified_release_zip_when_the_snapshot_is_missing() -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        paths, install_root, rollback_root = _terminal_install(tmp)
        (install_root / "BaluffoUpdater.exe").write_text("helper", encoding="utf-8")
        shutil.rmtree(rollback_root)
        zip_path = paths.downloads_dir / "baluffo-portable-1.4.0.zip"
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "w") as archive:
            archive.writestr("Baluffo.exe", "released")
            archive.writestr("ship/app/current.txt", "1.4.0\n")
        update_state.write_json_atomic(
            paths.manifest_cache_path,
            {
                "manifest": {
                    "version": "1.4.0",
                    "key_id": "desktop-ed25519-test",
                    "portable_artifact": {
                        "url": f"https://example.invalid/{zip_path.name}",
                        "sha256": du_shared.compute_sha256(zip_path),
                    },
                }
            },
        )
        session_root = Path(tmp) / "session"
        session_root.mkdir()
        (session_root / "desktop-session.json").write_text(
            '{"launcherPid": 4242, "launcherToken": "token-1"}',
            encoding="utf-8",
        )
        service = update_service.DesktopUpdateService(
            data_dir=paths.data_dir,
            current_version_getter=lambda: "1.4.0",
        )

        with (
            mock.patch.object(
                update_service, "resolve_desktop_session_root", return_value=session_root
            ),
            mock.patch.object(
                update_service,
                "read_desktop_session_state",
                return_value={"launcherPid": 4242, "launcherToken": "token-1"},
            ),
        ):
            result = service.request_repair()

        assert result["started"] is True
        assert result["status"]["repairAction"] == "reextract-zip"
        plan = update_state.read_json(paths.repair_plan_path, {})
        assert plan["mode"] == "reextract-zip"
        assert plan["expectedVersion"] == "1.4.0"


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


def test_request_repair_writes_a_credible_launcher_handoff() -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        paths, install_root, _rollback_root = _terminal_install(tmp)
        (install_root / "BaluffoUpdater.exe").write_text("helper", encoding="utf-8")
        session_root = Path(tmp) / "session"
        session_root.mkdir()
        (session_root / "desktop-session.json").write_text(
            '{"launcherPid": 4242, "launcherToken": "token-1"}',
            encoding="utf-8",
        )
        service = update_service.DesktopUpdateService(
            data_dir=paths.data_dir,
            current_version_getter=lambda: "1.4.0",
        )

        with (
            mock.patch.object(
                update_service, "resolve_desktop_session_root", return_value=session_root
            ),
            mock.patch.object(
                update_service,
                "read_desktop_session_state",
                return_value={"launcherPid": 4242, "launcherToken": "token-1"},
            ),
        ):
            result = service.request_repair()

        assert result["started"] is True
        assert result["exitRequested"] is True
        assert result["status"]["repairState"] == "repairing"
        assert result["status"]["repairAvailable"] is True
        assert paths.repair_request_path.is_file()
        plan = update_state.read_json(paths.repair_plan_path, {})
        assert plan["mode"] == "restore-snapshot"
        assert plan["launcherPid"] == 4242
        assert plan["helperPath"]
        assert Path(plan["helperPath"]).is_file()

        with mock.patch.object(update_state, "pid_is_running", return_value=True):
            assert update_state.updater_install_requested(paths.data_dir) is True


def test_launch_staged_update_helper_uses_the_repair_mode_for_a_repair_request() -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        paths, install_root, _rollback_root = _terminal_install(tmp)
        helper = Path(tmp) / "BaluffoUpdater-repair.exe"
        helper.write_text("helper", encoding="utf-8")
        session_root = Path(tmp) / "session"
        session_root.mkdir()
        (session_root / "desktop-session.json").write_text(
            '{"launcherPid": 4242, "launcherToken": "token-1"}',
            encoding="utf-8",
        )
        update_state.write_json_atomic(
            paths.repair_plan_path,
            {
                "planVersion": 1,
                "mode": "restore-snapshot",
                "dataDir": str(paths.data_dir),
                "installRoot": str(install_root),
                "helperPath": str(helper),
                "launcherPid": 4242,
                "launcherToken": "token-1",
                "desktopSessionRoot": str(session_root),
                "requestedAt": "2026-11-17T12:00:00Z",
            },
        )
        update_state.write_json_atomic(paths.repair_request_path, {"requestedAt": "now"})
        popen = mock.Mock()

        with (
            mock.patch.object(update_state, "pid_is_running", return_value=True),
            mock.patch.object(update_state.subprocess, "Popen", popen),
        ):
            update_state.launch_staged_update_helper(paths)

        command = popen.call_args.args[0]
        assert command[0] == str(helper)
        assert command[1:3] == ["--repair-desktop-install", "--repair-plan"]
        assert command[3] == str(paths.repair_plan_path)
        assert popen.call_args.kwargs["cwd"] == str(paths.updater_dir)


def test_a_fresh_check_acknowledges_a_completed_repair() -> None:
    status = update_service.DesktopUpdateService._acknowledge_repair_result(
        {
            "repairState": "repaired",
            "repairAction": "restore-snapshot",
            "repairError": "stale error",
            "repairAvailable": True,
        }
    )

    assert status["repairState"] == "idle"
    assert status["repairAction"] == ""
    assert status["repairError"] == ""
    assert status["repairAvailable"] is False


def test_a_lost_repair_handoff_does_not_stay_stuck_on_repairing() -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        paths, _install_root, _rollback_root = _terminal_install(tmp)
        update_state.save_status(
            paths,
            {
                **update_state.load_status(paths),
                "repairState": "repairing",
                "repairAction": "restore-snapshot",
            },
        )

        status = update_state.load_status(paths)

        assert status["repairState"] == "idle"
        assert status["repairAvailable"] is False
        assert "did not start" in status["repairError"]
