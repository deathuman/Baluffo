"""Rollback safety for the desktop updater install path.

The 0.3.063 update timed out waiting for readiness, then restored the previous
install while the new app was still running. Locked files survived the unlink,
the snapshot copy failed part-way, and the install was left with an empty
``ship/app/current.txt`` and no version tree: the bridge kept answering from
modules already in memory and returned 500 on the next lazy import. These tests
pin the two guarantees that stop that: stop the app before restoring, and read
the restored tree back from disk.
"""

import json
from pathlib import Path
from unittest import mock

import pytest

from src.ship import desktop_update_shared as du_shared
from src.ship import desktop_update_state as update_state
from src.ship import desktop_updater_install as updater_install
from tests.helpers.temp_paths import workspace_tmpdir
from tests.test_desktop_updater import (
    _patch_release_validation,
    _write_install_plan,
    updater,
)


def _rollback_fixture(tmp: str, monkeypatch: pytest.MonkeyPatch):
    """Install root, paths, plan and mocks shared by the rollback tests."""
    install_root = Path(tmp) / "portable"
    ship_root = install_root / "ship"
    data_dir = ship_root / "data"
    paths = du_shared.DesktopUpdatePaths.from_data_dir(data_dir)
    zip_path = paths.downloads_dir / "baluffo-portable-1.4.0.zip"
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    zip_path.write_text("zip", encoding="utf-8")
    rollback_root = paths.rollback_root / "1.4.0-20260414-120000"
    _write_install_plan(paths.install_plan_path, install_root, rollback_root, zip_path)
    du_shared.write_json_atomic(
        paths.manifest_cache_path,
        {
            "manifest": {
                "version": "1.4.0",
                "portable_artifact": {
                    "url": "https://example.com/baluffo-portable-1.4.0.zip",
                    "sha256": "expected-zip-sha",
                },
                "signature": "ignored-for-test",
                "key_id": "desktop-ed25519-test",
            }
        },
    )
    zip_context = mock.MagicMock()
    zip_context.__enter__.return_value = zip_context
    zip_context.__exit__.return_value = False
    _patch_release_validation(monkeypatch)
    monkeypatch.setattr(
        updater_install, "_recover_interrupted_install", lambda *args, **kwargs: False
    )
    monkeypatch.setattr(updater_install, "_wait_for_launcher_exit", lambda plan: None)
    monkeypatch.setattr(updater_install, "_copy_install_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(updater_install, "_sync_extract_to_install", lambda *args, **kwargs: None)
    monkeypatch.setattr(updater_install.zipfile, "ZipFile", mock.Mock(return_value=zip_context))
    monkeypatch.setattr(
        updater_install,
        "_verify_target_startup",
        mock.Mock(
            side_effect=RuntimeError(
                "Updated desktop app did not report startup readiness in time."
            )
        ),
    )
    return paths, install_root, rollback_root


def test_run_install_stops_the_relaunched_app_before_restoring_the_snapshot(monkeypatch) -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        paths, _install_root, rollback_root = _rollback_fixture(tmp, monkeypatch)
        order: list[str] = []

        def stop(plan, **kwargs):
            order.append("stop")
            return {}

        def restore(*args, **kwargs):
            order.append("restore")
            return {"files": 1, "currentVersion": "0.1.0"}

        def relaunch(*args, **kwargs):
            order.append("relaunch")

        monkeypatch.setattr(updater_install, "_stop_relaunched_app", stop)
        monkeypatch.setattr(updater_install, "_restore_install_snapshot", restore)
        monkeypatch.setattr(updater_install, "_launch_executable", relaunch)

        with pytest.raises(RuntimeError, match="startup readiness"):
            updater_install.run_install(paths.install_plan_path)

        assert order == ["relaunch", "stop", "restore", "relaunch"]
        assert rollback_root.exists()


def test_run_install_reports_rollback_failure_and_keeps_the_snapshot(monkeypatch) -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        paths, _install_root, rollback_root = _rollback_fixture(tmp, monkeypatch)
        stopped = {"pid": 4242, "terminated": True, "aliveAfter": False}
        monkeypatch.setattr(updater_install, "_stop_relaunched_app", lambda plan, **kwargs: stopped)
        monkeypatch.setattr(
            updater_install,
            "_restore_install_snapshot",
            mock.Mock(side_effect=OSError("Baluffo.exe is in use")),
        )
        launch_executable = mock.Mock()
        monkeypatch.setattr(updater_install, "_launch_executable", launch_executable)

        with pytest.raises(RuntimeError, match="startup readiness"):
            updater_install.run_install(paths.install_plan_path)

        status = update_state.load_status(paths, current_version="0.1.0")
        assert status["installState"] == "failed"
        assert status["installStage"] == "rollback_failed"
        assert "install_restore_failed" in status["lastError"]
        assert status["rollbackVerified"] is False
        assert status["rollbackPath"] == str(rollback_root)
        # Only the relaunch of the new build happened; a failed restore must not
        # start an app on a half-restored tree.
        assert launch_executable.call_count == 1


def test_stop_relaunched_app_kills_the_session_launcher_and_confirms_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session_root = tmp_path / "session"
    session_root.mkdir()
    (session_root / "desktop-session.json").write_text(
        json.dumps({"launcherPid": 4242}), encoding="utf-8"
    )
    killed: list[int] = []
    monkeypatch.setattr(updater_install, "_terminate_process_tree", killed.append)
    probes = iter([True, True, False])
    monkeypatch.setattr(updater_install, "pid_is_running", lambda pid: next(probes, False))

    status = updater_install._stop_relaunched_app(
        {"desktopSessionRoot": str(session_root)}, timeout_s=5
    )

    assert killed == [4242]
    assert status == {"pid": 4242, "terminated": True, "aliveAfter": False}


def test_restore_install_snapshot_replaces_the_tree_and_returns_the_previous_version(
    tmp_path: Path,
) -> None:
    install_root = tmp_path / "portable"
    snapshot_root = tmp_path / "rollback" / "runtime"
    (snapshot_root / "ship" / "app").mkdir(parents=True)
    (snapshot_root / "Baluffo.exe").write_text("previous", encoding="utf-8")
    (snapshot_root / "ship" / "app" / "current.txt").write_text("0.1.0\n", encoding="utf-8")
    (install_root / "ship" / "app").mkdir(parents=True)
    (install_root / "Baluffo.exe").write_text("updated", encoding="utf-8")
    (install_root / "ship" / "app" / "current.txt").write_text("1.4.0\n", encoding="utf-8")
    (install_root / "ship" / "app" / "only-in-update.txt").write_text("x", encoding="utf-8")
    (install_root / "ship" / "data").mkdir(parents=True)
    (install_root / "ship" / "data" / "runtime.db").write_text("data", encoding="utf-8")

    restored = updater_install._restore_install_snapshot(install_root, tmp_path / "rollback")

    assert restored == {"files": 2, "currentVersion": "0.1.0"}
    assert (install_root / "Baluffo.exe").read_text(encoding="utf-8") == "previous"
    assert not (install_root / "ship" / "app" / "only-in-update.txt").exists()
    assert (install_root / "ship" / "data" / "runtime.db").read_text(encoding="utf-8") == "data"


def test_restore_install_snapshot_rejects_a_missing_snapshot(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="snapshot is missing"):
        updater_install._restore_install_snapshot(tmp_path / "portable", tmp_path / "rollback")


def test_restore_install_snapshot_rejects_a_snapshot_without_current_pointer(
    tmp_path: Path,
) -> None:
    snapshot_root = tmp_path / "rollback" / "runtime"
    snapshot_root.mkdir(parents=True)
    (snapshot_root / "Baluffo.exe").write_text("previous", encoding="utf-8")

    with pytest.raises(RuntimeError, match="snapshot is incomplete"):
        updater_install._restore_install_snapshot(tmp_path / "portable", tmp_path / "rollback")


def test_run_install_records_specific_failure_when_relaunch_verification_fails(
    monkeypatch,
) -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        install_root = Path(tmp) / "portable"
        ship_root = install_root / "ship"
        data_dir = ship_root / "data"
        paths = du_shared.DesktopUpdatePaths.from_data_dir(data_dir)
        zip_path = paths.downloads_dir / "baluffo-portable-1.4.0.zip"
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        zip_path.write_text("zip", encoding="utf-8")
        rollback_root = paths.rollback_root / "1.4.0-20260414-120000"
        _write_install_plan(paths.install_plan_path, install_root, rollback_root, zip_path)
        du_shared.write_json_atomic(
            paths.manifest_cache_path,
            {
                "manifest": {
                    "version": "1.4.0",
                    "portable_artifact": {
                        "url": "https://example.com/baluffo-portable-1.4.0.zip",
                        "sha256": "expected-zip-sha",
                    },
                    "signature": "ignored-for-test",
                    "key_id": "desktop-ed25519-test",
                }
            },
        )
        zip_context = mock.MagicMock()
        zip_context.__enter__.return_value = zip_context
        zip_context.__exit__.return_value = False
        restore_snapshot = mock.Mock(return_value={"files": 1, "currentVersion": "0.1.0"})
        launch_executable = mock.Mock()

        _patch_release_validation(monkeypatch)
        monkeypatch.setattr(
            updater_install, "_recover_interrupted_install", lambda *args, **kwargs: False
        )
        monkeypatch.setattr(updater_install, "_wait_for_launcher_exit", lambda plan: None)
        monkeypatch.setattr(updater_install.zipfile, "ZipFile", mock.Mock(return_value=zip_context))
        monkeypatch.setattr(updater_install, "_copy_install_snapshot", lambda *args, **kwargs: None)
        monkeypatch.setattr(
            updater_install, "_sync_extract_to_install", lambda *args, **kwargs: None
        )
        monkeypatch.setattr(updater_install, "_launch_executable", launch_executable)
        monkeypatch.setattr(
            updater_install,
            "_verify_target_startup",
            mock.Mock(
                side_effect=RuntimeError(
                    "Updated desktop app did not report startup readiness in time."
                )
            ),
        )
        monkeypatch.setattr(updater_install, "_restore_install_snapshot", restore_snapshot)

        with pytest.raises(
            RuntimeError, match="Updated desktop app did not report startup readiness in time."
        ):
            updater.run_install(paths.install_plan_path)

        status = update_state.load_status(paths, current_version="0.1.0")
        assert status["lastError"].startswith("desktop_update_relaunch_verification_failed:")
        assert status["installState"] == "failed"
        assert status["installStage"] == "rolled_back"
        assert status["rollbackVerified"] is True
        restore_snapshot.assert_called_once()
        assert launch_executable.call_count == 2
