import json
from pathlib import Path
from unittest import mock

from src.ship import desktop_update as du
from src.ship import desktop_updater as updater
from tests.helpers.temp_paths import workspace_tmpdir


def _write_install_plan(plan_path: Path, install_root: Path, rollback_root: Path, zip_path: Path) -> dict[str, object]:
    plan = {
        "planVersion": 1,
        "installRoot": str(install_root),
        "tempHelperPath": str(install_root / "BaluffoUpdater.exe"),
        "targetVersion": "1.4.0",
        "currentVersion": "0.1.0",
        "manifestPath": str(install_root / "ship" / "data" / "updater" / du.MANIFEST_CACHE_FILE),
        "downloadedZipPath": str(zip_path),
        "expectedZipSha256": "expected-zip-sha",
        "manifestKeyId": "desktop-ed25519-test",
        "rollbackPath": str(rollback_root),
        "updaterWorkingDir": str(install_root / "ship" / "data" / "updater"),
        "createdAt": "2026-04-14T12:00:00Z",
        "launcherPid": 1234,
        "launcherToken": "token-1",
        "desktopSessionRoot": str(install_root / "session"),
    }
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    return plan


def test_launch_executable_uses_install_root_as_cwd_and_can_clear_version_override(monkeypatch) -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        install_root = Path(tmp) / "portable"
        runtime_exe = install_root / "Baluffo.exe"
        runtime_exe.parent.mkdir(parents=True, exist_ok=True)
        runtime_exe.write_text("exe", encoding="utf-8")
        popen_mock = mock.Mock()
        monkeypatch.setenv("BALUFFO_APP_VERSION_OVERRIDE", "0.0.9")
        monkeypatch.setattr(updater.subprocess, "Popen", popen_mock)

        updater._launch_executable(runtime_exe, clear_app_version_override=True)

        _, kwargs = popen_mock.call_args
        assert kwargs["cwd"] == str(install_root)
        assert isinstance(kwargs["env"], dict)
        assert "BALUFFO_APP_VERSION_OVERRIDE" not in kwargs["env"]


def test_helper_diagnostics_path_prefers_plan_field() -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        root = Path(tmp)
        plan_path = root / "install-plan.json"
        diag_path = root / "logs" / "helper.diagnostics.jsonl"
        plan_path.write_text(
            json.dumps(
                {
                    "helperDiagnosticsPath": str(diag_path),
                    "updaterWorkingDir": str(root / "updater"),
                }
            ),
            encoding="utf-8",
        )

        resolved = updater._helper_diagnostics_path_for_plan(plan_path)

        assert resolved == diag_path.resolve()


def test_recover_interrupted_install_restores_runtime_snapshot_and_backup(monkeypatch) -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        install_root = Path(tmp) / "portable"
        ship_root = install_root / "ship"
        data_dir = ship_root / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        rollback_root = paths.rollback_root / "1.4.0-20260414-120000"
        runtime_file = install_root / "Baluffo.exe"
        runtime_file.parent.mkdir(parents=True, exist_ok=True)
        runtime_file.write_text("partially-updated", encoding="utf-8")
        snapshot_file = rollback_root / "runtime" / "Baluffo.exe"
        snapshot_file.parent.mkdir(parents=True, exist_ok=True)
        snapshot_file.write_text("previous-runtime", encoding="utf-8")
        backup_ref = Path(tmp) / "backup-ref"
        backup_ref.mkdir(parents=True, exist_ok=True)
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "installState": "installing",
                "installStage": "migrating",
                "rollbackPath": str(rollback_root),
                "migrationBackupPath": str(backup_ref),
            },
        )
        restore_backup = mock.Mock()
        monkeypatch.setattr(updater.update_manager, "restore_data_backup", restore_backup)

        completed = updater._recover_interrupted_install(
            {"targetVersion": "1.4.0"},
            install_root=install_root,
            ship_root=ship_root,
            paths=paths,
            rollback_root=rollback_root,
        )

        status = du.load_status(paths, current_version="0.1.0")
        assert completed is False
        assert runtime_file.read_text(encoding="utf-8") == "previous-runtime"
        restore_backup.assert_called_once()
        assert status["installState"] == "idle"
        assert status["installStage"] == "idle"
        assert status["rollbackPath"] == ""
        assert status["migrationBackupPath"] == ""


def test_run_install_finishes_stale_verifying_state_when_target_is_already_healthy(monkeypatch) -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        install_root = Path(tmp) / "portable"
        ship_root = install_root / "ship"
        data_dir = ship_root / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        zip_path = paths.downloads_dir / "baluffo-portable-1.4.0.zip"
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        zip_path.write_text("zip", encoding="utf-8")
        rollback_root = paths.rollback_root / "1.4.0-20260414-120000"
        plan = _write_install_plan(paths.install_plan_path, install_root, rollback_root, zip_path)
        du.write_json_atomic(
            paths.manifest_cache_path,
            {
                "manifest": {
                    "version": "1.4.0",
                    "portable_artifact": {"sha256": "expected-zip-sha"},
                    "signature": "ignored-for-test",
                    "key_id": "desktop-ed25519-test",
                }
            },
        )
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "installState": "verifying",
                "installStage": "verifying",
                "downloadState": "downloaded",
                "downloadedZipPath": str(zip_path),
                "rollbackPath": str(rollback_root),
                "targetVersion": "1.4.0",
            },
        )
        wait_for_exit = mock.Mock()
        progress_cls = mock.Mock()
        progress_cls.return_value = mock.Mock(start=mock.Mock(), update=mock.Mock(), close=mock.Mock())
        monkeypatch.setattr(updater, "HelperProgressWindow", progress_cls)
        monkeypatch.setattr(updater, "validate_desktop_manifest", lambda manifest: None)
        monkeypatch.setattr(updater, "verify_manifest_signature", lambda manifest, public_keys=None: None)
        monkeypatch.setattr(updater, "compute_sha256", lambda path: "expected-zip-sha")
        monkeypatch.setattr(updater, "_verify_target_startup", lambda plan, timeout_s=90.0: None)
        monkeypatch.setattr(updater, "_wait_for_launcher_exit", wait_for_exit)

        result = updater.run_install(paths.install_plan_path)

        status = du.load_status(paths, current_version="1.4.0")
        assert result == {"ok": True, "installedVersion": "1.4.0"}
        assert wait_for_exit.call_count == 0
        assert status["installState"] == "installed"
        assert status["installStage"] == "installed"
        assert status["targetVersion"] == str(plan["targetVersion"])
