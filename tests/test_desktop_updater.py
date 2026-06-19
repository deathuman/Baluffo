import json
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from urllib import error as urllib_error

import pytest

from src.ship import desktop_update_shared as du_shared
from src.ship import desktop_update_state as update_state
from src.ship import desktop_updater as updater
from src.ship import desktop_updater_install as updater_install
from src.ship import desktop_updater_release as updater_release
from src.ship.desktop_update_constants import MANIFEST_CACHE_FILE
from tests.helpers.temp_paths import workspace_tmpdir


def _write_install_plan(
    plan_path: Path, install_root: Path, rollback_root: Path, zip_path: Path
) -> dict[str, object]:
    data_dir = plan_path.parent.parent
    plan = {
        "planVersion": 1,
        "installRoot": str(install_root),
        "dataDir": str(data_dir),
        "tempHelperPath": str(install_root / "BaluffoUpdater.exe"),
        "targetVersion": "1.4.0",
        "currentVersion": "0.1.0",
        "manifestPath": str(install_root / "ship" / "data" / "updater" / MANIFEST_CACHE_FILE),
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


def _patch_release_validation(monkeypatch, sha: str = "expected-zip-sha") -> None:
    monkeypatch.setattr(updater_release, "validate_desktop_manifest", lambda manifest: None)
    monkeypatch.setattr(
        updater_release, "load_desktop_update_public_keys", lambda candidate_paths=None: []
    )
    monkeypatch.setattr(
        updater_release, "verify_manifest_signature", lambda manifest, public_keys=None: None
    )
    monkeypatch.setattr(updater_release, "compute_sha256", lambda path: sha)


def test_launch_executable_uses_install_root_as_cwd_and_can_set_data_dir(
    monkeypatch,
) -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        install_root = Path(tmp) / "portable"
        data_dir = Path(tmp) / "AppData" / "Roaming" / "Baluffo"
        runtime_exe = install_root / "Baluffo.exe"
        runtime_exe.parent.mkdir(parents=True, exist_ok=True)
        runtime_exe.write_text("exe", encoding="utf-8")
        popen_mock = mock.Mock()
        monkeypatch.setenv("BALUFFO_APP_VERSION_OVERRIDE", "0.0.9")
        monkeypatch.setattr(updater.subprocess, "Popen", popen_mock)

        updater._launch_executable(
            runtime_exe,
            clear_app_version_override=True,
            data_dir=data_dir,
        )

        _, kwargs = popen_mock.call_args
        assert kwargs["cwd"] == str(install_root)
        assert isinstance(kwargs["env"], dict)
        assert "BALUFFO_APP_VERSION_OVERRIDE" not in kwargs["env"]
        assert kwargs["env"]["BALUFFO_DATA_DIR"] == str(data_dir.resolve())


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


def test_helper_failure_dialog_enabled_respects_no_dialog_env(monkeypatch) -> None:
    monkeypatch.setenv(updater.DESKTOP_UPDATER_NO_DIALOG_ENV, "1")

    assert updater._helper_failure_dialog_enabled() is False


def test_helper_relaunch_verify_timeout_s_uses_env_override(monkeypatch) -> None:
    monkeypatch.setenv(updater.DESKTOP_UPDATER_VERIFY_TIMEOUT_ENV, "7.5")

    assert updater._helper_relaunch_verify_timeout_s() == 7.5


def test_helper_relaunch_verify_timeout_s_falls_back_on_invalid_env(monkeypatch) -> None:
    monkeypatch.setenv(updater.DESKTOP_UPDATER_VERIFY_TIMEOUT_ENV, "fast")

    assert updater._helper_relaunch_verify_timeout_s(default=12.0) == 12.0


def test_helper_window_layout_uses_baluffo_dark_tokens() -> None:
    layout = updater._helper_window_layout("  ")

    assert layout["brandText"] == "Baluffo Update"
    assert layout["titleText"] == "Installing the latest portable build"
    assert layout["supportText"] == "Baluffo can stay closed while the update finishes."
    assert layout["initialMessage"] == "Preparing update"
    assert layout["size"] == {"width": 420, "height": 188}
    assert layout["tokens"]["window_bg"] == "#17141f"
    assert layout["tokens"]["accent"] == "#bb86fc"
    assert layout["tokens"]["panel_border"] == "#3d3550"


def test_drain_helper_queue_forwards_messages_and_close() -> None:
    progress = updater.HelperProgressWindow()
    progress._closed = mock.Mock(wait=mock.Mock(return_value=True), set=mock.Mock())
    close_window = mock.Mock()
    messages: list[str] = []

    progress.start(" ")
    progress.update("Extracting portable build")
    progress.close()

    should_close = updater._drain_helper_queue(
        progress,
        on_message=messages.append,
        on_close=close_window,
    )

    assert should_close is True
    assert messages == ["Preparing update", "Extracting portable build"]
    close_window.assert_called_once_with()
    progress._closed.wait.assert_called_once_with(timeout=2.0)
    progress._closed.set.assert_called_once_with()


def test_main_failure_path_still_uses_native_error_message(monkeypatch) -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        plan_path = Path(tmp) / "install-plan.json"
        plan_path.write_text("{}", encoding="utf-8")
        show_message = mock.Mock()

        class ImmediateThread:
            def __init__(self, *, target, daemon, name) -> None:
                self._target = target

            def start(self) -> None:
                self._target()

            def join(self) -> None:
                return None

        monkeypatch.setattr(
            updater,
            "parse_args",
            lambda argv=None: SimpleNamespace(install_plan=str(plan_path)),
        )
        monkeypatch.setattr(updater, "_show_message", show_message)
        monkeypatch.setattr(
            updater,
            "HelperProgressWindow",
            mock.Mock(return_value=mock.Mock(run=mock.Mock(), close=mock.Mock())),
        )
        monkeypatch.setattr(
            updater,
            "run_install",
            mock.Mock(side_effect=RuntimeError("boom during install")),
        )
        monkeypatch.setattr(updater.threading, "Thread", ImmediateThread)

        result = updater.main([])
        assert result == 1
        show_message.assert_called_once_with("Baluffo Update Failed", "boom during install")


def test_recover_interrupted_install_restores_runtime_snapshot_and_backup(monkeypatch) -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        install_root = Path(tmp) / "portable"
        ship_root = install_root / "ship"
        data_dir = ship_root / "data"
        paths = du_shared.DesktopUpdatePaths.from_data_dir(data_dir)
        rollback_root = paths.rollback_root / "1.4.0-20260414-120000"
        runtime_file = install_root / "Baluffo.exe"
        runtime_file.parent.mkdir(parents=True, exist_ok=True)
        runtime_file.write_text("partially-updated", encoding="utf-8")
        snapshot_file = rollback_root / "runtime" / "Baluffo.exe"
        snapshot_file.parent.mkdir(parents=True, exist_ok=True)
        snapshot_file.write_text("previous-runtime", encoding="utf-8")
        backup_ref = Path(tmp) / "backup-ref"
        backup_ref.mkdir(parents=True, exist_ok=True)
        update_state.save_status(
            paths,
            {
                **update_state.default_status_payload(current_version="0.1.0"),
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
        status = update_state.load_status(paths, current_version="0.1.0")
        assert completed is False
        assert runtime_file.read_text(encoding="utf-8") == "previous-runtime"
        restore_backup.assert_called_once()
        assert status["installState"] == "idle"
        assert status["installStage"] == "idle"
        assert status["rollbackPath"] == ""
        assert status["migrationBackupPath"] == ""


def test_run_install_finishes_stale_verifying_state_when_target_is_already_healthy(
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
        plan = _write_install_plan(paths.install_plan_path, install_root, rollback_root, zip_path)
        du_shared.write_json_atomic(
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
        wait_for_exit = mock.Mock()
        progress_cls = mock.Mock()
        progress_cls.return_value = mock.Mock(
            start=mock.Mock(), update=mock.Mock(), close=mock.Mock()
        )
        monkeypatch.setattr(updater, "HelperProgressWindow", progress_cls)
        _patch_release_validation(monkeypatch)
        monkeypatch.setattr(
            updater_install, "_verify_target_startup", lambda plan, timeout_s=90.0: None
        )
        monkeypatch.setattr(updater_install, "_wait_for_launcher_exit", wait_for_exit)

        result = updater.run_install(paths.install_plan_path)

        status = update_state.load_status(paths, current_version="1.4.0")
        assert result == {"ok": True, "installedVersion": "1.4.0"}
        assert wait_for_exit.call_count == 0
        assert status["installState"] == "installed"
        assert status["installStage"] == "installed"
        assert status["targetVersion"] == str(plan["targetVersion"])


def test_run_install_uses_env_override_for_relaunch_verification_timeout(monkeypatch) -> None:
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
                    "portable_artifact": {"sha256": "expected-zip-sha"},
                    "signature": "ignored-for-test",
                    "key_id": "desktop-ed25519-test",
                }
            },
        )
        zip_context = mock.MagicMock()
        zip_context.__enter__.return_value = zip_context
        zip_context.__exit__.return_value = False
        verify_startup = mock.Mock()

        monkeypatch.setenv(updater.DESKTOP_UPDATER_VERIFY_TIMEOUT_ENV, "6")
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
        launch_executable = mock.Mock()
        monkeypatch.setattr(updater_install, "_launch_executable", launch_executable)
        monkeypatch.setattr(updater_install, "_verify_target_startup", verify_startup)
        monkeypatch.setattr(updater_install, "_finalize_success", lambda *args, **kwargs: None)

        result = updater.run_install(paths.install_plan_path)

        assert result == {"ok": True, "installedVersion": "1.4.0"}
        launch_executable.assert_called_once_with(
            install_root / "Baluffo.exe",
            clear_app_version_override=True,
            data_dir=data_dir.resolve(),
        )
        verify_startup.assert_called_once_with(mock.ANY, timeout_s=6.0)


def test_run_install_recovers_manifest_cache_from_release_metadata(monkeypatch) -> None:
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
        manifest_payload = {
            "version": "1.4.0",
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-1.4.0.zip",
                "sha256": "expected-zip-sha",
            },
            "signature": "ignored-for-test",
            "key_id": "desktop-ed25519-test",
        }
        release_payload = [
            {
                "tag_name": "v1.4.0",
                "name": "Baluffo v1.4.0",
                "assets": [
                    {
                        "name": updater.DESKTOP_UPDATE_MANIFEST_ASSET,
                        "browser_download_url": "https://example.com/manifest.json",
                    }
                ],
            }
        ]
        zip_context = mock.MagicMock()
        zip_context.__enter__.return_value = zip_context
        zip_context.__exit__.return_value = False

        def fetch_json(url: str, *, timeout_s: float = 20.0):
            if url.endswith("/releases?per_page=10"):
                return release_payload
            if url == "https://example.com/manifest.json":
                return manifest_payload
            raise AssertionError(url)

        monkeypatch.setattr(updater_release, "fetch_json", fetch_json)
        monkeypatch.setattr(
            updater_release, "resolve_release_repo", lambda **_: "deathuman/Baluffo"
        )
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
        monkeypatch.setattr(updater_install, "_launch_executable", lambda *args, **kwargs: None)
        monkeypatch.setattr(
            updater_install, "_verify_target_startup", lambda plan, timeout_s=90.0: None
        )
        monkeypatch.setattr(updater_install, "_finalize_success", lambda *args, **kwargs: None)

        result = updater.run_install(paths.install_plan_path)

        cached_manifest = json.loads(paths.manifest_cache_path.read_text(encoding="utf-8"))
        assert result == {"ok": True, "installedVersion": "1.4.0"}
        assert cached_manifest["manifest"]["version"] == "1.4.0"
        assert (
            cached_manifest["manifest"]["portable_artifact"]["url"]
            == "https://example.com/baluffo-portable-1.4.0.zip"
        )


def test_run_install_redownloads_zip_when_cached_artifact_is_missing(monkeypatch) -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        install_root = Path(tmp) / "portable"
        ship_root = install_root / "ship"
        data_dir = ship_root / "data"
        paths = du_shared.DesktopUpdatePaths.from_data_dir(data_dir)
        zip_path = paths.downloads_dir / "baluffo-portable-1.4.0.zip"
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
        download_mock = mock.Mock(
            side_effect=lambda url, destination: destination.write_text("zip", encoding="utf-8")
        )

        _patch_release_validation(monkeypatch)
        monkeypatch.setattr(updater_release, "download_file", download_mock)
        monkeypatch.setattr(
            updater_install, "_recover_interrupted_install", lambda *args, **kwargs: False
        )
        monkeypatch.setattr(updater_install, "_wait_for_launcher_exit", lambda plan: None)
        monkeypatch.setattr(updater_install.zipfile, "ZipFile", mock.Mock(return_value=zip_context))
        monkeypatch.setattr(updater_install, "_copy_install_snapshot", lambda *args, **kwargs: None)
        monkeypatch.setattr(
            updater_install, "_sync_extract_to_install", lambda *args, **kwargs: None
        )
        monkeypatch.setattr(updater_install, "_launch_executable", lambda *args, **kwargs: None)
        monkeypatch.setattr(
            updater_install, "_verify_target_startup", lambda plan, timeout_s=90.0: None
        )
        monkeypatch.setattr(updater_install, "_finalize_success", lambda *args, **kwargs: None)

        result = updater.run_install(paths.install_plan_path)

        assert result == {"ok": True, "installedVersion": "1.4.0"}
        download_mock.assert_called_once_with(
            "https://example.com/baluffo-portable-1.4.0.zip",
            zip_path.resolve(),
        )


def test_run_install_records_zip_reverification_failure_after_redownload(monkeypatch) -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        install_root = Path(tmp) / "portable"
        ship_root = install_root / "ship"
        data_dir = ship_root / "data"
        paths = du_shared.DesktopUpdatePaths.from_data_dir(data_dir)
        zip_path = paths.downloads_dir / "baluffo-portable-1.4.0.zip"
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

        _patch_release_validation(monkeypatch, sha="wrong-zip-sha")
        monkeypatch.setattr(
            updater_release,
            "download_file",
            lambda url, destination: destination.write_text("zip", encoding="utf-8"),
        )
        monkeypatch.setattr(updater_install, "_restore_install_snapshot", mock.Mock())

        with pytest.raises(RuntimeError, match="Downloaded desktop ZIP failed re-verification."):
            updater.run_install(paths.install_plan_path)

        status = update_state.load_status(paths, current_version="0.1.0")
        assert status["lastError"].startswith("desktop_update_zip_reverification_failed:")
        assert status["installState"] == "failed"
        assert status["installStage"] == "failed"


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
        restore_snapshot = mock.Mock()
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
        assert status["installStage"] == "failed"
        restore_snapshot.assert_called_once()
        assert launch_executable.call_count == 2


def test_verify_target_startup_retries_after_transient_bridge_refusal(monkeypatch) -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        install_root = Path(tmp) / "portable"
        ship_root = install_root / "ship"
        data_dir = Path(tmp) / "AppData" / "Roaming" / "Baluffo"
        paths = du_shared.DesktopUpdatePaths.from_data_dir(
            data_dir,
            install_root=install_root,
            ship_root=ship_root,
        )
        rollback_root = paths.rollback_root / "1.4.0-20260414-120000"
        plan = _write_install_plan(
            paths.install_plan_path,
            install_root,
            rollback_root,
            paths.downloads_dir / "baluffo-portable-1.4.0.zip",
        )
        session_root = Path(str(plan["desktopSessionRoot"]))
        session_root.mkdir(parents=True, exist_ok=True)
        (session_root / "desktop-session.json").write_text(
            json.dumps({"bridgePort": 8877}),
            encoding="utf-8",
        )
        update_state.write_success_marker(
            paths,
            app_version="1.4.0",
            bridge_port=8877,
            launcher_token="token-1",
        )
        health_calls = mock.Mock(
            side_effect=[
                urllib_error.URLError("connection refused"),
                {
                    "service": "baluffo-bridge",
                    "desktopMode": True,
                    "startupReady": True,
                    "appVersion": "1.4.0",
                },
            ]
        )
        monotonic_values = iter((0.0, 0.0, 1.0))
        monkeypatch.setattr(updater_install, "fetch_json", health_calls)
        monkeypatch.setattr(updater.time, "monotonic", lambda: next(monotonic_values))
        monkeypatch.setattr(updater.time, "sleep", lambda _seconds: None)

        updater._verify_target_startup(plan, timeout_s=10.0)

        assert health_calls.call_count == 2
