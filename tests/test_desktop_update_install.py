"""Tests for desktop update install behavior."""

import json
from pathlib import Path
from unittest import mock

import pytest

from src.ship import desktop_update_service as du_service
from src.ship import desktop_update_state as update_state
from tests.helpers.desktop_update_leaf_namespace import du
from tests.helpers.ports import ADMIN_BRIDGE_TEST_PORT
from tests.helpers.temp_paths import workspace_tmpdir


def _prepare_ready_install(
    data_dir: Path,
    *,
    install_root: Path | None = None,
    ship_root: Path | None = None,
) -> tuple[du.DesktopUpdatePaths, du.DesktopUpdateService, Path]:
    paths = du.DesktopUpdatePaths.from_data_dir(
        data_dir,
        install_root=install_root,
        ship_root=ship_root,
    )
    helper_path = paths.install_root / du.DESKTOP_UPDATE_HELPER_NAME
    download_path = paths.downloads_dir / "baluffo-portable-1.4.0.zip"
    helper_path.parent.mkdir(parents=True, exist_ok=True)
    helper_path.write_text("helper", encoding="utf-8")
    download_path.parent.mkdir(parents=True, exist_ok=True)
    download_path.write_text("zip", encoding="utf-8")
    du.write_json_atomic(
        paths.manifest_cache_path,
        {
            "manifest": {
                "schema_version": 2,
                "key_id": "desktop-ed25519-test",
                "channel": "stable",
                "version": "1.4.0",
                "published_at": "2026-04-14T12:00:00Z",
                "release_notes_url": "https://example.com/release",
                "min_desktop_updater_version": "2.0.0",
                "min_supported_current_version": "0.1.0",
                "data_schema_version": "2",
                "rollback_allowed": True,
                "portable_artifact": {
                    "url": "https://example.com/baluffo-portable-1.4.0.zip",
                    "sha256": du.compute_sha256(download_path),
                    "size_bytes": int(download_path.stat().st_size),
                },
                "migration_plan": [],
                "signature": "ignored-for-test",
            }
        },
    )
    du.save_status(
        paths,
        {
            **du.default_status_payload(current_version="0.1.0"),
            "availability": "available",
            "updateAvailable": True,
            "downloadState": "downloaded",
            "downloadedZipPath": str(download_path),
        },
    )
    service = du.DesktopUpdateService(
        data_dir=data_dir,
        install_root=install_root,
        ship_root=ship_root,
        current_version_getter=lambda: "0.1.0",
    )
    return paths, service, download_path


def test_request_install_writes_plan_and_launches_helper() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        install_root = Path(tmp) / "portable"
        ship_root = install_root / "ship"
        data_dir = ship_root / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        helper_path = install_root / du.DESKTOP_UPDATE_HELPER_NAME
        download_path = paths.downloads_dir / "baluffo-portable-1.4.0.zip"
        helper_path.parent.mkdir(parents=True, exist_ok=True)
        helper_path.write_text("helper", encoding="utf-8")
        download_path.parent.mkdir(parents=True, exist_ok=True)
        download_path.write_text("zip", encoding="utf-8")
        du.write_json_atomic(
            paths.manifest_cache_path,
            {
                "manifest": {
                    "schema_version": 2,
                    "key_id": "desktop-ed25519-test",
                    "channel": "stable",
                    "version": "1.4.0",
                    "published_at": "2026-04-14T12:00:00Z",
                    "release_notes_url": "https://example.com/release",
                    "min_desktop_updater_version": "2.0.0",
                    "min_supported_current_version": "0.1.0",
                    "data_schema_version": "2",
                    "rollback_allowed": True,
                    "portable_artifact": {
                        "url": "https://example.com/baluffo-portable-1.4.0.zip",
                        "sha256": du.compute_sha256(download_path),
                        "size_bytes": int(download_path.stat().st_size),
                    },
                    "migration_plan": [],
                    "signature": "ignored-for-test",
                }
            },
        )
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "availability": "available",
                "updateAvailable": True,
                "downloadState": "downloaded",
                "downloadedZipPath": str(download_path),
            },
        )
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        session_root = Path(tmp) / "session"
        session_root.mkdir(parents=True, exist_ok=True)
        (session_root / "desktop-session.json").write_text(
            json.dumps({"launcherPid": 1234, "launcherToken": "token-1"}),
            encoding="utf-8",
        )
        du.write_success_marker(
            paths,
            app_version="0.1.0",
            bridge_port=ADMIN_BRIDGE_TEST_PORT,
            launcher_token="stale-token",
        )

        with (
            mock.patch.object(du_service, "resolve_desktop_session_root", lambda: session_root),
            mock.patch.object(du_service.shutil, "disk_usage", return_value=mock.Mock(free=10**9)),
            mock.patch.object(update_state, "pid_is_running", return_value=True),
            mock.patch.object(du_service, "verify_manifest_signature"),
        ):
            result = service.request_install()
        assert result["started"] is True
        plan = json.loads(paths.install_plan_path.read_text(encoding="utf-8"))
        assert plan["launcherPid"] == 1234
        assert plan["launcherToken"] == "token-1"
        assert plan["targetVersion"] == "1.4.0"
        assert plan["dataDir"] == str(paths.data_dir)
        assert plan["helperStdoutPath"] == str(paths.helper_stdout_log_path)
        assert plan["helperStderrPath"] == str(paths.helper_stderr_log_path)
        assert plan["helperDiagnosticsPath"] == str(paths.helper_diagnostics_log_path)
        assert result["status"]["installStage"] == "preparing"
        assert result["status"]["installStageLabel"] == "Preparing update"
        assert result["status"]["rollbackPath"]
        assert paths.handoff_request_path.is_file()
        assert not paths.success_marker_path.exists()
        assert not paths.handoff_diagnostics_path.exists()


def test_request_install_not_ready_leaves_no_partial_handoff_artifacts() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")

        result = service.request_install()

        assert result["started"] is False
        assert result["errorCode"] == "install_not_ready"
        assert not paths.install_plan_path.exists()
        assert not paths.handoff_request_path.exists()


def test_request_install_fails_preflight_when_data_root_lacks_space() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths, service, _download_path = _prepare_ready_install(data_dir)

        with mock.patch.object(
            du_service.shutil,
            "disk_usage",
            return_value=mock.Mock(free=1),
        ):
            result = service.request_install()

        assert result["started"] is False
        assert result["errorCode"] == "install_preflight_failed"
        assert "data root" in result["error"]
        assert not paths.install_plan_path.exists()
        assert not paths.handoff_request_path.exists()


def test_request_install_fails_preflight_when_install_root_lacks_space() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        install_root = Path(tmp) / "portable"
        ship_root = install_root / "ship"
        data_dir = Path(tmp) / "AppData" / "Roaming" / "Baluffo"
        paths, service, _download_path = _prepare_ready_install(
            data_dir,
            install_root=install_root,
            ship_root=ship_root,
        )
        high_space = mock.Mock(free=10**9)
        low_space = mock.Mock(free=1)

        with mock.patch.object(
            du_service.shutil,
            "disk_usage",
            side_effect=[high_space, low_space],
        ) as disk_usage_mock:
            result = service.request_install()

        assert result["started"] is False
        assert result["errorCode"] == "install_preflight_failed"
        assert "install root" in result["error"]
        assert disk_usage_mock.call_args_list[0].args[0] == paths.updater_dir
        assert disk_usage_mock.call_args_list[1].args[0] == paths.install_root
        assert not paths.install_plan_path.exists()
        assert not paths.handoff_request_path.exists()


def test_request_install_returns_handoff_unconfirmed_when_post_write_verification_fails() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        install_root = Path(tmp) / "portable"
        ship_root = install_root / "ship"
        data_dir = ship_root / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        helper_path = install_root / du.DESKTOP_UPDATE_HELPER_NAME
        download_path = paths.downloads_dir / "baluffo-portable-1.4.0.zip"
        helper_path.parent.mkdir(parents=True, exist_ok=True)
        helper_path.write_text("helper", encoding="utf-8")
        download_path.parent.mkdir(parents=True, exist_ok=True)
        download_path.write_text("zip", encoding="utf-8")
        du.write_json_atomic(
            paths.manifest_cache_path,
            {
                "manifest": {
                    "schema_version": 2,
                    "key_id": "desktop-ed25519-test",
                    "channel": "stable",
                    "version": "1.4.0",
                    "published_at": "2026-04-14T12:00:00Z",
                    "release_notes_url": "https://example.com/release",
                    "min_desktop_updater_version": "2.0.0",
                    "min_supported_current_version": "0.1.0",
                    "data_schema_version": "2",
                    "rollback_allowed": True,
                    "portable_artifact": {
                        "url": "https://example.com/baluffo-portable-1.4.0.zip",
                        "sha256": du.compute_sha256(download_path),
                        "size_bytes": int(download_path.stat().st_size),
                    },
                    "migration_plan": [],
                    "signature": "ignored-for-test",
                }
            },
        )
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "availability": "available",
                "updateAvailable": True,
                "downloadState": "downloaded",
                "downloadedZipPath": str(download_path),
            },
        )
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        session_root = Path(tmp) / "session"
        session_root.mkdir(parents=True, exist_ok=True)
        (session_root / "desktop-session.json").write_text(
            json.dumps({"launcherPid": 1234, "launcherToken": "token-1"}),
            encoding="utf-8",
        )

        with (
            mock.patch.object(du_service, "resolve_desktop_session_root", lambda: session_root),
            mock.patch.object(du_service.shutil, "disk_usage", return_value=mock.Mock(free=10**9)),
            mock.patch.object(update_state, "pid_is_running", return_value=False),
            mock.patch.object(du_service, "verify_manifest_signature"),
        ):
            result = service.request_install()

        status = du.load_status(paths, current_version="0.1.0")
        diagnostics_raw = paths.handoff_diagnostics_path.read_text(encoding="utf-8")
        diagnostics = json.loads(diagnostics_raw)
        assert result["started"] is False
        assert result["errorCode"] == "install_handoff_unconfirmed"
        assert status["downloadState"] == "downloaded"
        assert status["installState"] == "ready"
        assert status["downloadedZipPath"] == str(download_path)
        assert not paths.install_plan_path.exists()
        assert not paths.handoff_request_path.exists()
        assert diagnostics["handoffRequestPresent"] is True
        assert diagnostics["installPlanValid"] is True
        assert diagnostics["launcherPid"] == 1234
        assert diagnostics["launcherPidRunning"] is False
        assert diagnostics["desktopSessionFilePresent"] is True
        assert diagnostics["launcherPidMatchesSession"] is True
        assert diagnostics["launcherTokenMatchesSession"] is True
        assert "token-1" not in diagnostics_raw


def test_run_download_worker_failure_clears_install_ready_state_and_bad_zip() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        target = paths.downloads_dir / "baluffo-portable-1.4.0.zip"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("bad-zip", encoding="utf-8")
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "availability": "available",
                "updateAvailable": True,
                "latestVersion": "1.4.0",
                "targetVersion": "1.4.0",
                "downloadState": "downloading",
                "downloadedBytes": int(target.stat().st_size),
                "totalBytes": 123,
                "downloadPercent": 100,
                "installState": "ready",
                "installStage": "idle",
                "downloadedZipPath": str(target),
            },
        )
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        manifest = {
            "version": "1.4.0",
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-1.4.0.zip",
                "sha256": "a" * 64,
                "size_bytes": 123,
            },
        }

        with mock.patch.object(du_service, "download_file", return_value=target):
            service._run_download_worker(manifest)

        status = du.load_status(paths, current_version="0.1.0")
        assert status["availability"] == "available"
        assert status["updateAvailable"] is True
        assert status["targetVersion"] == "1.4.0"
        assert status["downloadState"] == "failed"
        assert status["installState"] == "idle"
        assert status["installStage"] == "idle"
        assert status["downloadedBytes"] == 0
        assert status["downloadPercent"] == 0
        assert status["downloadedZipPath"] == ""
        assert "checksum mismatch" in str(status["lastError"]).lower()
        assert not target.exists()


@pytest.mark.windows
def test_launch_staged_update_helper_uses_logged_spawn_contract() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        install_root = Path(tmp) / "portable"
        data_dir = install_root / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        temp_helper = Path(tmp) / "BaluffoUpdater-temp.exe"
        temp_helper.write_text("helper", encoding="utf-8")
        du.write_json_atomic(
            paths.install_plan_path,
            {
                "planVersion": 1,
                "installRoot": str(install_root),
                "dataDir": str(paths.data_dir),
                "tempHelperPath": str(temp_helper),
                "targetVersion": "1.4.0",
                "currentVersion": "0.1.0",
                "manifestPath": str(paths.manifest_cache_path),
                "downloadedZipPath": str(paths.downloads_dir / "baluffo-portable-1.4.0.zip"),
                "expectedZipSha256": "abc",
                "manifestKeyId": "desktop-ed25519-test",
                "rollbackPath": str(paths.rollback_root / "1.4.0-20260414-120000"),
                "updaterWorkingDir": str(paths.updater_dir),
                "helperStdoutPath": str(paths.helper_stdout_log_path),
                "helperStderrPath": str(paths.helper_stderr_log_path),
                "helperDiagnosticsPath": str(paths.helper_diagnostics_log_path),
                "createdAt": "2026-04-14T12:00:00Z",
                "launcherPid": 1234,
                "launcherToken": "token-1",
                "desktopSessionRoot": str(Path(tmp) / "session"),
            },
        )

        with mock.patch.object(update_state.subprocess, "Popen") as popen_mock:
            du.launch_staged_update_helper(paths)

        popen_mock.assert_called_once()
        _, kwargs = popen_mock.call_args
        expected_flags = 0
        if update_state.os.name == "nt":
            expected_flags = int(getattr(update_state.subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
        assert int(kwargs.get("creationflags") or 0) == expected_flags
        assert kwargs["cwd"] == str(paths.updater_dir)
        assert isinstance(kwargs["env"], dict)
        assert kwargs["env"]["TEMP"] == str(du.helper_runtime_tmpdir())
        assert kwargs["env"]["TMP"] == str(du.helper_runtime_tmpdir())
        assert kwargs["env"]["BALUFFO_DATA_DIR"] == str(paths.data_dir)
        assert kwargs["env"]["BALUFFO_INSTALL_ROOT"] == str(paths.install_root)
        assert kwargs["env"]["BALUFFO_SHIP_ROOT"] == str(paths.ship_root)
        assert Path(str(kwargs["stdout"].name)).resolve() == paths.helper_stdout_log_path.resolve()
        assert Path(str(kwargs["stderr"].name)).resolve() == paths.helper_stderr_log_path.resolve()
