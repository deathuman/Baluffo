from hashlib import sha256
from pathlib import Path
from unittest import mock

import pytest

from src.ship import desktop_update_constants as update_constants
from src.ship import desktop_update_service as du_service
from src.ship import desktop_update_shared as du_shared
from src.ship import desktop_update_state as update_state
from tests.helpers.temp_paths import workspace_tmpdir


def _write_credible_handoff_request(
    paths: du_shared.DesktopUpdatePaths,
    session_root: Path,
    *,
    install_state: str = "handoff_requested",
    launcher_pid: int = 1234,
    launcher_token: str = "token-1",
) -> None:
    session_root.mkdir(parents=True, exist_ok=True)
    update_state.write_json_atomic(
        session_root / "desktop-session.json",
        {
            "launcherPid": int(launcher_pid),
            "launcherToken": str(launcher_token),
        },
    )
    update_state.write_json_atomic(
        paths.install_plan_path,
        {
            "planVersion": 1,
            "installRoot": str(paths.install_root),
            "dataDir": str(paths.data_dir),
            "tempHelperPath": str(paths.install_root / update_constants.DESKTOP_UPDATE_HELPER_NAME),
            "targetVersion": "1.4.0",
            "currentVersion": "0.1.0",
            "manifestPath": str(paths.manifest_cache_path),
            "downloadedZipPath": str(paths.downloads_dir / "baluffo-portable-1.4.0.zip"),
            "expectedZipSha256": "a" * 64,
            "manifestKeyId": "desktop-ed25519-test",
            "rollbackPath": str(paths.rollback_root / "rollback-1"),
            "updaterWorkingDir": str(paths.updater_dir),
            "createdAt": "2026-04-19T12:00:00Z",
            "launcherPid": int(launcher_pid),
            "launcherToken": str(launcher_token),
            "desktopSessionRoot": str(session_root),
        },
    )
    update_state.write_json_atomic(
        paths.handoff_request_path,
        {"requestedAt": "2026-04-19T12:00:00Z"},
    )
    update_state.save_status(
        paths,
        {
            **update_state.default_status_payload(current_version="0.1.0"),
            "installState": str(install_state),
        },
    )


def test_pid_is_running_suppresses_expected_psutil_failures() -> None:
    class FakePsutilError(Exception):
        pass

    fake_psutil = mock.Mock()
    fake_psutil.Process.side_effect = FakePsutilError("missing process")
    fake_psutil.Error = FakePsutilError
    fake_psutil.STATUS_ZOMBIE = "zombie"

    with mock.patch.object(du_shared, "psutil", fake_psutil):
        assert du_shared.pid_is_running(4242) is False


def test_pid_is_running_does_not_suppress_unexpected_psutil_failures() -> None:
    fake_psutil = mock.Mock()
    fake_psutil.Process.side_effect = RuntimeError("programming bug")
    fake_psutil.Error = OSError
    fake_psutil.STATUS_ZOMBIE = "zombie"

    with (
        mock.patch.object(du_shared, "psutil", fake_psutil),
        pytest.raises(RuntimeError, match="programming bug"),
    ):
        du_shared.pid_is_running(4242)


def test_load_status_ignores_expected_handoff_session_root_resolution_failures() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du_shared.DesktopUpdatePaths.from_data_dir(data_dir)
        _write_credible_handoff_request(paths, Path(tmp) / "session")

        with (
            mock.patch.object(update_state, "pid_is_running", return_value=True),
            mock.patch.object(update_state, "_resolve_runtime_path", side_effect=OSError("bad")),
        ):
            status = update_state.load_status(paths, current_version="0.1.0")

        assert status["installState"] == "idle"


def test_load_status_does_not_suppress_unexpected_handoff_session_root_failures() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du_shared.DesktopUpdatePaths.from_data_dir(data_dir)
        _write_credible_handoff_request(paths, Path(tmp) / "session")

        with (
            mock.patch.object(update_state, "pid_is_running", return_value=True),
            mock.patch.object(
                update_state,
                "_resolve_runtime_path",
                side_effect=AssertionError("unexpected"),
            ),
            pytest.raises(AssertionError, match="unexpected"),
        ):
            update_state.load_status(paths, current_version="0.1.0")


def test_resolve_ship_current_version_suppresses_expected_update_state_failures() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        ship_root = Path(tmp) / "portable" / "ship"

        with mock.patch.object(
            du_shared,
            "ensure_ship_update_state",
            side_effect=RuntimeError("missing current pointer"),
        ):
            assert du_shared._resolve_ship_current_version(ship_root) == ""


def test_resolve_ship_current_version_does_not_suppress_unexpected_failures() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        ship_root = Path(tmp) / "portable" / "ship"

        with (
            mock.patch.object(
                du_shared,
                "ensure_ship_update_state",
                side_effect=AssertionError("unexpected"),
            ),
            pytest.raises(AssertionError, match="unexpected"),
        ):
            du_shared._resolve_ship_current_version(ship_root)


def _ready_install_service(
    data_dir: Path,
) -> tuple[du_shared.DesktopUpdatePaths, du_service.DesktopUpdateService]:
    paths = du_shared.DesktopUpdatePaths.from_data_dir(data_dir)
    zip_path = paths.downloads_dir / "baluffo-portable-1.4.0.zip"
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    zip_path.write_bytes(b"portable update")
    expected_sha = sha256(zip_path.read_bytes()).hexdigest()
    update_state.save_status(
        paths,
        {
            **update_state.default_status_payload(current_version="0.1.0"),
            "availability": "available",
            "updateAvailable": True,
            "downloadState": "downloaded",
            "downloadedZipPath": str(zip_path),
        },
    )
    service = du_service.DesktopUpdateService(
        data_dir=data_dir,
        current_version_getter=lambda: "0.1.0",
    )
    manifest = {
        "version": "1.4.0",
        "portable_artifact": {"sha256": expected_sha},
        "key_id": "desktop-ed25519-test",
    }
    service._load_cached_manifest_parts = mock.Mock(return_value=({}, manifest, {}, []))
    return paths, service


def _download_manifest(content: bytes) -> dict[str, object]:
    return {
        "version": "1.5.0",
        "portable_artifact": {
            "url": "https://example.com/baluffo-portable-1.5.0.zip",
            "sha256": sha256(content).hexdigest(),
            "size_bytes": len(content),
        },
    }


def test_run_download_worker_progress_status_bug_is_not_suppressed() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        service = du_service.DesktopUpdateService(
            data_dir=data_dir,
            current_version_getter=lambda: "0.1.0",
        )
        content = b"portable-zip"

        def flaky_save_status(
            update_paths: du_shared.DesktopUpdatePaths, payload: dict[str, object]
        ) -> dict[str, object]:
            if payload.get("downloadState") == "downloading":
                raise AssertionError("unexpected progress bug")
            return update_state.save_status(update_paths, payload)

        def fake_download(
            _url: str,
            target: Path,
            *,
            on_progress=None,
            timeout_s: float = 300.0,
        ) -> Path:
            if callable(on_progress):
                on_progress(5, len(content))
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            return target

        with (
            mock.patch.object(du_service, "save_status", side_effect=flaky_save_status),
            mock.patch.object(du_service, "download_file", side_effect=fake_download),
            pytest.raises(AssertionError, match="unexpected progress bug"),
        ):
            service._run_download_worker(_download_manifest(content))


def test_download_update_start_does_not_suppress_unexpected_thread_failures() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du_shared.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du_service.DesktopUpdateService(
            data_dir=data_dir,
            current_version_getter=lambda: "0.1.0",
        )
        update_state.write_json_atomic(
            paths.manifest_cache_path,
            {"cachedAt": du_shared.iso_now(), "manifest": _download_manifest(b"portable-zip")},
        )
        update_state.save_status(
            paths,
            {
                **update_state.default_status_payload(current_version="0.1.0"),
                "availability": "available",
                "updateAvailable": True,
                "lastCheckedAt": du_shared.iso_now(),
            },
        )

        with (
            mock.patch.object(
                du_service.threading,
                "Thread",
                side_effect=AssertionError("unexpected thread bug"),
            ),
            pytest.raises(AssertionError, match="unexpected thread bug"),
        ):
            service.download_update()


def test_request_install_preflight_returns_structured_expected_failures() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        _paths, service = _ready_install_service(Path(tmp) / "portable" / "ship" / "data")

        with mock.patch.object(
            service,
            "_ensure_install_preflight",
            side_effect=OSError("disk unavailable"),
        ):
            result = service.request_install()

        assert result["started"] is False
        assert result["errorCode"] == "install_preflight_failed"
        assert result["error"] == "disk unavailable"


def test_request_install_preflight_does_not_suppress_unexpected_failures() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        _paths, service = _ready_install_service(Path(tmp) / "portable" / "ship" / "data")

        with (
            mock.patch.object(
                service,
                "_ensure_install_preflight",
                side_effect=AssertionError("unexpected"),
            ),
            pytest.raises(AssertionError, match="unexpected"),
        ):
            service.request_install()


def test_request_install_helper_staging_returns_structured_expected_failures() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        _paths, service = _ready_install_service(Path(tmp) / "portable" / "ship" / "data")
        session_root = Path(tmp) / "session"
        session_root.mkdir(parents=True, exist_ok=True)
        update_state.write_json_atomic(
            session_root / "desktop-session.json",
            {"launcherPid": 1234, "launcherToken": "token-1"},
        )

        with (
            mock.patch.object(service, "_ensure_install_preflight", return_value=None),
            mock.patch.object(
                du_service, "resolve_desktop_session_root", return_value=session_root
            ),
            mock.patch.object(du_service.shutil, "copy2", side_effect=OSError("copy failed")),
        ):
            result = service.request_install()

        assert result["started"] is False
        assert result["errorCode"] == "install_start_failed"
        assert result["error"] == "Could not stage the updater helper: copy failed"


def test_request_install_helper_staging_does_not_suppress_unexpected_failures() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        _paths, service = _ready_install_service(Path(tmp) / "portable" / "ship" / "data")
        session_root = Path(tmp) / "session"
        session_root.mkdir(parents=True, exist_ok=True)
        update_state.write_json_atomic(
            session_root / "desktop-session.json",
            {"launcherPid": 1234, "launcherToken": "token-1"},
        )

        with (
            mock.patch.object(service, "_ensure_install_preflight", return_value=None),
            mock.patch.object(
                du_service, "resolve_desktop_session_root", return_value=session_root
            ),
            mock.patch.object(
                du_service.shutil,
                "copy2",
                side_effect=AssertionError("unexpected"),
            ),
            pytest.raises(AssertionError, match="unexpected"),
        ):
            service.request_install()


def test_request_install_handoff_write_returns_structured_expected_failures() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        _paths, service = _ready_install_service(Path(tmp) / "portable" / "ship" / "data")
        session_root = Path(tmp) / "session"
        session_root.mkdir(parents=True, exist_ok=True)
        update_state.write_json_atomic(
            session_root / "desktop-session.json",
            {"launcherPid": 1234, "launcherToken": "token-1"},
        )

        with (
            mock.patch.object(service, "_ensure_install_preflight", return_value=None),
            mock.patch.object(
                du_service, "resolve_desktop_session_root", return_value=session_root
            ),
            mock.patch.object(du_service.shutil, "copy2", return_value=None),
            mock.patch.object(
                du_service,
                "write_json_atomic",
                side_effect=OSError("state write failed"),
            ),
        ):
            result = service.request_install()

        assert result["started"] is False
        assert result["errorCode"] == "install_start_failed"
        assert result["error"] == "Could not start the desktop update install: state write failed"


def test_request_install_handoff_write_does_not_suppress_unexpected_failures() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        _paths, service = _ready_install_service(Path(tmp) / "portable" / "ship" / "data")
        session_root = Path(tmp) / "session"
        session_root.mkdir(parents=True, exist_ok=True)
        update_state.write_json_atomic(
            session_root / "desktop-session.json",
            {"launcherPid": 1234, "launcherToken": "token-1"},
        )

        with (
            mock.patch.object(service, "_ensure_install_preflight", return_value=None),
            mock.patch.object(
                du_service, "resolve_desktop_session_root", return_value=session_root
            ),
            mock.patch.object(du_service.shutil, "copy2", return_value=None),
            mock.patch.object(
                du_service,
                "write_json_atomic",
                side_effect=AssertionError("unexpected"),
            ),
            pytest.raises(AssertionError, match="unexpected"),
        ):
            service.request_install()
