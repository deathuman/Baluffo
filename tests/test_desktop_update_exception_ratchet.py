from pathlib import Path
from unittest import mock

import pytest

from src.ship import desktop_update_constants as update_constants
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
