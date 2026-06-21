"""Tests for desktop update state handoff behavior."""

import base64
import json
from pathlib import Path
from unittest import mock

from src.ship import desktop_update_state as update_state
from tests.helpers.desktop_update_leaf_namespace import du
from tests.helpers.temp_paths import workspace_tmpdir


def _write_required_root_html(version_root: Path) -> None:
    for name in ("index.html", "jobs.html", "saved.html", "admin.html"):
        (version_root / name).write_text("<html></html>\n", encoding="utf-8")


def _write_credible_handoff_request(
    paths: du.DesktopUpdatePaths,
    session_root: Path,
    *,
    install_state: str = "handoff_requested",
    launcher_pid: int = 1234,
    launcher_token: str = "token-1",
) -> None:
    session_root.mkdir(parents=True, exist_ok=True)
    du.write_json_atomic(
        session_root / "desktop-session.json",
        {
            "launcherPid": int(launcher_pid),
            "launcherToken": str(launcher_token),
        },
    )
    du.write_json_atomic(
        paths.install_plan_path,
        {
            "planVersion": 1,
            "installRoot": str(paths.install_root),
            "dataDir": str(paths.data_dir),
            "tempHelperPath": str(paths.install_root / du.DESKTOP_UPDATE_HELPER_NAME),
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
    du.write_json_atomic(paths.handoff_request_path, {"requestedAt": "2026-04-19T12:00:00Z"})
    du.save_status(
        paths,
        {
            **du.default_status_payload(current_version="0.1.0"),
            "installState": str(install_state),
        },
    )


def test_updater_install_requested_clears_stale_persisted_handoff_state() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "installState": "handoff_requested",
            },
        )

        assert du.updater_install_requested(data_dir) is False

        status = du.load_status(paths, current_version="0.1.0")
        assert status["installState"] == "idle"
        assert status["installStage"] == "idle"
        assert status["lastError"] == "Stale desktop update handoff state was cleared."


def test_updater_install_requested_ignores_installing_without_handoff_marker() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "installState": "installing",
                "installStage": "verifying",
            },
        )

        assert du.updater_install_requested(data_dir) is False


def test_updater_install_requested_clears_stale_handoff_marker_without_plan() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        du.write_json_atomic(paths.handoff_request_path, {"requestedAt": "2026-04-14T12:00:00Z"})

        assert du.updater_install_requested(data_dir) is False
        assert not paths.handoff_request_path.exists()


def test_updater_install_requested_accepts_credible_handoff_state() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        session_root = Path(tmp) / "session"
        _write_credible_handoff_request(
            paths,
            session_root,
            install_state="waiting_for_exit",
        )
        with mock.patch.object(update_state, "pid_is_running", return_value=True):
            assert du.updater_install_requested(data_dir) is True
            status = du.load_status(paths, current_version="0.1.0")

        assert status["installState"] == "waiting_for_exit"
        assert status["installStage"] == "waiting_for_exit"
        assert status["installStageLabel"] == "Closing Baluffo"


def test_updater_install_requested_ignores_default_state_after_pipeline_completion() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"

        assert du.updater_install_requested(data_dir) is False


def test_load_status_derives_install_stage_label_from_install_state() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        session_root = Path(tmp) / "session"
        _write_credible_handoff_request(paths, session_root, install_state="waiting_for_exit")

        with mock.patch.object(update_state, "pid_is_running", return_value=True):
            status = du.load_status(paths, current_version="0.1.0")

        assert status["installStage"] == "waiting_for_exit"
        assert status["installStageLabel"] == "Closing Baluffo"


def test_load_desktop_update_public_keys_reads_packaged_fallback() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        ship_root = Path(tmp) / "portable" / "ship"
        app_dir = ship_root / "app"
        version_dir = app_dir / "versions" / "0.1.0" / "packaging"
        version_dir.mkdir(parents=True, exist_ok=True)
        (app_dir / "current.txt").write_text("0.1.0\n", encoding="utf-8")
        (app_dir / "desktop-update-public-keys.json").write_text(
            json.dumps({"desktop-ed25519-2026-01": base64.b64encode(b"p" * 32).decode("ascii")}),
            encoding="utf-8",
        )

        keys = du.load_desktop_update_public_keys(
            candidate_paths=du.desktop_update_public_key_candidate_paths(ship_root)
        )

        assert keys["desktop-ed25519-2026-01"] == b"p" * 32


def test_load_desktop_update_public_keys_repairs_missing_current_pointer():
    with workspace_tmpdir("desktop-update") as tmp:
        ship_root = Path(tmp) / "portable" / "ship"
        app_dir = ship_root / "app"
        version_root = app_dir / "versions" / "0.1.0"
        packaging_dir = version_root / "packaging"
        (version_root / "src").mkdir(parents=True, exist_ok=True)
        packaging_dir.mkdir(parents=True, exist_ok=True)
        (version_root / "src" / "admin_bridge.py").write_text("print('ok')\n", encoding="utf-8")
        _write_required_root_html(version_root)
        (app_dir / "update-state.json").write_text(
            json.dumps(
                {
                    "current_version": "0.1.0",
                    "previous_version": "",
                    "last_update_status": "ready",
                    "last_error_code": "",
                    "updated_at": du.iso_now(),
                }
            ),
            encoding="utf-8",
        )
        (packaging_dir / du.PUBLIC_KEYS_FILE).write_text(
            json.dumps({"desktop-ed25519-2026-01": base64.b64encode(b"k" * 32).decode("ascii")}),
            encoding="utf-8",
        )

        keys = du.load_desktop_update_public_keys(
            candidate_paths=du.desktop_update_public_key_candidate_paths(ship_root)
        )

        assert keys["desktop-ed25519-2026-01"] == b"k" * 32
        assert (app_dir / "current.txt").read_text(encoding="utf-8").strip() == "0.1.0"
