"""AppImage update install: stage, snapshot, swap, verify, roll back.

The Linux build replaces one file, so these pin the file contract the Windows
directory install never exercised: the staged copy must land beside the target,
the snapshot must be able to put the old build back, and the relaunch must keep
the AppImage's own launch mode.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest

from src.ship import desktop_update_service as service_mod
from src.ship import desktop_update_state as update_state
from src.ship import desktop_updater_install as install_mod
from src.ship import desktop_updater_install_appimage as appimage_mod
from src.ship import desktop_updater_ui as ui_mod
from src.ship.desktop_artifact_paths import (
    APPIMAGE_EXTRACT_AND_RUN_ARG,
    ARTIFACT_KIND_APPIMAGE,
    DesktopArtifact,
)
from tests.helpers.desktop_update_leaf_namespace import du
from tests.helpers.temp_paths import workspace_tmpdir

NEW_VERSION = "0.3.025"


def _sha(path: Path) -> str:
    return str(du.compute_sha256(path))


def _prepare(
    tmp: str,
    *,
    target_text: str = "old build",
) -> tuple[Path, Path, Any, Path, dict[str, object]]:
    root = Path(tmp)
    install_root = root / "payload"
    data_dir = root / "data"
    apps_dir = root / "apps"
    apps_dir.mkdir(parents=True, exist_ok=True)
    target = apps_dir / "Baluffo-0.3.024-x86_64.AppImage"
    target.write_text(target_text, encoding="utf-8")
    target.chmod(0o755)
    paths = du.DesktopUpdatePaths.from_data_dir(
        data_dir,
        install_root=install_root,
        ship_root=install_root / "ship",
    )
    downloaded = paths.downloads_dir / f"Baluffo-{NEW_VERSION}-x86_64.AppImage"
    downloaded.parent.mkdir(parents=True, exist_ok=True)
    downloaded.write_text("new build", encoding="utf-8")
    du.write_json_atomic(
        paths.manifest_cache_path,
        {
            "manifest": {
                "schema_version": 2,
                "key_id": "desktop-ed25519-test",
                "channel": "stable",
                "version": NEW_VERSION,
                "published_at": "2026-10-23T12:00:00Z",
                "release_notes_url": "https://example.com/release",
                "min_desktop_updater_version": "2.0.0",
                "min_supported_current_version": "0.0.0",
                "data_schema_version": "2",
                "rollback_allowed": True,
                "portable_artifact": {
                    "url": f"https://example.com/{downloaded.name}",
                    "sha256": _sha(downloaded),
                    "size_bytes": int(downloaded.stat().st_size),
                },
                "migration_plan": [],
                "signature": "ignored-for-test",
            }
        },
    )
    rollback = apps_dir / f".{target.name}.{NEW_VERSION}.rollback"
    plan = {
        "planVersion": 1,
        "artifactKind": ARTIFACT_KIND_APPIMAGE,
        "targetArtifactPath": str(target),
        "relaunchArgs": [APPIMAGE_EXTRACT_AND_RUN_ARG],
        "installRoot": str(install_root),
        "dataDir": str(paths.data_dir),
        "tempHelperPath": str(root / "BaluffoUpdater-temp"),
        "targetVersion": NEW_VERSION,
        "currentVersion": "0.3.024",
        "manifestPath": str(paths.manifest_cache_path),
        "downloadedZipPath": str(downloaded),
        "expectedZipSha256": _sha(downloaded),
        "manifestKeyId": "desktop-ed25519-test",
        "rollbackPath": str(rollback),
        "updaterWorkingDir": str(paths.updater_dir),
        "helperStdoutPath": str(paths.helper_stdout_log_path),
        "helperStderrPath": str(paths.helper_stderr_log_path),
        "helperDiagnosticsPath": str(paths.helper_diagnostics_log_path),
        "createdAt": "2026-10-23T12:00:00Z",
        "launcherPid": 1234,
        "launcherToken": "token-1",
        "desktopSessionRoot": str(root / "session"),
    }
    return target, downloaded, paths, rollback, plan


def _run(plan: dict[str, object], plan_path: Path) -> dict[str, object]:
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    with (
        mock.patch.object(install_mod, "_wait_for_launcher_exit"),
        mock.patch.object(install_mod, "_verify_target_startup"),
        mock.patch.object(install_mod, "_launch_executable"),
    ):
        return install_mod.run_install(plan_path)


def test_appimage_install_replaces_the_file_and_cleans_its_snapshot() -> None:
    with workspace_tmpdir("appimage-install") as tmp:
        target, _downloaded, paths, rollback, plan = _prepare(tmp)
        result = _run(plan, paths.install_plan_path)

        assert result == {"ok": True, "installedVersion": NEW_VERSION}
        assert target.read_text(encoding="utf-8") == "new build"
        assert not rollback.exists()
        assert not list(target.parent.glob(".*.staged"))
        status = du.load_status(paths, current_version=NEW_VERSION)
        assert status["installState"] == "installed"
        assert status["downloadState"] == "idle"


def test_appimage_install_relaunches_with_the_recorded_launch_mode() -> None:
    with workspace_tmpdir("appimage-install-launch") as tmp:
        target, _downloaded, paths, _rollback, plan = _prepare(tmp)
        plan_path = paths.install_plan_path
        plan_path.write_text(json.dumps(plan), encoding="utf-8")
        with (
            mock.patch.object(install_mod, "_wait_for_launcher_exit"),
            mock.patch.object(install_mod, "_verify_target_startup"),
            mock.patch.object(install_mod, "_launch_executable") as launch_mock,
        ):
            install_mod.run_install(plan_path)

        assert launch_mock.call_count == 1
        assert launch_mock.call_args.args[0] == target
        assert launch_mock.call_args.kwargs["args"] == (APPIMAGE_EXTRACT_AND_RUN_ARG,)
        assert launch_mock.call_args.kwargs["data_dir"] == paths.data_dir


def test_appimage_install_restores_the_previous_build_when_startup_fails() -> None:
    with workspace_tmpdir("appimage-install-rollback") as tmp:
        target, _downloaded, paths, rollback, plan = _prepare(tmp)
        plan_path = paths.install_plan_path
        plan_path.write_text(json.dumps(plan), encoding="utf-8")
        with (
            mock.patch.object(install_mod, "_wait_for_launcher_exit"),
            mock.patch.object(
                install_mod,
                "_verify_target_startup",
                side_effect=RuntimeError(
                    "Updated desktop app did not report startup readiness in time."
                ),
            ),
            mock.patch.object(install_mod, "_launch_executable") as launch_mock,
        ):
            try:
                install_mod.run_install(plan_path)
            except RuntimeError:
                pass
            else:  # pragma: no cover - the helper must not swallow a failed update
                raise AssertionError("a failed relaunch must raise")

        assert target.read_text(encoding="utf-8") == "old build"
        assert not rollback.exists()
        assert launch_mock.call_count == 2
        status = du.load_status(paths, current_version="0.3.024")
        assert status["installState"] == "failed"
        assert "relaunch_verification_failed" in str(status["lastError"])


def test_stage_snapshot_and_restore_keep_the_target_runnable() -> None:
    with workspace_tmpdir("appimage-file-contract") as tmp:
        target = Path(tmp) / "Baluffo.AppImage"
        target.write_text("old", encoding="utf-8")
        new = Path(tmp) / "download.AppImage"
        new.write_text("new", encoding="utf-8")
        staged = target.with_name(".Baluffo.AppImage.staged")
        rollback = Path(tmp) / ".Baluffo.AppImage.rollback"

        appimage_mod.stage_appimage(new, staged)
        appimage_mod.snapshot_appimage(target, rollback)
        appimage_mod.replace_appimage(staged, target)

        assert target.read_text(encoding="utf-8") == "new"
        assert not staged.exists()
        assert appimage_mod.restore_appimage(target, rollback) is True
        assert target.read_text(encoding="utf-8") == "old"


def test_restore_missing_target_uses_a_leftover_snapshot() -> None:
    with workspace_tmpdir("appimage-missing-target") as tmp:
        target = Path(tmp) / "Baluffo.AppImage"
        rollback = Path(tmp) / ".Baluffo.AppImage.rollback"
        rollback.write_text("old", encoding="utf-8")

        assert appimage_mod.restore_missing_target(target, rollback) is True
        assert target.read_text(encoding="utf-8") == "old"
        assert appimage_mod.restore_appimage(target, rollback) is False


def test_helper_progress_window_needs_a_desktop_session_not_windows() -> None:
    with mock.patch.object(ui_mod, "os", SimpleNamespace(name="posix")):
        assert ui_mod._helper_window_available(env={}) is False
        assert ui_mod._helper_window_available(env={"DISPLAY": ":0"}) is True
        assert ui_mod._helper_window_available(env={"WAYLAND_DISPLAY": "wayland-0"}) is True
    with mock.patch.object(ui_mod, "os", SimpleNamespace(name="nt")):
        assert ui_mod._helper_window_available(env={}) is True


def _appimage_descriptor(target: Path) -> DesktopArtifact:
    return DesktopArtifact(
        kind=ARTIFACT_KIND_APPIMAGE,
        payload_root=target.parent,
        helper_name="baluffo-updater",
        manifest_asset="baluffo-desktop-update-manifest-linux.json",
        replace_target=target,
    )


def test_preflight_refuses_a_folder_it_cannot_write_the_new_build_into() -> None:
    with workspace_tmpdir("appimage-preflight-readonly") as tmp:
        target = Path(tmp) / "Baluffo.AppImage"
        target.write_text("old", encoding="utf-8")
        artifact = _appimage_descriptor(target)

        with mock.patch.object(
            Path,
            "write_text",
            side_effect=PermissionError("read-only file system"),
        ):
            try:
                service_mod.DesktopUpdateService._ensure_appimage_preflight(
                    artifact,
                    required_free=1,
                )
            except RuntimeError as exc:
                assert "not writable" in str(exc)
            else:  # pragma: no cover - a surviving install would fail much later
                raise AssertionError("an unwritable folder must fail the preflight")


def test_appimage_install_refuses_a_plan_that_names_no_target() -> None:
    with workspace_tmpdir("appimage-plan-no-target") as tmp:
        _target, _downloaded, paths, _rollback, plan = _prepare(tmp)
        plan.pop("targetArtifactPath")

        with pytest.raises(RuntimeError, match="does not name the AppImage"):
            _run(plan, paths.install_plan_path)


def test_install_plan_names_the_appimage_and_its_launch_mode() -> None:
    with workspace_tmpdir("appimage-plan") as tmp:
        target, downloaded, paths, _rollback, _plan = _prepare(tmp)
        helper = paths.install_root / "baluffo-updater"
        helper.parent.mkdir(parents=True, exist_ok=True)
        helper.write_text("helper", encoding="utf-8")
        session_root = Path(tmp) / "session"
        session_root.mkdir(parents=True, exist_ok=True)
        (session_root / "desktop-session.json").write_text(
            json.dumps({"launcherPid": 1234, "launcherToken": "token-1"}),
            encoding="utf-8",
        )
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.3.024"),
                "availability": "available",
                "updateAvailable": True,
                "downloadState": "downloaded",
                "downloadedZipPath": str(downloaded),
            },
        )
        service = du.DesktopUpdateService(
            data_dir=paths.data_dir,
            install_root=paths.install_root,
            ship_root=paths.ship_root,
            current_version_getter=lambda: "0.3.024",
        )
        artifact = DesktopArtifact(
            kind=ARTIFACT_KIND_APPIMAGE,
            payload_root=paths.install_root,
            helper_name="baluffo-updater",
            manifest_asset="baluffo-desktop-update-manifest-linux.json",
            replace_target=target,
            relaunch_args=(APPIMAGE_EXTRACT_AND_RUN_ARG,),
        )
        with (
            mock.patch.object(service_mod, "desktop_artifact", return_value=artifact),
            mock.patch.object(service_mod, "resolve_desktop_session_root", lambda: session_root),
            mock.patch.object(
                service_mod.shutil,
                "disk_usage",
                return_value=SimpleNamespace(free=10**9),
            ),
            mock.patch.object(update_state, "pid_is_running", return_value=True),
        ):
            result = service.request_install()

        assert result["started"] is True, result
        plan = json.loads(paths.install_plan_path.read_text(encoding="utf-8"))
        assert plan["artifactKind"] == ARTIFACT_KIND_APPIMAGE
        assert plan["targetArtifactPath"] == str(target)
        assert plan["relaunchArgs"] == [APPIMAGE_EXTRACT_AND_RUN_ARG]
        assert plan["rollbackPath"].startswith(str(target.parent))
        temp_helper = Path(plan["tempHelperPath"])
        assert temp_helper.name.startswith("BaluffoUpdater-")
        assert temp_helper.suffix == ""
        assert temp_helper.is_file()
