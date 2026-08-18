"""Tests for desktop update status payload behavior."""

import json
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from src.ship import desktop_update_shared as du_shared
from src.ship import desktop_update_state as update_state
from src.ship.desktop_app import config as desktop_app_config
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


def test_resolve_github_api_base_honors_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(du.GITHUB_API_BASE_ENV, "http://127.0.0.1:9000/api/")

    assert du.resolve_github_api_base() == "http://127.0.0.1:9000/api"


def test_resolve_release_repo_prefers_packaged_desktop_update_config() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        ship_root = Path(tmp) / "portable" / "ship"
        packaging_dir = ship_root / "app" / "versions" / "0.1.0" / "packaging"
        packaging_dir.mkdir(parents=True, exist_ok=True)
        (ship_root / "app" / "current.txt").write_text("0.1.0\n", encoding="utf-8")
        (packaging_dir / "desktop-update-config.json").write_text(
            json.dumps({"repo": "owner/app-release"}),
            encoding="utf-8",
        )
        (packaging_dir / "github-app-sync-config.json").write_text(
            json.dumps({"repo": "owner/sync-backup"}),
            encoding="utf-8",
        )
        repo = du.resolve_release_repo(
            install_root=ship_root.parent,
            ship_root=ship_root,
        )

        assert repo == "owner/app-release"


def test_resolve_release_repo_repairs_missing_current_pointer() -> None:
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
        (packaging_dir / "desktop-update-config.json").write_text(
            json.dumps({"repo": "owner/repaired-release"}),
            encoding="utf-8",
        )

        repo = du.resolve_release_repo(
            install_root=ship_root.parent,
            ship_root=ship_root,
        )

        assert repo == "owner/repaired-release"
        assert (app_dir / "current.txt").read_text(encoding="utf-8").strip() == "0.1.0"


def test_resolve_desktop_session_root_falls_back_to_temp_when_primary_is_not_writable() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        local_app_data = Path(tmp) / "local-app-data"
        xdg_data = Path(tmp) / "xdg-data"
        temp_root = Path(tmp) / "temp-root"
        original_write_text = Path.write_text

        def flaky_write_text(
            self: Path,
            data: str,
            encoding: str | None = None,
            errors: str | None = None,
            newline: str | None = None,
        ) -> int:
            path_text = str(self)
            blocked = {"local-app-data", "xdg-data"}
            if ".baluffo-write-probe" in path_text and any(p in path_text for p in blocked):
                raise OSError("read-only")
            return original_write_text(
                self, data, encoding=encoding, errors=errors, newline=newline
            )

        with (
            mock.patch.dict(
                du_shared.os.environ,
                {
                    "LOCALAPPDATA": str(local_app_data),
                    "XDG_DATA_HOME": str(xdg_data),
                    "USERNAME": "tester",
                    "TEMP": str(temp_root),
                    "TMP": str(temp_root),
                },
                clear=False,
            ),
            mock.patch.object(du_shared.tempfile, "gettempdir", return_value=str(temp_root)),
            mock.patch.object(Path, "write_text", new=flaky_write_text),
        ):
            session_root = du.resolve_desktop_session_root()

        assert session_root == (temp_root / "Baluffo-tester").resolve()


def test_resolve_desktop_session_root_honors_env_override() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        override_root = Path(tmp) / "override-session-root"

        session_root = du.resolve_desktop_session_root(
            {
                "BALUFFO_DESKTOP_SESSION_ROOT": str(override_root),
                "LOCALAPPDATA": str(Path(tmp) / "local-app-data"),
                "USERNAME": "tester",
            }
        )

        assert session_root == override_root.resolve()


def test_resolve_desktop_session_root_falls_back_to_runtime_temp_when_standard_locations_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        root = Path(tmp)
        temp_root = root / "temp"
        localappdata_root = root / "localappdata"
        xdg_root = root / "xdg-data"
        env = {
            "LOCALAPPDATA": str(localappdata_root),
            "USERNAME": "tester",
            "XDG_DATA_HOME": str(xdg_root),
        }
        local_candidate = (localappdata_root / "Baluffo").resolve()
        xdg_candidate = (xdg_root / "Baluffo").resolve()
        temp_candidate = (temp_root / "Baluffo-tester").resolve()
        blocked_parents = {local_candidate, xdg_candidate, temp_candidate}
        original_write_text = Path.write_text

        monkeypatch.setattr(desktop_app_config.tempfile, "gettempdir", lambda: str(temp_root))
        monkeypatch.setattr(desktop_app_config, "_RUNTIME_SESSION_ROOT", None)

        def blocked_write_text(self: Path, *args: Any, **kwargs: Any) -> int:
            if self.name == ".baluffo-write-probe" and self.parent in blocked_parents:
                raise OSError("blocked for test")
            return original_write_text(self, *args, **kwargs)

        monkeypatch.setattr(Path, "write_text", blocked_write_text)

        resolved = du.resolve_desktop_session_root(env)

    assert "BaluffoRuntime" in str(resolved)


def test_check_for_update_clears_stale_downloaded_state_for_newer_manifest() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        stale_zip = paths.downloads_dir / "baluffo-portable-1.4.0.zip"
        stale_zip.parent.mkdir(parents=True, exist_ok=True)
        stale_zip.write_text("stale-zip", encoding="utf-8")
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "availability": "available",
                "updateAvailable": True,
                "latestVersion": "1.4.0",
                "targetVersion": "1.4.0",
                "downloadState": "downloaded",
                "installState": "ready",
                "downloadedZipPath": str(stale_zip),
            },
        )
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        manifest = {
            "schema_version": 2,
            "key_id": "desktop-ed25519-test",
            "channel": "stable",
            "version": "1.5.0",
            "published_at": "2026-04-15T12:00:00Z",
            "release_notes_url": "https://example.com/release",
            "min_desktop_updater_version": "2.0.0",
            "min_supported_current_version": "0.1.0",
            "data_schema_version": "2",
            "rollback_allowed": True,
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-1.5.0.zip",
                "sha256": "a" * 64,
                "size_bytes": 123,
            },
            "migration_plan": [],
            "signature": "ignored-for-test",
        }

        with (
            mock.patch.object(
                service, "_resolve_latest_release", return_value={"id": 123, "tag_name": "v1.5.0"}
            ),
            mock.patch.object(service, "_resolve_manifest_from_release", return_value=manifest),
        ):
            status = service.check_for_update(force=True)

        assert status["targetVersion"] == "1.5.0"
        assert status["downloadState"] == "idle"
        assert status["installState"] == "idle"
        assert status["downloadedZipPath"] == ""


def test_get_status_payload_promotes_completed_stale_download_to_ready() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        zip_path = paths.downloads_dir / "baluffo-portable-1.5.0.zip"
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        zip_path.write_text("portable-zip", encoding="utf-8")
        manifest = {
            "schema_version": 2,
            "key_id": "desktop-ed25519-test",
            "channel": "stable",
            "version": "1.5.0",
            "published_at": "2026-04-15T12:00:00Z",
            "release_notes_url": "https://example.com/release",
            "min_desktop_updater_version": "2.0.0",
            "min_supported_current_version": "0.1.0",
            "data_schema_version": "2",
            "rollback_allowed": True,
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-1.5.0.zip",
                "sha256": du.compute_sha256(zip_path),
                "size_bytes": int(zip_path.stat().st_size),
            },
            "migration_plan": [],
            "signature": "ignored-for-test",
        }
        du.write_json_atomic(
            paths.manifest_cache_path,
            {"cachedAt": du.iso_now(), "manifest": manifest},
        )
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "availability": "available",
                "updateAvailable": True,
                "latestVersion": "1.5.0",
                "targetVersion": "1.5.0",
                "downloadState": "downloading",
                "downloadedBytes": 12,
                "totalBytes": 100,
                "downloadPercent": 12,
            },
        )

        status = service.get_status_payload()

        assert status["downloadState"] == "downloaded"
        assert status["installState"] == "ready"
        assert status["downloadPercent"] == 100
        assert status["downloadedZipPath"] == str(zip_path)


def test_get_status_payload_preserves_handoff_state_with_downloaded_zip() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(
            data_dir=data_dir, current_version_getter=lambda: "0.1.31"
        )
        session_root = Path(tmp) / "session"
        _write_credible_handoff_request(paths, session_root, install_state="handoff_requested")
        zip_path = paths.downloads_dir / "baluffo-portable-0.1.32.zip"
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        zip_path.write_text("portable-zip", encoding="utf-8")
        manifest = {
            "schema_version": 2,
            "key_id": "desktop-ed25519-test",
            "channel": "stable",
            "version": "0.1.32",
            "published_at": "2026-04-19T19:49:37Z",
            "release_notes_url": "https://example.com/release",
            "min_desktop_updater_version": "2.0.0",
            "min_supported_current_version": "0.1.0",
            "data_schema_version": "2",
            "rollback_allowed": True,
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-0.1.32.zip",
                "sha256": du.compute_sha256(zip_path),
                "size_bytes": int(zip_path.stat().st_size),
            },
            "migration_plan": [],
            "signature": "ignored-for-test",
        }
        du.write_json_atomic(
            paths.manifest_cache_path,
            {"cachedAt": du.iso_now(), "manifest": manifest},
        )
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.31"),
                "availability": "available",
                "updateAvailable": True,
                "latestVersion": "0.1.32",
                "targetVersion": "0.1.32",
                "downloadState": "downloaded",
                "installState": "ready",
                "installStage": "idle",
                "installStageLabel": "",
                "downloadedZipPath": str(zip_path),
                "helperUpdatedAt": "2026-04-19T22:26:21.821985+00:00",
                "rollbackPath": str(paths.rollback_root / "0.1.32-20260419-222621"),
            },
        )

        with mock.patch.object(update_state, "pid_is_running", return_value=True):
            status = service.get_status_payload()

        assert status["downloadState"] == "downloaded"
        assert status["installState"] == "handoff_requested"
        assert status["installStage"] == "preparing"
        assert status["installStageLabel"] == "Preparing update"
        assert status["helperUpdatedAt"] == "2026-04-19T22:26:21.821985+00:00"
        assert status["rollbackPath"] == str(paths.rollback_root / "0.1.32-20260419-222621")


def test_get_status_payload_marks_interrupted_download_as_failed() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        manifest = {
            "schema_version": 2,
            "key_id": "desktop-ed25519-test",
            "channel": "stable",
            "version": "1.5.0",
            "published_at": "2026-04-15T12:00:00Z",
            "release_notes_url": "https://example.com/release",
            "min_desktop_updater_version": "2.0.0",
            "min_supported_current_version": "0.1.0",
            "data_schema_version": "2",
            "rollback_allowed": True,
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-1.5.0.zip",
                "sha256": "a" * 64,
                "size_bytes": 123,
            },
            "migration_plan": [],
            "signature": "ignored-for-test",
        }
        du.write_json_atomic(
            paths.manifest_cache_path,
            {"cachedAt": du.iso_now(), "manifest": manifest},
        )
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "availability": "available",
                "updateAvailable": True,
                "latestVersion": "1.5.0",
                "targetVersion": "1.5.0",
                "downloadState": "downloading",
                "downloadedBytes": 23,
                "totalBytes": 137,
                "downloadPercent": 16,
            },
        )

        status = service.get_status_payload()

        assert status["downloadState"] == "failed"
        assert status["installState"] == "idle"
        assert status["downloadPercent"] == 0
        assert "stopped before it finished" in status["lastError"]


def test_get_status_payload_normalizes_installed_target_to_up_to_date() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "1.5.0")
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "availability": "available",
                "updateAvailable": True,
                "latestVersion": "1.5.0",
                "targetVersion": "1.5.0",
                "installState": "installed",
                "installStage": "installed",
                "downloadState": "idle",
            },
        )

        status = service.get_status_payload()

        assert status["currentVersion"] == "1.5.0"
        assert status["availability"] == "up_to_date"
        assert status["updateAvailable"] is False
        assert status["installState"] == "idle"
        assert status["installStage"] == "idle"


def test_check_for_update_persists_release_notes_metadata_from_release_payload() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        release = {
            "id": 123,
            "tag_name": "v1.5.0",
            "name": "Baluffo v1.5.0",
            "body": "### Fixed\n- Release notes modal",
            "published_at": "2026-04-15T12:00:00Z",
            "html_url": "https://example.com/releases/v1.5.0",
        }
        manifest = {
            "schema_version": 2,
            "key_id": "desktop-ed25519-test",
            "channel": "stable",
            "version": "1.5.0",
            "published_at": "2026-04-15T12:00:00Z",
            "release_notes_url": "https://example.com/releases/v1.5.0",
            "min_desktop_updater_version": "2.0.0",
            "min_supported_current_version": "0.1.0",
            "data_schema_version": "2",
            "rollback_allowed": True,
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-1.5.0.zip",
                "sha256": "a" * 64,
                "size_bytes": 123,
            },
            "migration_plan": [],
            "signature": "ignored-for-test",
        }

        with (
            mock.patch.object(service, "_resolve_latest_release", return_value=release),
            mock.patch.object(service, "_resolve_manifest_from_release", return_value=manifest),
        ):
            status = service.check_for_update(force=True)

        cached = du.read_json(paths.manifest_cache_path, {})

        assert status["releaseNotesUrl"] == "https://example.com/releases/v1.5.0"
        assert status["releaseNotesTitle"] == "Baluffo v1.5.0"
        assert status["releaseNotesBody"] == "### Fixed\n- Release notes modal"
        assert status["releaseNotesPublishedAt"] == "2026-04-15T12:00:00Z"
        assert cached["manifest"] == manifest
        assert cached["releaseNotes"] == {
            "releaseNotesUrl": "https://example.com/releases/v1.5.0",
            "releaseNotesTitle": "Baluffo v1.5.0",
            "releaseNotesBody": "### Fixed\n- Release notes modal",
            "releaseNotesPublishedAt": "2026-04-15T12:00:00Z",
        }
        assert "releaseNotesBody" not in cached["manifest"]


def test_check_for_update_throttle_reuses_cached_release_notes() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        manifest = {
            "schema_version": 2,
            "key_id": "desktop-ed25519-test",
            "channel": "stable",
            "version": "1.5.0",
            "published_at": "2026-04-15T12:00:00Z",
            "release_notes_url": "https://example.com/releases/v1.5.0",
            "min_desktop_updater_version": "2.0.0",
            "min_supported_current_version": "0.1.0",
            "data_schema_version": "2",
            "rollback_allowed": True,
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-1.5.0.zip",
                "sha256": "a" * 64,
                "size_bytes": 123,
            },
            "migration_plan": [],
            "signature": "ignored-for-test",
        }
        du.write_json_atomic(
            paths.manifest_cache_path,
            {
                "cachedAt": du.iso_now(),
                "releaseId": 123,
                "releaseTag": "v1.5.0",
                "manifest": manifest,
                "releaseNotes": {
                    "releaseNotesUrl": "https://example.com/releases/v1.5.0",
                    "releaseNotesTitle": "Baluffo v1.5.0",
                    "releaseNotesBody": "### Fixed\n- Cached notes",
                    "releaseNotesPublishedAt": "2026-04-15T12:00:00Z",
                },
            },
        )
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "lastCheckedAt": du.iso_now(),
            },
        )

        with (
            mock.patch.object(service, "_resolve_latest_release") as latest_release_mock,
            mock.patch.object(service, "_resolve_manifest_from_release") as manifest_mock,
        ):
            status = service.check_for_update(force=False)

        latest_release_mock.assert_not_called()
        manifest_mock.assert_not_called()
        assert status["releaseNotesTitle"] == "Baluffo v1.5.0"
        assert status["releaseNotesBody"] == "### Fixed\n- Cached notes"
        assert status["releaseNotesPublishedAt"] == "2026-04-15T12:00:00Z"


def test_get_status_payload_backfills_release_notes_from_cache() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        manifest = {
            "schema_version": 2,
            "key_id": "desktop-ed25519-test",
            "channel": "stable",
            "version": "1.5.0",
            "published_at": "2026-04-15T12:00:00Z",
            "release_notes_url": "https://example.com/releases/v1.5.0",
            "min_desktop_updater_version": "2.0.0",
            "min_supported_current_version": "0.1.0",
            "data_schema_version": "2",
            "rollback_allowed": True,
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-1.5.0.zip",
                "sha256": "a" * 64,
                "size_bytes": 123,
            },
            "migration_plan": [],
            "signature": "ignored-for-test",
        }
        du.write_json_atomic(
            paths.manifest_cache_path,
            {
                "cachedAt": du.iso_now(),
                "releaseId": 123,
                "releaseTag": "v1.5.0",
                "manifest": manifest,
                "releaseNotes": {
                    "releaseNotesUrl": "https://example.com/releases/v1.5.0",
                    "releaseNotesTitle": "",
                    "releaseNotesBody": "### Fixed\n- Cached after restart",
                    "releaseNotesPublishedAt": "2026-04-15T12:00:00Z",
                },
            },
        )

        status = service.get_status_payload()

        assert status["releaseNotesUrl"] == "https://example.com/releases/v1.5.0"
        assert status["releaseNotesTitle"] == "1.5.0"
        assert status["releaseNotesBody"] == "### Fixed\n- Cached after restart"
        assert status["releaseNotesPublishedAt"] == "2026-04-15T12:00:00Z"
