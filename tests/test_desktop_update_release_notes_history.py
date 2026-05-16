from pathlib import Path
from unittest import mock

import pytest

from src.ship import desktop_update as du
from tests.helpers.temp_paths import workspace_tmpdir


def _manifest(version: str = "1.5.0") -> dict[str, object]:
    return {
        "schema_version": 2,
        "key_id": "desktop-ed25519-test",
        "channel": "stable",
        "version": version,
        "published_at": "2026-04-15T12:00:00Z",
        "release_notes_url": f"https://example.com/releases/v{version}",
        "min_desktop_updater_version": "2.0.0",
        "min_supported_current_version": "0.1.0",
        "data_schema_version": "2",
        "rollback_allowed": True,
        "portable_artifact": {
            "url": f"https://example.com/baluffo-portable-{version}.zip",
            "sha256": "a" * 64,
            "size_bytes": 123,
        },
        "migration_plan": [],
        "signature": "ignored-for-test",
    }


def _release(version: str, body: str) -> dict[str, object]:
    return {
        "id": int(version.replace(".", "")),
        "tag_name": f"v{version}",
        "name": f"Baluffo v{version}",
        "body": body,
        "published_at": "2026-04-15T12:00:00Z",
        "html_url": f"https://example.com/releases/v{version}",
    }


def test_check_for_update_caches_release_notes_history() -> None:
    with workspace_tmpdir("desktop-update-history") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        release = _release("1.5.0", "### Fixed\n- Latest notes")
        service._stable_releases = [
            release,
            _release("1.4.0", "### Added\n- Previous notes"),
        ]

        with (
            mock.patch.object(service, "_resolve_latest_release", return_value=release),
            mock.patch.object(service, "_resolve_manifest_from_release", return_value=_manifest()),
        ):
            status = service.check_for_update(force=True)

        cached = du.read_json(paths.manifest_cache_path, {})
        assert [entry["releaseVersion"] for entry in status["releaseNotesHistory"]] == [
            "1.5.0",
            "1.4.0",
        ]
        assert status["releaseNotesHistory"][1]["releaseNotesBody"] == "### Added\n- Previous notes"
        assert cached["releaseNotesHistory"] == status["releaseNotesHistory"]


def test_cached_release_notes_history_survives_throttle_and_restart() -> None:
    with workspace_tmpdir("desktop-update-history") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        history = [
            {
                "releaseNotesUrl": "https://example.com/releases/v1.5.0",
                "releaseNotesTitle": "Baluffo v1.5.0",
                "releaseNotesBody": "### Fixed\n- Cached notes",
                "releaseNotesPublishedAt": "2026-04-15T12:00:00Z",
                "releaseTag": "v1.5.0",
                "releaseVersion": "1.5.0",
            },
            {
                "releaseNotesUrl": "https://example.com/releases/v1.4.0",
                "releaseNotesTitle": "Baluffo v1.4.0",
                "releaseNotesBody": "### Added\n- Cached previous notes",
                "releaseNotesPublishedAt": "2026-04-14T12:00:00Z",
                "releaseTag": "v1.4.0",
                "releaseVersion": "1.4.0",
            },
        ]
        du.write_json_atomic(
            paths.manifest_cache_path,
            {
                "cachedAt": du.iso_now(),
                "releaseId": 123,
                "releaseTag": "v1.5.0",
                "manifest": _manifest(),
                "releaseNotes": history[0],
                "releaseNotesHistory": history,
            },
        )
        du.save_status(
            paths,
            {**du.default_status_payload(current_version="0.1.0"), "lastCheckedAt": du.iso_now()},
        )

        with mock.patch.object(service, "_resolve_latest_release") as latest_release_mock:
            throttled = service.check_for_update(force=False)
        latest_release_mock.assert_not_called()

        restarted = service.get_status_payload()
        assert throttled["releaseNotesHistory"] == history
        assert restarted["releaseNotesHistory"] == history


def test_old_release_note_cache_backfills_single_history_entry() -> None:
    with workspace_tmpdir("desktop-update-history") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        du.write_json_atomic(
            paths.manifest_cache_path,
            {
                "manifest": _manifest(),
                "releaseNotes": {
                    "releaseNotesUrl": "https://example.com/releases/v1.5.0",
                    "releaseNotesTitle": "",
                    "releaseNotesBody": "### Fixed\n- Cached after restart",
                    "releaseNotesPublishedAt": "2026-04-15T12:00:00Z",
                },
            },
        )

        status = service.get_status_payload()

        assert status["releaseNotesHistory"] == [
            {
                "releaseNotesUrl": "https://example.com/releases/v1.5.0",
                "releaseNotesTitle": "1.5.0",
                "releaseNotesBody": "### Fixed\n- Cached after restart",
                "releaseNotesPublishedAt": "2026-04-15T12:00:00Z",
                "releaseTag": "",
                "releaseVersion": "1.5.0",
            }
        ]


def test_resolve_latest_release_filters_unstable_releases_and_keeps_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("desktop-update-history") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")

        monkeypatch.setattr(du, "resolve_release_repo", lambda **_kw: "owner/repo")
        monkeypatch.setattr(du, "resolve_github_api_base", lambda: "https://api.example.test")
        monkeypatch.setattr(
            du,
            "fetch_json",
            lambda _url: [
                {"id": 125, "tag_name": "v1.6.0", "draft": True},
                {"id": 124, "tag_name": "v1.5.1", "prerelease": True},
                {"id": 123, "tag_name": "v1.5.0"},
                {"id": 122, "tag_name": "v1.4.0"},
            ],
        )

        latest = service._resolve_latest_release()

        assert latest["tag_name"] == "v1.5.0"
        assert [release["tag_name"] for release in service._stable_releases] == [
            "v1.5.0",
            "v1.4.0",
        ]
