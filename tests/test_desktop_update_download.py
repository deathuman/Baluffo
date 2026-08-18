"""Tests for desktop update download behavior."""

from hashlib import sha256
from pathlib import Path
from typing import Any
from unittest import mock

from src.ship import desktop_update_service as du_service
from tests.helpers.desktop_update_leaf_namespace import du
from tests.helpers.temp_paths import workspace_tmpdir


def test_download_update_returns_structured_ready_failure() -> None:
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
                "lastCheckedAt": du.iso_now(),
                "downloadState": "downloaded",
                "installState": "ready",
                "downloadedZipPath": str(zip_path),
            },
        )

        result = service.download_update()

        assert result["started"] is False
        assert result["errorCode"] == "update_ready_to_install"
        assert result["status"]["installState"] == "ready"


def test_download_update_returns_structured_start_failure() -> None:
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
                "lastCheckedAt": du.iso_now(),
            },
        )

        with mock.patch.object(du.threading.Thread, "start", side_effect=RuntimeError("boom")):
            result = service.download_update()

        status = du.load_status(paths, current_version="0.1.0")
        assert result["started"] is False
        assert result["errorCode"] == "download_start_failed"
        assert status["downloadState"] == "failed"
        assert "Could not start the desktop update download" in status["lastError"]


def test_get_status_payload_preserves_failed_download_error_after_reconciliation() -> None:
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
                "downloadState": "failed",
                "lastError": "socket timeout",
            },
        )

        status = service.get_status_payload()

        assert status["downloadState"] == "failed"
        assert status["lastError"] == "socket timeout"


def test_run_download_worker_does_not_abort_when_progress_status_write_fails(monkeypatch) -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        content = b"portable-zip"
        expected_hash = sha256(content).hexdigest()
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
                "sha256": expected_hash,
                "size_bytes": len(content),
            },
            "migration_plan": [],
            "signature": "ignored-for-test",
        }
        original_save_status = du.save_status

        def flaky_save_status(
            update_paths: du.DesktopUpdatePaths, payload: dict[str, Any]
        ) -> dict[str, Any]:
            if payload.get("downloadState") == "downloading":
                raise OSError("status file busy")
            return dict(original_save_status(update_paths, payload))

        def fake_download(
            url: str,
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

        monkeypatch.setattr(du_service, "save_status", flaky_save_status)
        monkeypatch.setattr(du_service, "download_file", fake_download)

        service._run_download_worker(manifest)

        status = du.load_status(paths, current_version="0.1.0")
        assert status["downloadState"] == "downloaded"
        assert status["installState"] == "ready"
        assert status["downloadedZipPath"]
