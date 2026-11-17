"""Verified ZIP re-extract repair for a terminal failure with no snapshot."""

import zipfile
from pathlib import Path
from unittest import mock

import pytest

from src.ship import desktop_update_shared as du_shared
from src.ship import desktop_update_state as update_state
from src.ship import desktop_updater_install as updater_install
from src.ship import desktop_updater_release as updater_release
from tests.helpers.temp_paths import workspace_tmpdir

VERSION = "1.4.0"


def _write_release_zip(
    path: Path,
    *,
    version: str = VERSION,
    include_pointer: bool = True,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("Baluffo.exe", "released")
        if include_pointer:
            archive.writestr("ship/app/current.txt", f"{version}\n")
        archive.writestr("ship/app/release-only.txt", "new")
    return path


def _prepare(
    tmp: str,
    *,
    include_pointer: bool = True,
    sha256: str = "",
) -> tuple[du_shared.DesktopUpdatePaths, Path, Path, Path]:
    install_root = Path(tmp) / "portable"
    data_dir = install_root / "ship" / "data"
    paths = du_shared.DesktopUpdatePaths.from_data_dir(data_dir)
    (install_root / "ship" / "app").mkdir(parents=True, exist_ok=True)
    (install_root / "Baluffo.exe").write_text("broken", encoding="utf-8")
    (install_root / "ship" / "app" / "current.txt").write_text(VERSION, encoding="utf-8")
    (install_root / "ship" / "app" / "stale.txt").write_text("stale", encoding="utf-8")
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "runtime.db").write_text("data", encoding="utf-8")

    zip_path = _write_release_zip(
        paths.downloads_dir / f"baluffo-portable-{VERSION}.zip",
        include_pointer=include_pointer,
    )
    du_shared.write_json_atomic(
        paths.manifest_cache_path,
        {
            "cachedAt": "2026-11-17T12:00:00Z",
            "manifest": {
                "schema_version": 2,
                "key_id": "desktop-ed25519-test",
                "channel": "stable",
                "version": VERSION,
                "published_at": "2026-04-14T12:00:00Z",
                "min_desktop_updater_version": "2.0.0",
                "min_supported_current_version": "0.1.0",
                "data_schema_version": "2",
                "rollback_allowed": True,
                "portable_artifact": {
                    "url": f"https://example.invalid/{zip_path.name}",
                    "sha256": sha256 or du_shared.compute_sha256(zip_path),
                    "size_bytes": int(zip_path.stat().st_size),
                },
                "migration_plan": [],
                "signature": "ignored-for-test",
            },
        },
    )
    rollback_root = paths.rollback_root / f"{VERSION}-20260414-120000"
    update_state.save_status(
        paths,
        {
            **update_state.default_status_payload(current_version="0.1.0"),
            "targetVersion": VERSION,
            "downloadState": "downloaded",
            "downloadedZipPath": str(zip_path),
            "installState": "failed",
            "installStage": "rollback_failed",
            "lastError": "install_restore_failed: install is locked",
            "rollbackPath": str(rollback_root),
            "rollbackVerified": False,
        },
    )
    return paths, install_root, rollback_root, zip_path


def test_reextract_rebuilds_the_install_from_a_verified_release_zip() -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        paths, install_root, rollback_root, zip_path = _prepare(tmp)

        result = updater_install.repair_terminal_install(paths, mode="reextract-zip")

        assert result["ok"] is True
        assert result["action"] == "reextract-zip"
        assert result["version"] == VERSION
        assert result["files"] == 3
        assert result["zipPath"] == str(zip_path)
        assert result["snapshotRemoved"] is True
        assert (install_root / "Baluffo.exe").read_text(encoding="utf-8") == "released"
        assert (install_root / "ship" / "app" / "current.txt").read_text(
            encoding="utf-8"
        ).strip() == VERSION
        assert (install_root / "ship" / "app" / "release-only.txt").read_text(
            encoding="utf-8"
        ) == "new"
        assert not (install_root / "ship" / "app" / "stale.txt").exists()
        assert (install_root / "ship" / "data" / "runtime.db").read_text(encoding="utf-8") == "data"
        assert not rollback_root.exists()

        status = update_state.load_status(paths, current_version=VERSION)
        assert status["installState"] == "idle"
        assert status["installStage"] == "idle"
        assert status["lastError"] == ""
        assert status["rollbackPath"] == ""
        assert status["repairState"] == "repaired"
        assert status["repairAvailable"] is False
        assert status["repairAction"] == "reextract-zip"
        assert status["lastRepair"]["action"] == "reextract-zip"
        assert status["lastRepair"]["zipPath"] == str(zip_path)
        assert status["lastRepair"]["zipSha256"] == du_shared.compute_sha256(zip_path)
        assert status["lastRepair"]["previousInstallStage"] == "rollback_failed"
        assert status["lastRepair"]["previousError"] == "install_restore_failed: install is locked"


def test_reextract_refuses_a_zip_that_fails_the_release_digest() -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        paths, install_root, _rollback_root, zip_path = _prepare(tmp, sha256="0" * 64)

        def write_bad_zip(_url: str, target: Path) -> Path:
            return _write_release_zip(Path(target))

        with (
            mock.patch.object(updater_release, "download_file", side_effect=write_bad_zip),
            pytest.raises(RuntimeError, match="failed re-verification"),
        ):
            updater_install.repair_terminal_install(paths, mode="reextract-zip")

        assert (install_root / "Baluffo.exe").read_text(encoding="utf-8") == "broken"
        assert (install_root / "ship" / "app" / "stale.txt").exists()
        assert (install_root / "ship" / "data" / "runtime.db").read_text(encoding="utf-8") == "data"
        status = update_state.load_status(paths, current_version=VERSION)
        assert status["installStage"] == "rollback_failed"
        assert status["lastError"] == "install_restore_failed: install is locked"
        assert (install_root / "ship" / "app" / "current.txt").exists()
        assert zip_path.is_file()


def test_reextract_refuses_a_zip_without_the_portable_runtime() -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        paths, install_root, _rollback_root, _zip_path = _prepare(tmp, include_pointer=False)

        with pytest.raises(RuntimeError, match="incomplete"):
            updater_install.repair_terminal_install(paths, mode="reextract-zip")

        assert (install_root / "Baluffo.exe").read_text(encoding="utf-8") == "broken"
        assert (install_root / "ship" / "app" / "stale.txt").exists()
        status = update_state.load_status(paths, current_version=VERSION)
        assert status["installStage"] == "rollback_failed"
