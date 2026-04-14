import base64
from pathlib import Path
from unittest import mock

from scripts.build_desktop_update_release import build_manifest, parse_args
from tests.helpers.temp_paths import workspace_tmpdir


def test_parse_args_defaults_to_current_app_version() -> None:
    with mock.patch("sys.argv", ["build_desktop_update_release.py"]):
        args = parse_args()
    assert str(args.version).strip()
    assert str(args.output).endswith("baluffo-desktop-update-manifest.json")
    assert str(args.portable_zip) == ""
    assert str(args.ship_zip) == ""


def test_build_manifest_derives_release_urls_and_signs_payload() -> None:
    with workspace_tmpdir("desktop-update-release") as tmp:
        root = Path(tmp)
        portable_zip = root / "dist" / "baluffo-portable-1.2.3.zip"
        ship_zip = root / "dist" / "baluffo-ship-1.2.3.zip"
        portable_zip.parent.mkdir(parents=True, exist_ok=True)
        portable_zip.write_text("portable", encoding="utf-8")
        ship_zip.write_text("ship", encoding="utf-8")
        args = parse_args(
            [
                "--version",
                "1.2.3",
                "--portable-zip",
                str(portable_zip),
                "--ship-zip",
                str(ship_zip),
                "--github-repo",
                "owner/repo",
                "--key-id",
                "desktop-ed25519-2026-01",
                "--private-key-b64",
                base64.b64encode(b"x" * 32).decode("ascii"),
                "--migration-step",
                "backup_v2",
                "--rollback-allowed",
            ]
        )

        with mock.patch(
            "scripts.build_desktop_update_release.sign_manifest", return_value="signed-manifest"
        ):
            manifest = build_manifest(args)

        assert manifest["schema_version"] == 1
        assert manifest["key_id"] == "desktop-ed25519-2026-01"
        assert manifest["version"] == "1.2.3"
        assert manifest["rollback_allowed"] is True
        assert manifest["migration_plan"] == ["backup_v2"]
        assert manifest["signature"] == "signed-manifest"
        assert manifest["release_notes_url"] == "https://github.com/owner/repo/releases/tag/v1.2.3"
        assert manifest["portable_artifact"]["url"] == (
            "https://github.com/owner/repo/releases/download/v1.2.3/baluffo-portable-1.2.3.zip"
        )
        assert manifest["ship_recovery_artifact"]["url"] == (
            "https://github.com/owner/repo/releases/download/v1.2.3/baluffo-ship-1.2.3.zip"
        )
        assert int(manifest["portable_artifact"]["size_bytes"]) == int(portable_zip.stat().st_size)
        assert len(str(manifest["portable_artifact"]["sha256"])) == 64


def test_build_manifest_requires_portable_release_url_without_repo() -> None:
    with workspace_tmpdir("desktop-update-release") as tmp:
        portable_zip = Path(tmp) / "baluffo-portable-1.2.3.zip"
        portable_zip.write_text("portable", encoding="utf-8")
        args = parse_args(
            [
                "--version",
                "1.2.3",
                "--portable-zip",
                str(portable_zip),
                "--ship-zip",
                "",
                "--key-id",
                "desktop-ed25519-2026-01",
                "--private-key-b64",
                base64.b64encode(b"x" * 32).decode("ascii"),
            ]
        )

        with mock.patch(
            "scripts.build_desktop_update_release.sign_manifest", return_value="signed-manifest"
        ):
            try:
                build_manifest(args)
            except RuntimeError as exc:
                assert "Portable release URL is required" in str(exc)
            else:
                raise AssertionError(
                    "build_manifest should require a portable release URL when no repo is provided."
                )


def test_build_manifest_uses_versioned_default_portable_zip_and_omits_ship_by_default() -> None:
    with workspace_tmpdir("desktop-update-release") as tmp:
        root = Path(tmp)
        portable_zip = root / "dist" / "baluffo-portable-1.2.3.zip"
        portable_zip.parent.mkdir(parents=True, exist_ok=True)
        portable_zip.write_text("portable", encoding="utf-8")

        with mock.patch("scripts.build_desktop_update_release.ROOT", root):
            args = parse_args(
                [
                    "--version",
                    "1.2.3",
                    "--github-repo",
                    "owner/repo",
                    "--key-id",
                    "desktop-ed25519-2026-01",
                    "--private-key-b64",
                    base64.b64encode(b"x" * 32).decode("ascii"),
                ]
            )

            with mock.patch(
                "scripts.build_desktop_update_release.sign_manifest", return_value="signed-manifest"
            ):
                manifest = build_manifest(args)

        assert manifest["version"] == "1.2.3"
        assert manifest["portable_artifact"]["url"] == (
            "https://github.com/owner/repo/releases/download/v1.2.3/baluffo-portable-1.2.3.zip"
        )
        assert "ship_recovery_artifact" not in manifest
