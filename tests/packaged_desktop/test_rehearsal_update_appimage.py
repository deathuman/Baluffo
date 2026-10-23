from __future__ import annotations

from pathlib import Path

import pytest

from src import packaged_desktop_smoke as smoke
from src.ship.desktop_update_manifest import (
    Ed25519SigningClass,
    verify_manifest_signature,
)
from src.ship.packaged_smoke import rehearsal_update_appimage as appimage_rehearsal
from tests.helpers.temp_paths import workspace_tmpdir


@pytest.mark.skipif(Ed25519SigningClass is None, reason="cryptography not available")
def test_rehearsal_manifest_names_the_linux_asset_and_verifies() -> None:
    with workspace_tmpdir("appimage-rehearsal-manifest") as tmp:
        artifact = Path(tmp) / "Baluffo-0.3.025-x86_64.AppImage"
        artifact.write_bytes(b"new build")
        private_key = Ed25519SigningClass.generate()

        manifest = appimage_rehearsal.build_rehearsal_manifest(
            version="0.3.025",
            artifact_url="http://127.0.0.1:9/assets/Baluffo-0.3.025-x86_64.AppImage",
            artifact_path=artifact,
            release_notes_url="http://127.0.0.1:9/release-notes",
            private_key=private_key,
        )

    assert manifest["version"] == "0.3.025"
    assert manifest["portable_artifact"]["size_bytes"] == len(b"new build")
    verify_manifest_signature(
        manifest,
        public_keys={
            appimage_rehearsal.REHEARSAL_KEY_ID: private_key.public_key().public_bytes_raw()
        },
    )


def test_packaged_smoke_accepts_the_appimage_rehearsal_flags() -> None:
    args = smoke.parse_args(
        [
            "--desktop-update-rehearsal-appimage",
            "--update-appimage-before",
            "/tmp/before.AppImage",
            "--update-appimage-after",
            "/tmp/after.AppImage",
            "--update-target-version",
            "0.3.025",
            "--update-before-version",
            "0.0.9",
        ]
    )

    assert args.desktop_update_rehearsal_appimage is True
    assert args.update_appimage_before == "/tmp/before.AppImage"
    assert args.update_appimage_after == "/tmp/after.AppImage"
    assert args.update_target_version == "0.3.025"
    assert args.update_before_version == "0.0.9"


def test_appimage_rehearsal_requires_both_artifacts() -> None:
    args = smoke.parse_args(["--desktop-update-rehearsal-appimage"])

    with pytest.raises(RuntimeError, match="--update-appimage-before"):
        smoke._run_appimage_update_rehearsal(args)
