"""Only a build that can name what it would replace may offer a desktop update.

A Windows portable build replaces a directory tree; an AppImage replaces the
file it was launched from. A frozen build that can name neither -- a Linux
package started from an extraction directory, say -- must report the block
instead of offering a button that can only fail. A source checkout keeps the
machinery usable on every platform so the suite can exercise it anywhere.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from src.ship import desktop_artifact_paths as artifact_mod
from src.ship import desktop_update_service as service_mod
from src.ship import desktop_update_shared as shared
from tests.helpers.desktop_update_leaf_namespace import du
from tests.helpers.temp_paths import workspace_tmpdir

POSIX_PLATFORM = SimpleNamespace(name="posix", environ={})
WINDOWS_PLATFORM = SimpleNamespace(name="nt", environ={})


def _frozen(value: bool) -> Any:
    return mock.patch.object(artifact_mod.sys, "frozen", value, create=True)


def _platform(value: SimpleNamespace) -> Any:
    return mock.patch.object(artifact_mod, "os", value)


def test_source_checkout_keeps_the_portable_directory_contract() -> None:
    with _frozen(False), _platform(POSIX_PLATFORM):
        artifact = artifact_mod.desktop_artifact(env={}, argv=["baluffo"])

    assert artifact.kind == artifact_mod.ARTIFACT_KIND_PORTABLE_DIR
    assert artifact.install_supported is True
    assert artifact.helper_name == du.DESKTOP_UPDATE_HELPER_NAME
    assert artifact.manifest_asset == du.DESKTOP_UPDATE_MANIFEST_ASSET


def test_frozen_windows_build_replaces_its_portable_directory() -> None:
    with _frozen(True), _platform(WINDOWS_PLATFORM):
        artifact = artifact_mod.desktop_artifact(env={}, argv=["Baluffo.exe"])

    assert artifact.kind == artifact_mod.ARTIFACT_KIND_PORTABLE_DIR
    assert artifact.install_supported is True
    assert artifact.replace_target is None
    assert artifact.manifest_asset == du.DESKTOP_UPDATE_MANIFEST_ASSET


def test_frozen_appimage_build_replaces_the_file_it_was_launched_from() -> None:
    with workspace_tmpdir("desktop-update-artifact") as tmp:
        target = Path(tmp) / "Baluffo-0.3.024-x86_64.AppImage"
        target.write_text("appimage", encoding="utf-8")
        with _frozen(True), _platform(POSIX_PLATFORM):
            artifact = artifact_mod.desktop_artifact(
                env={"APPIMAGE": str(target)},
                argv=["baluffo"],
            )

    assert artifact.kind == artifact_mod.ARTIFACT_KIND_APPIMAGE
    assert artifact.install_supported is True
    assert artifact.replace_target == target.resolve()
    assert artifact.helper_name == "baluffo-updater"
    assert artifact.manifest_asset == du.DESKTOP_UPDATE_MANIFEST_ASSET_LINUX


def test_extract_and_run_builds_relaunch_with_the_same_mode() -> None:
    with workspace_tmpdir("desktop-update-artifact-mode") as tmp:
        target = Path(tmp) / "Baluffo-0.3.024-x86_64.AppImage"
        target.write_text("appimage", encoding="utf-8")
        with _frozen(True), _platform(POSIX_PLATFORM):
            artifact = artifact_mod.desktop_artifact(
                env={"APPIMAGE": str(target), "APPIMAGE_EXTRACT_AND_RUN": "1"},
                argv=["baluffo"],
            )

    assert artifact.relaunch_args == (artifact_mod.APPIMAGE_EXTRACT_AND_RUN_ARG,)


def test_frozen_build_without_a_named_artifact_reports_the_block() -> None:
    with _frozen(True), _platform(POSIX_PLATFORM):
        artifact = artifact_mod.desktop_artifact(env={}, argv=["/tmp/baluffo"])
        with mock.patch.object(shared, "desktop_artifact", return_value=artifact):
            assert shared.desktop_update_install_supported() is False

    assert artifact.install_supported is False


def test_platform_unsupported_status_withdraws_the_download_offer() -> None:
    status = shared.platform_unsupported_status(
        {
            "currentVersion": "0.3.025",
            "latestVersion": "0.3.026",
            "targetVersion": "0.3.026",
            "availability": "available",
            "updateAvailable": True,
            "downloadState": "downloaded",
            "installState": "ready",
            "lastError": "stale failure",
        }
    )

    assert status["availability"] == "blocked"
    assert status["updateAvailable"] is False
    assert status["downloadState"] == "idle"
    assert status["installState"] == "blocked"
    assert status["blockedReason"] == "platform_unsupported"
    assert status["lastError"] == ""


def test_status_payload_reports_the_platform_block_on_an_unsupported_build() -> None:
    with workspace_tmpdir("desktop-update-platform-status") as tmp:
        data_dir = Path(tmp) / "ship" / "data"
        service = du.DesktopUpdateService(
            data_dir=data_dir, current_version_getter=lambda: "0.3.025"
        )
        with mock.patch.object(service_mod, "desktop_update_install_supported", return_value=False):
            status: dict[str, Any] = service.get_status_payload()

    assert status["availability"] == "blocked"
    assert status["updateAvailable"] is False
    assert status["blockedReason"] == "platform_unsupported"
    assert status["installState"] == "blocked"


def test_check_for_update_skips_the_release_lookup_on_an_unsupported_build() -> None:
    with workspace_tmpdir("desktop-update-platform-check") as tmp:
        data_dir = Path(tmp) / "ship" / "data"
        service = du.DesktopUpdateService(
            data_dir=data_dir, current_version_getter=lambda: "0.3.025"
        )
        with (
            mock.patch.object(service_mod, "desktop_update_install_supported", return_value=False),
            mock.patch.object(
                service,
                "_resolve_latest_release",
                side_effect=AssertionError("release lookup must not run"),
            ),
        ):
            status = service.check_for_update(force=True)

    assert status["blockedReason"] == "platform_unsupported"
    assert status["availability"] == "blocked"
    assert status["lastCheckedAt"] == ""


def test_check_for_update_still_reaches_the_release_feed_on_a_supported_build() -> None:
    with workspace_tmpdir("desktop-update-platform-check-supported") as tmp:
        data_dir = Path(tmp) / "ship" / "data"
        service = du.DesktopUpdateService(
            data_dir=data_dir, current_version_getter=lambda: "0.3.025"
        )
        with (
            mock.patch.object(service_mod, "desktop_update_install_supported", return_value=True),
            mock.patch.object(
                service,
                "_resolve_latest_release",
                side_effect=RuntimeError("offline for test"),
            ) as lookup,
        ):
            status = service.check_for_update(force=True)

    lookup.assert_called_once()
    assert status["availability"] == "error"
    assert status["blockedReason"] != "platform_unsupported"
