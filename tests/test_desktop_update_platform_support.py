"""Only the platform with an updater installer may offer a desktop update.

The panel's Download/Install path drives the packaged Windows helper; on Linux
the AppImage ships no such helper, so a check used to end in a failed install.
These tests pin the status the page receives and that no release is fetched.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from src.ship import desktop_update_service as service_mod
from src.ship import desktop_update_shared as shared
from tests.helpers.desktop_update_leaf_namespace import du
from tests.helpers.temp_paths import workspace_tmpdir


def test_install_support_follows_the_build(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shared.os, "name", "nt")
    monkeypatch.setattr(shared.sys, "frozen", True, raising=False)
    assert shared.desktop_update_install_supported() is True

    monkeypatch.setattr(shared.os, "name", "posix")
    assert shared.desktop_update_install_supported() is False

    # A checkout ships no helper either, but it must keep the update machinery
    # usable (and testable) on the platform the suite happens to run on.
    monkeypatch.setattr(shared.sys, "frozen", False, raising=False)
    assert shared.desktop_update_install_supported() is True


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


def test_check_for_update_still_reaches_the_release_feed_on_windows() -> None:
    with workspace_tmpdir("desktop-update-platform-check-windows") as tmp:
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
