"""A stale download-progress write must not undo an install already on disk."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.ship.desktop_update_state import INSTALL_WRITE_PHASE
from tests.helpers.desktop_update_leaf_namespace import du
from tests.helpers.temp_paths import workspace_tmpdir


def _paths(tmp: str) -> Any:
    return du.DesktopUpdatePaths.from_data_dir(Path(tmp) / "portable" / "ship" / "data")


def _ready_to_install(paths: Any) -> dict[str, Any]:
    du.save_status(
        paths,
        {
            **du.default_status_payload(current_version="0.1.0"),
            "downloadState": "downloaded",
            "downloadedBytes": 120,
            "totalBytes": 120,
            "downloadPercent": 100,
            "installState": "ready",
            "downloadedZipPath": "C:/downloads/baluffo-portable-1.4.0.zip",
        },
    )
    return dict(du.load_status(paths, current_version="0.1.0"))


def test_stale_download_progress_write_cannot_regress_an_install_in_flight() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        paths = _paths(tmp)
        # The poller builds this snapshot before the helper starts installing.
        poller_snapshot = _ready_to_install(paths)
        du.save_status(
            paths,
            {
                **du.load_status(paths, current_version="0.1.0"),
                "installState": "installing",
                "installStage": "extracting",
                "installStageLabel": "Extracting update",
                "helperUpdatedAt": "2026-05-01T12:00:01Z",
            },
            phase=INSTALL_WRITE_PHASE,
        )

        du.save_status(
            paths,
            {
                **poller_snapshot,
                "downloadState": "downloading",
                "downloadedBytes": 12,
                "downloadPercent": 10,
            },
        )

        status = du.load_status(paths, current_version="0.1.0")
        assert status["installState"] == "installing"
        assert status["installStage"] == "extracting"
        assert status["helperUpdatedAt"] == "2026-05-01T12:00:01Z"
        assert status["revision"] == 3


def test_stale_download_progress_write_cannot_overwrite_the_completion() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        paths = _paths(tmp)
        poller_snapshot = _ready_to_install(paths)
        du.save_status(
            paths,
            {
                **du.load_status(paths, current_version="0.1.0"),
                "installState": "installed",
                "installStage": "installed",
                "installStageLabel": "Installed",
                "helperUpdatedAt": "2026-05-01T12:00:02Z",
                "downloadState": "idle",
                "downloadedBytes": 0,
                "totalBytes": 0,
                "downloadPercent": 0,
                "downloadedZipPath": "",
                "lastError": "",
            },
            phase=INSTALL_WRITE_PHASE,
        )

        du.save_status(
            paths,
            {
                **poller_snapshot,
                "downloadState": "downloading",
                "downloadedBytes": 60,
                "downloadPercent": 50,
                "lastError": "Downloaded portable ZIP checksum mismatch.",
            },
        )

        status = du.load_status(paths, current_version="0.1.0")
        assert status["installState"] == "installed"
        assert status["downloadState"] == "idle"
        assert status["downloadPercent"] == 0
        assert status["downloadedZipPath"] == ""
        assert status["lastError"] == ""
        assert status["revision"] == 3


def test_install_phase_write_applies_from_a_stale_revision() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        paths = _paths(tmp)
        # The helper read this snapshot, then a poll write landed first.
        helper_snapshot = _ready_to_install(paths)
        du.save_status(
            paths,
            {
                **du.load_status(paths, current_version="0.1.0"),
                "lastCheckedAt": "2026-05-01T12:00:01Z",
            },
        )

        du.save_status(
            paths,
            {
                **helper_snapshot,
                "installState": "installing",
                "installStage": "extracting",
                "installStageLabel": "Extracting update",
                "helperUpdatedAt": "2026-05-01T12:00:02Z",
            },
            phase=INSTALL_WRITE_PHASE,
        )

        status = du.load_status(paths, current_version="0.1.0")
        assert status["installState"] == "installing"
        assert status["installStage"] == "extracting"
        assert status["revision"] == 3


def test_fresh_status_write_is_not_guarded() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        paths = _paths(tmp)
        _ready_to_install(paths)
        du.save_status(
            paths,
            {
                **du.load_status(paths, current_version="0.1.0"),
                "installState": "installing",
                "installStage": "extracting",
            },
            phase=INSTALL_WRITE_PHASE,
        )

        # A writer that re-read the current revision may still move the phase.
        du.save_status(
            paths,
            {
                **du.load_status(paths, current_version="0.1.0"),
                "installState": "idle",
                "installStage": "idle",
                "lastError": "Install cancelled.",
            },
        )

        status = du.load_status(paths, current_version="0.1.0")
        assert status["installState"] == "idle"
        assert status["lastError"] == "Install cancelled."


def test_every_status_save_bumps_the_revision() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        paths = _paths(tmp)
        assert du.default_status_payload(current_version="0.1.0")["revision"] == 0
        assert (
            du.save_status(paths, du.default_status_payload(current_version="0.1.0"))["revision"]
            == 1
        )
        assert (
            du.save_status(paths, du.load_status(paths, current_version="0.1.0"))["revision"] == 2
        )
