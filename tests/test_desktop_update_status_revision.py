"""A stale download-progress write must not undo an install already on disk."""

from __future__ import annotations

import multiprocessing
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from src.ship import desktop_update_state as update_state
from src.ship.desktop_update_state import INSTALL_WRITE_PHASE
from tests.helpers.desktop_update_leaf_namespace import du
from tests.helpers.file_locks import held_lock_file
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


def test_save_status_holds_the_write_lock_across_its_write() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        paths = _paths(tmp)
        real_write = update_state.write_json_atomic

        def probe(path: Path, payload: dict[str, Any]) -> None:
            if path == paths.install_state_path:
                # The save must still hold the lock here, so a second writer
                # cannot acquire it until the write completes.
                with pytest.raises(TimeoutError):
                    with update_state._install_state_write_lock(paths):
                        pass
            real_write(path, payload)

        with (
            mock.patch.object(update_state, "_INSTALL_STATE_LOCK_TIMEOUT_S", 0.2),
            mock.patch.object(update_state, "write_json_atomic", probe),
        ):
            status = du.save_status(paths, du.default_status_payload(current_version="0.1.0"))

        assert status["revision"] == 1


def test_save_status_does_not_seed_a_held_lock_file() -> None:
    """A held lock file must make the save wait, not write into the range.

    Seeding the file before locking writes byte 0 into the holder's range, which
    Windows rejects with ``PermissionError`` instead of the expected timeout.
    """
    with workspace_tmpdir("desktop-update") as tmp:
        paths = _paths(tmp)
        state_path = paths.install_state_path
        lock_path = state_path.with_name(
            f"{state_path.name}{update_state._INSTALL_STATE_LOCK_SUFFIX}"
        )
        with (
            mock.patch.object(update_state, "_INSTALL_STATE_LOCK_TIMEOUT_S", 0.2),
            held_lock_file(lock_path),
            pytest.raises(TimeoutError),
        ):
            du.save_status(paths, du.default_status_payload(current_version="0.1.0"))


def test_stale_status_write_cannot_erase_a_repair_failure() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        paths = _paths(tmp)
        poller_snapshot = _ready_to_install(paths)
        du.save_status(
            paths,
            {
                **du.load_status(paths, current_version="0.1.0"),
                "installState": "failed",
                "installStage": "rollback_failed",
                "installStageLabel": "Update repair required",
                "repairState": "repair_failed",
                "repairError": "Repair could not replace the install.",
                "repairFailedAt": "2026-05-01T12:00:03Z",
                "helperUpdatedAt": "2026-05-01T12:00:03Z",
            },
            phase=INSTALL_WRITE_PHASE,
        )

        du.save_status(
            paths,
            {
                **poller_snapshot,
                "lastCheckedAt": "2026-05-01T12:00:04Z",
            },
        )

        status = du.load_status(paths, current_version="0.1.0")
        assert status["installState"] == "failed"
        assert status["installStage"] == "rollback_failed"
        assert status["repairState"] == "repair_failed"
        assert status["repairError"] == "Repair could not replace the install."
        assert status["repairFailedAt"] == "2026-05-01T12:00:03Z"


def _contention_worker(data_dir: str, barrier: Any, worker: int, writes: int) -> None:
    """One independent process, released with every other writer at the barrier."""
    paths = du.DesktopUpdatePaths.from_data_dir(Path(data_dir))
    barrier.wait(timeout=60)
    for index in range(writes):
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "lastCheckedAt": f"{worker}-{index}",
            },
        )


def test_concurrent_processes_do_not_lose_status_writes() -> None:
    """Every save must see the revision the previous save wrote.

    A save reads the revision and writes ``revision + 1``, so a read-modify-write
    that is not serialized drops writes and the final revision falls short of the
    save count. With the write lock removed this failed every run (final revision
    3-7 of 24), because the six writers start together and their read/write gaps
    overlap.
    """
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        workers = 6
        writes = 4
        context = multiprocessing.get_context("spawn")
        barrier = context.Barrier(workers)
        processes = [
            context.Process(
                target=_contention_worker,
                args=(str(data_dir), barrier, index, writes),
            )
            for index in range(workers)
        ]
        try:
            for process in processes:
                process.start()
            for process in processes:
                process.join(timeout=60)
                assert process.exitcode == 0, f"contention worker exited {process.exitcode}"
        finally:
            for process in processes:
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=10)

        status = du.load_status(
            du.DesktopUpdatePaths.from_data_dir(data_dir),
            current_version="0.1.0",
        )
        assert status["revision"] == workers * writes
