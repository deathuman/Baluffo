from __future__ import annotations

from pathlib import Path

from src.storage import BaluffoStore
from tests.helpers.temp_paths import workspace_tmpdir


def _store(data_dir: Path) -> BaluffoStore:
    return BaluffoStore(
        data_dir,
        vacuum_freelist_threshold_bytes=100,
        vacuum_freelist_ratio=0.20,
        vacuum_large_database_bytes=1000,
        vacuum_large_database_ratio=0.35,
    )


def test_compaction_status_reports_ratio_blocked_large_allocation() -> None:
    with workspace_tmpdir("baluffo-store-compaction-space") as data_dir:
        with _store(data_dir) as store:
            sizes = {
                "databaseBytes": 2000,
                "walBytes": 0,
                "shmBytes": 0,
                "freelistBytes": 160,
            }

            status = store.compaction_status(sizes=sizes)

            assert status["status"] == "not-run"
            assert status["space"] == {
                "databaseBytes": 2000,
                "allocatedBytes": 1840,
                "freelistBytes": 160,
                "observedFreelistRatio": 0.08,
                "effectiveFreelistRatio": 0.35,
                "vacuumDueBySize": False,
                "vacuumBlockedBy": "freelist-ratio",
                "largeAllocatedLowFreelist": True,
            }
            assert store._compaction_needed(sizes) is False


def test_compaction_status_marks_the_ratio_boundary_as_due() -> None:
    with workspace_tmpdir("baluffo-store-compaction-space-boundary") as data_dir:
        with _store(data_dir) as store:
            sizes = {
                "databaseBytes": 2000,
                "walBytes": 0,
                "shmBytes": 0,
                "freelistBytes": 700,
            }

            status = store.compaction_status(sizes=sizes)

            assert status["space"]["observedFreelistRatio"] == 0.35
            assert status["space"]["vacuumDueBySize"] is True
            assert status["space"]["vacuumBlockedBy"] == ""
            assert status["space"]["largeAllocatedLowFreelist"] is False


def test_compaction_status_names_the_byte_threshold_when_it_blocks() -> None:
    with workspace_tmpdir("baluffo-store-compaction-space-bytes") as data_dir:
        with _store(data_dir) as store:
            sizes = {
                "databaseBytes": 2000,
                "walBytes": 0,
                "shmBytes": 0,
                "freelistBytes": 50,
            }

            status = store.compaction_status(sizes=sizes)

            assert status["space"]["vacuumBlockedBy"] == "freelist-bytes"
            assert status["space"]["largeAllocatedLowFreelist"] is True
            assert status["space"]["vacuumDueBySize"] is False
