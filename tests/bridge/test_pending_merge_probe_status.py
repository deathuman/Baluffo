"""Which probe status decides whether a run's jobs count is trusted."""

from __future__ import annotations

from pathlib import Path

from src.bridge.storage_health import close_storage_stores, get_storage_store
from src.storage.source_registry_runtime import SourceRegistryRuntimeStore
from tests.bridge.test_source_registry_concurrent_mutations import (
    _B_PENDING,
    _apply_approval,
)


def test_failed_probe_status_without_an_error_message_still_keeps_jobs_count(
    tmp_path: Path,
) -> None:
    """A healthy source-level ``lastStatus`` must not mask the failed probe."""
    try:
        current = {
            **_B_PENDING,
            "jobsFound": 5,
            "lastProbedAt": "2026-07-01T09:00:00+00:00",
        }
        store = get_storage_store(tmp_path)
        SourceRegistryRuntimeStore(store).replace_state(
            state={"active": [], "pending": [current], "rejected": []},
            tombstones={},
            reason="test_seed",
        )
        run = {
            **current,
            "jobsFound": 0,
            "sampleCount": 0,
            "lastProbedAt": "2026-07-01T10:00:00+00:00",
            "lastStatus": "ok",
            "lastProbeStatus": "error",
        }

        state, _, _ = _apply_approval(
            tmp_path,
            pending_rows=[run],
            auto_approve_enabled=False,
        )

        [row] = state["pending"]
        assert row["jobsFound"] == 5
        assert row["lastProbeStatus"] == "error"
    finally:
        close_storage_stores()


def test_stale_last_status_cannot_suppress_a_newer_probe(tmp_path: Path) -> None:
    """The merge never reads the jobs-fetch status; the probe timestamp decides."""
    try:
        current = {
            **_B_PENDING,
            "jobsFound": 3,
            "lastProbedAt": "2026-07-01T09:00:00+00:00",
        }
        store = get_storage_store(tmp_path)
        SourceRegistryRuntimeStore(store).replace_state(
            state={"active": [], "pending": [current], "rejected": []},
            tombstones={},
            reason="test_seed",
        )
        # A row without probe metadata: only ``lastProbedAt`` says the run
        # probed, and the stale jobs-fetch status must not veto its count.
        run = {
            **current,
            "jobsFound": 6,
            "sampleCount": 6,
            "lastProbedAt": "2026-07-01T10:00:00+00:00",
            "lastStatus": "error",
        }

        state, _, _ = _apply_approval(
            tmp_path,
            pending_rows=[run],
            auto_approve_enabled=False,
        )

        [row] = state["pending"]
        assert row["jobsFound"] == 6
    finally:
        close_storage_stores()


def test_probe_success_wins_over_a_stale_source_status(tmp_path: Path) -> None:
    """A jobs-fetch error on the run snapshot must not mask a successful probe."""
    try:
        current = {
            **_B_PENDING,
            "jobsFound": 3,
            "lastProbedAt": "2026-07-01T09:00:00+00:00",
            "lastStatus": "ok",
        }
        store = get_storage_store(tmp_path)
        SourceRegistryRuntimeStore(store).replace_state(
            state={"active": [], "pending": [current], "rejected": []},
            tombstones={},
            reason="test_seed",
        )
        run = {
            **current,
            "jobsFound": 6,
            "sampleCount": 6,
            "lastProbedAt": "2026-07-01T10:00:00+00:00",
            "lastProbeStatus": "ok",
            "lastStatus": "error",
        }

        state, _, _ = _apply_approval(
            tmp_path,
            pending_rows=[run],
            auto_approve_enabled=False,
        )

        [row] = state["pending"]
        assert row["jobsFound"] == 6
        assert row["lastProbeStatus"] == "ok"
        # The jobs-fetch status is authority-owned; a stale run snapshot may not
        # roll it back.
        assert row["lastStatus"] == "ok"
    finally:
        close_storage_stores()
