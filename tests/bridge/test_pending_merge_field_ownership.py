"""A run snapshot may not roll back fields the discovery run never writes."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.bridge.storage_health import close_storage_stores, get_storage_store
from src.source_registry import save_registry_state_atomic
from src.storage.source_registry_runtime import SourceRegistryRuntimeStore
from tests.bridge.test_source_registry_concurrent_mutations import (
    _B_PENDING,
    _apply_approval,
    _paths,
)

# ``ok`` is retired: no registry-row writer and no reader.
_AUTHORITY_FIELDS = {
    "lastCheckedAt": "2026-07-01T09:30:00+00:00",
    "httpStatus": 200,
    "lastValidationStatus": "ok_with_jobs",
    "coverageAuditOpenings": 12,
    "promotionReason": "curated_seed_live_jobs",
}
_STALE_RUN_VALUES = {
    "lastCheckedAt": "2026-07-01T08:00:00+00:00",
    "httpStatus": 410,
    "lastValidationStatus": "stale",
    "coverageAuditOpenings": 1,
    "promotionReason": "stale_run_reason",
}


@pytest.mark.parametrize("authority", ["sqlite", "json"])
def test_run_snapshot_cannot_roll_back_authority_owned_fields(
    tmp_path: Path, authority: str
) -> None:
    """Discovery never writes these, so the run's copy is a stale snapshot."""
    try:
        current = {
            **_B_PENDING,
            "jobsFound": 3,
            "lastProbedAt": "2026-07-01T09:00:00+00:00",
            **_AUTHORITY_FIELDS,
        }
        if authority == "sqlite":
            store = get_storage_store(tmp_path)
            SourceRegistryRuntimeStore(store).replace_state(
                state={"active": [], "pending": [current], "rejected": []},
                tombstones={},
                reason="test_seed",
            )
        else:
            paths = _paths(tmp_path)
            save_registry_state_atomic(
                paths["active"],
                paths["pending"],
                paths["rejected"],
                {"active": [], "pending": [current], "rejected": []},
            )
        run = {
            **current,
            "jobsFound": 6,
            "sampleCount": 6,
            "lastProbedAt": "2026-07-01T10:00:00+00:00",
            "lastProbeStatus": "ok",
            **_STALE_RUN_VALUES,
        }

        state, _, returned_authority = _apply_approval(
            tmp_path,
            pending_rows=[run],
            auto_approve_enabled=False,
        )

        assert returned_authority == authority
        [row] = state["pending"]
        # The fresh probe still lands; only the authority-owned fields hold.
        assert row["jobsFound"] == 6
        for field, value in _AUTHORITY_FIELDS.items():
            assert row[field] == value, field
    finally:
        close_storage_stores()
