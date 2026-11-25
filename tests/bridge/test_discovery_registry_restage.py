"""The bridge's failed-publish restage must use the authority's pending merge."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from src.bridge import discovery_service_registry
from src.source_registry import (
    REGISTRY_REASON_REPEATED_ZERO_JOBS,
    hide_repeated_zero_job_pending,
)
from src.source_registry_identity import source_identity, source_url_fingerprint
from tests.bridge.test_discovery_service_hardening import _make_service
from tests.helpers.mutation import append_and_return

_NOW = "2026-07-01T10:00:00+00:00"

_X_ZERO_JOB_PENDING = {
    "id": "x",
    "adapter": "static",
    "name": "Studio X",
    "careersUrl": "https://x.example/careers",
    "jobsFound": 0,
    "deferCount": 0,
}


def _failed_publish_report(*, run_id: str, pending_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "runId": run_id,
        "startedAt": "2026-07-01T09:00:00+00:00",
        "finishedAt": "2026-07-01T10:00:00+00:00",
        "summary": {"queuedCandidateCount": len(pending_rows)},
        "runtime": {
            "autoApproval": {"enabled": False, "approvedCount": 0, "status": "completed"},
            "registryFinalization": {
                "status": "failed",
                "error": "authority write failed",
                "pendingCount": len(pending_rows),
                "pendingRows": pending_rows,
            },
        },
        "candidates": [],
    }


def _write_reconciliation_retry(
    path: Path, *, run_id: str, report: dict[str, Any], finished_at: str = _NOW
) -> None:
    path.write_text(
        json.dumps({"retries": {run_id: {"finishedAt": finished_at, "report": report}}}),
        encoding="utf-8",
    )


def test_failed_publish_restage_carries_a_run_zero_job_hide(tmp_path: Path) -> None:
    """The restage must not keep a second copy of the pending merge.

    The run hid the row through the repeated-zero-job policy before its publish
    failed. A restage that only carried observations would leave the board
    visible again, which is the bug a parallel merge introduces.
    """
    report_path = tmp_path / "source-discovery-report.json"
    current = {
        **_X_ZERO_JOB_PENDING,
        "name": "Admin Studio X",
        "stateChangedAt": "2026-06-01T09:00:00+00:00",
        "stateChangedBy": "admin",
    }
    hidden = hide_repeated_zero_job_pending(
        {
            **_X_ZERO_JOB_PENDING,
            "name": "Run Studio X",
            "deferCount": 3,
            "lastDemotedAt": _NOW,
        },
        at=_NOW,
    )
    report_path.write_text(
        json.dumps(
            _failed_publish_report(run_id="discovery_hidden_during_run", pending_rows=[hidden])
        ),
        encoding="utf-8",
    )
    persisted_states: list[dict[str, list[dict[str, Any]]]] = []

    def persist_state_and_auto_sync(
        next_state: dict[str, list[dict[str, Any]]], **_kwargs: Any
    ) -> dict[str, list[dict[str, Any]]]:
        persisted_states.append(json.loads(json.dumps(next_state)))
        return next_state

    service = _make_service(
        tmp_path,
        report_path=report_path,
        now_iso="2026-07-01T10:05:00+00:00",
        bridge_log=lambda *_args, **_kwargs: None,
        load_state=lambda: {"active": [], "pending": [dict(current)], "rejected": []},
        persist_state_and_auto_sync=persist_state_and_auto_sync,
    )

    service.reconcile_terminal_discovery_report_from_state()

    assert len(persisted_states) == 1
    [row] = persisted_states[0]["pending"]
    assert row["hiddenFromDefault"] is True
    assert row["candidateState"] == "hidden"
    assert row["pendingReason"] == REGISTRY_REASON_REPEATED_ZERO_JOBS
    assert row["deferCount"] == 0
    assert row["name"] == "Admin Studio X"
    finalization = json.loads(report_path.read_text(encoding="utf-8"))["runtime"][
        "registryFinalization"
    ]
    assert finalization["pendingUpdatedCount"] == 1
    assert "pendingRestagedCount" not in finalization


def test_restage_surfaces_the_pending_merge_audit(tmp_path: Path) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    placed = {
        "id": "placed-row",
        "adapter": "static",
        "name": "Placed Studio",
        "careersUrl": "https://placed.example/careers",
    }
    new = {
        "id": "new-row",
        "adapter": "static",
        "name": "New Studio",
        "careersUrl": "https://new.example/careers",
        "jobsFound": 2,
    }
    report_path.write_text(
        json.dumps(_failed_publish_report(run_id="discovery_audit", pending_rows=[new, placed])),
        encoding="utf-8",
    )
    state: dict[str, list[dict[str, Any]]] = {
        "active": [{"id": "placed-row", "adapter": "static"}],
        "pending": [],
        "rejected": [],
    }
    persisted: list[dict[str, list[dict[str, Any]]]] = []
    service = _make_service(
        tmp_path,
        report_path=report_path,
        now_iso=_NOW,
        bridge_log=lambda *_args, **_kwargs: None,
        load_state=lambda: json.loads(json.dumps(state)),
        persist_state_and_auto_sync=lambda next_state, **_kwargs: append_and_return(
            persisted, json.loads(json.dumps(next_state)), next_state
        ),
    )

    service.reconcile_terminal_discovery_report_from_state()

    finalization = json.loads(report_path.read_text(encoding="utf-8"))["runtime"][
        "registryFinalization"
    ]
    assert len(persisted) == 1
    assert finalization["pendingRestagedCount"] == 1
    assert finalization["pendingMergeAudit"] == {"rowsDroppedPlaced": 1}


def test_restage_audits_fill_only_conflicts(tmp_path: Path) -> None:
    """A run's renamed source is visible instead of silently dropped."""
    report_path = tmp_path / "source-discovery-report.json"
    current = {
        "id": "migrated-row",
        "adapter": "static",
        "name": "Admin Studio",
        "careersUrl": "https://old.example/careers",
        "jobsFound": 1,
        "lastProbedAt": "2026-07-01T09:00:00+00:00",
    }
    run = {
        "id": "migrated-row",
        "adapter": "static",
        "name": "Run Studio",
        "careersUrl": "https://new.example/careers",
        "jobsFound": 4,
        "lastProbedAt": _NOW,
    }
    report_path.write_text(
        json.dumps(_failed_publish_report(run_id="discovery_conflicts", pending_rows=[run])),
        encoding="utf-8",
    )
    logs: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    service = _make_service(
        tmp_path,
        report_path=report_path,
        now_iso="2026-07-01T10:05:00+00:00",
        bridge_log=lambda *args, **kwargs: logs.append((args, kwargs)),
        load_state=lambda: {"active": [], "pending": [dict(current)], "rejected": []},
    )

    service.reconcile_terminal_discovery_report_from_state()

    finalization = json.loads(report_path.read_text(encoding="utf-8"))["runtime"][
        "registryFinalization"
    ]
    assert finalization["pendingUpdatedCount"] == 1
    assert finalization["pendingMergeConflicts"] == [
        {
            "identity": source_identity(current),
            "field": "name",
            "authorityValue": "Admin Studio",
            "runValue": "Run Studio",
        },
        {
            "identity": source_identity(current),
            "field": "careersUrl",
            "authorityValue": "https://old.example/careers",
            "runValue": "https://new.example/careers",
        },
    ]
    assert finalization["pendingMergeAudit"]["fieldConflicts"] == 2
    audit_log = next(
        kwargs
        for args, kwargs in logs
        if args[:2] == ("info", "discovery_registry_pending_merge_audit")
    )
    assert audit_log["fieldConflicts"] == 2
    assert audit_log["truncated"] is False
    assert audit_log["conflicts"] == finalization["pendingMergeConflicts"]


def test_lock_timeout_records_a_retry_and_the_next_reconcile_restages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lock timeout must not drop the run's pending rows."""
    report_path = tmp_path / "source-discovery-report.json"
    staged = {
        "id": "retry-row",
        "adapter": "static",
        "name": "Retry Studio",
        "careersUrl": "https://retry.example/careers",
        "jobsFound": 4,
    }
    report_path.write_text(
        json.dumps(_failed_publish_report(run_id="discovery_retry", pending_rows=[staged])),
        encoding="utf-8",
    )
    state: dict[str, list[dict[str, Any]]] = {"active": [], "pending": [], "rejected": []}
    persisted_states: list[dict[str, list[dict[str, Any]]]] = []

    def load_state() -> dict[str, list[dict[str, Any]]]:
        return {bucket: [dict(row) for row in rows] for bucket, rows in state.items()}

    def persist_state_and_auto_sync(
        next_state: dict[str, list[dict[str, Any]]], **_kwargs: Any
    ) -> dict[str, list[dict[str, Any]]]:
        nonlocal state
        state = json.loads(json.dumps(next_state))
        persisted_states.append(json.loads(json.dumps(next_state)))
        return next_state

    service = _make_service(
        tmp_path,
        report_path=report_path,
        now_iso=_NOW,
        bridge_log=lambda *_args, **_kwargs: None,
        load_state=load_state,
        persist_state_and_auto_sync=persist_state_and_auto_sync,
    )
    real_lock = discovery_service_registry.source_registry_write_lock
    lock_calls: list[int] = []

    @contextmanager
    def flaky_lock(data_dir: Path | str, *, timeout_s: float | None = None) -> Iterator[None]:
        lock_calls.append(1)
        if len(lock_calls) == 1:
            raise discovery_service_registry.SourceRegistryWriteLockTimeout("busy")
        with real_lock(data_dir, timeout_s=timeout_s):
            yield

    monkeypatch.setattr(discovery_service_registry, "source_registry_write_lock", flaky_lock)
    retry_path = tmp_path / "source-registry-reconcile-retry.json"

    service.reconcile_terminal_discovery_report_from_state()

    retries = json.loads(retry_path.read_text(encoding="utf-8"))["retries"]
    assert list(retries) == ["discovery_retry"]
    assert set(retries["discovery_retry"]) == {"finishedAt", "report"}
    assert persisted_states == []

    service.reconcile_terminal_discovery_report_from_state()

    assert len(persisted_states) == 1
    [row] = persisted_states[0]["pending"]
    assert row["id"] == "retry-row"
    assert json.loads(retry_path.read_text(encoding="utf-8"))["retries"] == {}


def test_terminal_reconciliation_takes_the_registry_write_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Load, merge and persist must be one critical section.

    An admin mutation that lands between the bridge's load and its whole-state
    write would otherwise be overwritten.
    """
    entered: list[str] = []
    real_lock = discovery_service_registry.source_registry_write_lock

    @contextmanager
    def recording_lock(data_dir: Path | str, *, timeout_s: float | None = None) -> Iterator[None]:
        entered.append(str(Path(data_dir).resolve()))
        with real_lock(data_dir, timeout_s=timeout_s):
            yield

    monkeypatch.setattr(discovery_service_registry, "source_registry_write_lock", recording_lock)
    report_path = tmp_path / "source-discovery-report.json"
    report_path.write_text(
        json.dumps(_failed_publish_report(run_id="discovery_lock", pending_rows=[])),
        encoding="utf-8",
    )
    service = _make_service(
        tmp_path,
        report_path=report_path,
        now_iso="2026-07-01T10:05:00+00:00",
        bridge_log=lambda *_args, **_kwargs: None,
        load_state=lambda: {"active": [], "pending": [], "rejected": []},
        persist_state_and_auto_sync=lambda state, **_kwargs: state,
    )

    service.reconcile_terminal_discovery_report_from_state()

    assert entered == [str(tmp_path.resolve())]


def test_restage_reads_tombstones_from_the_configured_path(tmp_path: Path) -> None:
    """A custom data layout must not silently disable resurrection protection."""
    report_path = tmp_path / "source-discovery-report.json"
    deleted = {
        "id": "deleted-row",
        "adapter": "static",
        "name": "Deleted Studio",
        "careersUrl": "https://deleted.example/careers",
        "jobsFound": 4,
    }
    live = {
        "id": "live-row",
        "adapter": "static",
        "name": "Live Studio",
        "careersUrl": "https://live.example/careers",
        "jobsFound": 2,
    }
    report_path.write_text(
        json.dumps(
            _failed_publish_report(run_id="discovery_tombstone_path", pending_rows=[deleted, live])
        ),
        encoding="utf-8",
    )
    tombstone_path = tmp_path / "custom-layout" / "deleted-sources.json"
    tombstone_path.parent.mkdir(parents=True)
    tombstone_path.write_text(
        json.dumps(
            {
                source_identity(deleted): {
                    "sourceId": source_identity(deleted),
                    "sourceUrlFingerprint": "",
                }
            }
        ),
        encoding="utf-8",
    )
    persisted: list[dict[str, list[dict[str, Any]]]] = []
    service = _make_service(
        tmp_path,
        report_path=report_path,
        now_iso=_NOW,
        bridge_log=lambda *_args, **_kwargs: None,
        load_state=lambda: {"active": [], "pending": [], "rejected": []},
        persist_state_and_auto_sync=lambda next_state, **_kwargs: append_and_return(
            persisted, json.loads(json.dumps(next_state)), next_state
        ),
        tombstones_path=tombstone_path,
    )

    service.reconcile_terminal_discovery_report_from_state()

    assert [row["id"] for row in persisted[0]["pending"]] == ["live-row"]
    finalization = json.loads(report_path.read_text(encoding="utf-8"))["runtime"][
        "registryFinalization"
    ]
    assert finalization["pendingRestagedCount"] == 1
    assert finalization["pendingMergeAudit"] == {"rowsDroppedTombstoned": 1}


def test_drain_retries_without_a_launch_or_completion(tmp_path: Path) -> None:
    """A bridge restart alone must retry rows left by a lock timeout."""
    staged = {
        "id": "restart-row",
        "adapter": "static",
        "name": "Restart Studio",
        "careersUrl": "https://restart.example/careers",
        "jobsFound": 3,
    }
    report = _failed_publish_report(run_id="discovery_restart", pending_rows=[staged])
    retry_path = tmp_path / "source-registry-reconcile-retry.json"
    _write_reconciliation_retry(retry_path, run_id="discovery_restart", report=report)
    persisted: list[dict[str, list[dict[str, Any]]]] = []
    service = _make_service(
        tmp_path,
        report_path=tmp_path / "source-discovery-report.json",
        now_iso=_NOW,
        bridge_log=lambda *_args, **_kwargs: None,
        load_state=lambda: {"active": [], "pending": [], "rejected": []},
        persist_state_and_auto_sync=lambda next_state, **_kwargs: append_and_return(
            persisted, json.loads(json.dumps(next_state)), next_state
        ),
    )

    attempted = service.drain_pending_registry_reconciliation()

    assert attempted == 1
    assert [row["id"] for row in persisted[0]["pending"]] == ["restart-row"]
    assert json.loads(retry_path.read_text(encoding="utf-8"))["retries"] == {}


def test_drain_clears_a_retry_whose_row_is_suppressed_by_fingerprint(tmp_path: Path) -> None:
    """The applied-check must use the merge's suppression rules, not a subset."""
    deleted = {
        "id": "fingerprint-row",
        "adapter": "static",
        "name": "Fingerprint Studio",
        "listing_url": "https://fingerprint.example/jobs",
    }
    report = _failed_publish_report(run_id="discovery_fingerprint", pending_rows=[deleted])
    retry_path = tmp_path / "source-registry-reconcile-retry.json"
    _write_reconciliation_retry(retry_path, run_id="discovery_fingerprint", report=report)
    tombstone_path = tmp_path / "source-registry-tombstones.json"
    tombstone_path.write_text(
        json.dumps(
            {
                "other-identity": {
                    "sourceId": "other-identity",
                    "sourceUrlFingerprint": source_url_fingerprint(deleted),
                }
            }
        ),
        encoding="utf-8",
    )
    persisted: list[dict[str, list[dict[str, Any]]]] = []
    service = _make_service(
        tmp_path,
        report_path=tmp_path / "source-discovery-report.json",
        now_iso=_NOW,
        bridge_log=lambda *_args, **_kwargs: None,
        load_state=lambda: {"active": [], "pending": [], "rejected": []},
        persist_state_and_auto_sync=lambda next_state, **_kwargs: append_and_return(
            persisted, json.loads(json.dumps(next_state)), next_state
        ),
        tombstones_path=tombstone_path,
    )

    attempted = service.drain_pending_registry_reconciliation()

    assert attempted == 1
    assert persisted == []
    assert json.loads(retry_path.read_text(encoding="utf-8"))["retries"] == {}


def test_drain_clears_a_retry_after_the_state_was_already_persisted(tmp_path: Path) -> None:
    """A crash between the state write and the retry clear must not write twice."""
    staged = {
        "id": "applied-row",
        "adapter": "static",
        "name": "Applied Studio",
        "careersUrl": "https://applied.example/careers",
        "jobsFound": 3,
    }
    report = _failed_publish_report(run_id="discovery_applied", pending_rows=[staged])
    retry_path = tmp_path / "source-registry-reconcile-retry.json"
    _write_reconciliation_retry(retry_path, run_id="discovery_applied", report=report)
    persisted: list[dict[str, list[dict[str, Any]]]] = []
    state: dict[str, list[dict[str, Any]]] = {"active": [], "pending": [], "rejected": []}

    def load_state() -> dict[str, list[dict[str, Any]]]:
        return deepcopy(state)

    def persist_state_and_auto_sync(
        next_state: dict[str, list[dict[str, Any]]], **_kwargs: Any
    ) -> dict[str, list[dict[str, Any]]]:
        state.clear()
        state.update(deepcopy(next_state))
        persisted.append(deepcopy(next_state))
        return next_state

    service = _make_service(
        tmp_path,
        report_path=tmp_path / "source-discovery-report.json",
        now_iso=_NOW,
        bridge_log=lambda *_args, **_kwargs: None,
        load_state=load_state,
        persist_state_and_auto_sync=persist_state_and_auto_sync,
    )

    assert service.drain_pending_registry_reconciliation() == 1
    assert len(persisted) == 1

    # The state write landed; the process died before the retry file was cleared.
    _write_reconciliation_retry(retry_path, run_id="discovery_applied", report=report)

    assert service.drain_pending_registry_reconciliation() == 1
    assert len(persisted) == 1
    assert json.loads(retry_path.read_text(encoding="utf-8"))["retries"] == {}


def test_drain_does_not_overwrite_a_newer_report(tmp_path: Path) -> None:
    """A retry's state merge must not replace a newer terminal report."""
    stale = {
        "id": "stale-row",
        "adapter": "static",
        "name": "Stale Studio",
        "careersUrl": "https://stale.example/careers",
        "jobsFound": 2,
    }
    stale_report = _failed_publish_report(run_id="discovery_old", pending_rows=[stale])
    current_report = {
        "runId": "discovery_new",
        "startedAt": "2026-07-01T11:00:00+00:00",
        "finishedAt": "2026-07-01T11:05:00+00:00",
        "summary": {"queuedCandidateCount": 0},
        "runtime": {
            "registryFinalization": {
                "status": "completed",
                "activeCount": 0,
                "pendingCount": 0,
                "rejectedCount": 0,
            }
        },
        "candidates": [],
    }
    report_path = tmp_path / "source-discovery-report.json"
    report_path.write_text(json.dumps(current_report), encoding="utf-8")
    retry_path = tmp_path / "source-registry-reconcile-retry.json"
    _write_reconciliation_retry(retry_path, run_id="discovery_old", report=stale_report)
    persisted: list[dict[str, list[dict[str, Any]]]] = []
    service = _make_service(
        tmp_path,
        report_path=report_path,
        now_iso=_NOW,
        bridge_log=lambda *_args, **_kwargs: None,
        load_state=lambda: {"active": [], "pending": [], "rejected": []},
        persist_state_and_auto_sync=lambda next_state, **_kwargs: append_and_return(
            persisted, json.loads(json.dumps(next_state)), next_state
        ),
    )

    attempted = service.drain_pending_registry_reconciliation()

    assert attempted == 1
    assert [row["id"] for row in persisted[0]["pending"]] == ["stale-row"]
    assert json.loads(report_path.read_text(encoding="utf-8")) == current_report
    assert json.loads(retry_path.read_text(encoding="utf-8"))["retries"] == {}


def test_drain_continues_after_one_retry_fails(tmp_path: Path) -> None:
    """One bad retry must not stop the later entries from reconciling."""
    bad = {
        "id": "bad-row",
        "adapter": "static",
        "name": "Bad Studio",
        "careersUrl": "https://bad.example/careers",
        "jobsFound": 1,
    }
    good = {
        "id": "good-row",
        "adapter": "static",
        "name": "Good Studio",
        "careersUrl": "https://good.example/careers",
        "jobsFound": 4,
    }
    retry_path = tmp_path / "source-registry-reconcile-retry.json"
    retry_path.write_text(
        json.dumps(
            {
                "retries": {
                    "discovery_bad": {
                        "finishedAt": _NOW,
                        "report": _failed_publish_report(
                            run_id="discovery_bad", pending_rows=[bad]
                        ),
                    },
                    "discovery_good": {
                        "finishedAt": _NOW,
                        "report": _failed_publish_report(
                            run_id="discovery_good", pending_rows=[good]
                        ),
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    persisted: list[dict[str, list[dict[str, Any]]]] = []
    events: list[tuple[str, str, dict[str, Any]]] = []
    service = _make_service(
        tmp_path,
        report_path=tmp_path / "source-discovery-report.json",
        now_iso=_NOW,
        bridge_log=lambda level, message, **fields: events.append((level, message, fields)),
        load_state=lambda: {"active": [], "pending": [], "rejected": []},
        persist_state_and_auto_sync=lambda next_state, **_kwargs: append_and_return(
            persisted, json.loads(json.dumps(next_state)), next_state
        ),
    )
    real_reconcile = service._reconcile_terminal_discovery_registry_state
    calls: list[str] = []

    def reconcile(*, run_id: str, **kwargs: Any) -> tuple[dict[str, Any], int, bool]:
        calls.append(run_id)
        if run_id == "discovery_bad":
            raise OSError("bad retry")
        return real_reconcile(run_id=run_id, **kwargs)

    with mock.patch.object(
        service, "_reconcile_terminal_discovery_registry_state", side_effect=reconcile
    ):
        attempted = service.drain_pending_registry_reconciliation()

    assert attempted == 2
    assert calls == ["discovery_bad", "discovery_good"]
    assert [row["id"] for row in persisted[0]["pending"]] == ["good-row"]
    retries = json.loads(retry_path.read_text(encoding="utf-8"))["retries"]
    assert list(retries) == ["discovery_bad"]
    assert any(
        level == "warn"
        and message == "discovery_registry_reconciliation_retry_failed"
        and fields.get("runId") == "discovery_bad"
        for level, message, fields in events
    )


def test_drain_abandons_a_retry_older_than_the_age_limit(tmp_path: Path) -> None:
    """A stale report snapshot must not be retried forever."""
    report = _failed_publish_report(
        run_id="discovery_stale",
        pending_rows=[
            {
                "id": "stale-row",
                "adapter": "static",
                "name": "Stale Studio",
                "careersUrl": "https://stale.example/careers",
            }
        ],
    )
    retry_path = tmp_path / "source-registry-reconcile-retry.json"
    _write_reconciliation_retry(
        retry_path,
        run_id="discovery_stale",
        report=report,
        finished_at="2026-06-01T10:00:00+00:00",
    )
    events: list[tuple[str, str, dict[str, Any]]] = []
    service = _make_service(
        tmp_path,
        report_path=tmp_path / "source-discovery-report.json",
        now_iso=_NOW,
        bridge_log=lambda level, message, **fields: events.append((level, message, fields)),
        load_state=lambda: {"active": [], "pending": [], "rejected": []},
        persist_state_and_auto_sync=lambda next_state, **_kwargs: next_state,
    )
    with mock.patch.object(
        service,
        "_reconcile_terminal_discovery_registry_state",
        side_effect=AssertionError("expired retry must not reconcile"),
    ):
        attempted = service.drain_pending_registry_reconciliation()

    assert attempted == 1
    assert json.loads(retry_path.read_text(encoding="utf-8"))["retries"] == {}
    assert any(
        level == "warn"
        and message == "discovery_registry_reconciliation_retry_abandoned"
        and fields.get("reason") == "age_limit"
        for level, message, fields in events
    )


def test_drain_abandons_a_retry_after_the_attempt_limit(tmp_path: Path) -> None:
    """A permanently failing retry must stop after a bounded number of drains."""
    report = _failed_publish_report(
        run_id="discovery_stuck",
        pending_rows=[
            {
                "id": "stuck-row",
                "adapter": "static",
                "name": "Stuck Studio",
                "careersUrl": "https://stuck.example/careers",
            }
        ],
    )
    retry_path = tmp_path / "source-registry-reconcile-retry.json"
    _write_reconciliation_retry(retry_path, run_id="discovery_stuck", report=report)
    events: list[tuple[str, str, dict[str, Any]]] = []
    service = _make_service(
        tmp_path,
        report_path=tmp_path / "source-discovery-report.json",
        now_iso=_NOW,
        bridge_log=lambda level, message, **fields: events.append((level, message, fields)),
        load_state=lambda: {"active": [], "pending": [], "rejected": []},
        persist_state_and_auto_sync=lambda next_state, **_kwargs: next_state,
    )
    calls: list[str] = []

    def fail(*, run_id: str, **kwargs: Any) -> tuple[dict[str, Any], int, bool]:
        calls.append(run_id)
        raise OSError("still stuck")

    max_attempts = discovery_service_registry._RECONCILE_RETRY_MAX_ATTEMPTS
    with mock.patch.object(
        service, "_reconcile_terminal_discovery_registry_state", side_effect=fail
    ):
        for attempt in range(1, max_attempts + 1):
            assert service.drain_pending_registry_reconciliation() == 1
            retries = json.loads(retry_path.read_text(encoding="utf-8"))["retries"]
            assert retries["discovery_stuck"]["attempts"] == attempt
        assert service.drain_pending_registry_reconciliation() == 1

    assert len(calls) == max_attempts
    assert json.loads(retry_path.read_text(encoding="utf-8"))["retries"] == {}
    assert any(
        level == "warn"
        and message == "discovery_registry_reconciliation_retry_abandoned"
        and fields.get("reason") == "attempt_limit"
        for level, message, fields in events
    )
