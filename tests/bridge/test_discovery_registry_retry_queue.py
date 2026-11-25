"""The reconciliation retry queue is bounded and stores a minimal snapshot."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from src.bridge import discovery_service_registry
from src.bridge.discovery_registry_retry_queue import (
    RECONCILE_RETRY_MAX_ENTRIES,
    _entry_serialized_size,
    _oldest_run_id,
    evict_retries_over_budget,
    retry_queue_bytes,
)
from tests.bridge.test_discovery_service_hardening import _make_service

_NOW = "2026-07-01T10:00:00+00:00"


def _report(run_id: str) -> dict[str, Any]:
    return {
        "runId": run_id,
        "startedAt": "2026-07-01T09:00:00+00:00",
        "finishedAt": _NOW,
        "summary": {
            "queuedCandidateCount": 1,
            "approvedCandidateCount": 0,
            "liveCandidateCount": 0,
            "ignoredSummaryField": 9,
        },
        "runtime": {
            "autoApproval": {
                "enabled": False,
                "approvedCount": 0,
                "status": "completed",
                "error": "ignored",
                "authorityMode": "sqlite",
                "ignoredAutoField": True,
            },
            "registryFinalization": {
                "status": "failed",
                "error": "authority write failed",
                "pendingCount": 1,
                "activeCount": 0,
                "rejectedCount": 0,
                "candidateCount": 4,
                "pendingRows": [
                    {
                        "id": "row-1",
                        "adapter": "static",
                        "name": "Retry Studio",
                        "jobsFound": 1,
                    }
                ],
                "pendingMergeAudit": {"fieldConflicts": 1},
                "pendingMergeConflicts": [
                    {
                        "identity": "row-1",
                        "field": "name",
                        "authorityValue": "Old",
                        "runValue": "New",
                    }
                ],
            },
            "lifecycle": {
                "heartbeatAt": "2026-07-01T09:59:00+00:00",
                "ignoredLifecycleField": "finalize",
            },
            "taskProgress": {"stage": "finalize"},
            "candidateReview": {"recommendationCounts": {}},
            "urlPatchStats": {"patched": 1},
        },
        "candidates": [
            {
                "id": "row-1",
                "adapter": "static",
                "name": "Retry Studio",
                "jobsFound": 1,
            }
        ],
        "failures": [{"error": "ignored"}],
        "taskProgress": {"stage": "finalize"},
    }


def _make_queue_service(tmp_path: Path, events: list[dict[str, Any]]) -> Any:
    return _make_service(
        tmp_path,
        report_path=tmp_path / "source-discovery-report.json",
        now_iso=_NOW,
        bridge_log=lambda level, message, **fields: events.append(
            {"level": level, "message": message, **fields}
        ),
        load_state=lambda: {"active": [], "pending": [], "rejected": []},
        persist_state_and_auto_sync=lambda state, **_kwargs: state,
    )


def _auto_approval_report() -> dict[str, Any]:
    pending_row = {
        "id": "pending-row",
        "adapter": "static",
        "name": "Pending Studio",
        "careersUrl": "https://pending.example/careers",
        "jobsFound": 3,
        "sampleCount": 3,
        "weakSignal": False,
        "status": "healthy",
        "candidateState": "validated",
    }
    return {
        "runId": "run-auto",
        "startedAt": "2026-07-01T09:00:00+00:00",
        "finishedAt": _NOW,
        "summary": {"approvedCandidateCount": 0, "liveCandidateCount": 0},
        "runtime": {
            "autoApproval": {"enabled": True, "approvedCount": 0, "status": "completed"},
            "registryFinalization": {
                "status": "failed",
                "error": "authority write failed",
                "pendingCount": 1,
                "activeCount": 0,
                "rejectedCount": 0,
                "pendingRows": [pending_row],
            },
        },
        "candidates": [
            dict(pending_row),
            {
                "id": "cap-row",
                "adapter": "static",
                "name": "Cap Studio",
                "careersUrl": "https://cap.example/careers",
                "jobsFound": 3,
                "sampleCount": 3,
                "weakSignal": False,
                "deferred": True,
                "deferReason": "adapter_cap",
                "status": "healthy",
                "rankReasons": ["medium_confidence", "jobs_found_bonus"],
            },
            {
                "id": "weak-cap-row",
                "adapter": "static",
                "name": "Weak Cap Studio",
                "careersUrl": "https://weak-cap.example/careers",
                "jobsFound": 3,
                "weakSignal": True,
                "deferred": True,
                "deferReason": "adapter_cap",
                "status": "healthy",
            },
            {
                "id": "other-row",
                "adapter": "static",
                "name": "Other Studio",
                "careersUrl": "https://other.example/careers",
                "jobsFound": 2,
                "status": "healthy",
            },
        ],
    }


def test_retry_snapshot_keeps_auto_approval_candidates(tmp_path: Path) -> None:
    state: dict[str, list[dict[str, Any]]] = {"active": [], "pending": [], "rejected": []}
    persisted: list[dict[str, list[dict[str, Any]]]] = []
    events: list[dict[str, Any]] = []

    def load_state() -> dict[str, list[dict[str, Any]]]:
        return {bucket: [dict(row) for row in rows] for bucket, rows in state.items()}

    def persist(next_state: dict[str, list[dict[str, Any]]], **_kwargs: Any) -> Any:
        persisted.append(next_state)
        state.clear()
        state.update({bucket: [dict(row) for row in rows] for bucket, rows in next_state.items()})
        return next_state

    service = _make_service(
        tmp_path,
        report_path=tmp_path / "source-discovery-report.json",
        now_iso=_NOW,
        bridge_log=lambda level, message, **fields: events.append(
            {"level": level, "message": message, **fields}
        ),
        load_state=load_state,
        persist_state_and_auto_sync=persist,
    )
    service._record_reconciliation_retry(
        run_id="run-auto",
        finished_at=_NOW,
        report=_auto_approval_report(),
        reason="lock_timeout",
    )

    retry_path = tmp_path / "source-registry-reconcile-retry.json"
    snapshot = json.loads(retry_path.read_text(encoding="utf-8"))["retries"]["run-auto"]["report"]
    assert [row["id"] for row in snapshot["candidates"]] == ["cap-row"]
    assert snapshot["pendingCandidateIds"] == ["pending-row"]

    assert service.drain_pending_registry_reconciliation() == 1
    assert len(persisted) == 1
    assert {row["id"] for row in persisted[0]["active"]} == {"pending-row", "cap-row"}
    assert persisted[0]["pending"] == []
    assert json.loads(retry_path.read_text(encoding="utf-8"))["retries"] == {}


def test_retry_queue_drops_the_oldest_entry_at_the_cap(tmp_path: Path) -> None:
    events: list[dict[str, Any]] = []
    service = _make_queue_service(tmp_path, events)
    cap = RECONCILE_RETRY_MAX_ENTRIES

    for index in range(cap + 1):
        service._record_reconciliation_retry(
            run_id=f"run-{index}",
            finished_at=f"2026-07-01T10:00:{index:02d}+00:00",
            report=_report(f"run-{index}"),
            reason="lock_timeout",
        )

    retries = json.loads(
        (tmp_path / "source-registry-reconcile-retry.json").read_text(encoding="utf-8")
    )["retries"]
    assert len(retries) == cap
    assert "run-0" not in retries
    assert f"run-{cap}" in retries

    evicted = next(
        event
        for event in events
        if event["message"] == "discovery_registry_reconciliation_retry_evicted"
    )
    assert evicted["reason"] == "queue_limit"
    assert evicted["cap"] == cap
    assert evicted["queueSize"] == cap
    assert evicted["evictedRunIds"] == ["run-0"]
    assert evicted["keptRunId"] == f"run-{cap}"


def test_retry_entry_stores_only_the_fields_the_drain_needs(tmp_path: Path) -> None:
    events: list[dict[str, Any]] = []
    service = _make_queue_service(tmp_path, events)

    service._record_reconciliation_retry(
        run_id="run-minimal",
        finished_at=_NOW,
        report=_report("run-minimal"),
        reason="lock_timeout",
    )

    entry = json.loads(
        (tmp_path / "source-registry-reconcile-retry.json").read_text(encoding="utf-8")
    )["retries"]["run-minimal"]
    assert set(entry) == {"finishedAt", "report"}
    snapshot = entry["report"]
    assert set(snapshot) == {
        "startedAt",
        "finishedAt",
        "summary",
        "runtime",
        "pendingCandidateIds",
    }
    assert snapshot["summary"] == {
        "approvedCandidateCount": 0,
        "liveCandidateCount": 0,
    }
    assert set(snapshot["runtime"]) == {
        "autoApproval",
        "registryFinalization",
        "lifecycle",
    }
    assert snapshot["runtime"]["autoApproval"] == {
        "enabled": False,
        "approvedCount": 0,
        "status": "completed",
    }
    assert set(snapshot["runtime"]["registryFinalization"]) == {
        "status",
        "error",
        "pendingCount",
        "activeCount",
        "rejectedCount",
        "pendingRows",
    }
    assert snapshot["runtime"]["lifecycle"] == {"heartbeatAt": "2026-07-01T09:59:00+00:00"}
    assert "candidates" not in snapshot
    assert snapshot["pendingCandidateIds"] == ["row-1"]
    assert "failures" not in snapshot
    assert "taskProgress" not in snapshot


def test_retry_snapshot_keeps_a_candidate_the_pending_row_does_not_cover(
    tmp_path: Path,
) -> None:
    """A run candidate the pending row diverges from stays whole."""
    events: list[dict[str, Any]] = []
    service = _make_queue_service(tmp_path, events)
    report = _report("run-uncovered")
    report["runtime"]["registryFinalization"]["pendingRows"] = [
        {"id": "row-1", "adapter": "static", "name": "Authority Studio", "jobsFound": 1}
    ]
    report["candidates"] = [
        {
            "id": "row-1",
            "adapter": "static",
            "name": "Run Studio",
            "jobsFound": 1,
            "status": "healthy",
        }
    ]

    service._record_reconciliation_retry(
        run_id="run-uncovered",
        finished_at=_NOW,
        report=report,
        reason="lock_timeout",
    )

    snapshot = json.loads(
        (tmp_path / "source-registry-reconcile-retry.json").read_text(encoding="utf-8")
    )["retries"]["run-uncovered"]["report"]
    assert snapshot["candidates"] == report["candidates"]
    assert "pendingCandidateIds" not in snapshot


def test_drain_restores_candidate_evidence_folded_into_pending_rows(tmp_path: Path) -> None:
    """The run's probe failure must still block approval after the merge drops it."""
    run_row = {
        "id": "folded-row",
        "adapter": "static",
        "name": "Folded Studio",
        "careersUrl": "https://folded.example/careers",
        "jobsFound": 3,
        "sampleCount": 3,
        "weakSignal": False,
        "status": "error",
        "lastProbeError": "probe failed",
    }
    report = {
        "runId": "run-folded",
        "startedAt": "2026-07-01T09:00:00+00:00",
        "finishedAt": _NOW,
        "summary": {"approvedCandidateCount": 0, "liveCandidateCount": 0},
        "runtime": {
            "autoApproval": {"enabled": True, "approvedCount": 0, "status": "completed"},
            "registryFinalization": {
                "status": "failed",
                "error": "authority write failed",
                "pendingCount": 1,
                "activeCount": 0,
                "rejectedCount": 0,
                "pendingRows": [dict(run_row)],
            },
        },
        "candidates": [dict(run_row)],
    }
    authority_pending = {
        "id": "folded-row",
        "adapter": "static",
        "name": "Folded Studio",
        "careersUrl": "https://folded.example/careers",
        "jobsFound": 3,
        "status": "healthy",
    }
    state: dict[str, list[dict[str, Any]]] = {
        "active": [],
        "pending": [authority_pending],
        "rejected": [],
    }
    persisted: list[dict[str, list[dict[str, Any]]]] = []

    def load_state() -> dict[str, list[dict[str, Any]]]:
        return {bucket: [dict(row) for row in rows] for bucket, rows in state.items()}

    def persist(next_state: dict[str, list[dict[str, Any]]], **_kwargs: Any) -> Any:
        persisted.append(json.loads(json.dumps(next_state)))
        return next_state

    service = _make_service(
        tmp_path,
        report_path=tmp_path / "source-discovery-report.json",
        now_iso=_NOW,
        bridge_log=lambda *_args, **_kwargs: None,
        load_state=load_state,
        persist_state_and_auto_sync=persist,
    )
    service._record_reconciliation_retry(
        run_id="run-folded",
        finished_at=_NOW,
        report=report,
        reason="lock_timeout",
    )

    retry_path = tmp_path / "source-registry-reconcile-retry.json"
    snapshot = json.loads(retry_path.read_text(encoding="utf-8"))["retries"]["run-folded"]["report"]
    assert "candidates" not in snapshot
    assert snapshot["pendingCandidateIds"] == ["folded-row"]

    assert service.drain_pending_registry_reconciliation() == 1
    assert persisted == []
    assert [row["id"] for row in state["pending"]] == ["folded-row"]
    assert state["active"] == []
    assert json.loads(retry_path.read_text(encoding="utf-8"))["retries"] == {}


def test_drain_rewrites_the_retry_file_a_bounded_number_of_times(tmp_path: Path) -> None:
    """A drain must not reload and rewrite the whole queue once per entry."""
    events: list[dict[str, Any]] = []
    service = _make_queue_service(tmp_path, events)
    for index in range(5):
        service._record_reconciliation_retry(
            run_id=f"run-{index}",
            finished_at=f"2026-07-01T10:00:{index:02d}+00:00",
            report=_report(f"run-{index}"),
            reason="lock_timeout",
        )

    with (
        mock.patch.object(
            service,
            "_load_reconciliation_retries",
            wraps=service._load_reconciliation_retries,
        ) as load_mock,
        mock.patch.object(
            service,
            "_save_reconciliation_retries",
            wraps=service._save_reconciliation_retries,
        ) as save_mock,
    ):
        assert service.drain_pending_registry_reconciliation() == 5

    # One read for the queue and one for the final rewrite, which persists every
    # attempt counter and resolved removal at once. The in-flight marker is the
    # only per-entry write, and it is not the retry file.
    assert load_mock.call_count == 2
    assert save_mock.call_count == 1
    inflight = json.loads(
        (tmp_path / "source-registry-reconcile-retry-inflight.json").read_text(encoding="utf-8")
    )
    assert inflight == {}


def test_crash_loop_abandons_only_the_entry_it_crashes_on(tmp_path: Path) -> None:
    """Ten restarts must not spend an attempt on every queued report."""
    events: list[dict[str, Any]] = []
    service = _make_queue_service(tmp_path, events)
    for index in range(3):
        service._record_reconciliation_retry(
            run_id=f"run-{index}",
            finished_at=f"2026-07-01T10:00:0{index}+00:00",
            report=_report(f"run-{index}"),
            reason="lock_timeout",
        )
    retry_path = tmp_path / "source-registry-reconcile-retry.json"
    inflight_path = tmp_path / "source-registry-reconcile-retry-inflight.json"
    max_attempts = discovery_service_registry._RECONCILE_RETRY_MAX_ATTEMPTS

    with mock.patch.object(
        service,
        "_reconcile_terminal_discovery_registry_state",
        side_effect=SystemExit("bridge died"),
    ):
        for _ in range(max_attempts):
            with pytest.raises(SystemExit):
                service.drain_pending_registry_reconciliation()

    retries = json.loads(retry_path.read_text(encoding="utf-8"))["retries"]
    # Only the entry each crashed drain reached spent an attempt; the two it
    # never got to keep the counts the last completed drain left them.
    assert retries["run-0"]["attempts"] == max_attempts - 1
    assert "attempts" not in retries["run-1"]
    assert "attempts" not in retries["run-2"]
    assert json.loads(inflight_path.read_text(encoding="utf-8")) == {"runId": "run-0"}

    assert service.drain_pending_registry_reconciliation() == 3

    abandoned = [
        event
        for event in events
        if event["message"] == "discovery_registry_reconciliation_retry_abandoned"
    ]
    assert [(event["runId"], event["reason"]) for event in abandoned] == [
        ("run-0", "attempt_limit")
    ]
    assert json.loads(retry_path.read_text(encoding="utf-8"))["retries"] == {}


def test_retry_queue_evicts_the_oldest_entry_over_the_byte_budget(tmp_path: Path) -> None:
    """The entry cap bounds count, not bytes; the budget bounds the accumulated parse cost."""
    events: list[dict[str, Any]] = []
    service = _make_queue_service(tmp_path, events)
    retry_path = tmp_path / "source-registry-reconcile-retry.json"
    for index in range(2):
        service._record_reconciliation_retry(
            run_id=f"run-{index}",
            finished_at=f"2026-07-01T10:00:0{index}+00:00",
            report=_report(f"run-{index}"),
            reason="lock_timeout",
        )
    queued = json.loads(retry_path.read_text(encoding="utf-8"))["retries"]
    budget = retry_queue_bytes(queued)

    with mock.patch.object(discovery_service_registry, "RECONCILE_RETRY_MAX_BYTES", budget):
        service._record_reconciliation_retry(
            run_id="run-2",
            finished_at="2026-07-01T10:00:02+00:00",
            report=_report("run-2"),
            reason="lock_timeout",
        )

    retries = json.loads(retry_path.read_text(encoding="utf-8"))["retries"]
    assert list(retries) == ["run-1", "run-2"]
    evicted = next(
        event
        for event in events
        if event["message"] == "discovery_registry_reconciliation_retry_evicted"
        and event["reason"] == "byte_budget"
    )
    assert evicted["evictedRunIds"] == ["run-0"]
    assert evicted["keptRunId"] == "run-2"
    assert evicted["budgetBytes"] == budget
    assert evicted["queueBytes"] <= budget


def test_retry_queue_warns_when_one_entry_exceeds_the_byte_budget(tmp_path: Path) -> None:
    """A single oversized report is kept: it is the repair the queue exists for."""
    events: list[dict[str, Any]] = []
    service = _make_queue_service(tmp_path, events)

    with mock.patch.object(discovery_service_registry, "RECONCILE_RETRY_MAX_BYTES", 1):
        service._record_reconciliation_retry(
            run_id="run-huge",
            finished_at=_NOW,
            report=_report("run-huge"),
            reason="lock_timeout",
        )

    retries = json.loads(
        (tmp_path / "source-registry-reconcile-retry.json").read_text(encoding="utf-8")
    )["retries"]
    assert list(retries) == ["run-huge"]
    oversized = next(
        event
        for event in events
        if event["message"] == "discovery_registry_reconciliation_retry_oversized"
    )
    assert oversized["runId"] == "run-huge"
    assert oversized["budgetBytes"] == 1
    assert oversized["entryBytes"] > 1


def test_drain_keeps_a_retry_re_recorded_during_the_pass(tmp_path: Path) -> None:
    """The reconcile's own lock timeout re-records the entry; it must survive."""
    events: list[dict[str, Any]] = []
    service = _make_queue_service(tmp_path, events)
    service._record_reconciliation_retry(
        run_id="run-rerecorded",
        finished_at=_NOW,
        report=_report("run-rerecorded"),
        reason="lock_timeout",
    )
    retry_path = tmp_path / "source-registry-reconcile-retry.json"

    def reconcile(
        *, run_id: str, finished_at: str, report: dict[str, Any], **_kwargs: Any
    ) -> tuple[dict[str, Any], int, bool]:
        service._record_reconciliation_retry(
            run_id=run_id,
            finished_at=finished_at,
            report=report,
            reason="lock_timeout",
        )
        return report, 0, False

    with mock.patch.object(
        service, "_reconcile_terminal_discovery_registry_state", side_effect=reconcile
    ):
        assert service.drain_pending_registry_reconciliation() == 1

    retries = json.loads(retry_path.read_text(encoding="utf-8"))["retries"]
    assert list(retries) == ["run-rerecorded"]
    assert retries["run-rerecorded"]["attempts"] == 1


def test_drain_keeps_a_retry_re_recorded_with_a_newer_snapshot(tmp_path: Path) -> None:
    """A newer report for the same run id is neither charged nor cleared."""
    events: list[dict[str, Any]] = []
    service = _make_queue_service(tmp_path, events)
    service._record_reconciliation_retry(
        run_id="run-newer",
        finished_at=_NOW,
        report=_report("run-newer"),
        reason="lock_timeout",
    )
    retry_path = tmp_path / "source-registry-reconcile-retry.json"
    newer_finished_at = "2026-07-01T11:00:00+00:00"

    def reconcile(
        *, run_id: str, report: dict[str, Any], **_kwargs: Any
    ) -> tuple[dict[str, Any], int, bool]:
        service._record_reconciliation_retry(
            run_id=run_id,
            finished_at=newer_finished_at,
            report=_report(run_id),
            reason="lock_timeout",
        )
        return report, 0, True

    with mock.patch.object(
        service, "_reconcile_terminal_discovery_registry_state", side_effect=reconcile
    ):
        assert service.drain_pending_registry_reconciliation() == 1

    retries = json.loads(retry_path.read_text(encoding="utf-8"))["retries"]
    assert retries["run-newer"]["finishedAt"] == newer_finished_at
    assert "attempts" not in retries["run-newer"]


def _budget_entry(index: int, payload_bytes: int, fill: str = "x") -> dict[str, Any]:
    return {
        "finishedAt": f"2026-07-01T10:00:{index:02d}+00:00",
        "report": {"blob": fill * payload_bytes},
    }


def _reserializing_evictions(
    retries: dict[str, dict[str, Any]],
    *,
    keep_run_id: str,
    max_bytes: int,
) -> list[str]:
    """The pre-change loop: re-serialize the whole queue before each eviction."""
    budget = max(1, int(max_bytes))
    evicted: list[str] = []
    while len(retries) > 1 and retry_queue_bytes(retries) > budget:
        oldest = _oldest_run_id(retries, keep_run_id=keep_run_id)
        if not oldest:
            break
        retries.pop(oldest)
        evicted.append(oldest)
    return evicted


def test_entry_size_is_the_queue_delta_around_its_comma() -> None:
    """The running total is exact only if each entry's size matches the queue delta."""
    retries = {
        "run-plain": _budget_entry(0, 40),
        "run-é漢": _budget_entry(1, 300, "é"),
        'run-"quoted"': _budget_entry(2, 90),
    }
    while retries:
        run_id = next(iter(retries))
        before = retry_queue_bytes(retries)
        entry = retries.pop(run_id)
        # Every entry but the last one leaves its separating comma behind.
        separator = 1 if retries else 0
        assert before - retry_queue_bytes(retries) == (
            _entry_serialized_size(run_id, entry) + separator
        )


@pytest.mark.parametrize("keep_run_id", ["run-0", "run-7"])
@pytest.mark.parametrize("budget_divisor", [1, 2, 3, 5, 10])
def test_running_total_evicts_the_same_entries_as_reserializing(
    budget_divisor: int,
    keep_run_id: str,
) -> None:
    """The O(n) running total must match the old per-eviction re-serialization."""
    retries = {
        f"run-{index}": _budget_entry(index, payload, "é" if index % 2 else "x")
        for index, payload in enumerate((40, 4000, 300, 2000, 900, 7000, 100, 1500))
    }
    budget = max(1, retry_queue_bytes(retries) // budget_divisor)
    reference = {run_id: dict(entry) for run_id, entry in retries.items()}

    expected = _reserializing_evictions(reference, keep_run_id=keep_run_id, max_bytes=budget)
    evicted = evict_retries_over_budget(retries, keep_run_id=keep_run_id, max_bytes=budget)

    assert evicted == expected
    assert len(retries) == 1 or retry_queue_bytes(retries) <= budget
