"""Recover a discovery registry publish that crashed before its terminal report."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tests.bridge.test_discovery_service_hardening import _make_service
from tests.helpers.mutation import append_and_return

_NOW = "2026-07-01T10:00:00+00:00"


def _running_report(
    *, run_id: str, pending_rows: list[dict[str, Any]], finished_at: str
) -> dict[str, Any]:
    return {
        "runId": run_id,
        "startedAt": "2026-07-01T09:00:00+00:00",
        "finishedAt": finished_at,
        "summary": {"queuedCandidateCount": len(pending_rows)},
        "runtime": {
            "autoApproval": {"enabled": True, "approvedCount": 0, "status": "running"},
            "registryFinalization": {
                "status": "running",
                "pendingCount": len(pending_rows),
                "activeCount": 0,
                "rejectedCount": 0,
                "pendingRows": pending_rows,
            },
        },
        "candidates": [],
    }


def test_restage_recovers_a_stale_running_report(tmp_path: Path) -> None:
    """A crash between the pre-publish report and the authority write is recoverable."""
    report_path = tmp_path / "source-discovery-report.json"
    staged = {
        "id": "crash-row",
        "adapter": "static",
        "name": "Crash Studio",
        "careersUrl": "https://crash.example/careers",
        "jobsFound": 3,
        "weakSignal": True,
    }
    report_path.write_text(
        json.dumps(
            _running_report(
                run_id="discovery_crash",
                pending_rows=[staged],
                finished_at="2026-07-01T09:50:00+00:00",
            )
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
    )

    service.reconcile_terminal_discovery_report_from_state()

    assert len(persisted) == 1
    assert [row["id"] for row in persisted[0]["pending"]] == ["crash-row"]
    saved_runtime = json.loads(report_path.read_text(encoding="utf-8"))["runtime"]
    finalization = saved_runtime["registryFinalization"]
    assert finalization["status"] == "failed"
    assert finalization["error"] == "stale_running_report_recovered"
    assert finalization["recoveredAt"] == _NOW
    assert finalization["pendingRestagedCount"] == 1
    assert saved_runtime["autoApproval"]["status"] == "failed"
    assert saved_runtime["autoApproval"]["error"] == "stale_running_report_recovered"


def test_restage_ignores_a_fresh_running_report(tmp_path: Path) -> None:
    """A live publish must not have its rows restaged underneath it."""
    report_path = tmp_path / "source-discovery-report.json"
    staged = {
        "id": "live-row",
        "adapter": "static",
        "name": "Live Studio",
        "careersUrl": "https://live.example/careers",
        "jobsFound": 3,
    }
    report_path.write_text(
        json.dumps(
            _running_report(
                run_id="discovery_live",
                pending_rows=[staged],
                finished_at=_NOW,
            )
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
    )

    service.reconcile_terminal_discovery_report_from_state()

    assert persisted == []
    finalization = json.loads(report_path.read_text(encoding="utf-8"))["runtime"][
        "registryFinalization"
    ]
    assert finalization["status"] == "running"
    assert "pendingRestagedCount" not in finalization


def test_restage_ignores_a_stale_running_report_without_staged_rows(tmp_path: Path) -> None:
    """An old running report with no crash snapshot is not a recovery source."""
    report_path = tmp_path / "source-discovery-report.json"
    report_path.write_text(
        json.dumps(
            _running_report(
                run_id="discovery_no_snapshot",
                pending_rows=[],
                finished_at="2026-07-01T09:50:00+00:00",
            )
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
    )

    service.reconcile_terminal_discovery_report_from_state()

    assert persisted == []
    finalization = json.loads(report_path.read_text(encoding="utf-8"))["runtime"][
        "registryFinalization"
    ]
    assert finalization["status"] == "running"
