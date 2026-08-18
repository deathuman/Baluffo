"""Regression: in-progress discovery must stay active when taskProgress omits ``active``."""

from __future__ import annotations

from typing import Any

from src.bridge.report_normalizer import normalize_discovery_report_contract


def test_normalize_discovery_report_in_progress_taskprogress_without_active_is_active() -> None:
    payload = {
        "schemaVersion": 1,
        "runId": "run_missing_active",
        "mode": "dynamic",
        "startedAt": "2026-03-30T12:00:00+00:00",
        "finishedAt": "",
        "summary": {
            "phaseKey": "starting",
            "phaseLabel": "Initializing scan",
            "foundEndpointCount": 0,
            "probedCandidateCount": 0,
            "queuedCandidateCount": 0,
        },
        "taskProgress": {
            "phaseKey": "starting",
            "phaseLabel": "Initializing scan",
            "mode": "indeterminate",
            "ratio": 0,
            "counts": {},
        },
        "candidates": [],
        "failures": [],
    }
    out = normalize_discovery_report_contract(payload)
    assert out["taskProgress"]["active"] is True


def test_normalize_discovery_report_finished_omitted_active_is_inactive() -> None:
    payload = {
        "schemaVersion": 1,
        "runId": "run_done",
        "mode": "dynamic",
        "startedAt": "2026-03-30T12:00:00+00:00",
        "finishedAt": "2026-03-30T12:30:00+00:00",
        "summary": {
            "phaseKey": "completed",
            "phaseLabel": "Discovery completed",
        },
        "taskProgress": {
            "phaseKey": "completed",
            "phaseLabel": "Discovery completed",
            "mode": "determinate",
            "ratio": 1,
            "counts": {},
        },
        "candidates": [],
        "failures": [],
    }
    out = normalize_discovery_report_contract(payload)
    assert out["taskProgress"]["active"] is False


def test_normalize_discovery_ship_seed_stub_is_not_active() -> None:
    """Packaged default source-discovery-report.json must not look like a live run."""
    payload: dict[str, Any] = {"summary": {}, "candidates": [], "failures": []}
    out = normalize_discovery_report_contract(payload)
    assert out["taskProgress"]["active"] is False
    assert out["taskProgress"]["phaseLabel"] == ""
    assert out["taskProgress"]["phaseKey"] == ""
