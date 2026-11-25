"""The discovery progress signature must move only when the counters move."""

from __future__ import annotations

from typing import Any

from src.bridge.discovery_service_watch import _discovery_progress_signature


def _report(*, probed: int, updated_at: str = "2026-03-20T12:00:00Z") -> dict[str, Any]:
    return {
        "runId": "discovery_live_1",
        "startedAt": "2026-03-20T12:00:00Z",
        "summary": {"phaseKey": "probing_candidates", "probedCandidateCount": probed},
        "taskProgress": {
            "updatedAt": updated_at,
            "countsUpdatedAt": updated_at,
            "ratio": probed / 100,
            "counts": {"probedCandidates": probed, "probeTotal": 100},
        },
        "runtime": {"totalDurationMs": 123, "lifecycle": {"heartbeatAt": updated_at}},
    }


def test_signature_ignores_report_rewrites_that_do_not_move_the_counts() -> None:
    first = _report(probed=5)
    rewritten = _report(probed=5, updated_at="2026-03-20T12:05:00Z")
    rewritten["summary"]["phaseLabel"] = "Probing candidates"
    rewritten["summary"]["suppressedStaticCount"] = 9
    rewritten["taskProgress"]["ratio"] = 0.5
    # The worker rewrites its report on a cadence; none of these fields prove work.
    assert _discovery_progress_signature(
        rewritten, run_matches=True
    ) == _discovery_progress_signature(first, run_matches=True)


def test_signature_moves_only_with_the_progress_counts() -> None:
    first = _report(probed=5)
    moved = _report(probed=6)
    assert _discovery_progress_signature(moved, run_matches=True) != _discovery_progress_signature(
        first, run_matches=True
    )


def test_signature_is_empty_for_another_run() -> None:
    assert _discovery_progress_signature(_report(probed=5), run_matches=False) == ""
