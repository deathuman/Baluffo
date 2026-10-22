import pytest

from src import admin_bridge

pytestmark = pytest.mark.usefixtures("admin_bridge_entrypoint_root")


def test_summarize_discovery_report_keeps_browser_rescued_split() -> None:
    summary, status = admin_bridge.summarize_discovery_report(
        {
            "startedAt": "2026-03-01T00:00:00+00:00",
            "finishedAt": "2026-03-01T00:01:00+00:00",
            "summary": {
                "queuedCandidateCount": 3,
                "failedProbeCount": 4,
                "failuresResolvedByBrowser": 2,
                "probedCandidateCount": 40,
            },
            "candidates": [],
            "failures": [],
        }
    )

    assert int(summary.get("failedProbeCount") or 0) == 4
    assert int(summary.get("failuresResolvedByBrowser") or 0) == 2
    assert status == "warning"
