"""Registry conflicts report which job-evidence path the refresh used."""

from __future__ import annotations

from pathlib import Path

from src.bridge.registry_conflicts import summarize_registry_conflicts_payload
from src.bridge.registry_conflicts_summary import load_registry_conflicts_summary_payload


def test_summary_preserves_the_job_evidence_block() -> None:
    summary = summarize_registry_conflicts_payload(
        {
            "summary": {"conflictCount": 0},
            "warnings": [],
            "jobEvidence": {
                "source": "sqlite_source_index",
                "rowCount": 5,
                "truncated": False,
            },
            "conflicts": [],
        }
    )

    assert summary["jobEvidence"] == {
        "source": "sqlite_source_index",
        "rowCount": 5,
        "truncated": False,
    }


def test_summary_reports_unavailable_evidence_before_the_first_refresh(
    tmp_path: Path,
) -> None:
    payload = load_registry_conflicts_summary_payload(
        registry_summary={"activeCount": 1},
        source_state_path=tmp_path / "jobs-source-state.json",
    )

    assert payload["summaryStatus"] == "pending"
    assert payload["jobEvidence"] == {
        "source": "unavailable",
        "rowCount": 0,
        "truncated": False,
    }
