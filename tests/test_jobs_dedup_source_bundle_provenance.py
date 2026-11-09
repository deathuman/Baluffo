from __future__ import annotations

from typing import Any, cast

from src.jobs.reporting_dedup_evidence import build_dedup_evidence
from tests.helpers.jobs_rows import dedup_row

_SOURCE_LINK = "https://example.com/jobs/1"
_TARGET_LINK = "https://example.com/jobs/2"


def _entry(source_job_id: str, link: str) -> dict[str, str]:
    return {
        "source": "google_sheets_1er2oaxo",
        "sourceJobId": source_job_id,
        "jobLink": link,
        "adapter": "google_sheets",
    }


def _source_row(**overrides: Any) -> dict[str, Any]:
    payload = dedup_row(
        id="job-1",
        dedupKey="key-1",
        title="Level Designer",
        company="Studio One",
        jobLink=_SOURCE_LINK,
        sourceBundleCount=1,
        sourceBundle=[_entry("sheet-2", _TARGET_LINK)],
    )
    payload.update(overrides)
    return cast(dict[str, Any], payload)


def _target_row(row_id: str = "job-2", **overrides: Any) -> dict[str, Any]:
    payload = dedup_row(
        id=row_id,
        dedupKey=f"key-{row_id}",
        title="Senior Engineer",
        company="Studio Two",
        jobLink=_TARGET_LINK,
        sourceBundleCount=0,
        sourceBundle=[],
    )
    payload.update(overrides)
    return cast(dict[str, Any], payload)


def test_dedup_evidence_reports_carried_source_bundle_provenance_mismatch() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [_source_row(), _target_row()],
        seeded_from_existing_output=True,
    )

    assert evidence["carriedSourceBundleProvenanceMismatchCount"] == 1
    assert evidence["carriedSourceBundleProvenanceMismatchMovableCount"] == 1
    assert evidence["carriedSourceBundleProvenanceMismatchAmbiguousCount"] == 0
    example = evidence["carriedSourceBundleProvenanceMismatchExamples"][0]
    assert example["status"] == "movable"
    assert example["sourceRowId"] == "job-1"
    assert example["targetRowId"] == "job-2"
    assert example["entrySourceJobId"] == "sheet-2"

    recommendation = evidence["carriedBundleReconciliationRecommendation"]
    assert recommendation["recommendedAction"] == "rebuild_carried_source_bundle_metadata"
    assert recommendation["destructiveActionAllowed"] is False
    assert recommendation["sourceBundleProvenanceMismatchCount"] == 1
    assert recommendation["sourceBundleProvenanceMismatchAmbiguousCount"] == 0

    gate = evidence["dedupAuditGate"]
    assert gate["status"] == "warning"
    assert gate["carriedSourceBundleProvenanceMismatchCount"] == 1
    assert gate["carriedSourceBundleProvenanceMismatchAmbiguousCount"] == 0
    assert "carried_source_bundle_provenance_mismatch_present" in gate["warnings"]
    detail = next(
        detail
        for detail in gate["warningDetails"]
        if detail["key"] == "carried_source_bundle_provenance_mismatch_present"
    )
    assert detail["count"] == 1
    assert "source_bundle_reconcile.py" in detail["nextAction"]
    assert detail["examples"][0]["title"] == "Level Designer"


def test_dedup_evidence_reports_ambiguous_provenance_targets_without_moving() -> None:
    rows = [
        _source_row(sourceBundle=[_entry("sheet-9", "https://example.com/jobs/9")]),
        _target_row("job-2", jobLink="https://example.com/jobs/9"),
        _target_row("job-3", jobLink="https://example.com/jobs/9"),
    ]

    evidence = build_dedup_evidence({"mergedCount": 0}, rows, seeded_from_existing_output=True)

    assert evidence["carriedSourceBundleProvenanceMismatchCount"] == 1
    assert evidence["carriedSourceBundleProvenanceMismatchMovableCount"] == 0
    assert evidence["carriedSourceBundleProvenanceMismatchAmbiguousCount"] == 1
    example = evidence["carriedSourceBundleProvenanceMismatchExamples"][0]
    assert example["status"] == "ambiguous"
    assert example["candidateRowIds"] == ["job-2", "job-3"]
    assert (
        evidence["carriedBundleReconciliationRecommendation"][
            "sourceBundleProvenanceMismatchAmbiguousCount"
        ]
        == 1
    )

    gate = evidence["dedupAuditGate"]
    assert gate["carriedSourceBundleProvenanceMismatchAmbiguousCount"] == 1
    detail = next(
        detail
        for detail in gate["warningDetails"]
        if detail["key"] == "carried_source_bundle_provenance_mismatch_present"
    )
    assert detail["counts"] == {"mismatches": 1, "ambiguous": 1}


def test_dedup_evidence_omits_recommendation_when_bundles_match_their_rows() -> None:
    evidence = build_dedup_evidence(
        {"mergedCount": 0},
        [
            dedup_row(
                sourceBundleCount=1,
                sourceBundle=[_entry("gh-1", "https://example.com/jobs/1")],
            )
        ],
        seeded_from_existing_output=True,
    )

    assert evidence["carriedSourceBundleProvenanceMismatchCount"] == 0
    assert evidence["carriedSourceBundleProvenanceMismatchMovableCount"] == 0
    assert evidence["carriedSourceBundleProvenanceMismatchAmbiguousCount"] == 0
    assert evidence["carriedSourceBundleProvenanceMismatchExamples"] == []
    assert evidence["carriedBundleReconciliationRecommendation"] == {}
    assert evidence["dedupAuditGate"]["status"] == "pass"
