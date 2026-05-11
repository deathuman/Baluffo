from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path

import pytest

MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "tools"
    / "measurements"
    / "pipeline"
    / "dedup_pressure_report.py"
)
SPEC = importlib.util.spec_from_file_location("dedup_pressure_report", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
dedup_pressure_report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(dedup_pressure_report)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _write_fetch_report(path: Path) -> None:
    _write_json(
        path,
        {
            "startedAt": "2026-05-11T08:00:00+00:00",
            "finishedAt": "2026-05-11T08:30:00+00:00",
            "dedupEvidence": {
                "dedupAuditGate": {
                    "status": "blocked",
                    "lifecycleUxReady": False,
                    "blockers": [
                        "provider_static_disagreement_needs_review",
                        "high_risk_review_queue_causes_need_review",
                    ],
                    "warnings": ["carried_source_bundle_collisions_present"],
                    "googleSheetsRoleBucketUnresolvedCount": 4,
                    "googleSheetsRoleBucketGuardBlockedCount": 12,
                    "googleSheetsRoleBucketHistoricalCount": 1,
                    "providerStaticDisagreementBlockedCount": 3,
                    "currentRunBlockingReviewQueueCount": 7,
                    "currentRunMonitorReviewQueueCount": 5,
                    "currentRunNonPrimaryMergeCounts": {
                        "blocking": 2,
                        "monitor": 3,
                        "secondaryKey": 1,
                        "sparseIdentity": 1,
                        "monitorSecondaryKey": 2,
                        "monitorSparseIdentity": 1,
                    },
                },
                "mergeReasonCounts": {
                    "primaryUrl": 5,
                    "secondaryKey": 1,
                    "sparseIdentity": 1,
                },
                "currentRunMergeGateTierCounts": {
                    "blocking": 2,
                    "monitor": 3,
                    "monitorWeakNonProviderIdentity": 3,
                },
                "currentRunMergeExamplesByReason": {
                    "secondaryKey": [
                        {
                            "mergeReason": "secondary_key",
                            "existingDedupKey": "url:abc",
                            "incomingSource": "static_source::example",
                            "title": "LEGAL",
                            "company": "Sledgehammer Games",
                            "incomingJobLink": "https://example.com/footer-LEGALLinkUrl",
                            "bundleEvidenceOrigin": "current_run",
                            "blocksLifecycle": True,
                            "recommendedReviewAction": "review_current_run_merge",
                            "suspectedCause": "current_run_non_primary_merge",
                        }
                    ],
                    "sparseIdentity": [
                        {
                            "mergeReason": "sparse_identity",
                            "existingDedupKey": "title_company:abc",
                            "incomingSource": "google_sheets::example",
                            "title": "Designer",
                            "company": "Example Studio",
                            "incomingJobLink": "https://jobs.example.com/designer",
                            "bundleEvidenceOrigin": "current_run",
                            "blocksLifecycle": True,
                            "recommendedReviewAction": "review_current_run_merge",
                            "suspectedCause": "google_sheets_role_bucket_needs_review",
                        }
                    ],
                },
                "reviewQueueCauseCounts": {
                    "parser_or_directory_text_pollution": 9,
                    "listing_page_bundle": 2,
                    "provider_static_disagreement": 3,
                    "unknown": 5,
                    "likely_legitimate_multi_role_family": 0,
                },
                "providerStaticDisagreementGateCounts": {
                    "blocked": 3,
                    "warning": 1,
                },
                "providerStaticDisagreementClassificationCounts": {
                    "static_parser_url_variant": 2,
                    "title_company_collision": 1,
                },
                "googleSheetsRoleBucketAudit": {
                    "classificationCounts": {
                        "fixed_by_generic_role_guard": 12,
                        "needs_narrow_dedup_guard": 4,
                    }
                },
                "currentRunBlockingReviewQueueCauseCounts": {
                    "google_sheets_role_bucket_needs_review": 4,
                    "parser_or_directory_text_pollution": 2,
                },
                "currentRunMonitorReviewQueueCauseCounts": {
                    "google_sheets_role_bucket_needs_review": 3,
                    "parser_or_directory_text_pollution": 2,
                },
                "sourceBundleComposition": {
                    "provider": 1,
                    "static": 3,
                    "other": 8,
                },
                "identityShapeCounts": {
                    "many_unique_urls_same_title": 6,
                    "provider_id_backed": 2,
                },
                "identityQualityCounts": {
                    "provider_id_strong": 2,
                    "other_source_id_untrusted": 6,
                },
                "googleSheetsRoleBucketAuditCounts": {
                    "role_family_needs_manual_review": 4,
                    "not_google_sheets_role_bucket": 3,
                },
                "reviewQueue": [
                    {
                        "title": "%HEADER_COMPANY_WEBSITE%",
                        "company": "Lucky VR",
                        "suspectedCause": "parser_or_directory_text_pollution",
                        "recommendedReviewAction": "review_many_urls_same_title",
                        "sourceBundleCount": 16,
                        "bundleEvidenceOrigin": "current_run",
                        "jobLink": "https://example.com/noise",
                    }
                ],
            },
        },
    )


def test_dedup_pressure_report_summarizes_gate_and_top_causes(tmp_path: Path) -> None:
    report_path = tmp_path / "data" / "jobs-fetch-report.json"
    _write_fetch_report(report_path)

    summary = dedup_pressure_report.build_dedup_pressure_summary(
        repo_root=tmp_path, fetch_report_path=report_path, limit=3
    )
    text = dedup_pressure_report.render_text_summary(summary)

    assert summary["dedupAuditGate"]["status"] == "blocked"
    assert summary["dedupAuditGate"]["blockerCount"] == 2
    assert summary["providerStatic"]["staticUrlVariantCount"] == 2
    assert summary["googleSheetsRoleBuckets"]["unresolvedCount"] == 4
    assert summary["staticAndParserPressure"]["parserDirectoryCauseCount"] == 11
    assert summary["currentRunNonPrimaryMergeBreakdown"]["totalBlocking"] == 2
    assert summary["currentRunNonPrimaryMergeBreakdown"]["totalMonitor"] == 3
    assert summary["topSuspectedCauses"] == [
        {"cause": "parser_or_directory_text_pollution", "count": 9},
        {"cause": "unknown", "count": 5},
        {"cause": "provider_static_disagreement", "count": 3},
    ]
    assert "Provider/static blocked disagreements: 3" in text
    assert "Current-run monitor review queue: 5" in text
    assert "Current-Run Non-Primary Merge Breakdown" in text
    assert "Sledgehammer Games - LEGAL" in text
    assert "Lucky VR - %HEADER_COMPANY_WEBSITE%" in text


def test_dedup_pressure_report_summarizes_non_primary_merge_breakdown(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "data" / "jobs-fetch-report.json"
    _write_fetch_report(report_path)

    summary = dedup_pressure_report.build_dedup_pressure_summary(
        repo_root=tmp_path, fetch_report_path=report_path, limit=1
    )
    breakdown = summary["currentRunNonPrimaryMergeBreakdown"]

    assert breakdown["reasonCounts"] == {"secondaryKey": 1, "sparseIdentity": 1}
    assert breakdown["monitorReasonCounts"] == {"secondaryKey": 2, "sparseIdentity": 1}
    assert breakdown["currentRunBlockingReviewQueueCauseCounts"] == {
        "google_sheets_role_bucket_needs_review": 4,
        "parser_or_directory_text_pollution": 2,
    }
    assert breakdown["currentRunMonitorReviewQueueCauseCounts"] == {
        "google_sheets_role_bucket_needs_review": 3,
        "parser_or_directory_text_pollution": 2,
    }
    assert breakdown["mergeGateTierCounts"] == {
        "blocking": 2,
        "monitor": 3,
        "monitorWeakNonProviderIdentity": 3,
    }
    assert breakdown["sourceBundleComposition"] == {"provider": 1, "static": 3, "other": 8}
    assert breakdown["identityShapeCounts"] == {
        "many_unique_urls_same_title": 6,
        "provider_id_backed": 2,
    }
    assert breakdown["googleSheetsRoleBucketAuditCounts"] == {
        "role_family_needs_manual_review": 4,
        "not_google_sheets_role_bucket": 3,
    }
    assert breakdown["sampleFamilyCounts"] == {
        "parser_static_noise": 1,
        "needs_manual_review": 1,
    }
    assert breakdown["reasonRows"][0]["reason"] == "secondaryKey"
    assert breakdown["reasonRows"][0]["sampleFamilyCounts"] == {"parser_static_noise": 1}
    assert breakdown["reasonRows"][0]["samples"] == [
        {
            "mergeReason": "secondary_key",
            "title": "LEGAL",
            "company": "Sledgehammer Games",
            "incomingSource": "static_source::example",
            "incomingJobLink": "https://example.com/footer-LEGALLinkUrl",
            "existingDedupKey": "url:abc",
            "bundleEvidenceOrigin": "current_run",
            "blocksLifecycle": True,
            "recommendedReviewAction": "review_current_run_merge",
            "suspectedCause": "current_run_non_primary_merge",
            "sampleFamily": "parser_static_noise",
            "recommendedNextAction": "fix_static_extraction_filter",
        }
    ]


def test_dedup_pressure_report_classifies_provider_like_sparse_samples(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "data" / "jobs-fetch-report.json"
    _write_fetch_report(report_path)
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    sparse_sample = payload["dedupEvidence"]["currentRunMergeExamplesByReason"]["sparseIdentity"][0]
    sparse_sample["incomingSource"] = "gracklehq"
    sparse_sample["incomingJobLink"] = "https://gracklehq.com/rd/376928"
    _write_json(report_path, payload)

    summary = dedup_pressure_report.build_dedup_pressure_summary(
        repo_root=tmp_path, fetch_report_path=report_path, limit=1
    )
    breakdown = summary["currentRunNonPrimaryMergeBreakdown"]

    assert breakdown["sampleFamilyCounts"] == {
        "parser_static_noise": 1,
        "provider_or_job_detail_candidate": 1,
    }
    assert breakdown["reasonRows"][1]["samples"][0]["sampleFamily"] == (
        "provider_or_job_detail_candidate"
    )
    assert breakdown["reasonRows"][1]["samples"][0]["recommendedNextAction"] == (
        "review_provider_identity_or_sparse_key"
    )


def test_dedup_pressure_report_prefers_blocking_examples_by_reason(tmp_path: Path) -> None:
    report_path = tmp_path / "data" / "jobs-fetch-report.json"
    _write_fetch_report(report_path)
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["dedupEvidence"]["currentRunMergeExamplesByReason"]["secondaryKey"] = [
        {
            "mergeReason": "secondary_key",
            "existingDedupKey": "url:monitor",
            "incomingSource": "static_source::example",
            "title": "Monitor Only",
            "company": "Example Studio",
            "incomingJobLink": "https://example.com/jobs/monitor",
            "bundleEvidenceOrigin": "current_run",
            "blocksLifecycle": False,
            "recommendedReviewAction": "monitor",
            "suspectedCause": "weak_non_provider_identity",
        }
    ]
    payload["dedupEvidence"]["currentRunBlockingMergeExamplesByReason"] = {
        "secondaryKey": [
            {
                "mergeReason": "secondary_key",
                "existingDedupKey": "url:blocking",
                "incomingSource": "greenhouse:listing_url:https://boards.greenhouse.io/example",
                "title": "Blocking Provider",
                "company": "Example Studio",
                "incomingJobLink": "https://boards.greenhouse.io/example/jobs/1",
                "bundleEvidenceOrigin": "current_run",
                "blocksLifecycle": True,
                "recommendedReviewAction": "review_current_run_merge",
                "suspectedCause": "current_run_non_primary_merge",
            }
        ]
    }
    _write_json(report_path, payload)

    summary = dedup_pressure_report.build_dedup_pressure_summary(
        repo_root=tmp_path, fetch_report_path=report_path, limit=1
    )
    secondary_row = summary["currentRunNonPrimaryMergeBreakdown"]["reasonRows"][0]

    assert secondary_row["reason"] == "secondaryKey"
    assert secondary_row["samples"][0]["title"] == "Blocking Provider"
    assert secondary_row["samples"][0]["blocksLifecycle"] is True


def test_dedup_pressure_report_picks_latest_fetch_report(tmp_path: Path) -> None:
    older = tmp_path / "data" / "jobs-fetch-report.json"
    newer = tmp_path / "_out" / "runs" / "20260511_090000" / "jobs-fetch-report.json"
    _write_fetch_report(older)
    _write_fetch_report(newer)
    os.utime(older, (1_000_000.0, 1_000_000.0))
    os.utime(newer, (1_000_001.0, 1_000_001.0))

    summary = dedup_pressure_report.build_dedup_pressure_summary(repo_root=tmp_path)

    assert Path(summary["fetchReport"]) == newer.resolve()


def test_dedup_pressure_report_fails_cleanly_without_fetch_report(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="jobs-fetch-report.json"):
        dedup_pressure_report.build_dedup_pressure_summary(repo_root=tmp_path)


def test_dedup_pressure_report_json_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    report_path = tmp_path / "data" / "jobs-fetch-report.json"
    _write_fetch_report(report_path)

    exit_code = dedup_pressure_report.main(
        ["--repo-root", str(tmp_path), "--fetch-report", str(report_path), "--json", "--limit", "1"]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["topSuspectedCauses"] == [
        {"cause": "parser_or_directory_text_pollution", "count": 9}
    ]
