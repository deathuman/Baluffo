import json
from pathlib import Path

from scripts import source_policy_soak_report as soak


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _gate_ids(report: dict[str, object]) -> set[str]:
    return {str(row.get("id")) for row in report.get("qualityGates", []) if isinstance(row, dict)}


def _cdpr_provider() -> dict[str, str]:
    return {
        "id": "smartrecruiters:company_id:cdprojektred",
        "name": "CDPR Provider",
        "adapter": "smartrecruiters",
        "company_id": "CDPROJEKTRED",
    }


def test_source_state_resolves_active_successful_static(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "jobs-source-state.json",
        {
            "sources": {
                "static:cdpr-active": {
                    "lastStatus": "ok",
                    "lastKeptCount": 8,
                    "lastSuccessfulAt": "2026-01-01T00:00:00Z",
                    "lastFetchedAt": "2026-01-01T00:00:00Z",
                    "providerCoverageStatus": "validated_provider",
                    "providerCoverageConsecutiveSuccesses": 2,
                    "providerCoverageLatestKeptCount": 8,
                }
            }
        },
    )
    _write_json(
        data_dir / "source-registry-active.json",
        [
            _cdpr_provider(),
            {
                "id": "static:cdpr-active",
                "name": "CDPR Active Static",
                "adapter": "static",
                "listing_url": "https://cdprojektred.com/jobs",
            },
        ],
    )
    _write_json(
        data_dir / "source-registry-pending.json",
        [
            {
                "id": "static:cdpr-hidden",
                "name": "CDPR Hidden Static",
                "adapter": "static",
                "listing_url": "https://www.cdprojektred.com/en/jobs",
                "hiddenFromDefault": True,
                "duplicateOfSourceId": "static:cdpr-active",
                "pendingReason": "duplicate_family_weaker_variant",
            }
        ],
    )

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["providerCoverageLinkBackfill"]
    candidate = next(row for row in section["links"] if row["recommendedAction"] == "needs_review")

    assert section["candidateLinkCount"] == 1
    assert section["mediumConfidenceLinkCount"] == 1
    assert section["resolvedBySourceStateCount"] == 1
    assert section["unresolvedAmbiguousCount"] == 0
    assert section["ambiguityGroups"] == []
    assert candidate["staticSourceId"] == "static:cdpr-active"
    assert candidate["confidence"] == 0.8
    assert candidate["recommendedAction"] == "needs_review"
    assert "source_state_disambiguation" in candidate["reasons"]
    assert "source_state_kept_jobs" in candidate["evidenceReasons"]
    review = section["reviewCandidates"][0]
    payload = review["recommendedApiPayload"]
    ignored = review["ignoredAlternatives"][0]
    assert review["confidenceTier"] == "medium"
    assert review["apiEligible"] is True
    assert review["whyNotHighConfidence"]
    assert review["resolutionReason"] == "source_state_disambiguation"
    assert review["sourceStateEvidence"]["lastKeptCount"] == 8
    assert review["sourceStateEvidence"]["providerCoverageConsecutiveSuccesses"] == 2
    assert ignored["staticSourceId"] == "static:cdpr-hidden"
    assert ignored["reasonIgnored"] == "resolved_by_source_state"
    assert payload == {
        "action": "apply_migration_identity_link",
        "providerSourceId": "smartrecruiters:company_id:cdprojektred",
        "staticSourceId": "static:cdpr-active",
        "staticSourceName": "CDPR Active Static",
        "confidence": 0.8,
        "reasons": [
            "redundant_static_rule_exact_match",
            "source_state_disambiguation",
        ],
        "recommendationSource": "provider_coverage_link_backfill",
        "recommendedAction": "needs_review",
    }


def test_exact_advisory_identity_resolves_rule_ambiguity(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "jobs-source-state.json",
        {"sources": {"static:cdpr-active": {"lastStatus": "ok", "lastKeptCount": 12}}},
    )
    _write_json(
        data_dir / "source-registry-active.json",
        [
            _cdpr_provider(),
            {
                "id": "static:cdpr-active",
                "name": "CDPR Active Static",
                "adapter": "static",
                "listing_url": "https://cdprojektred.com/jobs",
            },
        ],
    )
    _write_json(data_dir / "source-registry-pending.json", [])
    _write_json(
        data_dir / "source-discovery-candidates.json",
        [
            {
                "sourceIdentity": "static:cdpr-advisory",
                "name": "CDPR Advisory Static",
                "adapter": "static",
                "currentAdapter": "static",
                "currentUrl": "https://www.cdprojektred.com/en/jobs",
                "detectedProviderFamily": "smartrecruiters",
                "detectedProviderId": "CDPROJEKTRED",
                "recommendedAction": "already_covered_by_provider",
                "duplicateOfActiveSource": True,
            }
        ],
    )

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["providerCoverageLinkBackfill"]
    candidate = next(
        row
        for row in section["links"]
        if row["recommendedAction"] == "backfill_migration_identity_candidate"
    )

    assert section["candidateLinkCount"] == 1
    assert section["highConfidenceLinkCount"] == 1
    assert section["resolvedByAdvisoryIdentityCount"] == 0
    assert section["mediumConfidenceLinkCount"] == 0
    assert candidate["staticSourceId"] == "static:cdpr-active"
    assert candidate["confidence"] >= 0.9
    assert candidate["recommendedAction"] == "backfill_migration_identity_candidate"
    assert "redundant_static_rule_exact_match" in candidate["reasons"]
    review = section["reviewCandidates"][0]
    assert review["confidenceTier"] == "high"
    assert review["apiEligible"] is True
    assert review["whyNotHighConfidence"] == ""
    assert review["recommendedApiPayload"]["recommendedAction"] == (
        "backfill_migration_identity_candidate"
    )


def test_multiple_active_successful_statics_remain_ambiguous(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "jobs-source-state.json",
        {
            "sources": {
                "static:cdpr-one": {"lastStatus": "ok", "lastKeptCount": 4},
                "static:cdpr-two": {"lastStatus": "ok", "lastKeptCount": 5},
            }
        },
    )
    _write_json(
        data_dir / "source-registry-active.json",
        [
            _cdpr_provider(),
            {
                "id": "static:cdpr-one",
                "name": "CDPR Static One",
                "adapter": "static",
                "listing_url": "https://cdprojektred.com/jobs",
            },
            {
                "id": "static:cdpr-two",
                "name": "CDPR Static Two",
                "adapter": "static",
                "listing_url": "https://www.cdprojektred.com/en/jobs",
            },
        ],
    )
    _write_json(data_dir / "source-registry-pending.json", [])

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["providerCoverageLinkBackfill"]

    assert section["candidateLinkCount"] == 2
    assert section["blockedCount"] == 2
    assert section["highConfidenceLinkCount"] == 0
    assert section["mediumConfidenceLinkCount"] == 0
    assert section["reviewCandidates"] == []
    assert len(section["blockedCandidates"]) == 2
    assert section["blockedReasonCounts"]["ambiguous_static_match"] == 2
    assert (
        section["blockedExamples"][0]["staticSourceId"]
        == section["blockedCandidates"][0]["staticSourceId"]
    )
    assert section["unresolvedAmbiguousCount"] == 2
    assert section["ambiguityGroups"][0]["candidateStatics"][0]["evidenceScore"] > 0
    assert "provider_coverage_link_unresolved_ambiguity_examples" in _gate_ids(report)


def test_duplicate_pending_does_not_beat_active_canonical(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "jobs-source-state.json",
        {
            "sources": {
                "static:cdpr-active": {
                    "lastStatus": "ok",
                    "lastKeptCount": 1,
                    "providerCoverageStatus": "validated_provider",
                    "providerCoverageConsecutiveSuccesses": 2,
                    "providerCoverageLatestKeptCount": 1,
                },
                "static:cdpr-pending": {
                    "lastStatus": "ok",
                    "lastKeptCount": 20,
                    "providerCoverageStatus": "validated_provider",
                    "providerCoverageConsecutiveSuccesses": 3,
                    "providerCoverageLatestKeptCount": 20,
                },
            }
        },
    )
    _write_json(
        data_dir / "source-registry-active.json",
        [
            _cdpr_provider(),
            {
                "id": "static:cdpr-active",
                "name": "CDPR Active Static",
                "adapter": "static",
                "listing_url": "https://cdprojektred.com/jobs",
            },
        ],
    )
    _write_json(
        data_dir / "source-registry-pending.json",
        [
            {
                "id": "static:cdpr-pending",
                "name": "CDPR Pending Duplicate",
                "adapter": "static",
                "listing_url": "https://www.cdprojektred.com/en/jobs",
                "duplicateOfSourceId": "static:cdpr-active",
                "pendingReason": "duplicate_family_weaker_variant",
            }
        ],
    )

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["providerCoverageLinkBackfill"]
    candidate = next(row for row in section["links"] if row["recommendedAction"] == "needs_review")

    assert section["candidateLinkCount"] == 1
    assert section["mediumConfidenceLinkCount"] == 1
    assert candidate["staticSourceId"] == "static:cdpr-active"
    ignored = [row for row in section["links"] if row["staticSourceId"] == "static:cdpr-pending"][0]
    assert ignored["recommendedAction"] == "insufficient_evidence"
    assert "duplicate_static_row" in ignored["disambiguationBlockers"]


def test_source_state_without_success_threshold_stays_blocked(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "jobs-source-state.json",
        {
            "sources": {
                "static:cdpr-active": {
                    "lastStatus": "ok",
                    "lastKeptCount": 8,
                    "lastSuccessfulAt": "2026-01-01T00:00:00Z",
                    "lastFetchedAt": "2026-01-01T00:00:00Z",
                    "providerCoverageStatus": "validated_provider",
                    "providerCoverageConsecutiveSuccesses": 1,
                    "providerCoverageLatestKeptCount": 8,
                }
            }
        },
    )
    _write_json(
        data_dir / "source-registry-active.json",
        [
            _cdpr_provider(),
            {
                "id": "static:cdpr-active",
                "name": "CDPR Active Static",
                "adapter": "static",
                "listing_url": "https://cdprojektred.com/jobs",
            },
        ],
    )
    _write_json(
        data_dir / "source-registry-pending.json",
        [
            {
                "id": "static:cdpr-hidden",
                "name": "CDPR Hidden Static",
                "adapter": "static",
                "listing_url": "https://www.cdprojektred.com/en/jobs",
                "hiddenFromDefault": True,
                "duplicateOfSourceId": "static:cdpr-active",
                "pendingReason": "duplicate_family_weaker_variant",
            }
        ],
    )

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["providerCoverageLinkBackfill"]

    assert section["candidateLinkCount"] == 2
    assert section["reviewCandidates"] == []
    blocked = next(
        row for row in section["blockedCandidates"] if row["staticSourceId"] == "static:cdpr-active"
    )
    assert "ambiguous_static_match" in blocked["blockers"]
    assert "insufficient_provider_success_history" in blocked["disambiguationBlockers"]


def test_review_candidate_markdown_and_registry_read_only(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    out_dir = tmp_path / "_out"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "jobs-source-state.json",
        {
            "sources": {
                "static:cdpr-active": {
                    "lastStatus": "ok",
                    "lastKeptCount": 3,
                    "providerCoverageStatus": "validated_provider",
                    "providerCoverageConsecutiveSuccesses": 2,
                    "providerCoverageLatestKeptCount": 3,
                }
            }
        },
    )
    active_payload = [
        _cdpr_provider(),
        {
            "id": "static:cdpr-active",
            "name": "CDPR Active Static",
            "adapter": "static",
            "listing_url": "https://cdprojektred.com/jobs",
        },
    ]
    pending_payload = [
        {
            "id": "static:cdpr-pending",
            "name": "CDPR Pending Static",
            "adapter": "static",
            "listing_url": "https://www.cdprojektred.com/en/jobs",
            "duplicateOfSourceId": "static:cdpr-active",
        }
    ]
    _write_json(data_dir / "source-registry-active.json", active_payload)
    _write_json(data_dir / "source-registry-pending.json", pending_payload)
    before_active = (data_dir / "source-registry-active.json").read_text(encoding="utf-8")
    before_pending = (data_dir / "source-registry-pending.json").read_text(encoding="utf-8")

    report = soak.build_soak_report(data_dir)
    outputs = soak.write_soak_report(report, out_dir)
    markdown = Path(outputs["markdown"]).read_text(encoding="utf-8")

    assert "### Review candidates" in markdown
    assert "CDPR Active Static" in markdown
    assert "`True`" in markdown
    assert (data_dir / "source-registry-active.json").read_text(encoding="utf-8") == before_active
    assert (data_dir / "source-registry-pending.json").read_text(encoding="utf-8") == before_pending
