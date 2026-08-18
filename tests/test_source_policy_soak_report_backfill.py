import json
from pathlib import Path
from typing import Any

from scripts import source_policy_soak_report as soak


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _gate_ids(report: dict[str, Any]) -> set[str]:
    return {str(row.get("id")) for row in report.get("qualityGates", []) if isinstance(row, dict)}


def test_provider_coverage_backfill_exact_redundant_rule_candidate(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "source-registry-active.json",
        [
            {
                "id": "smartrecruiters:company_id:cdprojektred",
                "name": "CDPR Provider",
                "adapter": "smartrecruiters",
                "company_id": "CDPROJEKTRED",
            },
            {
                "id": "static:cdpr",
                "name": "CDPR Static",
                "adapter": "static",
                "listing_url": "https://www.cdprojektred.com/en/jobs",
            },
        ],
    )
    _write_json(data_dir / "source-registry-pending.json", [])

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["providerCoverageLinkBackfill"]
    link = section["links"][0]

    assert section["candidateLinkCount"] == 1
    assert section["highConfidenceLinkCount"] == 1
    assert link["recommendedAction"] == "backfill_migration_identity_candidate"
    assert link["confidence"] >= 0.9
    assert link["staticSourceId"] == "static:cdpr"
    next_action = report["sections"]["providerCoverageNextAction"]
    assert next_action["action"] == "review_one_migration_link"
    assert next_action["priority"] == 4
    assert next_action["requiresHumanApproval"] is True
    assert next_action["evidenceCounts"]["apiEligibleReviewCandidateCount"] == 1
    assert "provider_coverage_link_high_confidence_candidates" in _gate_ids(report)


def test_provider_coverage_backfill_nextlevel_jazzhr_requires_exact_board_url(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "source-registry-active.json",
        [
            {
                "id": "jazzhr:board_url:https://lostboysinteractive.applytojob.com/apply",
                "name": "Lost Boys Interactive (JazzHR)",
                "adapter": "jazzhr",
                "board_url": "https://lostboysinteractive.applytojob.com/apply",
            },
            {
                "id": "jazzhr:board_url:https://nextlevelgames.applytojob.com/apply",
                "name": "Next Level Games (JazzHR)",
                "adapter": "jazzhr",
                "board_url": "https://nextlevelgames.applytojob.com/apply",
            },
            {
                "id": "static:nextlevel",
                "name": "Next Level Games (Sheet)",
                "adapter": "static",
                "listing_url": (
                    "https://nextlevelgames.com/"
                    "jobs-at-next-level-games-subsidiary-of-nintendo-co-ltd/"
                ),
            },
        ],
    )
    _write_json(data_dir / "source-registry-pending.json", [])

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["providerCoverageLinkBackfill"]

    assert section["candidateLinkCount"] == 1
    assert section["highConfidenceLinkCount"] == 1
    assert [row["providerSourceName"] for row in section["reviewCandidates"]] == [
        "Next Level Games (JazzHR)"
    ]
    assert section["reviewCandidates"][0]["selectedStaticSourceId"] == "static:nextlevel"
    assert all(
        row.get("providerSourceName") != "Lost Boys Interactive (JazzHR)"
        or row.get("staticSourceId") != "static:nextlevel"
        for row in section["links"]
    )


def test_provider_coverage_backfill_blocks_duplicate_review_static_targets(
    tmp_path: Path, monkeypatch
) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "source-registry-active.json",
        [
            {
                "id": "smartrecruiters:company_id:alpha",
                "name": "Alpha Provider",
                "adapter": "smartrecruiters",
                "company_id": "alpha",
            },
            {
                "id": "smartrecruiters:company_id:beta",
                "name": "Beta Provider",
                "adapter": "smartrecruiters",
                "company_id": "beta",
            },
            {
                "id": "static:shared",
                "name": "Shared Static",
                "adapter": "static",
                "listing_url": "https://shared.example/jobs",
            },
        ],
    )
    _write_json(data_dir / "source-registry-pending.json", [])
    monkeypatch.setattr(
        soak,
        "REDUNDANT_STATIC_IF_PROVIDER",
        [
            {
                "hosts": ["shared.example"],
                "adapter": "smartrecruiters",
                "provider_id_field": "company_id",
                "provider_id_value": "alpha",
            },
            {
                "hosts": ["shared.example"],
                "adapter": "smartrecruiters",
                "provider_id_field": "company_id",
                "provider_id_value": "beta",
            },
        ],
    )

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["providerCoverageLinkBackfill"]

    assert section["candidateLinkCount"] == 2
    assert section["blockedCount"] == 2
    assert section["highConfidenceLinkCount"] == 0
    assert section["reviewCandidates"] == []
    assert section["blockedReasonCounts"]["static_link_target_collision"] == 2
    assert {row["providerSourceId"] for row in section["blockedCandidates"]} == {
        "smartrecruiters:company_id:alpha",
        "smartrecruiters:company_id:beta",
    }


def test_provider_coverage_backfill_duplicate_advisory_candidate(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "source-registry-active.json",
        [{"name": "Static Studio Provider", "adapter": "greenhouse", "slug": "staticstudio"}],
    )
    _write_json(data_dir / "source-registry-pending.json", [])
    _write_json(
        data_dir / "source-discovery-candidates.json",
        [
            {
                "sourceIdentity": "static:studio",
                "name": "Static Studio",
                "adapter": "static",
                "currentAdapter": "static",
                "currentUrl": "https://studio.example/careers",
                "detectedProviderFamily": "greenhouse",
                "detectedProviderId": "staticstudio",
                "recommendedAction": "already_covered_by_provider",
                "duplicateOfActiveSource": True,
            }
        ],
    )

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["providerCoverageLinkBackfill"]
    link = section["links"][0]

    assert section["candidateLinkCount"] == 1
    assert section["mediumConfidenceLinkCount"] == 1
    assert link["recommendedAction"] == "needs_review"
    assert link["confidence"] >= 0.75
    assert link["staticSourceId"] == "static:studio"


def test_provider_coverage_backfill_blocks_provider_shaped_self_link(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "source-registry-active.json",
        [
            {
                "id": "workable:account:selfstudio",
                "name": "Self Studio Provider",
                "adapter": "workable",
                "account": "selfstudio",
            }
        ],
    )
    _write_json(data_dir / "source-registry-pending.json", [])
    _write_json(
        data_dir / "source-discovery-candidates.json",
        [
            {
                "sourceIdentity": "workable:account:selfstudio",
                "providerStagingSourceIdentity": "workable:account:selfstudio",
                "name": "Self Studio Provider",
                "adapter": "workable",
                "currentAdapter": "workable",
                "currentUrl": "https://apply.workable.com/selfstudio/",
                "detectedProviderFamily": "workable",
                "detectedProviderId": "selfstudio",
                "recommendedAction": "already_covered_by_provider",
                "duplicateOfActiveSource": True,
            }
        ],
    )

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["providerCoverageLinkBackfill"]
    blocked = section["blockedCandidates"][0]

    assert section["candidateLinkCount"] == 1
    assert section["blockedCount"] == 1
    assert section["mediumConfidenceLinkCount"] == 0
    assert section["reviewCandidates"] == []
    assert blocked["apiEligible"] is False
    assert blocked["providerSourceId"] == "workable:account:selfstudio"
    assert blocked["staticSourceId"] == "workable:account:selfstudio"
    assert blocked["blockers"] == ["provider_shaped_self_link"]


def test_provider_coverage_backfill_advisory_matches_custom_provider_id_and_generic_static(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "source-registry-active.json",
        [
            {
                "id": "custom-provider-row",
                "name": "Generic Static Provider",
                "adapter": "greenhouse",
                "slug": "genericstudio",
            }
        ],
    )
    _write_json(data_dir / "source-registry-pending.json", [])
    _write_json(
        data_dir / "source-discovery-candidates.json",
        [
            {
                "sourceIdentity": "generic-static:studio",
                "name": "Generic Static Studio",
                "adapter": "generic_static",
                "currentAdapter": "generic_static",
                "discoveryStage": "generic_static",
                "currentUrl": "https://generic.example/jobs",
                "detectedProviderFamily": "greenhouse",
                "detectedProviderId": "genericstudio",
                "recommendedAction": "already_covered_by_provider",
                "duplicateOfActiveSource": True,
            }
        ],
    )

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["providerCoverageLinkBackfill"]
    link = section["links"][0]

    assert section["candidateLinkCount"] == 1
    assert section["mediumConfidenceLinkCount"] == 1
    assert link["recommendedAction"] == "needs_review"
    assert link["providerSourceId"] == "custom-provider-row"
    assert link["staticSourceId"] == "generic-static:studio"


def test_provider_coverage_backfill_exact_advisory_beats_company_name_only_alternative(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "source-registry-active.json",
        [
            {
                "id": "greenhouse:slug:exactstudio",
                "name": "Exact Studio",
                "adapter": "greenhouse",
                "slug": "exactstudio",
            },
            {
                "id": "static:name-only",
                "name": "Exact Studio",
                "adapter": "static",
                "listing_url": "https://name-only.example/jobs",
            },
        ],
    )
    _write_json(data_dir / "source-registry-pending.json", [])
    _write_json(
        data_dir / "source-discovery-candidates.json",
        [
            {
                "sourceIdentity": "static:exact",
                "name": "Exact Careers",
                "adapter": "static",
                "currentAdapter": "static",
                "currentUrl": "https://exact.example/jobs",
                "detectedProviderFamily": "greenhouse",
                "detectedProviderId": "exactstudio",
                "recommendedAction": "already_covered_by_provider",
                "duplicateOfActiveSource": True,
            }
        ],
    )

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["providerCoverageLinkBackfill"]
    candidate = next(row for row in section["links"] if row["recommendedAction"] == "needs_review")

    assert section["candidateLinkCount"] == 1
    assert section["mediumConfidenceLinkCount"] == 1
    assert section["companyNameOnlyIgnoredCount"] == 1
    assert section["blockerCounts"]["company_name_only_ignored"] == 1
    assert candidate["staticSourceId"] == "static:exact"


def test_provider_coverage_backfill_company_name_only_is_not_candidate(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "source-registry-active.json",
        [
            {
                "id": "greenhouse:slug:nameonly",
                "name": "Name Only Studio",
                "adapter": "greenhouse",
                "slug": "nameonly",
            },
            {
                "id": "static:nameonly",
                "name": "Name Only Studio",
                "adapter": "static",
                "listing_url": "https://name-only.example/jobs",
            },
        ],
    )
    _write_json(data_dir / "source-registry-pending.json", [])

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["providerCoverageLinkBackfill"]

    assert section["candidateLinkCount"] == 0
    assert section["companyNameOnlyIgnoredCount"] == 1
    assert section["blockerCounts"]["company_name_only_ignored"] == 1
    assert section["blockerExamples"][0]["blocker"] == "company_name_only_ignored"


def test_provider_coverage_backfill_exact_rule_beats_weak_host_only_alternative(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "source-registry-active.json",
        [
            {
                "id": "smartrecruiters:company_id:cdprojektred",
                "name": "CDPR Provider",
                "adapter": "smartrecruiters",
                "company_id": "CDPROJEKTRED",
                "base_url": "https://weak.example/jobs",
            },
            {
                "id": "static:cdpr",
                "name": "CDPR Static",
                "adapter": "static",
                "listing_url": "https://cdprojektred.com/jobs",
            },
            {
                "id": "static:weak-host",
                "name": "Weak Host Static",
                "adapter": "static",
                "listing_url": "https://weak.example/jobs",
            },
        ],
    )
    _write_json(data_dir / "source-registry-pending.json", [])

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["providerCoverageLinkBackfill"]
    candidate = next(
        row
        for row in section["links"]
        if row["recommendedAction"] == "backfill_migration_identity_candidate"
    )

    assert section["candidateLinkCount"] == 1
    assert section["highConfidenceLinkCount"] == 1
    assert section["hostOnlyMatchCount"] == 1
    assert section["blockerCounts"]["host_only_match"] == 1
    assert candidate["staticSourceId"] == "static:cdpr"


def test_provider_coverage_backfill_ambiguous_static_matches_warn(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "source-registry-active.json",
        [
            {
                "id": "smartrecruiters:company_id:cdprojektred",
                "name": "CDPR Provider",
                "adapter": "smartrecruiters",
                "company_id": "CDPROJEKTRED",
            },
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
    assert section["ambiguousProviderCount"] == 1
    assert section["ambiguousStaticCandidateCount"] == 2
    assert section["ambiguityGroups"][0]["candidateStaticCount"] == 2
    assert {row["staticSourceId"] for row in section["ambiguityGroups"][0]["candidateStatics"]} == {
        "static:cdpr-one",
        "static:cdpr-two",
    }
    assert section["blockerCounts"]["ambiguous_static_match"] == 2
    assert {row["recommendedAction"] for row in section["links"]} == {"ambiguous_static_match"}
    next_action = report["sections"]["providerCoverageNextAction"]
    assert next_action["action"] == "resolve_link_ambiguity"
    assert next_action["priority"] == 6
    assert next_action["requiresHumanApproval"] is False
    assert next_action["evidenceCounts"]["apiEligibleReviewCandidateCount"] == 0
    assert "ambiguous_static_match" in next_action["blockedBy"]
    assert "provider_coverage_link_ambiguous_static_match" in _gate_ids(report)
    assert "provider_coverage_link_unresolved_ambiguity_examples" in _gate_ids(report)


def test_provider_coverage_backfill_already_linked_count_is_separate(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "source-registry-active.json",
        [
            {
                "id": "greenhouse:slug:linked",
                "name": "Linked Provider",
                "adapter": "greenhouse",
                "slug": "linked",
                "migrationSourceIdentity": "static:linked",
            }
        ],
    )
    _write_json(data_dir / "source-registry-pending.json", [])

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["providerCoverageLinkBackfill"]

    assert section["alreadyLinkedCount"] == 1
    assert section["candidateLinkCount"] == 0
    assert section["links"][0]["recommendedAction"] == "already_linked"


def test_provider_coverage_backfill_markdown_and_registry_read_only(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    out_dir = tmp_path / "_out"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    active_payload = [
        {
            "id": "smartrecruiters:company_id:cdprojektred",
            "name": "CDPR Provider",
            "adapter": "smartrecruiters",
            "company_id": "CDPROJEKTRED",
        },
        {
            "id": "static:cdpr",
            "name": "CDPR Static",
            "adapter": "static",
            "listing_url": "https://cdprojektred.com/jobs",
        },
    ]
    _write_json(data_dir / "source-registry-active.json", active_payload)
    _write_json(data_dir / "source-registry-pending.json", [])
    before_active = (data_dir / "source-registry-active.json").read_text(encoding="utf-8")

    report = soak.build_soak_report(data_dir)
    outputs = soak.write_soak_report(report, out_dir)
    markdown = Path(outputs["markdown"]).read_text(encoding="utf-8")

    assert "Provider Coverage Link Backfill" in markdown
    assert "Ambiguity groups:" in markdown
    assert "Resolved examples:" in markdown
    assert "Advisory only" in markdown
    assert report["sections"]["providerCoverageLinkBackfill"]["blockerCounts"] == {}
    assert (data_dir / "source-registry-active.json").read_text(encoding="utf-8") == before_active
