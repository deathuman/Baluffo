import json
from pathlib import Path

from scripts import source_policy_soak_report as soak


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _gate_ids(report: dict[str, object]) -> set[str]:
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
    assert "provider_coverage_link_high_confidence_candidates" in _gate_ids(report)


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
    assert link["recommendedAction"] == "backfill_migration_identity_candidate"
    assert link["confidence"] >= 0.75
    assert link["staticSourceId"] == "static:studio"


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
    assert link["providerSourceId"] == "custom-provider-row"
    assert link["staticSourceId"] == "generic-static:studio"


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
    assert section["blockerCounts"]["company_name_only_ignored"] == 1
    assert section["blockerExamples"][0]["blocker"] == "company_name_only_ignored"


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
    assert section["blockerCounts"]["ambiguous_static_match"] == 2
    assert {row["recommendedAction"] for row in section["links"]} == {"ambiguous_static_match"}
    assert "provider_coverage_link_ambiguous_static_match" in _gate_ids(report)


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
    assert "Advisory only" in markdown
    assert report["sections"]["providerCoverageLinkBackfill"]["blockerCounts"] == {}
    assert (data_dir / "source-registry-active.json").read_text(encoding="utf-8") == before_active
