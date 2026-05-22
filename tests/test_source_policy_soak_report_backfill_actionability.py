import gzip
import json
from pathlib import Path

from scripts import source_policy_soak_report as soak


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_json_gz(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, mode="wt", encoding="utf-8") as handle:
        json.dump(payload, handle)


def test_provider_coverage_backfill_prefers_real_static_over_provider_shaped_static(
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
                "id": "smartrecruiters:legacy_static:cdprojektred",
                "name": "CDPR Provider-Shaped Static",
                "adapter": "static",
                "listing_url": "https://cdprojektred.com/en/jobs",
            },
            {
                "id": "static:cdpr",
                "name": "CDPR Static",
                "adapter": "static",
                "listing_url": "https://cdprojektred.com/en/jobs",
            },
        ],
    )
    _write_json(data_dir / "source-registry-pending.json", [])

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["providerCoverageLinkBackfill"]
    review = section["reviewCandidates"][0]

    assert section["candidateLinkCount"] == 1
    assert section["blockedCount"] == 0
    assert section["resolvedByRegistryStaticCount"] == 1
    assert review["selectedStaticSourceId"] == "static:cdpr"
    assert review["apiEligible"] is True
    assert report["sections"]["providerCoverageNextAction"]["action"] == "review_one_migration_link"


def test_provider_shaped_self_link_is_non_actionable_and_not_next_action(
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
    next_action = report["sections"]["providerCoverageNextAction"]

    assert blocked["actionability"] == "non_actionable"
    assert section["actionableBlockedCount"] == 0
    assert section["nonActionableBlockedCount"] == 1
    assert next_action["action"] == "none"
    assert next_action["evidenceCounts"]["actionableBlockedLinkCandidateCount"] == 0


def test_ambiguous_static_match_remains_actionable_when_registry_backed(
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

    section = soak.build_soak_report(data_dir)["sections"]["providerCoverageLinkBackfill"]

    assert section["actionableBlockedCount"] == 2
    assert section["nonActionableBlockedCount"] == 0


def test_provider_coverage_backfill_reads_lean_registry_metadata_links(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json_gz(
        data_dir / "source-registry-active.json.gz",
        [
            {
                "id": "greenhouse:slug:linked",
                "name": "Linked Provider",
                "adapter": "greenhouse",
            }
        ],
    )
    _write_json_gz(
        data_dir / "source-registry-metadata.json.gz",
        {
            "greenhouse:slug:linked": {
                "slug": "linked",
                "migrationSourceIdentity": "static:linked",
                "migrationLinkedBy": "admin_provider_link_backfill",
            }
        },
    )
    _write_json(data_dir / "source-registry-pending.json", [])

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["providerCoverageLinkBackfill"]

    assert section["alreadyLinkedCount"] == 1
    assert section["candidateLinkCount"] == 0
    assert section["links"][0]["recommendedAction"] == "already_linked"
    assert section["links"][0]["staticSourceId"] == "static:linked"


def test_provider_coverage_backfill_dedupes_duplicate_advisory_links() -> None:
    advisory = {
        "id": "static:ubisoft",
        "name": "Ubisoft Static",
        "adapter": "static",
        "listing_url": "https://www.ubisoft.com/en-us/company/careers/",
        "recommendedAction": "already_covered_by_provider",
        "existingProviderSourceId": "smartrecruiters:company_id:ubisoft2",
        "migrationSourceIdentity": "static:ubisoft",
    }

    section, _gates = soak._provider_coverage_link_backfill_section(
        active_rows=[
            {
                "id": "smartrecruiters:company_id:ubisoft2",
                "name": "Ubisoft Provider",
                "adapter": "smartrecruiters",
                "company_id": "ubisoft2",
            },
            {
                "id": "static:ubisoft",
                "name": "Ubisoft Static",
                "adapter": "static",
                "listing_url": "https://www.ubisoft.com/en-us/company/careers/",
            },
        ],
        pending_rows=[],
        discovery_candidates=[advisory, dict(advisory)],
        source_state_rows={},
    )

    assert section["candidateLinkCount"] == 1
    assert section["highConfidenceLinkCount"] == 1
    assert section["mediumConfidenceLinkCount"] == 0
    assert len(section["reviewCandidates"]) == 1
    assert section["reviewCandidates"][0]["selectedStaticSourceId"] == "static:ubisoft"


def test_provider_coverage_backfill_withholds_api_payload_without_registry_static() -> None:
    advisory = {
        "id": "static:ubisoft",
        "name": "Ubisoft Static",
        "adapter": "static",
        "listing_url": "https://www.ubisoft.com/en-us/company/careers/",
        "recommendedAction": "already_covered_by_provider",
        "existingProviderSourceId": "smartrecruiters:company_id:ubisoft2",
        "migrationSourceIdentity": "static:ubisoft",
    }

    section, _gates = soak._provider_coverage_link_backfill_section(
        active_rows=[
            {
                "id": "smartrecruiters:company_id:ubisoft2",
                "name": "Ubisoft Provider",
                "adapter": "smartrecruiters",
                "company_id": "ubisoft2",
            }
        ],
        pending_rows=[],
        discovery_candidates=[advisory],
        source_state_rows={},
    )

    assert section["candidateLinkCount"] == 1
    assert section["mediumConfidenceLinkCount"] == 1
    assert section["blockedCandidates"] == []
    assert len(section["reviewCandidates"]) == 1
    assert section["reviewCandidates"][0]["apiEligible"] is False
    assert section["reviewCandidates"][0]["recommendedApiPayload"] == {}
