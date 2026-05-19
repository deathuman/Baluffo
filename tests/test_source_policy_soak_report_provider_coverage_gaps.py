import json
from pathlib import Path

from scripts import source_policy_soak_report as soak


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_provider_coverage_gaps_summarize_first_slice_buckets(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_json(
        data_dir / "jobs-fetch-report.json",
        {
            "sources": [
                {
                    "id": "provider:fetched",
                    "sourceIdentity": "provider:fetched",
                    "name": "Fetched Provider",
                    "adapter": "greenhouse",
                    "status": "ok",
                    "keptCount": 0,
                    "migrationSourceIdentity": "static:fetched",
                },
                {
                    "id": "provider:no-identity",
                    "sourceIdentity": "provider:no-identity",
                    "name": "No Identity Provider",
                    "adapter": "greenhouse",
                    "status": "ok",
                    "keptCount": 3,
                },
                {
                    "name": "greenhouse_boards",
                    "adapter": "greenhouse",
                    "status": "ok",
                    "keptCount": 99,
                },
            ],
        },
    )
    _write_json(
        data_dir / "jobs-source-state.json",
        {
            "sources": {
                "Fetched Provider": {
                    "id": "provider:fetched",
                    "sourceIdentity": "provider:fetched",
                    "lastAdapter": "greenhouse",
                    "lastStatus": "ok",
                    "lastKeptCount": 0,
                    "migrationSourceIdentity": "static:fetched",
                    "providerCoverageStatus": "needs_review",
                    "providerCoverageConsecutiveSuccesses": 0,
                },
                "Validated Provider": {
                    "id": "provider:validated",
                    "sourceIdentity": "provider:validated",
                    "lastAdapter": "greenhouse",
                    "lastStatus": "ok",
                    "lastKeptCount": 4,
                    "migrationSourceIdentity": "static:linked",
                    "providerCoverageStatus": "validated_provider",
                    "providerCoverageConsecutiveSuccesses": 1,
                    "providerCoverageLatestKeptCount": 4,
                },
                "lever_sources": {
                    "lastAdapter": "lever",
                    "lastStatus": "ok",
                    "lastKeptCount": 100,
                },
            }
        },
    )
    _write_json(
        data_dir / "source-registry-active.json",
        [
            {
                "id": "provider:validated",
                "name": "Validated Provider",
                "adapter": "greenhouse",
                "slug": "validated",
                "migrationSourceIdentity": "static:linked",
            },
            {
                "id": "static:linked",
                "name": "Linked Static",
                "adapter": "static",
                "pages": ["https://linked.example/jobs"],
            },
        ],
    )
    _write_json(
        data_dir / "source-registry-pending.json",
        [
            {
                "id": "provider:not-fetched",
                "name": "Not Fetched Provider",
                "adapter": "greenhouse",
                "pendingReason": "provider_migration_candidate",
                "migrationSourceIdentity": "static:not-fetched",
            },
            {
                "id": "provider:fetched",
                "name": "Fetched Provider",
                "adapter": "greenhouse",
                "pendingReason": "provider_migration_candidate",
                "migrationSourceIdentity": "static:fetched",
            },
        ],
    )
    _write_json(data_dir / "source-registry-rejected.json", [])
    _write_json(
        data_dir / "source-discovery-candidates.json",
        [
            {
                "id": "static:icims",
                "name": "iCIMS Static",
                "adapter": "static",
                "listing_url": "https://careers-example.icims.com/jobs/search",
                "jobsFound": 1,
            }
        ],
    )

    report = soak.build_soak_report(data_dir)
    gaps = report["sections"]["providerCoverageGaps"]

    assert gaps["bucketCounts"]["unsupportedProviderDetected"] == 1
    assert gaps["bucketCounts"]["stagedProviderNotFetched"] == 1
    assert gaps["bucketCounts"]["fetchedButNotValidated"] == 1
    assert gaps["bucketCounts"]["validatedProviderMissingMigrationSourceIdentity"] == 1
    assert gaps["bucketCounts"]["staticStillActiveDespiteValidatedProvider"] == 1
    assert gaps["totalGapCount"] == 5
    assert gaps["unsupportedProviderDetected"]["examples"][0]["detectedProviderFamily"] == "icims"
    assert gaps["stagedProviderNotFetched"]["examples"][0]["blockerReason"] == "not_fetched"
    assert gaps["fetchedButNotValidated"]["examples"][0]["providerCoverageStatus"] == "needs_review"
    assert (
        gaps["validatedProviderMissingMigrationSourceIdentity"]["examples"][0]["providerSourceName"]
        == "No Identity Provider"
    )
    assert (
        gaps["staticStillActiveDespiteValidatedProvider"]["examples"][0]["registryBucket"]
        == "active"
    )

    markdown = soak.render_markdown_report(report)

    assert "## Provider Coverage Gaps" in markdown
    assert "unsupportedProviderDetected" in markdown


def test_provider_coverage_next_action_plans_unsupported_provider_family_last(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(data_dir / "source-registry-active.json", [])
    _write_json(data_dir / "source-registry-pending.json", [])
    _write_json(data_dir / "source-registry-rejected.json", [])
    _write_json(
        data_dir / "source-discovery-candidates.json",
        [
            {
                "id": "static:icims",
                "name": "iCIMS Static",
                "adapter": "static",
                "listing_url": "https://careers-example.icims.com/jobs/search",
                "jobsFound": 1,
            }
        ],
    )

    report = soak.build_soak_report(data_dir)
    next_action = report["sections"]["providerCoverageNextAction"]

    assert next_action["action"] == "plan_unsupported_provider_family"
    assert next_action["priority"] == 5
    assert next_action["evidenceCounts"]["unsupportedProviderDetectedCount"] == 1
    assert next_action["blockedBy"] == ["unsupported_provider_family"]
