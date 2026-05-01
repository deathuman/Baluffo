import json
from pathlib import Path

from scripts import source_policy_soak_report as soak


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _gate_ids(report: dict[str, object]) -> set[str]:
    return {str(row.get("id")) for row in report.get("qualityGates", []) if isinstance(row, dict)}


def test_provider_migration_activation_counts_advisory_actions(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(data_dir / "source-registry-active.json", [])
    _write_json(data_dir / "source-registry-pending.json", [])
    _write_json(
        data_dir / "source-discovery-candidates.json",
        [
            {
                "id": "static:add",
                "name": "Add Provider",
                "adapter": "static",
                "listing_url": "https://add.example/jobs",
                "detectedProviderUrl": "https://boards.greenhouse.io/addprovider",
                "jobsFound": 3,
                "candidateState": "staged_provider_candidate",
                "createdFromAdvisory": True,
            },
            {
                "id": "static:review",
                "name": "Review Provider",
                "adapter": "static",
                "detectedProviderFamily": "greenhouse",
                "jobsFound": 2,
            },
            {
                "id": "static:unsupported",
                "name": "Unsupported Provider",
                "adapter": "static",
                "listing_url": "https://unsupported.example/jobs",
                "detectedProviderUrl": "https://jobs.jobvite.com/unsupported",
                "jobsFound": 1,
            },
            {
                "id": "static:insufficient",
                "name": "Insufficient Provider Evidence",
                "adapter": "static",
                "listing_url": "https://insufficient.example/jobs",
            },
            {
                "id": "static:probe",
                "name": "Needs Probe",
                "adapter": "static",
                "listing_url": "https://probe.example/jobs",
                "browserFallbackRecommended": True,
            },
            {
                "id": "static:dupe-active",
                "name": "Duplicate Active",
                "adapter": "static",
                "listing_url": "https://dupe-active.example/jobs",
                "detectedProviderUrl": "https://boards.greenhouse.io/dupeactive",
                "duplicateOfActiveSource": True,
            },
            {
                "id": "static:dupe-pending",
                "name": "Duplicate Pending",
                "adapter": "static",
                "listing_url": "https://dupe-pending.example/jobs",
                "detectedProviderUrl": "https://boards.greenhouse.io/dupepending",
                "duplicateOfPendingSource": True,
            },
        ],
    )

    report = soak.build_soak_report(data_dir)
    activation = report["sections"]["providerMigrationActivation"]

    assert activation["advisoryTotalCandidates"] == 7
    assert activation["addProviderSourceCount"] == 3
    assert activation["reviewProviderMigrationCount"] == 1
    assert activation["unsupportedProviderCount"] == 1
    assert activation["insufficientEvidenceCount"] == 1
    assert activation["needsProbeCount"] == 1
    assert activation["duplicateActiveSkippedCount"] == 1
    assert activation["duplicatePendingSkippedCount"] == 1
    assert activation["stagedProviderCandidateCount"] == 1
    assert report["summary"]["addProviderSourceCount"] == 3


def test_provider_migration_pending_candidates_fetch_and_validate_counts(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    _write_json(
        data_dir / "jobs-fetch-report.json",
        {
            "sources": [
                {
                    "id": "provider:fetched",
                    "name": "Fetched Provider",
                    "sourceIdentity": "provider:fetched",
                }
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
                    "providerCoverageStatus": "validated_provider",
                    "migrationSourceIdentity": "static:fetched",
                }
            }
        },
    )
    _write_json(data_dir / "source-registry-active.json", [])
    _write_json(
        data_dir / "source-registry-pending.json",
        [
            {
                "id": "provider:fetched",
                "name": "Fetched Provider",
                "adapter": "greenhouse",
                "pendingReason": "provider_migration_candidate",
                "migrationSourceIdentity": "static:fetched",
            },
            {
                "id": "provider:not-fetched",
                "name": "Not Fetched Provider",
                "adapter": "greenhouse",
                "pendingReason": "provider_migration_candidate",
                "migrationSourceIdentity": "static:not-fetched",
            },
        ],
    )

    report = soak.build_soak_report(data_dir)
    activation = report["sections"]["providerMigrationActivation"]

    assert activation["pendingProviderMigrationCandidateCount"] == 2
    assert activation["providerMigrationCandidatesFetchedCount"] == 1
    assert activation["providerMigrationCandidatesValidatedCount"] == 1
    assert activation["providerMigrationCandidatesNoFetchCount"] == 1
    assert "pending_provider_migration_not_fetched" not in _gate_ids(report)


def test_provider_migration_pending_candidates_without_fetch_warns(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(data_dir / "source-registry-active.json", [])
    _write_json(
        data_dir / "source-registry-pending.json",
        [
            {
                "id": "provider:not-fetched",
                "name": "Not Fetched Provider",
                "adapter": "greenhouse",
                "pendingReason": "provider_migration_candidate",
                "migrationSourceIdentity": "static:not-fetched",
            }
        ],
    )

    report = soak.build_soak_report(data_dir)

    assert (
        report["sections"]["providerMigrationActivation"]["providerMigrationCandidatesNoFetchCount"]
        == 1
    )
    assert "pending_provider_migration_not_fetched" in _gate_ids(report)


def test_provider_migration_activation_warning_gates(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "source-registry-active.json",
        [{"id": "greenhouse:active", "adapter": "greenhouse", "name": "Active Provider"}],
    )
    _write_json(data_dir / "source-registry-pending.json", [])
    _write_json(
        data_dir / "source-discovery-candidates.json",
        [
            {
                "id": "static:add",
                "name": "Add Provider",
                "adapter": "static",
                "listing_url": "https://add.example/jobs",
                "detectedProviderUrl": "https://boards.greenhouse.io/addprovider",
            },
            {
                "id": "static:insufficient",
                "name": "Insufficient Provider Evidence",
                "adapter": "static",
                "listing_url": "https://insufficient.example/jobs",
            },
            {
                "id": "static:probe",
                "name": "Needs Probe",
                "adapter": "static",
                "listing_url": "https://probe.example/jobs",
                "browserFallbackRecommended": True,
            },
        ],
    )

    report = soak.build_soak_report(data_dir)
    gate_ids = _gate_ids(report)

    assert report["status"] == "warning"
    assert "provider_advisory_without_staging" in gate_ids
    assert "active_provider_without_migration_identity" in gate_ids
    assert "provider_migration_mostly_insufficient_or_probe" in gate_ids


def test_staged_provider_without_pending_warns(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(data_dir / "source-registry-active.json", [])
    _write_json(data_dir / "source-registry-pending.json", [])
    _write_json(
        data_dir / "source-discovery-candidates.json",
        [
            {
                "id": "provider:staged",
                "name": "Staged Provider",
                "adapter": "greenhouse",
                "candidateState": "staged_provider_candidate",
                "createdFromAdvisory": True,
            }
        ],
    )

    report = soak.build_soak_report(data_dir)

    assert "staged_provider_without_pending" in _gate_ids(report)


def test_provider_migration_activation_prefers_discovery_staging_diagnostics(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(data_dir / "source-registry-active.json", [])
    _write_json(data_dir / "source-registry-pending.json", [])
    _write_json(
        data_dir / "source-discovery-report.json",
        {
            "candidateReview": {
                "providerMigration": {
                    "totalCandidates": 4,
                    "actionCounts": {
                        "add_provider_source": 2,
                        "review_provider_migration": 1,
                        "insufficient_evidence": 1,
                    },
                    "stageableProviderCandidateCount": 2,
                    "stagedProviderCandidateCount": 0,
                    "stagingSkippedCount": 4,
                    "stagingBlockedByProviderRowBuildFailureCount": 1,
                    "stagingBlockedByIdentityCollisionCount": 1,
                    "stagingBlockedByAdapterMismatchCount": 1,
                    "stagingBlockerCounts": {
                        "provider_row_build_failure": 1,
                        "identity_collision": 1,
                        "adapter_mismatch": 1,
                    },
                    "stagingBlockerExamples": [
                        {
                            "name": "Blocked Provider",
                            "providerStagingDecision": "skipped",
                            "providerStagingBlockers": ["adapter_mismatch"],
                        }
                    ],
                }
            }
        },
    )

    report = soak.build_soak_report(data_dir)
    activation = report["sections"]["providerMigrationActivation"]
    gate_ids = _gate_ids(report)

    assert activation["advisoryTotalCandidates"] == 4
    assert activation["addProviderSourceCount"] == 2
    assert activation["reviewProviderMigrationCount"] == 1
    assert activation["stageableProviderCandidateCount"] == 2
    assert activation["stagingBlockedByProviderRowBuildFailureCount"] == 1
    assert activation["stagingBlockedByIdentityCollisionCount"] == 1
    assert activation["stagingBlockedByAdapterMismatchCount"] == 1
    assert activation["stagingBlockerExamples"][0]["name"] == "Blocked Provider"
    assert "provider_advisory_without_staging" in gate_ids
    assert "stageable_provider_without_staging" in gate_ids
    assert "provider_staging_row_build_failure" in gate_ids
    assert "provider_staging_identity_collision" in gate_ids
    assert "provider_staging_adapter_mismatch" in gate_ids
