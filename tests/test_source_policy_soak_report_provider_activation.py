import json
from pathlib import Path
from typing import Any

from scripts import source_policy_soak_report as soak


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _gate_ids(report: dict[str, object]) -> set[str]:
    gates = report.get("qualityGates", [])
    if not isinstance(gates, list):
        return set()
    return {str(row.get("id")) for row in gates if isinstance(row, dict)}


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


def test_provider_migration_activation_computes_stale_report_staging_diagnostics(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(data_dir / "source-registry-active.json", [])
    _write_json(data_dir / "source-registry-pending.json", [])
    _write_json(
        data_dir / "source-discovery-report.json",
        {
            "summary": {"queuedCandidateCount": 1},
            "candidateReview": {},
        },
    )
    _write_json(
        data_dir / "source-discovery-candidates.json",
        [
            {
                "sourceIdentity": "static:studio",
                "name": "Static Studio",
                "adapter": "static",
                "listing_url": "https://studio.example/careers",
                "detectedProviderUrl": "https://boards.greenhouse.io/staticstudio",
                "jobsFound": 3,
            }
        ],
    )

    report = soak.build_soak_report(data_dir)
    activation = report["sections"]["providerMigrationActivation"]
    gate_ids = _gate_ids(report)

    assert activation["stagingDiagnosticsSource"] == "computed_from_candidates"
    assert activation["stagedProviderCandidateCount"] == 0
    assert activation["actualStagedProviderCandidateCount"] == 0
    assert activation["pendingProviderMigrationCandidateCount"] == 0
    assert activation["computedStageableProviderCandidateCount"] == 1
    assert activation["computedWouldStageProviderCandidateCount"] == 1
    assert activation["computedStagingSkippedCount"] == 0
    assert (
        report["sections"]["providerCoverageNextAction"]["action"]
        == "refresh_discovery_staging_evidence"
    )
    assert report["sections"]["providerCoverageNextAction"]["priority"] == 1
    assert report["sections"]["providerCoverageNextAction"]["requiresHumanApproval"] is False
    commands = report["sections"]["providerCoverageNextAction"]["safeLocalCommands"]
    assert (
        "python scripts/provider_migration_staging_refresh.py --data-dir data --out-dir _out --apply-pending"
        in commands
    )
    assert "python src/source_discovery.py" not in commands
    assert "provider_advisory_without_staging" in gate_ids
    assert "stageable_provider_without_staging" in gate_ids


def test_provider_coverage_next_action_prioritizes_stale_staging_over_review_candidates(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "source-discovery-report.json",
        {
            "summary": {"queuedCandidateCount": 1},
            "candidateReview": {},
        },
    )
    _write_json(
        data_dir / "source-discovery-candidates.json",
        [
            {
                "sourceIdentity": "static:studio",
                "name": "Static Studio",
                "adapter": "static",
                "listing_url": "https://studio.example/careers",
                "detectedProviderUrl": "https://boards.greenhouse.io/staticstudio",
                "jobsFound": 3,
            }
        ],
    )
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
    next_action = report["sections"]["providerCoverageNextAction"]

    assert next_action["action"] == "refresh_discovery_staging_evidence"
    assert next_action["evidenceCounts"]["apiEligibleReviewCandidateCount"] == 1


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

    assert activation["stagingDiagnosticsSource"] == "discovery_report"
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


def test_provider_coverage_next_action_fetches_unfetched_pending_candidates(
    tmp_path: Path,
) -> None:
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
    next_action = report["sections"]["providerCoverageNextAction"]

    assert next_action["action"] == "fetch_staged_provider_candidates"
    assert next_action["priority"] == 2
    assert "pending_provider_migration_not_fetched" in next_action["blockedBy"]
    assert next_action["evidenceCounts"]["pendingProviderMigrationAdapters"] == ["greenhouse"]
    assert next_action["evidenceCounts"]["pendingProviderMigrationSourceLoaders"] == [
        "greenhouse_boards"
    ]
    assert (
        "python src/jobs_fetcher.py --only-sources greenhouse_boards --include-pending-provider-migration"
        in next_action["safeLocalCommands"]
    )


def test_provider_coverage_next_action_debugs_unvalidated_provider_cases(
    tmp_path: Path,
) -> None:
    cases: list[dict[str, Any]] = [
        {
            "case_id": "fetched-provider-missing-detail-evidence",
            "fetch_report": {
                "sources": [
                    {
                        "id": "provider:fetched",
                        "sourceIdentity": "provider:fetched",
                        "name": "Fetched Provider",
                        "adapter": "greenhouse",
                        "status": "ok",
                        "keptCount": 2,
                        "migrationSourceIdentity": "static:fetched",
                    }
                ]
            },
            "pending_rows": [
                {
                    "id": "provider:fetched",
                    "name": "Fetched Provider",
                    "adapter": "greenhouse",
                    "pendingReason": "provider_migration_candidate",
                    "migrationSourceIdentity": "static:fetched",
                }
            ],
            "diagnostics_section": "next_action",
            "expected_priority": 3,
            "expected_blocked_by": "provider_fetch_not_validated",
            "expected_command": (
                "python src/jobs_fetcher.py --only-sources greenhouse_boards "
                "--include-pending-provider-migration --force-refresh-all"
            ),
            "absent_command": "python src/jobs_fetcher.py --force-refresh-all",
        },
        {
            "case_id": "aggregate-error-missing-detail-evidence",
            "fetch_report": {
                "sources": [
                    {
                        "name": "bamboohr_sources",
                        "adapter": "bamboohr",
                        "status": "error",
                        "error": "HTTP 401 for https://studio.bamboohr.com/careers",
                    }
                ]
            },
            "pending_rows": [
                {
                    "id": "bamboohr:listing_url:https://studio.bamboohr.com/careers",
                    "name": "Studio (BambooHR)",
                    "adapter": "bamboohr",
                    "listing_url": "https://studio.bamboohr.com/careers",
                    "pendingReason": "provider_migration_candidate",
                    "migrationSourceIdentity": "static:studio",
                }
            ],
            "diagnostics_section": "activation",
            "expected_command": (
                "python src/jobs_fetcher.py --only-sources bamboohr_sources "
                "--include-pending-provider-migration --force-refresh-all"
            ),
            "expected_example": {
                "aggregateFetchStatus": "error",
                "aggregateFetchErrorContains": "HTTP 401",
            },
        },
    ]

    for case in cases:
        case_id = str(case["case_id"])
        data_dir = tmp_path / case_id
        _write_json(data_dir / "jobs-fetch-report.json", case["fetch_report"])
        _write_json(data_dir / "source-registry-active.json", [])
        _write_json(data_dir / "source-registry-pending.json", case["pending_rows"])

        report = soak.build_soak_report(data_dir)
        activation = report["sections"]["providerMigrationActivation"]
        next_action = report["sections"]["providerCoverageNextAction"]
        diagnostics = (
            activation["providerValidationDiagnostics"]
            if case["diagnostics_section"] == "activation"
            else next_action["evidenceCounts"]["providerValidationDiagnostics"]
        )

        assert diagnostics["causeCounts"]["missingDetailEvidence"] == 1, case_id
        assert next_action["action"] == "debug_provider_validation", case_id
        if "expected_priority" in case:
            assert next_action["priority"] == case["expected_priority"], case_id
        if "expected_blocked_by" in case:
            assert case["expected_blocked_by"] in next_action["blockedBy"], case_id
        assert next_action["evidenceCounts"]["providerValidationMissingDetailEvidenceCount"] == 1, (
            case_id
        )
        assert case["expected_command"] in next_action["safeLocalCommands"], case_id
        if "absent_command" in case:
            assert case["absent_command"] not in next_action["safeLocalCommands"], case_id
        if "expected_example" in case:
            example = diagnostics["examples"]["missingDetailEvidence"][0]
            expected_example = dict(case["expected_example"])
            assert example["aggregateFetchStatus"] == expected_example["aggregateFetchStatus"], (
                case_id
            )
            assert (
                expected_example["aggregateFetchErrorContains"] in example["aggregateFetchError"]
            ), case_id


def test_provider_coverage_next_action_stops_debugging_exhausted_validation_cases(
    tmp_path: Path,
) -> None:
    cases: list[dict[str, Any]] = [
        {
            "case_id": "zero-kept-and-fetch-error-advance-to-unsupported-provider",
            "fetch_report": {
                "sources": [
                    {
                        "name": "bamboohr_sources",
                        "adapter": "bamboohr",
                        "status": "error",
                        "details": [
                            {
                                "id": "bamboohr:zero",
                                "sourceIdentity": "bamboohr:zero",
                                "name": "Zero Provider",
                                "adapter": "bamboohr",
                                "status": "ok",
                                "keptCount": 0,
                                "migrationSourceIdentity": "static:zero",
                            },
                            {
                                "id": "bamboohr:error",
                                "sourceIdentity": "bamboohr:error",
                                "name": "Error Provider",
                                "adapter": "bamboohr",
                                "status": "error",
                                "error": "HTTP 401",
                                "keptCount": 0,
                                "migrationSourceIdentity": "static:error",
                            },
                        ],
                    }
                ]
            },
            "pending_rows": [
                {
                    "id": "bamboohr:zero",
                    "name": "Zero Provider",
                    "adapter": "bamboohr",
                    "pendingReason": "provider_migration_candidate",
                    "migrationSourceIdentity": "static:zero",
                },
                {
                    "id": "bamboohr:error",
                    "name": "Error Provider",
                    "adapter": "bamboohr",
                    "pendingReason": "provider_migration_candidate",
                    "migrationSourceIdentity": "static:error",
                },
            ],
            "discovery_candidates": [
                {
                    "id": "static:icims",
                    "name": "iCIMS Static",
                    "adapter": "static",
                    "listing_url": "https://careers-example.icims.com/jobs/search",
                    "jobsFound": 1,
                }
            ],
            "expected_cause_counts": {"zeroKeptFetched": 1, "fetchError": 1},
            "expected_action": "plan_unsupported_provider_family",
            "expected_unsupported_provider_count": 1,
        },
        {
            "case_id": "auth-gated-oracle-hcm-does-not-loop",
            "fetch_report": {
                "sources": [
                    {
                        "name": "oracle_hcm_sources",
                        "adapter": "oracle_hcm",
                        "status": "error",
                        "details": [
                            {
                                "id": "oracle_hcm:site_path:/hcmUI/CandidateExperience/en/sites/CX_1/jobs",
                                "sourceIdentity": "oracle_hcm:site_path:/hcmUI/CandidateExperience/en/sites/CX_1/jobs",
                                "name": "Corsair (Oracle HCM)",
                                "adapter": "oracle_hcm",
                                "status": "error",
                                "classification": "anti_bot_or_challenge",
                                "error": "auth_gated_oracle_hcm: HTTP 401 Unauthorized",
                                "keptCount": 0,
                                "migrationSourceIdentity": "static:corsair",
                            }
                        ],
                    }
                ]
            },
            "pending_rows": [
                {
                    "id": "oracle_hcm:site_path:/hcmUI/CandidateExperience/en/sites/CX_1/jobs",
                    "name": "Corsair (Oracle HCM)",
                    "adapter": "oracle_hcm",
                    "listing_url": "https://edix.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/jobs",
                    "base_url": "https://edix.fa.us2.oraclecloud.com",
                    "site_path": "/hcmUI/CandidateExperience/en/sites/CX_1/jobs",
                    "pendingReason": "provider_migration_candidate",
                    "migrationSourceIdentity": "static:corsair",
                }
            ],
            "expected_cause_counts": {"fetchError": 1},
            "unexpected_action": "debug_provider_validation",
        },
    ]

    for case in cases:
        case_id = str(case["case_id"])
        data_dir = tmp_path / case_id
        _write_json(data_dir / "jobs-fetch-report.json", case["fetch_report"])
        _write_json(data_dir / "source-registry-active.json", [])
        _write_json(data_dir / "source-registry-pending.json", case["pending_rows"])
        if "discovery_candidates" in case:
            _write_json(
                data_dir / "source-discovery-candidates.json",
                case["discovery_candidates"],
            )

        report = soak.build_soak_report(data_dir)
        next_action = report["sections"]["providerCoverageNextAction"]
        diagnostics = next_action["evidenceCounts"]["providerValidationDiagnostics"]

        for key, expected in dict(case["expected_cause_counts"]).items():
            assert diagnostics["causeCounts"][key] == expected, f"{case_id}:{key}"
        if "expected_action" in case:
            assert next_action["action"] == case["expected_action"], case_id
        if "unexpected_action" in case:
            assert next_action["action"] != case["unexpected_action"], case_id
        if "expected_unsupported_provider_count" in case:
            assert (
                next_action["evidenceCounts"]["unsupportedProviderDetectedCount"]
                == case["expected_unsupported_provider_count"]
            ), case_id
        assert "provider_fetch_not_validated" not in next_action["blockedBy"], case_id
