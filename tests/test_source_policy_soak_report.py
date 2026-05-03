import json
from pathlib import Path

from scripts import source_policy_soak_report as soak


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _base_fetch_report() -> dict[str, object]:
    return {
        "providerCoverage": {
            "totalProviderCandidates": 1,
            "statusCounts": {"validated_provider": 1},
        },
        "staticSuppressionPolicy": {
            "eligibleCount": 1,
            "suppressedCount": 1,
            "pausedCount": 0,
            "warningCount": 0,
            "suppressedPairs": [
                {
                    "staticSourceId": "static:studio",
                    "staticSourceName": "Static Studio",
                    "providerSourceId": "provider:studio",
                    "providerSourceName": "Studio Provider",
                    "decision": "suppressed",
                    "reason": "prior_audit_safe",
                    "lastAuditStatus": "safe",
                    "providerCoverageStatus": "validated_provider",
                    "providerCoverageConsecutiveSuccesses": 2,
                    "providerCoverageLatestKeptCount": 3,
                    "staticOnlyCount": 0,
                    "overlapCount": 2,
                }
            ],
            "pausedPairs": [],
            "warningPairs": [],
        },
        "providerStaticOverlap": {
            "auditedPairCount": 1,
            "safePairCount": 1,
            "needsReviewPairCount": 0,
            "insufficientHistoryPairCount": 0,
            "pairs": [
                {
                    "staticSourceId": "static:studio",
                    "staticSourceName": "Static Studio",
                    "providerSourceId": "provider:studio",
                    "providerSourceName": "Studio Provider",
                    "auditStatus": "safe",
                    "providerCoverageStatus": "validated_provider",
                    "providerConsecutiveSuccesses": 2,
                    "latestProviderKeptCount": 3,
                    "staticOnlyCount": 0,
                    "overlapCount": 2,
                }
            ],
        },
        "redundantStaticProposals": {
            "totalProposalCount": 1,
            "safeRedundantCount": 1,
            "staticOnlyDetectedCount": 0,
            "proposals": [
                {
                    "staticSourceId": "static:studio",
                    "staticSourceName": "Static Studio",
                    "providerSourceId": "provider:studio",
                    "providerSourceName": "Studio Provider",
                    "proposal": "safe_redundant_static",
                    "recommendedAction": "keep_runtime_suppression",
                    "confidence": 0.9,
                    "lastAuditStatus": "safe",
                    "providerCoverageStatus": "validated_provider",
                    "providerCoverageConsecutiveSuccesses": 2,
                    "providerCoverageLatestKeptCount": 3,
                    "staticOnlyCount": 0,
                    "overlapCount": 2,
                }
            ],
        },
        "sources": [
            {
                "name": "Static Studio",
                "status": "excluded",
                "exclusionReason": "dynamic_redundant_provider",
            }
        ],
    }


def _write_clean_runtime(
    data_dir: Path,
    *,
    fetch_run_id: str = "fetch_clean_1",
    recommendations_updated_at: str | None = None,
) -> None:
    fetch_report = _base_fetch_report()
    if fetch_run_id:
        fetch_report["runId"] = fetch_run_id
    _write_json(data_dir / "jobs-fetch-report.json", fetch_report)
    _write_json(
        data_dir / "source-registry-active.json", [{"id": "static:studio", "adapter": "static"}]
    )
    _write_json(data_dir / "source-registry-pending.json", [])
    _write_json(data_dir / "source-registry-rejected.json", [])
    _write_json(data_dir / "source-sync.json", {"schemaVersion": 2, "active": [], "pending": []})
    recommendations = {
        "pairs": [
            {
                "staticSourceId": "static:studio",
                "providerSourceId": "provider:studio",
                "currentRecommendation": "stable_safe_redundant",
                "lastProposal": "safe_redundant_static",
                "safeRunCount": 3,
                "consecutiveSafeRunCount": 3,
                "destructiveActionAllowed": False,
            }
        ]
    }
    if recommendations_updated_at:
        recommendations["updatedAt"] = recommendations_updated_at
    _write_json(data_dir / "source-policy-recommendations.json", recommendations)
    _write_json(data_dir / "source-policy-review-state.json", {"pairs": {}})


def _gate_ids(report: dict[str, object]) -> set[str]:
    return {str(row.get("id")) for row in report.get("qualityGates", []) if isinstance(row, dict)}


def test_clean_state_reports_ok_and_writes_reports(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    out_dir = tmp_path / "_out"
    _write_clean_runtime(data_dir)

    report = soak.build_soak_report(data_dir)
    outputs = soak.write_soak_report(report, out_dir)

    assert report["status"] == "ok"
    assert report["mutation"] == {"readOnly": True, "writesOutsideOut": False}
    assert report["summary"]["validatedProviderCount"] == 1
    assert report["summary"]["dynamicRedundantStaticSuppressedCount"] == 1
    assert report["summary"]["stableSafeRedundantRecommendationCount"] == 1
    assert report["summary"]["conservativeStaticCleanupProposalCount"] == 1
    cleanup = report["sections"]["conservativeStaticCleanupProposals"]
    assert cleanup["proposalCount"] == 1
    assert cleanup["staleCount"] == 0
    assert cleanup["proposalFreshnessStatus"] == "fresh"
    assert cleanup["proposalFreshnessAgeSeconds"] == 0
    assert cleanup["proposalStaleThresholdSeconds"] == 86400
    assert cleanup["proposalReportRunId"] == "fetch_clean_1"
    assert cleanup["proposalReadinessHash"]
    assert cleanup["proposalReadyExamples"][0]["proposalReadinessEvidence"] == [
        "proposal_freshness:fresh",
        "proposal_disposition:proposal_ready",
    ]
    assert Path(outputs["json"]).exists()
    assert Path(outputs["markdown"]).exists()


def test_conservative_static_cleanup_proposal_is_report_only(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_clean_runtime(data_dir)

    report = soak.build_soak_report(data_dir)
    cleanup = report["sections"]["conservativeStaticCleanupProposals"]
    proposal = report["sections"]["conservativeStaticCleanupProposals"]["proposals"][0]

    assert cleanup["blockedReasonCounts"] == {}
    assert cleanup["proposalFreshnessStatus"] == "fresh"
    assert cleanup["proposalReportRunId"] == "fetch_clean_1"
    assert cleanup["proposalReadyExamples"][0]["staticSourceId"] == proposal["staticSourceId"]
    assert proposal["recommendedAction"] == "move_static_to_hidden_pending"
    assert proposal["destructiveActionAllowed"] is False
    assert proposal["requiresExplicitAdminAction"] is True
    assert proposal["decisionLogEvidenceRequired"] is True
    assert proposal["proposalDisposition"] == "proposal_ready"
    assert proposal["cleanRunEvidenceCount"] == 3
    assert proposal["suppressionEvidenceStatus"] == "observed_dynamic_suppression"
    assert proposal["blockers"] == []


def test_conservative_static_cleanup_blocks_static_only_evidence(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_clean_runtime(data_dir)
    recommendations = json.loads(
        (data_dir / "source-policy-recommendations.json").read_text(encoding="utf-8")
    )
    recommendations["pairs"][0]["staticOnlyDetectedRunCount"] = 1
    _write_json(data_dir / "source-policy-recommendations.json", recommendations)

    report = soak.build_soak_report(data_dir)
    cleanup = report["sections"]["conservativeStaticCleanupProposals"]

    assert cleanup["proposalCount"] == 0
    assert cleanup["blockedReasonCounts"]["static_only_evidence_present"] == 1
    assert cleanup["blockedExamples"][0]["proposalDisposition"] == "blocked"
    assert cleanup["blockedCandidates"][0]["blockers"] == ["static_only_evidence_present"]
    assert cleanup["blockedCandidates"][0]["proposalReadiness"] == "blocked"
    assert cleanup["blockedCandidates"][0]["proposalFreshnessStatus"] == "fresh"
    assert cleanup["blockedCandidates"][0]["proposalReadinessEvidence"] == [
        "blocker:static_only_evidence_present",
        "proposal_disposition:blocked",
    ]


def test_conservative_static_cleanup_never_mutates_seed_or_runtime_registry(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    _write_clean_runtime(data_dir)
    seed_path = data_dir / "defaults" / "source-registry-active.seed.json"
    _write_json(seed_path, [{"id": "seed:static", "adapter": "static"}])
    before_active = (data_dir / "source-registry-active.json").read_text(encoding="utf-8")
    before_seed = seed_path.read_text(encoding="utf-8")

    soak.build_soak_report(data_dir)

    assert (data_dir / "source-registry-active.json").read_text(encoding="utf-8") == before_active
    assert seed_path.read_text(encoding="utf-8") == before_seed


def test_registry_seed_files_are_used_when_runtime_registry_is_missing(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_clean_runtime(data_dir)
    _write_json(
        data_dir / "defaults" / "source-registry-active.seed.json",
        [{"id": "static:seed", "adapter": "static"}],
    )
    _write_json(data_dir / "defaults" / "source-registry-pending.seed.json", [])
    (data_dir / "source-registry-active.json").unlink()
    (data_dir / "source-registry-pending.json").unlink()

    report = soak.build_soak_report(data_dir)

    assert report["inputs"]["sourceRegistryActive"]["status"] == "seed"
    assert report["inputs"]["sourceRegistryPending"]["status"] == "seed"


def test_missing_runtime_artifacts_warn_but_do_not_fail(tmp_path: Path) -> None:
    report = soak.build_soak_report(tmp_path / "data")

    assert report["status"] == "warning"
    assert "missing_jobs_fetch_report" in _gate_ids(report)
    assert all(row["status"] != "failed" for row in report["qualityGates"])


def test_source_policy_artifacts_in_source_sync_trigger_failed_gates(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "source-sync.json",
        {
            "schemaVersion": 2,
            "active": [{"manualSuppressionOverride": "force_pause"}],
            "pending": [],
            "sourcePolicy": {"recommendations": []},
            "redundantStaticProposals": {},
        },
    )

    report = soak.build_soak_report(data_dir)

    assert report["status"] == "failed"
    assert "source_sync_contains_source_policy" in _gate_ids(report)
    assert "source_sync_unexpected_top_level_keys" in _gate_ids(report)


def test_malformed_artifacts_warn_without_crashing(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    (data_dir / "source-policy-review-state.json").write_text("{not-json", encoding="utf-8")

    report = soak.build_soak_report(data_dir)

    assert report["status"] == "warning"
    assert "malformed_artifact" in _gate_ids(report)


def test_static_only_detected_while_suppressed_warns(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    report_payload = _base_fetch_report()
    proposal = report_payload["redundantStaticProposals"]["proposals"][0]
    proposal["proposal"] = "static_only_jobs_detected"
    proposal["staticOnlyCount"] = 2
    report_payload["redundantStaticProposals"]["staticOnlyDetectedCount"] = 1
    _write_json(data_dir / "jobs-fetch-report.json", report_payload)
    _write_json(
        data_dir / "source-registry-active.json", [{"id": "static:studio", "adapter": "static"}]
    )

    report = soak.build_soak_report(data_dir)

    assert report["status"] == "warning"
    assert "static_only_detected_while_suppressed" in _gate_ids(report)


def test_force_pause_without_matching_paused_policy_warns(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_clean_runtime(data_dir)
    _write_json(
        data_dir / "source-policy-review-state.json",
        {
            "pairs": {
                "static:studio||provider:studio": {
                    "staticSourceId": "static:studio",
                    "providerSourceId": "provider:studio",
                    "manualSuppressionOverride": "force_pause",
                }
            }
        },
    )

    report = soak.build_soak_report(data_dir)

    assert report["status"] == "warning"
    assert "force_pause_not_paused" in _gate_ids(report)


def test_backup_payload_counts_are_reported(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    backup_path = tmp_path / "backup.json"
    _write_clean_runtime(data_dir)
    _write_json(
        backup_path,
        {
            "counts": {
                "sourcePolicyReviewPairs": 2,
                "sourcePolicyRecommendationPairs": 3,
            },
            "sourcePolicy": {},
        },
    )

    report = soak.build_soak_report(data_dir, backup_payload_path=backup_path)

    assert report["summary"]["sourcePolicyReviewPairs"] == 2
    assert report["summary"]["sourcePolicyRecommendationPairs"] == 3


def test_report_write_only_touches_out_dir(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    out_dir = tmp_path / "_out"
    _write_clean_runtime(data_dir)
    before = {
        path.relative_to(data_dir): path.read_text(encoding="utf-8")
        for path in data_dir.glob("*.json")
    }

    report = soak.build_soak_report(data_dir)
    soak.write_soak_report(report, out_dir)

    after = {
        path.relative_to(data_dir): path.read_text(encoding="utf-8")
        for path in data_dir.glob("*.json")
    }
    assert after == before
    assert sorted(path.name for path in out_dir.iterdir()) == [
        "source-policy-soak-report.json",
        "source-policy-soak-report.md",
    ]
