import json
from pathlib import Path

from scripts import source_policy_soak_report as soak
from tests.test_source_policy_soak_report import _write_clean_runtime, _write_json


def test_conservative_static_cleanup_blocks_dirty_source_sync(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_clean_runtime(data_dir)
    _write_json(
        data_dir / "source-sync.json",
        {
            "schemaVersion": 2,
            "active": [],
            "pending": [],
            "sourcePolicy": {"recommendations": []},
        },
    )

    report = soak.build_soak_report(data_dir)
    cleanup = report["sections"]["conservativeStaticCleanupProposals"]

    assert cleanup["proposalCount"] == 0
    assert cleanup["blockedReasonCounts"]["source_sync_not_clean"] == 1
    assert cleanup["blockedCandidates"][0]["proposalDisposition"] == "blocked"
    assert "source_sync_not_clean" in cleanup["blockedCandidates"][0]["blockers"]


def test_conservative_static_cleanup_examples_are_capped_and_stable(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_clean_runtime(data_dir)
    labels = ["Foxtrot", "Echo", "Delta", "Charlie", "Bravo", "Alpha"]
    recommendation_pairs = []
    suppressed_pairs = []
    proposal_rows = []
    active_rows = []
    for label in labels:
        static_id = f"static:{label.lower()}"
        provider_id = f"provider:{label.lower()}"
        recommendation_pairs.append(
            {
                "staticSourceId": static_id,
                "staticSourceName": f"Static {label}",
                "providerSourceId": provider_id,
                "providerSourceName": f"Provider {label}",
                "currentRecommendation": "stable_safe_redundant",
                "lastProposal": "safe_redundant_static",
                "safeRunCount": 3,
                "consecutiveSafeRunCount": 3,
                "destructiveActionAllowed": False,
            }
        )
        suppressed_pairs.append(
            {
                "staticSourceId": static_id,
                "staticSourceName": f"Static {label}",
                "providerSourceId": provider_id,
                "providerSourceName": f"Provider {label}",
                "decision": "suppressed",
                "reason": "prior_audit_safe",
                "lastAuditStatus": "safe",
            }
        )
        proposal_rows.append(
            {
                "staticSourceId": static_id,
                "staticSourceName": f"Static {label}",
                "providerSourceId": provider_id,
                "providerSourceName": f"Provider {label}",
                "proposal": "safe_redundant_static",
                "recommendedAction": "keep_runtime_suppression",
                "providerCoverageStatus": "validated_provider",
                "providerCoverageConsecutiveSuccesses": 2,
                "providerCoverageLatestKeptCount": 3,
                "staticOnlyCount": 0,
                "overlapCount": 2,
            }
        )
        active_rows.append({"id": static_id, "adapter": "static"})

    recommendations_payload = json.loads(
        (data_dir / "source-policy-recommendations.json").read_text(encoding="utf-8")
    )
    recommendations_payload["pairs"] = recommendation_pairs
    _write_json(data_dir / "source-policy-recommendations.json", recommendations_payload)

    fetch_report = json.loads((data_dir / "jobs-fetch-report.json").read_text(encoding="utf-8"))
    fetch_report["staticSuppressionPolicy"]["suppressedPairs"] = suppressed_pairs
    fetch_report["staticSuppressionPolicy"]["suppressedCount"] = len(suppressed_pairs)
    fetch_report["redundantStaticProposals"]["proposals"] = proposal_rows
    fetch_report["redundantStaticProposals"]["totalProposalCount"] = len(proposal_rows)
    fetch_report["redundantStaticProposals"]["safeRedundantCount"] = len(proposal_rows)
    _write_json(data_dir / "jobs-fetch-report.json", fetch_report)
    _write_json(data_dir / "source-registry-active.json", active_rows)

    report = soak.build_soak_report(data_dir)
    cleanup = report["sections"]["conservativeStaticCleanupProposals"]

    assert cleanup["proposalCount"] == 6
    assert len(cleanup["proposalReadyExamples"]) == 5
    assert [row["staticSourceName"] for row in cleanup["proposalReadyExamples"]] == [
        "Static Alpha",
        "Static Bravo",
        "Static Charlie",
        "Static Delta",
        "Static Echo",
    ]
