import copy
import json
from pathlib import Path

from src import jobs_fetcher as jf
from src.jobs.common.contracts_source_policy_recommendations import (
    build_source_policy_recommendations_artifact,
    read_source_policy_recommendations_artifact,
)
from src.jobs.common.registry_defaults import REDUNDANT_STATIC_IF_PROVIDER
from tests.helpers.temp_paths import workspace_tmpdir

STATIC_SOURCE_NAME = "static_source::static:listing_url:https://studio.example/jobs"
STATIC_SOURCE_ID = "static:listing_url:https://studio.example/jobs"
PROVIDER_SOURCE_NAME = "Studio Greenhouse"


def _proposal(**overrides):
    row = {
        "staticSourceId": STATIC_SOURCE_ID,
        "staticSourceName": STATIC_SOURCE_NAME,
        "providerSourceId": PROVIDER_SOURCE_NAME,
        "providerSourceName": PROVIDER_SOURCE_NAME,
        "proposal": "safe_redundant_static",
        "confidence": 0.9,
        "reasons": ["runtime_suppression_supported"],
        "recommendedAction": "keep_runtime_suppression",
        "destructiveActionAllowed": False,
        "lastAuditStatus": "safe",
        "providerCoverageStatus": "validated_provider",
        "providerCoverageConsecutiveSuccesses": 2,
        "providerCoverageLatestKeptCount": 4,
        "staticOnlyCount": 0,
        "overlapCount": 1,
    }
    row.update(overrides)
    return row


def _artifact_after(*proposals):
    artifact = {}
    for index, proposal in enumerate(proposals, start=1):
        artifact = build_source_policy_recommendations_artifact(
            prior_artifact=artifact,
            redundant_static_proposals={"proposals": [proposal]},
            observed_at=f"2026-04-30T12:00:0{index}+00:00",
        )
    return artifact


def _first_pair(artifact):
    assert artifact["summary"]["totalPairs"] == 1
    pair = artifact["pairs"][0]
    assert pair["destructiveActionAllowed"] is False
    return pair


def test_first_safe_proposal_creates_recommendation_pair():
    pair = _first_pair(_artifact_after(_proposal()))

    assert pair["lastProposal"] == "safe_redundant_static"
    assert pair["safeRunCount"] == 1
    assert pair["consecutiveSafeRunCount"] == 1
    assert pair["currentRecommendation"] == "needs_more_history"
    assert pair["currentRecommendedAction"] == "collect_more_history"


def test_repeated_safe_proposals_increment_safe_counts_and_become_stable():
    pair = _first_pair(_artifact_after(_proposal(), _proposal(), _proposal()))

    assert pair["safeRunCount"] == 3
    assert pair["consecutiveSafeRunCount"] == 3
    assert pair["currentRecommendation"] == "stable_safe_redundant"
    assert pair["currentRecommendedAction"] == "keep_runtime_suppression"


def test_needs_more_history_does_not_increment_safe_streak():
    pair = _first_pair(
        _artifact_after(
            _proposal(),
            _proposal(
                proposal="needs_more_history",
                recommendedAction="collect_more_history",
                lastAuditStatus="insufficient_history",
                overlapCount=0,
            ),
        )
    )

    assert pair["safeRunCount"] == 1
    assert pair["consecutiveSafeRunCount"] == 0
    assert pair["needsMoreHistoryRunCount"] == 1
    assert pair["currentRecommendation"] == "needs_more_history"


def test_static_only_detected_recommendation_wins_from_history():
    pair = _first_pair(
        _artifact_after(
            _proposal(),
            _proposal(
                proposal="static_only_jobs_detected",
                recommendedAction="pause_suppression",
                lastAuditStatus="needs_review",
                staticOnlyCount=1,
            ),
            _proposal(),
        )
    )

    assert pair["staticOnlyDetectedRunCount"] == 1
    assert pair["currentRecommendation"] == "static_only_detected"
    assert pair["currentRecommendedAction"] == "pause_suppression"


def test_provider_unstable_produces_needs_review():
    pair = _first_pair(
        _artifact_after(
            _proposal(
                proposal="provider_unstable",
                recommendedAction="pause_suppression",
                lastAuditStatus="provider_unstable",
                providerCoverageStatus="unstable_provider",
            )
        )
    )

    assert pair["providerUnstableRunCount"] == 1
    assert pair["currentRecommendation"] == "needs_review"
    assert pair["currentRecommendedAction"] == "review_pair"


def test_keep_static_produces_keep_static_recommendation():
    pair = _first_pair(
        _artifact_after(
            _proposal(
                proposal="keep_static",
                recommendedAction="keep_static_active",
                providerCoverageStatus="untested",
            )
        )
    )

    assert pair["currentRecommendation"] == "keep_static"
    assert pair["currentRecommendedAction"] == "keep_static_active"


def test_history_is_capped_to_latest_ten_rows():
    artifact = {}
    for index in range(12):
        artifact = build_source_policy_recommendations_artifact(
            prior_artifact=artifact,
            redundant_static_proposals={"proposals": [_proposal(confidence=0.5 + (index / 100))]},
            observed_at=f"2026-04-30T12:00:{index:02d}+00:00",
        )
    pair = _first_pair(artifact)

    assert len(pair["history"]) == 10
    assert pair["history"][0]["observedAt"] == "2026-04-30T12:00:02+00:00"
    assert pair["history"][-1]["observedAt"] == "2026-04-30T12:00:11+00:00"
    assert all(row["proposal"] == "safe_redundant_static" for row in pair["history"])


def test_missing_and_corrupt_prior_artifacts_are_safe(tmp_path):
    missing_artifact, missing_warning = read_source_policy_recommendations_artifact(
        tmp_path / "missing.json"
    )
    corrupt_path = tmp_path / "source-policy-recommendations.json"
    corrupt_path.write_text("{not-json", encoding="utf-8")
    corrupt_artifact, corrupt_warning = read_source_policy_recommendations_artifact(corrupt_path)

    assert missing_warning == "missing_prior_artifact"
    assert missing_artifact["pairs"] == []
    assert corrupt_warning == "malformed_prior_artifact"
    assert corrupt_artifact["pairs"] == []


def test_pipeline_exports_recommendations_without_mutating_policy_or_sources():
    calls = {"provider": 0}
    redundant_rules = copy.deepcopy(REDUNDANT_STATIC_IF_PROVIDER)

    def provider_loader(**_: object):
        calls["provider"] += 1
        return [
            {
                "title": "Provider Engineer",
                "company": "Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://boards.greenhouse.io/studio/jobs/provider-engineer",
                "sector": "Game",
                "sourceJobId": "provider-1",
            }
        ]

    previous_default_loaders = jf.default_source_loaders
    try:
        with workspace_tmpdir("jobs-fetcher-source-policy-recommendations") as tmp:
            out = Path(tmp)
            pending_registry_path = out / "source-registry-pending.json"
            pending_registry_payload = [{"id": "pending-static", "adapter": "static"}]
            pending_registry_path.write_text(
                json.dumps(pending_registry_payload),
                encoding="utf-8",
            )
            source_state = {
                "schemaVersion": jf.SCHEMA_VERSION,
                "sources": {
                    PROVIDER_SOURCE_NAME: {
                        "lastAdapter": "greenhouse",
                        "providerCoverageStatus": "validated_provider",
                        "providerCoverageConsecutiveSuccesses": 2,
                        "providerCoverageLatestKeptCount": 3,
                        "migrationSourceIdentity": STATIC_SOURCE_ID,
                    },
                    STATIC_SOURCE_NAME: {"lastKeptCount": 2},
                },
            }
            (out / "jobs-source-state.json").write_text(json.dumps(source_state), encoding="utf-8")
            (out / "jobs-fetch-report.json").write_text(
                json.dumps(
                    {
                        "providerStaticOverlap": {
                            "pairs": [
                                {
                                    "staticSourceId": STATIC_SOURCE_ID,
                                    "staticSourceName": STATIC_SOURCE_NAME,
                                    "providerSourceId": PROVIDER_SOURCE_NAME,
                                    "providerSourceName": PROVIDER_SOURCE_NAME,
                                    "providerCoverageStatus": "validated_provider",
                                    "providerConsecutiveSuccesses": 2,
                                    "latestProviderKeptCount": 4,
                                    "auditStatus": "safe",
                                    "staticOnlyCount": 0,
                                    "overlapCount": 1,
                                }
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )
            jf.default_source_loaders = lambda **_: [
                ("greenhouse_boards", provider_loader),
                (STATIC_SOURCE_NAME, lambda **_: []),
            ]

            report = jf.run_pipeline(output_dir=out, show_progress=False, force_refresh_all=True)
            artifact = json.loads(
                (out / "source-policy-recommendations.json").read_text(encoding="utf-8")
            )
            pending_registry_after = json.loads(pending_registry_path.read_text(encoding="utf-8"))

        assert calls == {"provider": 1}
        assert report["redundantStaticProposals"]["safeRedundantCount"] == 1
        assert report["sourcePolicyRecommendationExport"]["status"] == "ok"
        assert report["sourcePolicyRecommendationExport"]["updatedPairCount"] == 1
        assert report["outputs"]["sourcePolicyRecommendations"].endswith(
            "source-policy-recommendations.json"
        )
        assert artifact["summary"]["totalPairs"] == 1
        assert artifact["pairs"][0]["lastProposal"] == "safe_redundant_static"
        assert all(row["name"] != "sourcePolicyRecommendationExport" for row in report["sources"])
        assert pending_registry_after == pending_registry_payload
        assert REDUNDANT_STATIC_IF_PROVIDER == redundant_rules
    finally:
        jf.default_source_loaders = previous_default_loaders
