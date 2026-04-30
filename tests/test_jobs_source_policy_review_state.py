import copy
import json
from pathlib import Path

from src import jobs_fetcher as jf
from src.bridge.report_normalizer import normalize_fetch_report_contract
from src.fetcher_metrics import build_metrics
from src.jobs.common.contracts_fetch_report import normalize_fetch_report_payload
from src.jobs.common.contracts_source_policy_recommendations import (
    build_source_policy_recommendations_artifact,
)
from src.jobs.common.contracts_source_policy_review_state import (
    apply_source_policy_review_action,
    read_source_policy_review_state_artifact,
)
from src.jobs.common.contracts_static_suppression_policy import (
    normalize_prior_static_suppression_evidence,
)
from src.jobs.common.registry_defaults import REDUNDANT_STATIC_IF_PROVIDER
from src.jobs.pipeline_loader_selection import apply_dynamic_redundant_static_exclusions
from tests.helpers.temp_paths import workspace_tmpdir

STATIC_SOURCE_NAME = "static_source::static:listing_url:https://studio.example/jobs"
STATIC_SOURCE_ID = "static:listing_url:https://studio.example/jobs"
PROVIDER_SOURCE_NAME = "Studio Greenhouse"


def _proposal(proposal: str = "safe_redundant_static") -> dict[str, object]:
    return {
        "staticSourceId": STATIC_SOURCE_ID,
        "staticSourceName": STATIC_SOURCE_NAME,
        "providerSourceId": PROVIDER_SOURCE_NAME,
        "providerSourceName": PROVIDER_SOURCE_NAME,
        "proposal": proposal,
        "confidence": 0.9,
        "recommendedAction": "keep_runtime_suppression",
        "destructiveActionAllowed": False,
        "lastAuditStatus": "safe",
        "providerCoverageStatus": "validated_provider",
        "providerCoverageConsecutiveSuccesses": 2,
        "providerCoverageLatestKeptCount": 3,
        "staticOnlyCount": 0,
        "overlapCount": 1,
        "reasons": ["suppression_supported"],
    }


def _eligible_provider_state() -> dict[str, dict[str, object]]:
    return {
        PROVIDER_SOURCE_NAME: {
            "lastAdapter": "greenhouse",
            "providerCoverageStatus": "validated_provider",
            "providerCoverageConsecutiveSuccesses": 2,
            "providerCoverageLatestKeptCount": 3,
            "migrationSourceIdentity": STATIC_SOURCE_ID,
        }
    }


def _prior_report() -> dict[str, object]:
    return {
        "providerStaticOverlap": {
            "pairs": [
                {
                    "staticSourceId": STATIC_SOURCE_ID,
                    "staticSourceName": STATIC_SOURCE_NAME,
                    "providerSourceId": PROVIDER_SOURCE_NAME,
                    "providerSourceName": PROVIDER_SOURCE_NAME,
                    "providerCoverageStatus": "validated_provider",
                    "providerConsecutiveSuccesses": 2,
                    "latestProviderKeptCount": 3,
                    "auditStatus": "safe",
                    "auditReasons": [],
                    "staticOnlyCount": 0,
                    "overlapCount": 1,
                }
            ]
        }
    }


def _excluded_report(name: str, reason: str) -> dict[str, object]:
    return {
        "name": name,
        "status": "excluded",
        "adapter": "custom",
        "fetchStrategy": "auto",
        "studio": "",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": reason,
        "exclusionReason": reason,
        "durationMs": 0,
    }


def test_review_actions_update_only_review_state_and_bound_notes() -> None:
    artifact: dict[str, object] = {}
    artifact, pair = apply_source_policy_review_action(
        prior_artifact=artifact,
        action_payload={
            "action": "acknowledge",
            "staticSourceId": STATIC_SOURCE_ID,
            "staticSourceName": STATIC_SOURCE_NAME,
            "providerSourceId": PROVIDER_SOURCE_NAME,
            "providerSourceName": PROVIDER_SOURCE_NAME,
            "notes": "x" * 600,
            "updatedBy": "admin-test",
        },
        updated_at="2026-04-30T10:00:00Z",
    )
    assert pair["reviewState"] == "acknowledged"
    assert pair["manualSuppressionOverride"] == "none"
    assert len(pair["notes"]) == 500

    artifact, pair = apply_source_policy_review_action(
        prior_artifact=artifact,
        action_payload={
            "action": "reviewed",
            "staticSourceId": STATIC_SOURCE_ID,
            "providerSourceId": PROVIDER_SOURCE_NAME,
        },
        updated_at="2026-04-30T11:00:00Z",
    )
    assert pair["reviewState"] == "reviewed"
    assert artifact["summary"]["reviewedCount"] == 1


def test_snooze_requires_parseable_until_and_does_not_set_override() -> None:
    try:
        apply_source_policy_review_action(
            prior_artifact={},
            action_payload={
                "action": "snooze",
                "staticSourceId": STATIC_SOURCE_ID,
                "providerSourceId": PROVIDER_SOURCE_NAME,
                "snoozedUntil": "not-a-date",
            },
            updated_at="2026-04-30T10:00:00Z",
        )
    except ValueError as exc:
        assert "snoozedUntil" in str(exc)
    else:
        raise AssertionError("invalid snooze timestamp was accepted")

    artifact, pair = apply_source_policy_review_action(
        prior_artifact={},
        action_payload={
            "action": "snooze",
            "staticSourceId": STATIC_SOURCE_ID,
            "providerSourceId": PROVIDER_SOURCE_NAME,
            "snoozedUntil": "2026-05-07T10:00:00Z",
        },
        updated_at="2026-04-30T10:00:00Z",
    )
    assert pair["reviewState"] == "snoozed"
    assert pair["manualSuppressionOverride"] == "none"
    assert artifact["summary"]["snoozedCount"] == 1


def test_force_pause_and_clear_override_are_reversible() -> None:
    artifact, pair = apply_source_policy_review_action(
        prior_artifact={},
        action_payload={
            "action": "force_pause",
            "staticSourceId": STATIC_SOURCE_ID,
            "providerSourceId": PROVIDER_SOURCE_NAME,
        },
        updated_at="2026-04-30T10:00:00Z",
    )
    assert pair["manualSuppressionOverride"] == "force_pause"
    assert artifact["summary"]["forcePausedCount"] == 1

    artifact, pair = apply_source_policy_review_action(
        prior_artifact=artifact,
        action_payload={
            "action": "clear_override",
            "staticSourceId": STATIC_SOURCE_ID,
            "providerSourceId": PROVIDER_SOURCE_NAME,
        },
        updated_at="2026-04-30T11:00:00Z",
    )
    assert pair["manualSuppressionOverride"] == "none"
    assert artifact["summary"]["forcePausedCount"] == 0


def test_force_pause_pauses_otherwise_safe_dynamic_suppression() -> None:
    review_state, _pair = apply_source_policy_review_action(
        prior_artifact={},
        action_payload={
            "action": "force_pause",
            "staticSourceId": STATIC_SOURCE_ID,
            "providerSourceId": PROVIDER_SOURCE_NAME,
        },
        updated_at="2026-04-30T10:00:00Z",
    )
    filtered, excluded, policy = apply_dynamic_redundant_static_exclusions(
        [
            ("greenhouse_boards", lambda **_: []),
            (STATIC_SOURCE_NAME, lambda **_: []),
        ],
        source_state_rows=_eligible_provider_state(),
        build_excluded_source_report=_excluded_report,
        source_report_meta={"greenhouse_boards": {"adapter": "greenhouse"}},
        prior_static_suppression_evidence=normalize_prior_static_suppression_evidence(
            _prior_report()
        ),
        source_policy_review_state=review_state,
    )

    assert [name for name, _loader in filtered] == ["greenhouse_boards", STATIC_SOURCE_NAME]
    assert excluded == []
    assert policy["pausedPairs"][0]["reason"] == "manual_force_pause"


def test_clear_override_restores_normal_safe_suppression() -> None:
    review_state, _pair = apply_source_policy_review_action(
        prior_artifact={},
        action_payload={
            "action": "clear_override",
            "staticSourceId": STATIC_SOURCE_ID,
            "providerSourceId": PROVIDER_SOURCE_NAME,
        },
        updated_at="2026-04-30T10:00:00Z",
    )
    filtered, excluded, policy = apply_dynamic_redundant_static_exclusions(
        [
            ("greenhouse_boards", lambda **_: []),
            (STATIC_SOURCE_NAME, lambda **_: []),
        ],
        source_state_rows=_eligible_provider_state(),
        build_excluded_source_report=_excluded_report,
        source_report_meta={"greenhouse_boards": {"adapter": "greenhouse"}},
        prior_static_suppression_evidence=normalize_prior_static_suppression_evidence(
            _prior_report()
        ),
        source_policy_review_state=review_state,
    )

    assert [name for name, _loader in filtered] == ["greenhouse_boards"]
    assert excluded[0]["exclusionReason"] == "dynamic_redundant_provider"
    assert policy["suppressedPairs"][0]["reason"] == "prior_audit_safe"


def test_recommendation_artifact_merges_review_state_without_changing_evidence() -> None:
    review_state, _pair = apply_source_policy_review_action(
        prior_artifact={},
        action_payload={
            "action": "force_pause",
            "staticSourceId": STATIC_SOURCE_ID,
            "providerSourceId": PROVIDER_SOURCE_NAME,
        },
        updated_at="2026-04-30T10:00:00Z",
    )
    artifact = build_source_policy_recommendations_artifact(
        prior_artifact={},
        redundant_static_proposals={"proposals": [_proposal()]},
        observed_at="2026-04-30T10:00:00Z",
        review_state=review_state,
    )

    assert artifact["pairs"][0]["lastProposal"] == "safe_redundant_static"
    assert artifact["pairs"][0]["manualSuppressionOverride"] == "force_pause"
    assert artifact["pairs"][0]["destructiveActionAllowed"] is False
    assert artifact["summary"]["totalPairs"] == 1


def test_missing_and_corrupt_review_artifacts_are_safe(tmp_path: Path) -> None:
    missing, missing_warning = read_source_policy_review_state_artifact(
        tmp_path / "source-policy-review-state.json"
    )
    assert missing["summary"]["totalPairs"] == 0
    assert missing_warning == "missing_review_state_artifact"

    corrupt_path = tmp_path / "source-policy-review-state.json"
    corrupt_path.write_text("{", encoding="utf-8")
    corrupt, corrupt_warning = read_source_policy_review_state_artifact(corrupt_path)
    assert corrupt["summary"]["totalPairs"] == 0
    assert corrupt_warning == "malformed_review_state_artifact"


def test_review_state_paths_and_counts_normalize_through_reports_and_metrics() -> None:
    payload = {
        "summary": {"sourceCount": 0},
        "sources": [],
        "outputs": {
            "sourcePolicyRecommendations": "data/source-policy-recommendations.json",
            "sourcePolicyReviewState": "data/source-policy-review-state.json",
        },
        "sourcePolicyRecommendationExport": {
            "status": "ok",
            "artifactPath": "data/source-policy-recommendations.json",
            "reviewStatePath": "data/source-policy-review-state.json",
            "updatedPairCount": 1,
            "reviewStatePairCount": 2,
            "manualForcePausedCount": 1,
            "reviewStateWarning": "missing_review_state_artifact",
        },
    }

    normalized = normalize_fetch_report_payload(payload)
    bridge = normalize_fetch_report_contract(payload)
    metrics = build_metrics(payload, [], window=5)

    assert normalized["outputs"]["sourcePolicyReviewState"].endswith(
        "source-policy-review-state.json"
    )
    assert bridge["sourcePolicyRecommendationExport"]["manualForcePausedCount"] == 1
    assert metrics["latestRun"]["sourcePolicyRecommendationExport"]["reviewStatePairCount"] == 2


def test_pipeline_force_pause_does_not_mutate_registry_or_static_rules() -> None:
    calls = {"provider": 0, "static": 0}
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

    def static_loader(**_: object):
        calls["static"] += 1
        return []

    previous_default_loaders = jf.default_source_loaders
    try:
        with workspace_tmpdir("jobs-fetcher-source-policy-force-pause") as tmp:
            out = Path(tmp)
            pending_registry_path = out / "source-registry-pending.json"
            pending_registry_payload = [{"id": "pending-static", "adapter": "static"}]
            pending_registry_path.write_text(json.dumps(pending_registry_payload), encoding="utf-8")
            (out / "jobs-source-state.json").write_text(
                json.dumps(
                    {"schemaVersion": jf.SCHEMA_VERSION, "sources": _eligible_provider_state()}
                ),
                encoding="utf-8",
            )
            (out / "jobs-fetch-report.json").write_text(
                json.dumps(_prior_report()), encoding="utf-8"
            )
            review_state, _pair = apply_source_policy_review_action(
                prior_artifact={},
                action_payload={
                    "action": "force_pause",
                    "staticSourceId": STATIC_SOURCE_ID,
                    "providerSourceId": PROVIDER_SOURCE_NAME,
                },
                updated_at="2026-04-30T10:00:00Z",
            )
            (out / "source-policy-review-state.json").write_text(
                json.dumps(review_state), encoding="utf-8"
            )
            jf.default_source_loaders = lambda **_: [
                ("greenhouse_boards", provider_loader),
                (STATIC_SOURCE_NAME, static_loader),
            ]

            report = jf.run_pipeline(output_dir=out, show_progress=False, force_refresh_all=True)
            pending_registry_after = json.loads(pending_registry_path.read_text(encoding="utf-8"))

        assert calls == {"provider": 1, "static": 1}
        assert report["staticSuppressionPolicy"]["pausedPairs"][0]["reason"] == "manual_force_pause"
        assert all(
            row.get("exclusionReason") != "dynamic_redundant_provider"
            for row in report["sources"]
            if row["name"] == STATIC_SOURCE_NAME
        )
        assert pending_registry_after == pending_registry_payload
        assert REDUNDANT_STATIC_IF_PROVIDER == redundant_rules
    finally:
        jf.default_source_loaders = previous_default_loaders
