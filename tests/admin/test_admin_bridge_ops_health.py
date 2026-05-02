import json
from unittest import mock

from src import admin_bridge


def test_compute_ops_health_reports_alerts(admin_bridge_entrypoint_root):
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
            "startedAt": "2026-03-01T00:00:00+00:00",
            "finishedAt": "2026-03-01T00:10:00+00:00",
            "summary": {"outputCount": 100, "failedSources": 3, "sourceCount": 4},
            "sources": [],
        },
    )
    health = admin_bridge.compute_ops_health()
    assert health["service"] == "baluffo-bridge"
    assert health["appVersion"] == admin_bridge.get_app_version()
    assert health["startupReady"] is True
    assert "desktopMode" in health
    assert bool(health["desktopMode"]) == bool(admin_bridge.RUNTIME_CONFIG.desktop_mode)
    assert "owner" in health
    assert str(health["owner"]["mode"] or "") == str(admin_bridge.RUNTIME_CONFIG.owner_mode or "")
    assert "kpis" in health
    assert "alerts" in health
    assert "updater" in health
    assert (
        str((health["updater"] or {}).get("currentVersion") or "") == admin_bridge.get_app_version()
    )
    assert len(health["alerts"]) >= 1
    assert any(alert["id"] == "degraded_reliability" for alert in health["alerts"])


def test_compute_ops_health_guides_initial_fetch_when_none_has_succeeded(
    admin_bridge_entrypoint_root,
):
    health = admin_bridge.compute_ops_health()

    guidance = next(
        (alert for alert in health.get("alerts", []) if alert.get("id") == "fetch_never_run"),
        None,
    )

    assert guidance is not None
    assert guidance["severity"] == "warning"
    assert guidance["dismissible"] is False
    assert "Run Jobs Fetcher" in guidance["message"]


def test_compute_ops_health_reframes_stale_fetch_as_guidance(admin_bridge_entrypoint_root):
    admin_bridge.append_run_history(
        {
            "id": "fetch_stale_1",
            "runId": "fetch_stale_1",
            "type": "fetch",
            "status": "ok",
            "startedAt": "2026-03-01T00:00:00+00:00",
            "finishedAt": "2026-03-01T00:10:00+00:00",
            "durationMs": 600000,
            "summary": {"outputCount": 40, "failedSources": 0, "sourceCount": 4},
        }
    )

    with mock.patch.object(
        admin_bridge,
        "now_utc",
        return_value=admin_bridge.parse_iso("2026-03-02T13:00:00+00:00"),
    ):
        health = admin_bridge.compute_ops_health()

    stale_alert = next(
        (alert for alert in health.get("alerts", []) if alert.get("id") == "stale_fetch"),
        None,
    )

    assert stale_alert is not None
    assert stale_alert["severity"] == "warning"
    assert stale_alert["dismissible"] is True
    assert "full Jobs Fetcher run is suggested" in stale_alert["message"]


def test_compute_ops_health_reshows_fetch_never_run_even_if_previously_acked(
    admin_bridge_entrypoint_root,
):
    state = admin_bridge.load_alert_state()
    state["acked"]["fetch_never_run"] = admin_bridge.now_iso()
    admin_bridge.save_alert_state(state)

    health = admin_bridge.compute_ops_health()

    guidance = next(
        (alert for alert in health.get("alerts", []) if alert.get("id") == "fetch_never_run"),
        None,
    )

    assert guidance is not None
    assert guidance["dismissible"] is False
    assert "fetch_never_run" not in admin_bridge.load_alert_state().get("acked", {})


def test_compute_ops_health_includes_social_alerts(admin_bridge_entrypoint_root):
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
            "startedAt": "2026-03-01T00:00:00+00:00",
            "finishedAt": "2026-03-01T00:10:00+00:00",
            "summary": {"outputCount": 20, "failedSources": 0, "sourceCount": 3},
            "socialSummary": {
                "pilotWindowStartAt": "2026-03-01T00:00:00+00:00",
                "pilotWindowEndAt": "2026-03-01T00:10:00+00:00",
                "scheduledRunCount": 1,
                "keptCount": 2,
                "uniqueKeptCount": 2,
                "officialBoardOverlapCount": 0,
                "duplicateCount": 0,
                "duplicateRate": 0.0,
                "lowConfidenceDropped": 0,
                "sampleSize": 0,
                "reviewedCount": 0,
                "falsePositiveCount": 0,
                "falsePositiveRate": 0.0,
                "reviewArtifactPath": "data/social-experiment-review.json",
                "channels": {
                    "reddit": {
                        "keptCount": 1,
                        "uniqueKeptCount": 1,
                        "officialBoardOverlapCount": 0,
                        "duplicateCount": 0,
                        "duplicateRate": 0.0,
                        "lowConfidenceDropped": 0,
                    },
                    "mastodon": {
                        "keptCount": 1,
                        "uniqueKeptCount": 1,
                        "officialBoardOverlapCount": 0,
                        "duplicateCount": 0,
                        "duplicateRate": 0.0,
                        "lowConfidenceDropped": 0,
                    },
                },
            },
            "sources": [
                {
                    "name": "social_reddit",
                    "status": "error",
                    "fetchedCount": 30,
                    "keptCount": 0,
                    "lowConfidenceDropped": 70,
                },
                {
                    "name": "social_x",
                    "status": "error",
                    "fetchedCount": 20,
                    "keptCount": 0,
                    "lowConfidenceDropped": 60,
                },
                {
                    "name": "social_mastodon",
                    "status": "ok",
                    "fetchedCount": 20,
                    "keptCount": 0,
                    "lowConfidenceDropped": 20,
                },
            ],
        },
    )
    health = admin_bridge.compute_ops_health()
    social_kpis = health.get("kpis", {}).get("socialExperiment", {})
    assert int(social_kpis.get("keptCount") or 0) == 2
    assert int(social_kpis.get("uniqueKeptCount") or 0) == 2
    assert int(social_kpis.get("sampleSize") or 0) == 0
    assert int(social_kpis.get("reviewedCount") or 0) == 0
    assert float(social_kpis.get("falsePositiveRate") or 0) == 0.0
    ids = {str(row.get("id") or "") for row in health.get("alerts", [])}
    assert "social_sources_failing" in ids
    assert "social_zero_matches" in ids
    assert "social_low_confidence_spike" in ids
    assert "social_false_positive_spike" not in ids


def test_compute_ops_health_exposes_provider_coverage_summary(admin_bridge_entrypoint_root):
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
            "startedAt": "2026-04-30T12:00:00+00:00",
            "finishedAt": "2026-04-30T12:05:00+00:00",
            "summary": {"outputCount": 8, "failedSources": 0, "sourceCount": 1},
            "providerCoverage": {
                "totalProviderCandidates": 1,
                "statusCounts": {"validated_provider": 1},
                "validatedProviders": [
                    {
                        "name": "Studio Greenhouse",
                        "adapter": "greenhouse",
                        "providerCoverageStatus": "validated_provider",
                        "providerReplacementReadiness": "candidate",
                        "migrationSourceIdentity": "static:listing_url:https://studio.example/jobs",
                        "providerCoverageLatestKeptCount": 8,
                    }
                ],
            },
            "sources": [],
        },
    )

    health = admin_bridge.compute_ops_health()

    provider_coverage = health["kpis"]["providerCoverage"]
    assert provider_coverage["totalProviderCandidates"] == 1
    assert provider_coverage["statusCounts"]["validated_provider"] == 1
    assert provider_coverage["validatedProviders"][0]["name"] == "Studio Greenhouse"


def test_compute_ops_health_exposes_provider_static_overlap_audit(admin_bridge_entrypoint_root):
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
            "startedAt": "2026-04-30T12:00:00+00:00",
            "finishedAt": "2026-04-30T12:05:00+00:00",
            "summary": {"outputCount": 8, "failedSources": 0, "sourceCount": 1},
            "providerStaticOverlap": {
                "suppressedStaticCount": 1,
                "auditedPairCount": 1,
                "safePairCount": 1,
                "needsReviewPairCount": 0,
                "insufficientHistoryPairCount": 0,
                "staticOnlyJobCount": 0,
                "providerOnlyJobCount": 0,
                "overlapJobCount": 0,
                "pairs": [
                    {
                        "staticSourceName": "static_source::covered",
                        "providerSourceName": "Studio Greenhouse",
                        "providerAdapter": "greenhouse",
                        "providerCoverageStatus": "validated_provider",
                        "providerConsecutiveSuccesses": 2,
                        "latestProviderKeptCount": 8,
                        "auditStatus": "safe",
                    }
                ],
            },
            "staticSuppressionPolicy": {
                "eligibleCount": 1,
                "suppressedCount": 1,
                "pausedCount": 0,
                "warningCount": 0,
                "suppressedPairs": [
                    {
                        "staticSourceName": "static_source::covered",
                        "providerSourceName": "Studio Greenhouse",
                        "decision": "suppressed",
                        "reason": "prior_audit_safe",
                        "lastAuditStatus": "safe",
                    }
                ],
                "pausedPairs": [],
                "warningPairs": [],
            },
            "redundantStaticProposals": {
                "totalProposalCount": 1,
                "safeRedundantCount": 1,
                "keepStaticCount": 0,
                "needsMoreHistoryCount": 0,
                "needsReviewCount": 0,
                "providerUnstableCount": 0,
                "staticOnlyDetectedCount": 0,
                "proposals": [
                    {
                        "staticSourceName": "static_source::covered",
                        "providerSourceName": "Studio Greenhouse",
                        "proposal": "safe_redundant_static",
                        "confidence": 0.9,
                        "reasons": ["runtime_suppression_supported"],
                        "recommendedAction": "keep_runtime_suppression",
                        "destructiveActionAllowed": False,
                        "lastAuditStatus": "safe",
                    }
                ],
            },
            "sources": [],
        },
    )

    health = admin_bridge.compute_ops_health()

    overlap = health["kpis"]["providerStaticOverlap"]
    assert overlap["suppressedStaticCount"] == 1
    assert overlap["safePairCount"] == 1
    assert overlap["pairs"][0]["providerSourceName"] == "Studio Greenhouse"
    policy = health["kpis"]["staticSuppressionPolicy"]
    assert policy["suppressedCount"] == 1
    assert policy["suppressedPairs"][0]["reason"] == "prior_audit_safe"
    proposals = health["kpis"]["redundantStaticProposals"]
    assert proposals["safeRedundantCount"] == 1
    assert proposals["proposals"][0]["destructiveActionAllowed"] is False


def test_compute_ops_health_exposes_conservative_cleanup_proposals(
    admin_bridge_entrypoint_root,
):
    soak_report_path = (
        admin_bridge_entrypoint_root.parent / "_out" / "source-policy-soak-report.json"
    )
    soak_report_path.parent.mkdir(parents=True, exist_ok=True)
    soak_report_path.write_text(
        json.dumps(
            {
                "sections": {
                    "conservativeStaticCleanupProposals": {
                        "totalCandidateCount": 2,
                        "proposalCount": 1,
                        "blockedCount": 1,
                        "blockedReasonCounts": {
                            "static_only_evidence_present": 1,
                        },
                        "proposalReadyExamples": [
                            {
                                "staticSourceName": "Static Studio",
                                "providerSourceName": "Studio Provider",
                                "recommendedAction": "move_static_to_hidden_pending",
                                "requiresExplicitAdminAction": True,
                                "destructiveActionAllowed": False,
                            }
                        ],
                        "blockedExamples": [
                            {
                                "staticSourceName": "Static Blocked",
                                "providerSourceName": "Blocked Provider",
                                "blockers": ["static_only_evidence_present"],
                            }
                        ],
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    health = admin_bridge.compute_ops_health()

    cleanup = health["kpis"]["conservativeStaticCleanupProposals"]
    assert cleanup["proposalCount"] == 1
    assert cleanup["blockedCount"] == 1
    assert cleanup["blockedReasonCounts"]["static_only_evidence_present"] == 1
    assert (
        cleanup["proposalReadyExamples"][0]["recommendedAction"] == "move_static_to_hidden_pending"
    )


def test_alert_ack_suppresses_visible_alert(admin_bridge_entrypoint_root):
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
            "startedAt": "2026-03-01T00:00:00+00:00",
            "finishedAt": "2026-03-01T00:10:00+00:00",
            "summary": {"outputCount": 100, "failedSources": 3, "sourceCount": 4},
            "sources": [],
        },
    )
    initial = admin_bridge.compute_ops_health()
    alert_ids = [row["id"] for row in initial.get("alerts", [])]
    assert "degraded_reliability" in alert_ids
    state = admin_bridge.load_alert_state()
    state["acked"]["degraded_reliability"] = admin_bridge.now_iso()
    admin_bridge.save_alert_state(state)
    updated = admin_bridge.compute_ops_health()
    updated_ids = [row["id"] for row in updated.get("alerts", [])]
    assert "degraded_reliability" not in updated_ids
