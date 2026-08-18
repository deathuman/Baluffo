import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest import mock

from src import admin_bridge
from src.bridge import ops_health


def test_compute_ops_health_is_lightweight_liveness(admin_bridge_entrypoint_root):
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_LIFECYCLE_PATH,
        {"schemaVersion": 1, "updatedAt": "", "rows": []},
    )
    admin_bridge.start_lifecycle_run(
        run_id="fetch_lightweight_health_1",
        task_type="fetch",
        started_at="2026-05-07T10:00:00+00:00",
    )
    admin_bridge.heartbeat_lifecycle_run(
        "fetch_lightweight_health_1",
        "fetch",
        heartbeat_at="2026-05-07T10:02:00+00:00",
    )

    health = admin_bridge.compute_ops_health()

    assert health["service"] == "baluffo-bridge"
    assert health["status"] == "healthy"
    assert health["ok"] is True
    assert health["appVersion"] == admin_bridge.get_app_version()
    assert health["lifecycle"]["currentCount"] == 1
    assert health["lifecycle"]["latestHeartbeatAt"] == "2026-05-07T10:02:00+00:00"
    assert set(health["schedule"]) >= {"fetcher", "discovery", "pipeline"}
    assert health["schedule"]["pipeline"]["enabled"] is False
    assert "alerts" not in health
    assert "kpis" not in health


def test_compute_ops_health_ready_avoids_lifecycle_and_schedule_reads(admin_bridge_entrypoint_root):
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_LIFECYCLE_PATH,
        {"schemaVersion": 1, "updatedAt": "", "rows": []},
    )
    admin_bridge.start_lifecycle_run(
        run_id="fetch_ready_should_ignore_1",
        task_type="fetch",
        started_at="2026-05-07T10:00:00+00:00",
    )

    health = admin_bridge._get_ops_api().compute_ops_health_ready()

    assert health["service"] == "baluffo-bridge"
    assert health["status"] == "healthy"
    assert health["ok"] is True
    assert health["detailLevel"] == "ready"
    assert health["lifecycle"]["currentCount"] == 0
    assert health["schedule"] == {}


def test_dashboard_health_uses_lightweight_registry_summary_without_full_state() -> None:
    deps = SimpleNamespace(
        get_history=lambda: [],
        get_fetch_report=lambda: {},
        get_state=lambda: (_ for _ in ()).throw(AssertionError("loaded full registry state")),
        get_registry_summary_payload=lambda: {
            "generation": "sqlite-generation-1",
            "activeCount": 3,
            "pendingCount": 2,
            "rejectedCount": 1,
            "tombstoneCount": 4,
            "stateHash": "state",
            "tombstoneHash": "tombstone",
        },
        get_tombstones=lambda: {},
        get_sync_status_payload=lambda: {},
        now_iso=lambda: "2026-05-14T10:00:00+00:00",
        desktop_mode=True,
        desktop_last_activity_at="2026-05-14T10:00:00+00:00",
        owner_state={"startedAt": "2026-05-14T09:59:00+00:00"},
        load_alert_state_fn=lambda: {},
        save_alert_state_fn=lambda _payload: None,
        parse_schedule_metadata_fn=lambda: {"fetcher": {}, "discovery": {}},
        parse_iso=lambda _value: None,
        now_utc=lambda: datetime(2026, 5, 14, 10, 0, tzinfo=UTC),
        get_source_policy_soak_report=lambda: {},
        get_updater_status_payload=lambda: {},
        app_version="0.0.0-test",
        startup_ready=True,
    )

    health = ops_health.compute_ops_health(deps)
    kpis = health["kpis"]

    assert kpis["pendingApprovalsCount"] == 2
    assert kpis["registrySync"]["activeCount"] == 3
    assert kpis["registrySync"]["pendingCount"] == 2
    assert kpis["registrySync"]["rejectedCount"] == 1
    assert kpis["registrySync"]["tombstoneCount"] == 4


def test_dashboard_health_uses_json_summary_without_full_state() -> None:
    deps = SimpleNamespace(
        get_history=lambda: [],
        get_fetch_report=lambda: {},
        get_state=lambda: (_ for _ in ()).throw(AssertionError("loaded full registry state")),
        get_registry_summary_payload=lambda: {
            "generation": "",
            "reason": "json_summary",
            "activeCount": 7,
            "pendingCount": 5,
            "rejectedCount": 2,
            "tombstoneCount": 1,
            "summaryExact": False,
            "stateFingerprint": "json-artifacts",
        },
        get_tombstones=lambda: {},
        get_sync_status_payload=lambda: {},
        now_iso=lambda: "2026-05-14T10:00:00+00:00",
        desktop_mode=True,
        desktop_last_activity_at="2026-05-14T10:00:00+00:00",
        owner_state={"startedAt": "2026-05-14T09:59:00+00:00"},
        load_alert_state_fn=lambda: {},
        save_alert_state_fn=lambda _payload: None,
        parse_schedule_metadata_fn=lambda: {"fetcher": {}, "discovery": {}},
        parse_iso=lambda _value: None,
        now_utc=lambda: datetime(2026, 5, 14, 10, 0, tzinfo=UTC),
        get_source_policy_soak_report=lambda: {},
        get_updater_status_payload=lambda: {},
        app_version="0.0.0-test",
        startup_ready=True,
    )

    health = ops_health.compute_ops_health(deps)
    kpis = health["kpis"]

    assert kpis["pendingApprovalsCount"] == 5
    assert kpis["registrySync"]["activeCount"] == 7
    assert kpis["registrySync"]["pendingCount"] == 5
    assert kpis["registrySync"]["rejectedCount"] == 2
    assert kpis["registrySync"]["tombstoneCount"] == 1


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
    health = admin_bridge.compute_ops_dashboard_health()
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
    health = admin_bridge.compute_ops_dashboard_health()

    guidance = next(
        (alert for alert in health.get("alerts", []) if alert.get("id") == "fetch_never_run"),
        None,
    )

    assert guidance is not None
    assert guidance["severity"] == "warning"
    assert guidance["dismissible"] is False
    assert "Run Jobs Fetcher" in guidance["message"]


def test_evaluate_alerts_keeps_pipeline_never_run_after_bootstrap_fetch() -> None:
    saved_states: list[dict[str, Any]] = []

    result = ops_health.evaluate_alerts(
        history=[
            {
                "type": "fetch",
                "status": "ok",
                "finishedAt": "2026-05-17T10:00:00+00:00",
                "summary": {"outputCount": 12, "coverageScope": "bootstrap_sheets"},
            }
        ],
        latest_fetch_report={
            "finishedAt": "2026-05-17T10:00:00+00:00",
            "summary": {"outputCount": 12, "failedSources": 0, "sourceCount": 3},
        },
        pending_count=0,
        load_alert_state_fn=lambda: {"acked": {"pipeline_never_run": "2026-05-17T10:01:00Z"}},
        save_alert_state_fn=lambda state: saved_states.append(state),
        parse_iso=admin_bridge.parse_iso,
        now_iso=lambda: "2026-05-17T10:02:00+00:00",
        now_utc=lambda: datetime(2026, 5, 17, 10, 2, tzinfo=UTC),
    )

    guidance = next(
        (alert for alert in result["alerts"] if alert.get("id") == "pipeline_never_run"),
        None,
    )
    assert guidance is not None
    assert guidance["dismissible"] is False
    assert "pipeline_never_run" not in saved_states[-1]["acked"]


def test_evaluate_alerts_clears_pipeline_never_run_after_full_pipeline_success() -> None:
    result = ops_health.evaluate_alerts(
        history=[
            {
                "type": "pipeline",
                "status": "ok",
                "finishedAt": "2026-05-17T10:00:00+00:00",
            }
        ],
        latest_fetch_report={
            "finishedAt": "2026-05-17T10:00:00+00:00",
            "summary": {"outputCount": 12, "failedSources": 0, "sourceCount": 3},
        },
        pending_count=0,
        load_alert_state_fn=lambda: {},
        save_alert_state_fn=lambda _state: None,
        parse_iso=admin_bridge.parse_iso,
        now_iso=lambda: "2026-05-17T10:02:00+00:00",
        now_utc=lambda: datetime(2026, 5, 17, 10, 2, tzinfo=UTC),
    )

    alert_ids = {alert["id"] for alert in result["alerts"]}
    assert "pipeline_never_run" not in alert_ids


def test_evaluate_alerts_excludes_bootstrap_runs_from_output_drop_baseline() -> None:
    history = [
        {
            "type": "fetch",
            "status": "ok",
            "finishedAt": f"2026-05-17T0{idx}:00:00+00:00",
            "summary": {"outputCount": output},
        }
        for idx, output in enumerate([100, 105, 98], start=1)
    ]
    history.append(
        {
            "type": "fetch",
            "status": "ok",
            "finishedAt": "2026-05-17T04:00:00+00:00",
            "summary": {"outputCount": 8, "coverageScope": "bootstrap_sheets"},
        }
    )

    result = ops_health.evaluate_alerts(
        history=history,
        latest_fetch_report={
            "finishedAt": "2026-05-17T04:00:00+00:00",
            "summary": {
                "outputCount": 8,
                "failedSources": 0,
                "sourceCount": 3,
                "coverageScope": "bootstrap_sheets",
            },
        },
        pending_count=0,
        load_alert_state_fn=lambda: {},
        save_alert_state_fn=lambda _state: None,
        parse_iso=admin_bridge.parse_iso,
        now_iso=lambda: "2026-05-17T04:02:00+00:00",
        now_utc=lambda: datetime(2026, 5, 17, 4, 2, tzinfo=UTC),
    )

    alert_ids = {alert["id"] for alert in result["alerts"]}
    assert "output_drop" not in alert_ids


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
        health = admin_bridge.compute_ops_dashboard_health()

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

    health = admin_bridge.compute_ops_dashboard_health()

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
    health = admin_bridge.compute_ops_dashboard_health()
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

    health = admin_bridge.compute_ops_dashboard_health()

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

    health = admin_bridge.compute_ops_dashboard_health()

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
                        "staleCount": 0,
                        "blockedCount": 1,
                        "proposalGeneratedAt": "2026-05-01T00:00:00Z",
                        "proposalReportRunId": "fetch-123",
                        "proposalFreshnessStatus": "fresh",
                        "proposalFreshnessAgeSeconds": 0,
                        "proposalStaleThresholdSeconds": 86400,
                        "proposalReadinessHash": "abc123",
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
                                "proposalReadiness": "actionable",
                                "proposalReadinessReason": "proposal evidence is fresh and actionable",
                                "proposalFreshnessStatus": "fresh",
                                "proposalFreshnessAgeSeconds": 0,
                                "proposalGeneratedAt": "2026-05-01T00:00:00Z",
                                "proposalReportRunId": "fetch-123",
                                "proposalReadinessEvidence": [
                                    "proposal_freshness:fresh",
                                    "proposal_disposition:proposal_ready",
                                ],
                            }
                        ],
                        "blockedExamples": [
                            {
                                "staticSourceName": "Static Blocked",
                                "providerSourceName": "Blocked Provider",
                                "blockers": ["static_only_evidence_present"],
                                "proposalReadiness": "blocked",
                                "proposalReadinessReason": "static_only_evidence_present",
                                "proposalFreshnessStatus": "fresh",
                                "proposalFreshnessAgeSeconds": 0,
                                "proposalGeneratedAt": "2026-05-01T00:00:00Z",
                                "proposalReportRunId": "fetch-123",
                                "proposalReadinessEvidence": [
                                    "blocker:static_only_evidence_present",
                                    "proposal_disposition:blocked",
                                ],
                            }
                        ],
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    health = admin_bridge.compute_ops_dashboard_health()

    cleanup = health["kpis"]["conservativeStaticCleanupProposals"]
    assert cleanup["proposalCount"] == 1
    assert cleanup["staleCount"] == 0
    assert cleanup["blockedCount"] == 1
    assert cleanup["proposalGeneratedAt"] == "2026-05-01T00:00:00Z"
    assert cleanup["proposalReportRunId"] == "fetch-123"
    assert cleanup["proposalFreshnessStatus"] == "fresh"
    assert cleanup["proposalFreshnessAgeSeconds"] == 0
    assert cleanup["proposalStaleThresholdSeconds"] == 86400
    assert cleanup["proposalReadinessHash"] == "abc123"
    assert cleanup["blockedReasonCounts"]["static_only_evidence_present"] == 1
    assert (
        cleanup["proposalReadyExamples"][0]["recommendedAction"] == "move_static_to_hidden_pending"
    )
    assert cleanup["proposalReadyExamples"][0]["proposalReadiness"] == "actionable"
    assert cleanup["proposalReadyExamples"][0]["proposalFreshnessStatus"] == "fresh"
    assert cleanup["proposalReadyExamples"][0]["proposalReadinessReason"] == (
        "proposal evidence is fresh and actionable"
    )
    assert cleanup["blockedExamples"][0]["proposalReadiness"] == "blocked"
    assert cleanup["blockedExamples"][0]["proposalFreshnessStatus"] == "fresh"
    assert (
        cleanup["blockedExamples"][0]["proposalReadinessReason"] == "static_only_evidence_present"
    )


def test_compute_ops_health_exposes_dedup_review_state_summary(admin_bridge_entrypoint_root):
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
            "startedAt": "2026-05-02T00:00:00+00:00",
            "finishedAt": "2026-05-02T00:10:00+00:00",
            "summary": {"outputCount": 8, "failedSources": 0, "sourceCount": 1},
            "dedupEvidence": {
                "providerStaticDisagreementGateCounts": {
                    "blocked": 1,
                    "warning": 0,
                    "currentRunBlocked": 0,
                    "carriedBlocked": 1,
                    "carriedWarning": 0,
                    "autoSafeWarning": 0,
                    "locationPollutionWarning": 0,
                    "reviewedSafeWarning": 0,
                    "confirmedBlocking": 0,
                },
                "providerStaticDisagreementExamples": [
                    {
                        "title": "Executive Assistant",
                        "company": "Animoca Brands",
                        "dedupKey": "animoca-key-1",
                        "bundleEvidenceOrigin": "carried_from_existing_output",
                        "sourceBundleCount": 2,
                        "providerSourceJobIds": ["lever:animoca:123"],
                        "staticSourceJobIds": ["static:animoca:123"],
                        "providerSources": ["lever:animoca"],
                        "staticSources": ["static_source::animoca"],
                        "providerUrls": ["https://jobs.lever.co/animocabrands/123"],
                        "staticUrls": ["https://careers.animoca.com/jobs/123"],
                        "providerUrlHosts": ["jobs.lever.co"],
                        "staticUrlHosts": ["careers.animoca.com"],
                        "sharedIdentifierTokens": ["123"],
                        "distinctLocationCount": 1,
                        "sampleLocations": ["hong kong"],
                        "identityQuality": "provider_id_strong",
                        "disagreementClassification": "same_job_different_urls",
                        "disagreementGateDisposition": "blocked",
                        "disagreementGateEvidence": [
                            "carried_same_job_different_urls_requires_review"
                        ],
                    }
                ],
            },
        },
    )
    admin_bridge.DEDUP_REVIEW_STATE_PATH.write_text(
        """
{
  "pairs": {
    "review-key": {
      "disagreementClassification": "same_job_different_urls",
      "providerSourceJobIds": ["lever:animoca:123"],
      "staticSourceJobIds": ["static:animoca:123"],
      "dedupKey": "animoca-key-1",
      "reviewStatus": "reviewed_safe",
      "reviewedAt": "2026-05-02T10:00:00Z",
      "reviewedBy": "admin"
    }
  }
}
        """.strip(),
        encoding="utf-8",
    )

    health = admin_bridge.compute_ops_dashboard_health()

    review_state = health["kpis"]["dedupReviewState"]
    assert review_state["artifactPath"].endswith("dedup-review-state.json")
    assert review_state["status"] == "ok"
    assert review_state["readWarning"] == ""
    assert review_state["reviewedPairCount"] == 1
    assert review_state["reviewedSafeCount"] == 1
    assert review_state["confirmedBlockingCount"] == 0
    assert review_state["unresolvedBlockingCount"] == 0


def test_compute_ops_health_reports_missing_dedup_review_state_artifact(
    admin_bridge_entrypoint_root,
):
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
            "startedAt": "2026-05-02T00:00:00+00:00",
            "finishedAt": "2026-05-02T00:10:00+00:00",
            "summary": {"outputCount": 8, "failedSources": 0, "sourceCount": 1},
            "dedupEvidence": {
                "providerStaticDisagreementGateCounts": {
                    "blocked": 1,
                    "warning": 0,
                    "currentRunBlocked": 0,
                    "carriedBlocked": 1,
                    "carriedWarning": 0,
                    "autoSafeWarning": 0,
                    "locationPollutionWarning": 0,
                    "reviewedSafeWarning": 0,
                    "confirmedBlocking": 0,
                },
                "providerStaticDisagreementExamples": [
                    {
                        "title": "Executive Assistant",
                        "company": "Animoca Brands",
                        "dedupKey": "animoca-key-1",
                        "bundleEvidenceOrigin": "carried_from_existing_output",
                        "sourceBundleCount": 2,
                        "providerSourceJobIds": ["lever:animoca:123"],
                        "staticSourceJobIds": ["static:animoca:123"],
                        "providerSources": ["lever:animoca"],
                        "staticSources": ["static_source::animoca"],
                        "providerUrls": ["https://jobs.lever.co/animocabrands/123"],
                        "staticUrls": ["https://careers.animoca.com/jobs/123"],
                        "providerUrlHosts": ["jobs.lever.co"],
                        "staticUrlHosts": ["careers.animoca.com"],
                        "sharedIdentifierTokens": ["123"],
                        "distinctLocationCount": 1,
                        "sampleLocations": ["hong kong"],
                        "identityQuality": "provider_id_strong",
                        "disagreementClassification": "same_job_different_urls",
                        "disagreementGateDisposition": "blocked",
                        "disagreementGateEvidence": [
                            "carried_same_job_different_urls_requires_review"
                        ],
                    }
                ],
            },
        },
    )
    admin_bridge.DEDUP_REVIEW_STATE_PATH.unlink(missing_ok=True)

    health = admin_bridge.compute_ops_dashboard_health()

    review_state = health["kpis"]["dedupReviewState"]
    assert review_state["status"] == "warning"
    assert review_state["readWarning"] == "missing_dedup_review_state_artifact"
    assert review_state["reviewedPairCount"] == 0
    assert review_state["reviewedSafeCount"] == 0
    assert review_state["confirmedBlockingCount"] == 0
    assert review_state["unresolvedBlockingCount"] == 1


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
    initial = admin_bridge.compute_ops_dashboard_health()
    alert_ids = [row["id"] for row in initial.get("alerts", [])]
    assert "degraded_reliability" in alert_ids
    state = admin_bridge.load_alert_state()
    state["acked"]["degraded_reliability"] = admin_bridge.now_iso()
    admin_bridge.save_alert_state(state)
    updated = admin_bridge.compute_ops_dashboard_health()
    updated_ids = [row["id"] for row in updated.get("alerts", [])]
    assert "degraded_reliability" not in updated_ids
