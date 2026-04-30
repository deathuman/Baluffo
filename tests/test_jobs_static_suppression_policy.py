import copy
import json
from pathlib import Path

from src import jobs_fetcher as jf
from src.bridge.report_normalizer import normalize_fetch_report_contract
from src.fetcher_metrics import build_metrics
from src.jobs.common.contracts_fetch_report import normalize_fetch_report_payload
from src.jobs.common.contracts_static_suppression_policy import (
    normalize_prior_static_suppression_evidence,
    normalize_static_suppression_policy_payload,
)
from src.jobs.common.registry_defaults import REDUNDANT_STATIC_IF_PROVIDER
from src.jobs.pipeline_loader_selection import apply_dynamic_redundant_static_exclusions
from tests.helpers.temp_paths import workspace_tmpdir

STATIC_SOURCE_NAME = "static_source::static:listing_url:https://studio.example/jobs"
MIGRATION_SOURCE_IDENTITY = "static:listing_url:https://studio.example/jobs"
PROVIDER_SOURCE_NAME = "Studio Greenhouse"


def _eligible_provider_state(**overrides):
    row = {
        "lastAdapter": "greenhouse",
        "providerCoverageStatus": "validated_provider",
        "providerCoverageConsecutiveSuccesses": 2,
        "providerCoverageLatestKeptCount": 3,
        "migrationSourceIdentity": MIGRATION_SOURCE_IDENTITY,
    }
    row.update(overrides)
    return {PROVIDER_SOURCE_NAME: row}


def _excluded_report(name, reason):
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


def _prior_report(audit_status, *, static_only=0, reasons=None):
    return {
        "providerStaticOverlap": {
            "pairs": [
                {
                    "staticSourceId": MIGRATION_SOURCE_IDENTITY,
                    "staticSourceName": STATIC_SOURCE_NAME,
                    "providerSourceId": PROVIDER_SOURCE_NAME,
                    "providerSourceName": PROVIDER_SOURCE_NAME,
                    "providerCoverageStatus": "validated_provider",
                    "providerConsecutiveSuccesses": 2,
                    "latestProviderKeptCount": 3,
                    "auditStatus": audit_status,
                    "auditReasons": list(reasons or []),
                    "staticOnlyCount": static_only,
                    "overlapCount": 1 if audit_status == "safe" else 0,
                }
            ]
        }
    }


def _apply_with_prior(prior_report=None):
    filtered, excluded, policy = apply_dynamic_redundant_static_exclusions(
        [
            ("greenhouse_boards", lambda **_: []),
            (STATIC_SOURCE_NAME, lambda **_: []),
        ],
        source_state_rows=_eligible_provider_state(),
        build_excluded_source_report=_excluded_report,
        source_report_meta={"greenhouse_boards": {"adapter": "greenhouse"}},
        prior_static_suppression_evidence=normalize_prior_static_suppression_evidence(
            prior_report or {}
        ),
    )
    return [name for name, _loader in filtered], excluded, policy


def test_prior_safe_or_missing_audit_keeps_suppression_active():
    safe_filtered, safe_excluded, safe_policy = _apply_with_prior(_prior_report("safe"))
    missing_filtered, missing_excluded, missing_policy = _apply_with_prior()

    assert safe_filtered == ["greenhouse_boards"]
    assert safe_excluded[0]["exclusionReason"] == "dynamic_redundant_provider"
    assert safe_policy["suppressedPairs"][0]["reason"] == "prior_audit_safe"
    assert missing_filtered == ["greenhouse_boards"]
    assert missing_excluded[0]["exclusionReason"] == "dynamic_redundant_provider"
    assert missing_policy["suppressedPairs"][0]["reason"] == "missing_prior_evidence"


def test_prior_insufficient_history_suppresses_with_warning():
    filtered, excluded, policy = _apply_with_prior(_prior_report("insufficient_history"))

    assert filtered == ["greenhouse_boards"]
    assert excluded[0]["exclusionReason"] == "dynamic_redundant_provider"
    assert policy["warningCount"] == 1
    assert policy["warningPairs"][0]["reason"] == "prior_insufficient_history"
    assert policy["warningPairs"][0]["lastAuditStatus"] == "insufficient_history"


def test_prior_unsafe_audit_pauses_suppression():
    for prior in (
        _prior_report("needs_review"),
        _prior_report("provider_unstable"),
        _prior_report("safe", static_only=1),
        _prior_report("safe", reasons=["static_only_jobs_detected"]),
    ):
        filtered, excluded, policy = _apply_with_prior(prior)

        assert filtered == ["greenhouse_boards", STATIC_SOURCE_NAME]
        assert excluded == []
        assert policy["pausedCount"] == 1
        assert policy["pausedPairs"][0]["decision"] == "paused"


def test_pipeline_prior_pause_runs_static_without_dynamic_excluded_row_and_preserves_rules():
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
        return [
            {
                "title": "Static Only Designer",
                "company": "Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://studio.example/jobs/static-only-designer",
                "sector": "Game",
                "sourceJobId": "static-1",
            }
        ]

    previous_default_loaders = jf.default_source_loaders
    try:
        with workspace_tmpdir("jobs-fetcher-static-suppression-policy-paused") as tmp:
            out = Path(tmp)
            source_state = {
                "schemaVersion": jf.SCHEMA_VERSION,
                "sources": {
                    **_eligible_provider_state(),
                    STATIC_SOURCE_NAME: {"lastKeptCount": 2},
                },
            }
            (out / "jobs-source-state.json").write_text(json.dumps(source_state), encoding="utf-8")
            (out / "jobs-fetch-report.json").write_text(
                json.dumps(_prior_report("needs_review")), encoding="utf-8"
            )
            jf.default_source_loaders = lambda **_: [
                ("greenhouse_boards", provider_loader),
                (STATIC_SOURCE_NAME, static_loader),
            ]

            report = jf.run_pipeline(output_dir=out, show_progress=False, force_refresh_all=True)

        assert calls == {"provider": 1, "static": 1}
        static_rows = [row for row in report["sources"] if row["name"] == STATIC_SOURCE_NAME]
        assert static_rows and static_rows[0]["status"] == "ok"
        assert static_rows[0].get("exclusionReason", "") != "dynamic_redundant_provider"
        assert report["staticSuppressionPolicy"]["pausedCount"] == 1
        assert (
            report["staticSuppressionPolicy"]["pausedPairs"][0]["lastAuditStatus"] == "needs_review"
        )
        assert REDUNDANT_STATIC_IF_PROVIDER == redundant_rules
    finally:
        jf.default_source_loaders = previous_default_loaders


def test_static_suppression_policy_normalizes_through_report_bridge_and_metrics():
    payload = {
        "summary": {"sourceCount": 1},
        "sources": [],
        "staticSuppressionPolicy": {
            "eligibleCount": 2,
            "suppressedPairs": [
                {
                    "staticSourceName": STATIC_SOURCE_NAME,
                    "providerSourceName": PROVIDER_SOURCE_NAME,
                    "decision": "suppressed",
                    "reason": "prior_audit_safe",
                    "lastAuditStatus": "safe",
                }
            ],
            "warningPairs": [
                {
                    "staticSourceName": "static_source::warning",
                    "providerSourceName": PROVIDER_SOURCE_NAME,
                    "decision": "warning",
                    "reason": "prior_insufficient_history",
                    "lastAuditStatus": "insufficient_history",
                }
            ],
        },
    }

    normalized = normalize_fetch_report_payload(payload)
    bridge = normalize_fetch_report_contract(payload)
    metrics = build_metrics(payload, [], window=5)
    direct = normalize_static_suppression_policy_payload(payload["staticSuppressionPolicy"])

    assert normalized["staticSuppressionPolicy"]["suppressedCount"] == 1
    assert bridge["staticSuppressionPolicy"]["warningCount"] == 1
    assert metrics["latestRun"]["staticSuppressionPolicy"]["eligibleCount"] == 2
    assert direct["warningPairs"][0]["decision"] == "warning"
