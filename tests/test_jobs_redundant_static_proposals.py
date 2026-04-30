import copy
import json
from pathlib import Path

from src import jobs_fetcher as jf
from src.bridge.report_normalizer import normalize_fetch_report_contract
from src.fetcher_metrics import build_metrics
from src.jobs.common.contracts_fetch_report import normalize_fetch_report_payload
from src.jobs.common.contracts_redundant_static_proposals import (
    build_redundant_static_proposals_summary,
    normalize_redundant_static_proposals_payload,
)
from src.jobs.common.registry_defaults import REDUNDANT_STATIC_IF_PROVIDER
from tests.helpers.temp_paths import workspace_tmpdir

STATIC_SOURCE_NAME = "static_source::static:listing_url:https://studio.example/jobs"
STATIC_SOURCE_ID = "static:listing_url:https://studio.example/jobs"
PROVIDER_SOURCE_NAME = "Studio Greenhouse"


def _policy_pair(**overrides):
    row = {
        "staticSourceId": STATIC_SOURCE_ID,
        "staticSourceName": STATIC_SOURCE_NAME,
        "providerSourceId": PROVIDER_SOURCE_NAME,
        "providerSourceName": PROVIDER_SOURCE_NAME,
        "decision": "suppressed",
        "reason": "prior_audit_safe",
        "lastAuditStatus": "safe",
        "providerCoverageStatus": "validated_provider",
        "providerCoverageConsecutiveSuccesses": 2,
        "providerCoverageLatestKeptCount": 4,
        "auditReasons": ["provider_validated_repeated_success"],
        "staticOnlyCount": 0,
        "overlapCount": 1,
    }
    row.update(overrides)
    return row


def _overlap_pair(**overrides):
    row = {
        "staticSourceId": STATIC_SOURCE_ID,
        "staticSourceName": STATIC_SOURCE_NAME,
        "providerSourceId": PROVIDER_SOURCE_NAME,
        "providerSourceName": PROVIDER_SOURCE_NAME,
        "providerCoverageStatus": "validated_provider",
        "providerConsecutiveSuccesses": 2,
        "latestProviderKeptCount": 4,
        "auditStatus": "safe",
        "auditReasons": ["provider_validated_repeated_success"],
        "staticOnlyCount": 0,
        "overlapCount": 1,
    }
    row.update(overrides)
    return row


def _coverage(**overrides):
    row = {
        "name": PROVIDER_SOURCE_NAME,
        "providerCoverageStatus": "validated_provider",
        "providerCoverageConsecutiveSuccesses": 2,
        "providerCoverageLatestKeptCount": 4,
        "migrationSourceIdentity": STATIC_SOURCE_ID,
    }
    row.update(overrides)
    return {
        "totalProviderCandidates": 1,
        "statusCounts": {row["providerCoverageStatus"]: 1},
        "validatedProviders": [row]
        if row["providerCoverageStatus"] == "validated_provider"
        else [],
        "unstableOrFailedProviders": [row]
        if row["providerCoverageStatus"] in {"unstable_provider", "failed_provider"}
        else [],
        "needsReviewProviders": [row] if row["providerCoverageStatus"] == "needs_review" else [],
        "probingProviders": [],
        "readyLaterProviders": [],
    }


def _summary(*, policy_pairs=None, overlap_pairs=None, coverage=None):
    return build_redundant_static_proposals_summary(
        static_suppression_policy={
            "suppressedPairs": list(policy_pairs or []),
            "pausedPairs": [],
            "warningPairs": [],
        },
        provider_static_overlap={"pairs": list(overlap_pairs or [])},
        provider_coverage=coverage or _coverage(),
    )


def _first_proposal(summary):
    assert summary["totalProposalCount"] == 1
    proposal = summary["proposals"][0]
    assert proposal["destructiveActionAllowed"] is False
    return proposal


def test_safe_suppression_and_safe_overlap_proposes_safe_redundant_static():
    proposal = _first_proposal(
        _summary(policy_pairs=[_policy_pair()], overlap_pairs=[_overlap_pair()])
    )

    assert proposal["proposal"] == "safe_redundant_static"
    assert proposal["recommendedAction"] == "keep_runtime_suppression"
    assert proposal["confidence"] == 0.9


def test_insufficient_history_proposes_needs_more_history():
    proposal = _first_proposal(
        _summary(
            policy_pairs=[_policy_pair(lastAuditStatus="insufficient_history", overlapCount=0)],
            overlap_pairs=[
                _overlap_pair(auditStatus="insufficient_history", overlapCount=0, auditReasons=[])
            ],
        )
    )

    assert proposal["proposal"] == "needs_more_history"
    assert proposal["recommendedAction"] == "collect_more_history"


def test_static_only_evidence_proposes_static_only_detected():
    count_proposal = _first_proposal(
        _summary(overlap_pairs=[_overlap_pair(auditStatus="needs_review", staticOnlyCount=1)])
    )
    reason_proposal = _first_proposal(
        _summary(
            overlap_pairs=[
                _overlap_pair(
                    auditStatus="safe",
                    auditReasons=["static_only_jobs_detected"],
                    staticOnlyCount=0,
                )
            ]
        )
    )

    assert count_proposal["proposal"] == "static_only_jobs_detected"
    assert reason_proposal["proposal"] == "static_only_jobs_detected"


def test_provider_unstable_evidence_proposes_provider_unstable():
    for provider_status in ("unstable_provider", "failed_provider", "needs_review"):
        proposal = _first_proposal(
            _summary(
                overlap_pairs=[
                    _overlap_pair(
                        providerCoverageStatus=provider_status,
                        auditStatus="safe",
                    )
                ],
                coverage=_coverage(providerCoverageStatus=provider_status),
            )
        )
        assert proposal["proposal"] == "provider_unstable"
    audit_proposal = _first_proposal(
        _summary(overlap_pairs=[_overlap_pair(auditStatus="provider_unstable")])
    )
    assert audit_proposal["proposal"] == "provider_unstable"


def test_keep_static_requires_evaluated_pair_and_skips_unlinked_rows():
    keep_proposal = _first_proposal(
        _summary(
            overlap_pairs=[
                _overlap_pair(
                    providerCoverageStatus="untested",
                    auditStatus="safe",
                    providerConsecutiveSuccesses=0,
                    latestProviderKeptCount=0,
                )
            ],
            coverage={"validatedProviders": [], "probingProviders": []},
        )
    )
    empty = build_redundant_static_proposals_summary(
        static_suppression_policy={},
        provider_static_overlap={},
        provider_coverage={
            "validatedProviders": [
                {
                    "name": "Unlinked Provider",
                    "providerCoverageStatus": "validated_provider",
                    "migrationSourceIdentity": "static:listing_url:https://unlinked.example/jobs",
                }
            ]
        },
    )

    assert keep_proposal["proposal"] == "keep_static"
    assert empty["totalProposalCount"] == 0
    assert empty["proposals"] == []


def test_redundant_static_proposals_normalize_through_report_bridge_and_metrics():
    payload = {
        "summary": {"sourceCount": 1},
        "sources": [],
        "redundantStaticProposals": {
            "proposals": [
                {
                    **_policy_pair(),
                    "proposal": "safe_redundant_static",
                    "confidence": 2,
                    "reasons": ["runtime_suppression_supported"],
                    "recommendedAction": "keep_runtime_suppression",
                    "destructiveActionAllowed": True,
                }
            ]
        },
    }

    normalized = normalize_fetch_report_payload(payload)
    bridge = normalize_fetch_report_contract(payload)
    metrics = build_metrics(payload, [], window=5)
    direct = normalize_redundant_static_proposals_payload(payload["redundantStaticProposals"])

    assert normalized["redundantStaticProposals"]["safeRedundantCount"] == 1
    assert bridge["redundantStaticProposals"]["totalProposalCount"] == 1
    assert metrics["latestRun"]["redundantStaticProposals"]["safeRedundantCount"] == 1
    assert direct["proposals"][0]["confidence"] == 1.0
    assert direct["proposals"][0]["destructiveActionAllowed"] is False


def test_pipeline_proposals_are_report_only_and_do_not_mutate_rules_or_source_rows():
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
        with workspace_tmpdir("jobs-fetcher-redundant-static-proposals") as tmp:
            out = Path(tmp)
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
                json.dumps({"providerStaticOverlap": {"pairs": [_overlap_pair()]}}),
                encoding="utf-8",
            )
            jf.default_source_loaders = lambda **_: [
                ("greenhouse_boards", provider_loader),
                (STATIC_SOURCE_NAME, lambda **_: []),
            ]

            report = jf.run_pipeline(output_dir=out, show_progress=False, force_refresh_all=True)

        assert calls == {"provider": 1}
        static_rows = [row for row in report["sources"] if row["name"] == STATIC_SOURCE_NAME]
        assert len(static_rows) == 1
        assert static_rows[0]["exclusionReason"] == "dynamic_redundant_provider"
        assert report["redundantStaticProposals"]["safeRedundantCount"] == 1
        assert all(row["name"] != "redundantStaticProposals" for row in report["sources"])
        assert REDUNDANT_STATIC_IF_PROVIDER == redundant_rules
    finally:
        jf.default_source_loaders = previous_default_loaders
