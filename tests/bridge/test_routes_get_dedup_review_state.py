from __future__ import annotations

from pathlib import Path

from src.bridge.routes.get_routes import handle_get
from tests.bridge.test_routes_get import (
    FakeDesktopLocalDataStore,
    FakeHandler,
    make_stub_bridge_api,
)


def test_ops_fetch_report_merges_dedup_review_state_at_read_time(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.load_json_object = lambda path, default: {
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
                    "providerUrls": ["https://jobs.lever.co/animoca/123"],
                    "staticUrls": ["https://careers.animoca.com/jobs/123"],
                    "providerUrlHosts": ["jobs.lever.co"],
                    "staticUrlHosts": ["careers.animoca.com"],
                    "sharedIdentifierTokens": ["123"],
                    "distinctLocationCount": 1,
                    "sampleLocations": ["hong kong"],
                    "identityQuality": "provider_id_strong",
                    "disagreementClassification": "same_job_different_urls",
                    "disagreementGateDisposition": "blocked",
                    "disagreementGateEvidence": ["carried_same_job_different_urls_requires_review"],
                }
            ],
            "providerStaticTitleCompanyCollisionExamples": [],
            "providerStaticDisagreementCounts": {"total": 1, "currentRun": 0, "carried": 1},
            "providerStaticDisagreementClassificationCounts": {
                "same_job_different_urls": 1,
                "provider_redirect_or_canonical_url": 0,
                "static_parser_url_variant": 0,
                "title_company_collision": 0,
                "stale_carried_bundle": 0,
                "needs_manual_review": 0,
            },
            "providerStaticTitleCompanyCollisionAuditCounts": {
                "carried_location_pollution": 0,
                "carried_location_variant": 0,
                "possible_real_multi_location_conflict": 0,
                "not_carried": 0,
                "unknown": 0,
            },
            "reviewQueueCauseCounts": {},
            "riskReasonCounts": {},
            "outlierReasonCounts": {},
            "mergedCount": 0,
            "sourceBundleCollisionCount": 0,
            "currentRunSourceBundleCollisionCount": 0,
            "carriedSourceBundleCollisionCount": 0,
            "currentRunHighRiskReviewQueueCount": 0,
            "carriedHighRiskReviewQueueCount": 0,
        }
    }
    api.DEDUP_REVIEW_STATE_PATH.write_text(
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

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/fetch-report", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    payload = handler.sent[-1]["payload"]
    row = payload["dedupEvidence"]["providerStaticDisagreementExamples"][0]
    assert row["dedupReviewStatus"] == "reviewed_safe"
    assert row["disagreementGateDisposition"] == "warning"
    assert payload["dedupEvidence"]["providerStaticDisagreementGateCounts"]["blocked"] == 0
    assert payload["dedupEvidence"]["dedupAuditGate"]["lifecycleUxReady"] is True


def test_ops_fetch_report_tolerates_malformed_dedup_review_state(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.load_json_object = lambda path, default: {
        "dedupEvidence": {"providerStaticDisagreementExamples": []}
    }
    api.DEDUP_REVIEW_STATE_PATH.write_text("{bad json", encoding="utf-8")

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/fetch-report", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    payload = handler.sent[-1]["payload"]
    assert payload["dedupReviewStateReadWarning"] == "malformed_dedup_review_state_artifact"


def test_ops_fetch_report_tolerates_missing_dedup_review_state(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.load_json_object = lambda path, default: {
        "dedupEvidence": {"providerStaticDisagreementExamples": []}
    }
    api.DEDUP_REVIEW_STATE_PATH.unlink(missing_ok=True)

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/fetch-report", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    payload = handler.sent[-1]["payload"]
    assert payload["dedupReviewStateReadWarning"] == "missing_dedup_review_state_artifact"
