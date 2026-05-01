import json
from pathlib import Path

from src import admin_bridge
from src.bridge.routes.get_routes import handle_get
from src.bridge.source_policy_migration_links import ADMIN_MIGRATION_LINK_ACTOR
from tests.helpers.bridge_api import FakeHandler

PROVIDER_ID = "greenhouse:slug:studio"
STATIC_ID = "static:listing_url:https://studio.example/jobs"


def _api():
    return admin_bridge.build_bridge_api(admin_bridge.RUNTIME_CONFIG)


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _soak_report_path(data_root: Path) -> Path:
    return data_root.parent / "_out" / "source-policy-soak-report.json"


def _get_recommendations_payload() -> dict:
    handler = FakeHandler()
    handled = handle_get(
        handler,
        api=_api(),
        path="/source-policy/recommendations",
        query={},
    )
    assert handled is True
    assert handler.sent[-1]["status"] == 200
    return handler.sent[-1]["payload"]


def test_source_policy_recommendations_includes_link_review_candidates(
    admin_bridge_entrypoint_root,
) -> None:
    admin_bridge.save_json_atomic(
        admin_bridge.ACTIVE_PATH,
        [
            {
                "id": PROVIDER_ID,
                "name": "Studio Greenhouse",
                "adapter": "greenhouse",
                "migrationSourceIdentity": STATIC_ID,
                "migrationLinkedBy": ADMIN_MIGRATION_LINK_ACTOR,
                "registryState": "active",
            },
            {
                "id": STATIC_ID,
                "name": "Studio Static",
                "adapter": "static",
                "registryState": "active",
            },
        ],
    )
    _write_json(
        admin_bridge.SOURCE_POLICY_RECOMMENDATIONS_PATH,
        {"schemaVersion": 1, "pairs": []},
    )
    _write_json(admin_bridge.SOURCE_POLICY_REVIEW_STATE_PATH, {"schemaVersion": 1, "pairs": {}})
    _write_json(
        _soak_report_path(admin_bridge_entrypoint_root),
        {
            "sections": {
                "providerCoverageLinkBackfill": {
                    "activeProviderWithoutMigrationIdentityCount": 4,
                    "candidateLinkCount": 1,
                    "highConfidenceLinkCount": 0,
                    "mediumConfidenceLinkCount": 1,
                    "reviewCandidates": [
                        {
                            "providerSourceId": PROVIDER_ID,
                            "providerSourceName": "Studio Greenhouse",
                            "selectedStaticSourceId": STATIC_ID,
                            "selectedStaticSourceName": "Studio Static",
                            "confidence": 0.8,
                            "apiEligible": True,
                            "recommendedApiPayload": {
                                "action": "apply_migration_identity_link",
                                "providerSourceId": PROVIDER_ID,
                                "staticSourceId": STATIC_ID,
                                "staticSourceName": "Studio Static",
                                "confidence": 0.8,
                                "reasons": ["source_state_disambiguation"],
                                "recommendationSource": "provider_coverage_link_backfill",
                                "recommendedAction": "needs_review",
                            },
                        }
                    ],
                }
            }
        },
    )

    payload = _get_recommendations_payload()

    link_backfill = payload["providerCoverageLinkBackfill"]
    assert link_backfill["mediumConfidenceLinkCount"] == 1
    candidate = link_backfill["reviewCandidates"][0]
    assert candidate["providerSourceId"] == PROVIDER_ID
    assert candidate["currentProviderLinkState"] == {
        "providerBucket": "active",
        "migrationSourceIdentity": STATIC_ID,
        "migrationLinkedBy": ADMIN_MIGRATION_LINK_ACTOR,
        "adminBackfillOwned": True,
    }


def test_source_policy_recommendations_tolerates_missing_and_malformed_soak_report(
    admin_bridge_entrypoint_root,
) -> None:
    _write_json(
        admin_bridge.SOURCE_POLICY_RECOMMENDATIONS_PATH,
        {"schemaVersion": 1, "pairs": []},
    )
    _write_json(admin_bridge.SOURCE_POLICY_REVIEW_STATE_PATH, {"schemaVersion": 1, "pairs": {}})
    path = _soak_report_path(admin_bridge_entrypoint_root)
    if path.exists():
        path.unlink()

    missing_payload = _get_recommendations_payload()
    assert missing_payload["providerCoverageLinkBackfill"]["reviewCandidates"] == []
    assert missing_payload["suppressionEligibility"]["missingLinkedStaticRows"] == []
    assert missing_payload["warnings"] == []

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not-json", encoding="utf-8")

    malformed_payload = _get_recommendations_payload()

    assert malformed_payload["providerCoverageLinkBackfill"]["reviewCandidates"] == []
    assert malformed_payload["suppressionEligibility"]["missingLinkedStaticRows"] == []
    assert any(
        "source_policy_soak_report_unreadable" in item for item in malformed_payload["warnings"]
    )


def test_source_policy_recommendations_includes_suppression_eligibility(
    admin_bridge_entrypoint_root,
) -> None:
    _write_json(
        admin_bridge.SOURCE_POLICY_RECOMMENDATIONS_PATH,
        {"schemaVersion": 1, "pairs": []},
    )
    _write_json(admin_bridge.SOURCE_POLICY_REVIEW_STATE_PATH, {"schemaVersion": 1, "pairs": {}})
    _write_json(
        _soak_report_path(admin_bridge_entrypoint_root),
        {
            "sections": {
                "suppressionEligibility": {
                    "readyLinkedProviderCount": 1,
                    "selectedLinkedStaticCount": 0,
                    "missingLinkedStaticCount": 1,
                    "suppressedLinkedStaticCount": 0,
                    "missingLinkedStaticRows": [
                        {
                            "providerSourceId": PROVIDER_ID,
                            "providerSourceName": "Studio Greenhouse",
                            "migrationSourceIdentity": STATIC_ID,
                            "migrationSourceName": "Studio Static",
                            "providerCoverageStatus": "validated_provider",
                            "providerCoverageConsecutiveSuccesses": 2,
                            "providerCoverageLatestKeptCount": 4,
                            "providerReplacementReadiness": "ready_later",
                            "reason": "linked_static_not_in_default_loader_set",
                            "selectionReason": "linked_static_not_in_default_loader_set",
                            "registryBucket": "active",
                            "registryState": "active",
                            "adapter": "static",
                            "expectedLoaderName": "static_source::static:studio",
                            "linkedStaticRegistryBucket": "active",
                            "linkedStaticRegistryState": "active",
                            "linkedStaticAdapter": "static",
                            "expectedStaticLoaderName": "static_source::static:studio",
                            "generatedStaticLoaderName": "static_source::static:studio",
                            "actualSourceRowName": "",
                            "registrySourceIdentity": "static:studio",
                            "registryId": "static:studio",
                            "registryName": "Studio Static",
                            "registryListingUrl": "https://studio.example/jobs",
                            "possibleLoaderNames": ["static_source::static:studio"],
                            "loaderNameMatchStatus": "loader_not_generated",
                            "loaderNotGeneratedReason": "redundant_static_rule_filtered",
                            "foundInActiveRegistry": True,
                            "foundInSourceRows": False,
                            "linkedStaticFoundInRegistry": True,
                            "linkedStaticFoundInSourceRows": False,
                            "linkedStaticFoundInSelectedSources": False,
                        }
                    ],
                }
            }
        },
    )

    payload = _get_recommendations_payload()

    eligibility = payload["suppressionEligibility"]
    assert eligibility["readyLinkedProviderCount"] == 1
    assert eligibility["missingLinkedStaticCount"] == 1
    row = eligibility["missingLinkedStaticRows"][0]
    assert row["reason"] == "linked_static_not_in_default_loader_set"
    assert row["selectionReason"] == "linked_static_not_in_default_loader_set"
    assert row["expectedLoaderName"] == "static_source::static:studio"
    assert row["expectedStaticLoaderName"] == "static_source::static:studio"
    assert row["generatedStaticLoaderName"] == "static_source::static:studio"
    assert row["loaderNameMatchStatus"] == "loader_not_generated"
    assert row["loaderNotGeneratedReason"] == "redundant_static_rule_filtered"
    assert row["linkedStaticRegistryBucket"] == "active"
    assert row["linkedStaticFoundInSelectedSources"] is False


def test_source_policy_recommendations_includes_admin_owned_registry_link_without_soak(
    admin_bridge_entrypoint_root,
) -> None:
    admin_bridge.save_json_atomic(
        admin_bridge.ACTIVE_PATH,
        [
            {
                "id": PROVIDER_ID,
                "name": "Studio Greenhouse",
                "adapter": "greenhouse",
                "migrationSourceIdentity": STATIC_ID,
                "migrationSourceName": "Studio Static",
                "migrationLinkedBy": ADMIN_MIGRATION_LINK_ACTOR,
                "registryState": "active",
            },
            {
                "id": STATIC_ID,
                "name": "Studio Static",
                "adapter": "static",
                "registryState": "active",
            },
        ],
    )
    _write_json(
        admin_bridge.SOURCE_POLICY_RECOMMENDATIONS_PATH,
        {"schemaVersion": 1, "pairs": []},
    )
    _write_json(admin_bridge.SOURCE_POLICY_REVIEW_STATE_PATH, {"schemaVersion": 1, "pairs": {}})
    path = _soak_report_path(admin_bridge_entrypoint_root)
    if path.exists():
        path.unlink()

    payload = _get_recommendations_payload()

    linked = payload["providerCoverageLinkBackfill"]["linkedCandidates"]
    assert len(linked) == 1
    assert linked[0]["providerSourceId"] == PROVIDER_ID
    assert linked[0]["staticSourceId"] == STATIC_ID
    assert linked[0]["adminBackfillOwned"] is True
    assert linked[0]["migrationLinkedBy"] == ADMIN_MIGRATION_LINK_ACTOR


def test_source_policy_recommendations_includes_soak_already_linked_rows(
    admin_bridge_entrypoint_root,
) -> None:
    admin_bridge.save_json_atomic(
        admin_bridge.ACTIVE_PATH,
        [
            {
                "id": PROVIDER_ID,
                "name": "Studio Greenhouse",
                "adapter": "greenhouse",
                "registryState": "active",
            }
        ],
    )
    _write_json(
        admin_bridge.SOURCE_POLICY_RECOMMENDATIONS_PATH,
        {"schemaVersion": 1, "pairs": []},
    )
    _write_json(admin_bridge.SOURCE_POLICY_REVIEW_STATE_PATH, {"schemaVersion": 1, "pairs": {}})
    _write_json(
        _soak_report_path(admin_bridge_entrypoint_root),
        {
            "sections": {
                "providerCoverageLinkBackfill": {
                    "links": [
                        {
                            "providerSourceId": PROVIDER_ID,
                            "providerSourceName": "Studio Greenhouse",
                            "providerAdapter": "greenhouse",
                            "staticSourceId": STATIC_ID,
                            "staticSourceName": "Studio Static",
                            "recommendedAction": "already_linked",
                        }
                    ]
                }
            }
        },
    )

    payload = _get_recommendations_payload()

    linked = payload["providerCoverageLinkBackfill"]["linkedCandidates"]
    assert len(linked) == 1
    assert linked[0]["providerSourceId"] == PROVIDER_ID
    assert linked[0]["staticSourceId"] == STATIC_ID
    assert linked[0]["adminBackfillOwned"] is False


def test_source_policy_recommendations_marks_non_admin_owned_links_not_clearable(
    admin_bridge_entrypoint_root,
) -> None:
    admin_bridge.save_json_atomic(
        admin_bridge.ACTIVE_PATH,
        [
            {
                "id": PROVIDER_ID,
                "name": "Studio Greenhouse",
                "adapter": "greenhouse",
                "migrationSourceIdentity": STATIC_ID,
                "migrationSourceName": "Studio Static",
                "migrationLinkedBy": "manual_import",
                "registryState": "active",
            }
        ],
    )
    _write_json(
        admin_bridge.SOURCE_POLICY_RECOMMENDATIONS_PATH,
        {"schemaVersion": 1, "pairs": []},
    )
    _write_json(admin_bridge.SOURCE_POLICY_REVIEW_STATE_PATH, {"schemaVersion": 1, "pairs": {}})

    payload = _get_recommendations_payload()

    linked = payload["providerCoverageLinkBackfill"]["linkedCandidates"]
    assert len(linked) == 1
    assert linked[0]["migrationLinkedBy"] == "manual_import"
    assert linked[0]["adminBackfillOwned"] is False
