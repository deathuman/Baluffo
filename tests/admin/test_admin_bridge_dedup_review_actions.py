import json

from src import admin_bridge
from src.bridge.routes.post_routes import handle_post
from src.jobs.common.registry_defaults import REDUNDANT_STATIC_IF_PROVIDER
from tests.helpers.bridge_api import FakeHandler


def _api():
    api = admin_bridge.build_bridge_api(admin_bridge.RUNTIME_CONFIG)
    api.now_iso = lambda: "2026-05-02T10:00:00Z"
    return api


def _post(action_payload):
    handler = FakeHandler()
    handled = handle_post(
        handler,
        api=_api(),
        path="/dedup/review-action",
        payload=action_payload,
    )
    return handled, handler.sent[-1]


def _payload(action: str) -> dict:
    return {
        "action": action,
        "title": "Executive Assistant",
        "company": "Animoca Brands",
        "dedupKey": "animoca-key-1",
        "bundleEvidenceOrigin": "carried_from_existing_output",
        "disagreementClassification": "same_job_different_urls",
        "providerSourceJobIds": ["lever:animoca:123"],
        "staticSourceJobIds": ["static:animoca:123"],
        "providerSources": ["lever:animoca"],
        "staticSources": ["static_source::animoca"],
        "providerUrls": ["https://jobs.lever.co/animoca/123"],
        "staticUrls": ["https://careers.animoca.com/jobs/123"],
        "sharedIdentifierTokens": ["123"],
        "distinctLocationCount": 1,
        "sampleLocations": ["hong kong"],
        "identityQuality": "provider_id_strong",
    }


def test_dedup_review_actions_update_local_review_artifact_only(
    admin_bridge_entrypoint_root,
) -> None:
    active_before = admin_bridge.load_json_object(admin_bridge.ACTIVE_PATH, [])
    pending_before = admin_bridge.load_json_object(admin_bridge.PENDING_PATH, [])
    rejected_before = admin_bridge.load_json_object(admin_bridge.REJECTED_PATH, [])
    tombstones_before = admin_bridge.load_json_object(admin_bridge.TOMBSTONES_PATH, {})
    redundant_rules_before = json.dumps(REDUNDANT_STATIC_IF_PROVIDER, sort_keys=True)

    handled, response = _post({**_payload("reviewed_safe"), "reviewNote": "safe carried variant"})

    assert handled is True
    assert response["status"] == 200
    assert response["payload"]["pair"]["reviewStatus"] == "reviewed_safe"
    state = admin_bridge.load_json_object(admin_bridge.DEDUP_REVIEW_STATE_PATH, {})
    assert state["summary"]["reviewedSafeCount"] == 1
    assert admin_bridge.load_json_object(admin_bridge.ACTIVE_PATH, []) == active_before
    assert admin_bridge.load_json_object(admin_bridge.PENDING_PATH, []) == pending_before
    assert admin_bridge.load_json_object(admin_bridge.REJECTED_PATH, []) == rejected_before
    assert admin_bridge.load_json_object(admin_bridge.TOMBSTONES_PATH, {}) == tombstones_before
    assert json.dumps(REDUNDANT_STATIC_IF_PROVIDER, sort_keys=True) == redundant_rules_before


def test_dedup_review_clear_removes_pair(admin_bridge_entrypoint_root) -> None:
    _post(_payload("confirmed_blocking"))
    _handled, response = _post(_payload("clear_review"))

    assert response["status"] == 200
    assert response["payload"]["pair"] == {}
    state = admin_bridge.load_json_object(admin_bridge.DEDUP_REVIEW_STATE_PATH, {})
    assert state["summary"]["totalPairs"] == 0


def test_dedup_review_invalid_actions_are_rejected(admin_bridge_entrypoint_root) -> None:
    _handled, invalid_response = _post({**_payload("force_merge"), "action": "force_merge"})
    _handled, missing_response = _post(
        {
            "action": "reviewed_safe",
            "disagreementClassification": "same_job_different_urls",
            "providerSourceJobIds": [],
            "staticSourceJobIds": [],
            "dedupKey": "",
        }
    )

    assert invalid_response["status"] == 400
    assert missing_response["status"] == 400
